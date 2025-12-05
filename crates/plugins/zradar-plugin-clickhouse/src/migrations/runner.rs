//! Migration execution logic

use std::path::Path;
use std::collections::HashSet;
use tokio::fs;
use sha2::{Sha256, Digest};

use crate::client::ClickHouseClient;
use crate::migrations::types::{Migration, MigrationResult, MigrationError};
use crate::migrations::tracker::MigrationTracker;

impl ClickHouseClient {
    /// Run ClickHouse migrations from a directory with proper tracking
    pub async fn run_migrations<P: AsRef<Path>>(&self, migrations_dir: P) -> MigrationResult<()> {
        let runner = MigrationRunner::new(self.client());
        runner.run(migrations_dir).await
    }
    
    /// Verify migration checksums
    pub async fn verify_migrations<P: AsRef<Path>>(&self, migrations_dir: P) -> MigrationResult<bool> {
        let runner = MigrationRunner::new(self.client());
        runner.verify(migrations_dir).await
    }
}

pub struct MigrationRunner<'a> {
    client: &'a clickhouse::Client,
}

impl<'a> MigrationRunner<'a> {
    pub fn new(client: &'a clickhouse::Client) -> Self {
        Self { client }
    }
    
    /// Run all pending migrations
    pub async fn run<P: AsRef<Path>>(&self, migrations_dir: P) -> MigrationResult<()> {
        let migrations_path = migrations_dir.as_ref();
        
        if !migrations_path.exists() {
            tracing::warn!(
                path = %migrations_path.display(),
                "ClickHouse migrations directory not found, skipping"
            );
            return Ok(());
        }
        
        tracing::info!(path = %migrations_path.display(), "Running ClickHouse migrations...");
        
        // Initialize tracker
        let tracker = MigrationTracker::new(self.client);
        tracker.ensure_table().await?;
        
        // Get applied migrations
        let applied = tracker.get_applied().await?;
        let applied_versions: HashSet<String> = 
            applied.iter().map(|m| m.version.clone()).collect();
        
        tracing::info!("Found {} applied migrations", applied_versions.len());
        
        // Discover all migrations
        let mut migrations = self.discover_migrations(migrations_path).await?;
        migrations.sort_by(|a, b| a.version.cmp(&b.version));
        
        // Filter pending
        let pending: Vec<_> = migrations
            .into_iter()
            .filter(|m| !applied_versions.contains(&m.version))
            .collect();
        
        if pending.is_empty() {
            tracing::info!("✅ No pending migrations");
            return Ok(());
        }
        
        tracing::info!("Found {} pending migrations", pending.len());
        
        // Apply each migration
        for migration in pending {
            self.apply_migration(&migration, &tracker).await?;
        }
        
        tracing::info!("✨ All migrations completed successfully");
        Ok(())
    }
    
    /// Discover all migration files in directory
    async fn discover_migrations(&self, dir: &Path) -> MigrationResult<Vec<Migration>> {
        let mut migrations = Vec::new();
        let mut entries = fs::read_dir(dir).await?;
        
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("sql")
                && let Some(migration) = self.parse_migration(&path).await? {
                    migrations.push(migration);
                }
        }
        
        Ok(migrations)
    }
    
    /// Parse a migration file
    async fn parse_migration(&self, path: &Path) -> MigrationResult<Option<Migration>> {
        let filename = path.file_name()
            .and_then(|s| s.to_str())
            .ok_or_else(|| MigrationError::InvalidFilename("invalid UTF-8".to_string()))?;
        
        let (version, description) = match Self::parse_filename(filename) {
            Some(parsed) => parsed,
            None => {
                tracing::warn!(file = filename, "Skipping invalid migration filename");
                return Ok(None);
            }
        };
        
        let content = fs::read_to_string(path).await?;
        let checksum = Self::calculate_checksum(&content);
        
        Ok(Some(Migration {
            version,
            description,
            filepath: path.to_path_buf(),
            checksum,
        }))
    }
    
    /// Apply a single migration
    async fn apply_migration(&self, migration: &Migration, tracker: &MigrationTracker<'_>) -> MigrationResult<()> {
        tracing::info!(
            version = %migration.version,
            description = %migration.description,
            "Applying migration..."
        );
        
        let sql = fs::read_to_string(&migration.filepath).await?;
        let start = std::time::Instant::now();
        
        // Split SQL into individual statements
        // ClickHouse client doesn't support multi-statement queries
        let statements = Self::split_sql_statements(&sql);
        
        tracing::debug!(
            version = %migration.version,
            statement_count = statements.len(),
            "Executing {} SQL statements",
            statements.len()
        );
        
        // Execute each statement
        for (idx, statement) in statements.iter().enumerate() {
            if statement.trim().is_empty() {
                continue;
            }
            
            tracing::trace!(
                version = %migration.version,
                statement_num = idx + 1,
                "Executing statement {}/{}",
                idx + 1,
                statements.len()
            );
            
            self.client.query(statement).execute().await
                .map_err(|e| MigrationError::ExecutionFailed(
                    format!("Statement {}/{} failed: {}", idx + 1, statements.len(), e)
                ))?;
        }
        
        let execution_time_ms = start.elapsed().as_millis() as u32;
        
        // Record as applied
        tracker.record(
            &migration.version,
            &migration.description,
            &migration.checksum,
            execution_time_ms,
        ).await?;
        
        tracing::info!(
            version = %migration.version,
            execution_time_ms = execution_time_ms,
            "✅ Migration applied"
        );
        
        Ok(())
    }
    
    /// Split SQL file into individual statements
    /// Handles comments and multi-line statements
    fn split_sql_statements(sql: &str) -> Vec<String> {
        let mut statements = Vec::new();
        let mut current_statement = String::new();
        let mut in_comment = false;
        
        for line in sql.lines() {
            let trimmed = line.trim();
            
            // Skip empty lines
            if trimmed.is_empty() {
                continue;
            }
            
            // Handle single-line comments
            if trimmed.starts_with("--") {
                in_comment = false;
                continue;
            }
            
            // Handle multi-line comments
            if trimmed.starts_with("/*") {
                in_comment = true;
            }
            if trimmed.ends_with("*/") {
                in_comment = false;
                continue;
            }
            if in_comment {
                continue;
            }
            
            // Add line to current statement
            current_statement.push_str(line);
            current_statement.push('\n');
            
            // Check if statement is complete (ends with semicolon)
            if trimmed.ends_with(';') {
                // Remove the trailing semicolon and whitespace
                let stmt = current_statement.trim().trim_end_matches(';').trim();
                if !stmt.is_empty() {
                    statements.push(stmt.to_string());
                }
                current_statement.clear();
            }
        }
        
        // Add any remaining statement
        let stmt = current_statement.trim().trim_end_matches(';').trim();
        if !stmt.is_empty() {
            statements.push(stmt.to_string());
        }
        
        statements
    }
    
    /// Verify all migration checksums
    pub async fn verify<P: AsRef<Path>>(&self, migrations_dir: P) -> MigrationResult<bool> {
        let tracker = MigrationTracker::new(self.client);
        let applied = tracker.get_applied().await?;
        
        let mut all_valid = true;
        
        for applied_migration in applied {
            // Find corresponding file
            let mut entries = fs::read_dir(migrations_dir.as_ref()).await?;
            let mut found = false;
            
            while let Some(entry) = entries.next_entry().await? {
                let path = entry.path();
                if let Some(name) = path.file_name().and_then(|s| s.to_str())
                    && name.starts_with(&applied_migration.version) && name.ends_with(".sql") {
                        found = true;
                        let content = fs::read_to_string(&path).await?;
                        let current_checksum = Self::calculate_checksum(&content);
                        
                        if current_checksum != applied_migration.checksum {
                            tracing::error!(
                                version = %applied_migration.version,
                                "❌ Checksum mismatch - file modified!"
                            );
                            all_valid = false;
                        }
                    }
            }
            
            if !found {
                tracing::error!(
                    version = %applied_migration.version,
                    "❌ Applied migration file not found!"
                );
                all_valid = false;
            }
        }
        
        Ok(all_valid)
    }
    
    /// Parse migration filename
    fn parse_filename(filename: &str) -> Option<(String, String)> {
        let stem = filename.strip_suffix(".sql")?;
        let parts: Vec<&str> = stem.splitn(2, '_').collect();
        
        if parts.len() == 2 {
            Some((parts[0].to_string(), parts[1].replace('_', " ")))
        } else {
            None
        }
    }
    
    /// Calculate SHA256 checksum
    fn calculate_checksum(content: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(content.as_bytes());
        format!("{:x}", hasher.finalize())
    }
}

/// Convenience function for running migrations
pub async fn run_migrations<P: AsRef<Path>>(
    client: &clickhouse::Client,
    migrations_dir: P,
) -> MigrationResult<()> {
    let runner = MigrationRunner::new(client);
    runner.run(migrations_dir).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_sql_statements() {
        let sql = r#"
-- This is a comment
CREATE TABLE test1 (
    id String,
    name String
) ENGINE = MergeTree()
ORDER BY id;

-- Another comment
CREATE TABLE test2 (
    id String
) ENGINE = MergeTree()
ORDER BY id;

/* Multi-line
   comment */
CREATE TABLE test3 (id String) ENGINE = MergeTree() ORDER BY id;
"#;

        let statements = MigrationRunner::split_sql_statements(sql);
        
        assert_eq!(statements.len(), 3, "Should have 3 statements");
        assert!(statements[0].contains("CREATE TABLE test1"));
        assert!(statements[1].contains("CREATE TABLE test2"));
        assert!(statements[2].contains("CREATE TABLE test3"));
        
        // Verify no semicolons in statements
        for stmt in &statements {
            assert!(!stmt.ends_with(';'), "Statement should not end with semicolon: {}", stmt);
        }
        
        // Verify no comments in statements
        for stmt in &statements {
            assert!(!stmt.contains("--"), "Statement should not contain comments: {}", stmt);
            assert!(!stmt.contains("/*"), "Statement should not contain multi-line comments: {}", stmt);
        }
    }

    #[test]
    fn test_split_sql_with_empty_lines() {
        let sql = r#"

CREATE TABLE test1 (id String);


CREATE TABLE test2 (id String);

"#;

        let statements = MigrationRunner::split_sql_statements(sql);
        assert_eq!(statements.len(), 2);
    }

    #[test]
    fn test_split_sql_no_trailing_semicolon() {
        let sql = "CREATE TABLE test (id String)";
        let statements = MigrationRunner::split_sql_statements(sql);
        
        assert_eq!(statements.len(), 1);
        assert_eq!(statements[0], "CREATE TABLE test (id String)");
    }
}

