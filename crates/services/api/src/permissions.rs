//! Permission validation and risk assessment utilities

use crate::domain::{PermissionDefinition, PermissionInfo, RiskAssessment};
use crate::errors::{ControlError, Result};
use std::collections::{HashMap, HashSet};

/// Permission validator and expander
pub struct PermissionValidator {
    /// All available permission definitions
    definitions: HashMap<String, PermissionDefinition>,
}

impl PermissionValidator {
    /// Create a new permission validator with permission definitions
    pub fn new(definitions: Vec<PermissionDefinition>) -> Self {
        let definitions = definitions
            .into_iter()
            .map(|def| (def.id.clone(), def))
            .collect();

        Self { definitions }
    }

    /// Validate that all permissions exist and are active
    pub fn validate_permissions(&self, permissions: &[String]) -> Result<()> {
        for permission in permissions {
            // Wildcards are always valid
            if permission.ends_with(":*") || permission == "*" {
                continue;
            }

            // Check if permission exists
            if let Some(def) = self.definitions.get(permission) {
                if !def.is_active {
                    return Err(ControlError::InvalidInput(format!(
                        "Permission '{}' is not active",
                        permission
                    )));
                }
            } else {
                return Err(ControlError::InvalidInput(format!(
                    "Unknown permission: '{}'",
                    permission
                )));
            }
        }

        Ok(())
    }

    /// Expand wildcards into concrete permissions
    ///
    /// Examples:
    /// - `traces:*` expands to `[traces:read, traces:write, traces:delete, traces:export]`
    /// - `*` expands to all permissions
    pub fn expand_permissions(&self, permissions: &[String], scope: &str) -> Vec<String> {
        let mut expanded = HashSet::new();

        for permission in permissions {
            if permission == "*" {
                // Super wildcard - add all permissions applicable to this scope
                for def in self.definitions.values() {
                    if def.is_active && def.applicable_scopes.contains(&scope.to_string()) {
                        expanded.insert(def.id.clone());
                    }
                }
            } else if permission.ends_with(":*") {
                // Category wildcard (e.g., "traces:*")
                let category = permission.trim_end_matches(":*");
                for def in self.definitions.values() {
                    if def.is_active
                        && def.category == category
                        && def.applicable_scopes.contains(&scope.to_string())
                    {
                        expanded.insert(def.id.clone());
                    }
                }
            } else {
                // Concrete permission
                if let Some(def) = self.definitions.get(permission)
                    && def.is_active
                    && def.applicable_scopes.contains(&scope.to_string())
                {
                    expanded.insert(permission.clone());
                }
            }
        }

        expanded.into_iter().collect()
    }

    /// Check if a user has a specific permission (accounting for wildcards)
    pub fn has_permission(&self, granted: &[String], required: &str, scope: &str) -> bool {
        // Super admin
        if granted.contains(&"*".to_string()) {
            return true;
        }

        // Exact match
        if granted.contains(&required.to_string()) {
            return true;
        }

        // Category wildcard match
        if let Some((category, _)) = required.split_once(':') {
            let wildcard = format!("{}:*", category);
            if granted.contains(&wildcard) {
                return true;
            }
        }

        // Check if required permission exists and is valid for scope
        if let Some(def) = self.definitions.get(required)
            && !def.applicable_scopes.contains(&scope.to_string())
        {
            return false;
        }

        false
    }

    /// Assess the risk level of a set of permissions
    pub fn assess_risk(&self, permissions: &[String]) -> RiskAssessment {
        let expanded = self.expand_all_permissions(permissions);
        let mut high_risk_perms = Vec::new();

        for perm_id in expanded {
            if let Some(def) = self.definitions.get(&perm_id) {
                match def.risk_level.as_str() {
                    "high" | "critical" => {
                        high_risk_perms.push(PermissionInfo {
                            id: def.id.clone(),
                            name: def.name.clone(),
                            risk_level: def.risk_level.clone(),
                        });
                    }
                    _ => {}
                }
            }
        }

        RiskAssessment {
            has_high_risk: !high_risk_perms.is_empty(),
            high_risk_permissions: high_risk_perms,
        }
    }

    /// Expand all permissions (both org and project scopes)
    fn expand_all_permissions(&self, permissions: &[String]) -> Vec<String> {
        let mut expanded = HashSet::new();

        for permission in permissions {
            if permission == "*" {
                // Add all permissions
                for def in self.definitions.values() {
                    if def.is_active {
                        expanded.insert(def.id.clone());
                    }
                }
            } else if permission.ends_with(":*") {
                // Category wildcard
                let category = permission.trim_end_matches(":*");
                for def in self.definitions.values() {
                    if def.is_active && def.category == category {
                        expanded.insert(def.id.clone());
                    }
                }
            } else {
                // Concrete permission
                expanded.insert(permission.clone());
            }
        }

        expanded.into_iter().collect()
    }

    /// Check dependencies - ensure required permissions are also granted
    pub fn validate_dependencies(&self, permissions: &[String]) -> Result<Vec<String>> {
        let mut missing = Vec::new();
        let expanded = self.expand_all_permissions(permissions);

        for perm_id in &expanded {
            if let Some(def) = self.definitions.get(perm_id)
                && let Some(required) = &def.requires
            {
                for req_perm in required {
                    if !expanded.contains(req_perm) {
                        missing.push(format!(
                            "Permission '{}' requires '{}' but it was not granted",
                            perm_id, req_perm
                        ));
                    }
                }
            }
        }

        if missing.is_empty() {
            Ok(vec![])
        } else {
            Ok(missing)
        }
    }

    /// Get permission definition by ID
    pub fn get_definition(&self, permission_id: &str) -> Option<&PermissionDefinition> {
        self.definitions.get(permission_id)
    }

    /// List all permissions for a given scope
    pub fn list_by_scope(&self, scope: &str) -> Vec<&PermissionDefinition> {
        self.definitions
            .values()
            .filter(|def| def.is_active && def.applicable_scopes.contains(&scope.to_string()))
            .collect()
    }

    /// List all permissions by category
    pub fn list_by_category(&self, category: &str) -> Vec<&PermissionDefinition> {
        self.definitions
            .values()
            .filter(|def| def.is_active && def.category == category)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn create_test_definitions() -> Vec<PermissionDefinition> {
        vec![
            PermissionDefinition {
                id: "traces:read".to_string(),
                category: "traces".to_string(),
                action: "read".to_string(),
                name: "View Traces".to_string(),
                description: "View trace data".to_string(),
                applicable_scopes: vec!["project".to_string()],
                risk_level: "low".to_string(),
                requires: None,
                is_active: true,
                created_at: Utc::now(),
            },
            PermissionDefinition {
                id: "traces:write".to_string(),
                category: "traces".to_string(),
                action: "write".to_string(),
                name: "Write Traces".to_string(),
                description: "Send trace data".to_string(),
                applicable_scopes: vec!["project".to_string()],
                risk_level: "low".to_string(),
                requires: None,
                is_active: true,
                created_at: Utc::now(),
            },
            PermissionDefinition {
                id: "traces:delete".to_string(),
                category: "traces".to_string(),
                action: "delete".to_string(),
                name: "Delete Traces".to_string(),
                description: "Delete trace data".to_string(),
                applicable_scopes: vec!["project".to_string()],
                risk_level: "high".to_string(),
                requires: Some(vec!["traces:read".to_string()]),
                is_active: true,
                created_at: Utc::now(),
            },
            PermissionDefinition {
                id: "project:delete".to_string(),
                category: "project".to_string(),
                action: "delete".to_string(),
                name: "Delete Project".to_string(),
                description: "Delete project".to_string(),
                applicable_scopes: vec!["project".to_string()],
                risk_level: "critical".to_string(),
                requires: None,
                is_active: true,
                created_at: Utc::now(),
            },
        ]
    }

    #[test]
    fn test_expand_wildcard() {
        let validator = PermissionValidator::new(create_test_definitions());
        let perms = vec!["traces:*".to_string()];
        let expanded = validator.expand_permissions(&perms, "project");

        assert!(expanded.contains(&"traces:read".to_string()));
        assert!(expanded.contains(&"traces:write".to_string()));
        assert!(expanded.contains(&"traces:delete".to_string()));
        assert_eq!(expanded.len(), 3);
    }

    #[test]
    fn test_has_permission_with_wildcard() {
        let validator = PermissionValidator::new(create_test_definitions());
        let granted = vec!["traces:*".to_string()];

        assert!(validator.has_permission(&granted, "traces:read", "project"));
        assert!(validator.has_permission(&granted, "traces:write", "project"));
        assert!(!validator.has_permission(&granted, "project:delete", "project"));
    }

    #[test]
    fn test_super_admin_permission() {
        let validator = PermissionValidator::new(create_test_definitions());
        let granted = vec!["*".to_string()];

        assert!(validator.has_permission(&granted, "traces:read", "project"));
        assert!(validator.has_permission(&granted, "project:delete", "project"));
    }

    #[test]
    fn test_risk_assessment() {
        let validator = PermissionValidator::new(create_test_definitions());
        let perms = vec!["traces:delete".to_string(), "project:delete".to_string()];

        let assessment = validator.assess_risk(&perms);
        assert!(assessment.has_high_risk);
        assert_eq!(assessment.high_risk_permissions.len(), 2);
    }

    #[test]
    fn test_validate_dependencies() {
        let validator = PermissionValidator::new(create_test_definitions());

        // traces:delete requires traces:read
        let perms = vec!["traces:delete".to_string()];
        let missing = validator.validate_dependencies(&perms).unwrap();
        assert!(!missing.is_empty());

        // With required permission
        let perms = vec!["traces:read".to_string(), "traces:delete".to_string()];
        let missing = validator.validate_dependencies(&perms).unwrap();
        assert!(missing.is_empty());
    }
}
