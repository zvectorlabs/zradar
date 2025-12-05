//! PostgreSQL project repository implementation

use async_trait::async_trait;
use chrono::Utc;
use std::sync::Arc;
use uuid::Uuid;
use sqlx::Row;

use zradar_traits::{
    ProjectRepository, Project, ProjectMember, ProjectWithRole,
    CreateProjectRequest, UpdateProjectRequest, AddProjectMemberRequest,
};
use crate::client::PostgresClient;

pub struct PostgresProjectRepository {
    client: Arc<PostgresClient>,
}

impl PostgresProjectRepository {
    pub fn new(client: Arc<PostgresClient>) -> Self {
        Self { client }
    }
    
    fn row_to_project(row: &sqlx::postgres::PgRow) -> Project {
        Project {
            id: row.get("id"),
            organization_id: row.get("organization_id"),
            slug: row.get("slug"),
            name: row.get("name"),
            description: row.get("description"),
            environment: row.get("environment"),
            is_active: row.get("is_active"),
            retention_days: row.get("retention_days"),
            sampling_rate: row.get("sampling_rate"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
            settings: row.get("settings"),
            metadata: row.get("metadata"),
        }
    }
    
    fn row_to_member(row: &sqlx::postgres::PgRow) -> ProjectMember {
        ProjectMember {
            id: row.get("id"),
            project_id: row.get("project_id"),
            user_id: row.get("user_id"),
            role: row.get("role"),
            custom_role_id: row.get("custom_role_id"),
            permissions: row.get("permissions"),
            is_active: row.get("is_active"),
            added_by: row.get("added_by"),
            joined_at: row.get("joined_at"),
        }
    }
}

#[async_trait]
impl ProjectRepository for PostgresProjectRepository {
    async fn create_project(&self, org_id: Uuid, req: CreateProjectRequest) -> anyhow::Result<Project> {
        let id = Uuid::new_v4();
        let now = Utc::now();
        
        let row = sqlx::query(
            r#"
            INSERT INTO projects (
                id, organization_id, slug, name, description, environment,
                is_active, retention_days, sampling_rate,
                created_at, updated_at, settings, metadata
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            RETURNING *
            "#
        )
        .bind(id)
        .bind(org_id)
        .bind(&req.slug)
        .bind(&req.name)
        .bind(&req.description)
        .bind(req.environment.unwrap_or_else(|| "production".to_string()))
        .bind(true)
        .bind(req.retention_days.unwrap_or(90))
        .bind(req.sampling_rate.unwrap_or(1.0))
        .bind(now)
        .bind(now)
        .bind(serde_json::json!({})) // settings
        .bind(serde_json::json!({})) // metadata
        .fetch_one(self.client.pool())
        .await?;
        
        Ok(Self::row_to_project(&row))
    }
    
    async fn get_project(&self, id: Uuid) -> anyhow::Result<Option<Project>> {
        let row = sqlx::query("SELECT * FROM projects WHERE id = $1")
            .bind(id)
            .fetch_optional(self.client.pool())
            .await?;
        
        Ok(row.as_ref().map(Self::row_to_project))
    }
    
    async fn get_project_by_slug(&self, org_id: Uuid, slug: &str) -> anyhow::Result<Option<Project>> {
        let row = sqlx::query(
            "SELECT * FROM projects WHERE organization_id = $1 AND slug = $2"
        )
        .bind(org_id)
        .bind(slug)
        .fetch_optional(self.client.pool())
        .await?;
        
        Ok(row.as_ref().map(Self::row_to_project))
    }
    
    async fn list_org_projects(&self, org_id: Uuid) -> anyhow::Result<Vec<Project>> {
        let rows = sqlx::query(
            "SELECT * FROM projects WHERE organization_id = $1 ORDER BY created_at DESC"
        )
        .bind(org_id)
        .fetch_all(self.client.pool())
        .await?;
        
        Ok(rows.iter().map(Self::row_to_project).collect())
    }
    
    async fn list_user_projects(&self, org_id: Uuid, user_id: Uuid) -> anyhow::Result<Vec<ProjectWithRole>> {
        let rows = sqlx::query(
            r#"
            SELECT p.*, pm.role, pm.custom_role_id
            FROM projects p
            INNER JOIN project_members pm ON p.id = pm.project_id
            WHERE p.organization_id = $1 AND pm.user_id = $2 AND pm.is_active = true
            ORDER BY p.created_at DESC
            "#
        )
        .bind(org_id)
        .bind(user_id)
        .fetch_all(self.client.pool())
        .await?;
        
        Ok(rows.iter().map(|row| ProjectWithRole {
            project: Self::row_to_project(row),
            member_role: row.get("role"),
            member_permissions: row.try_get("permissions").unwrap_or_default(),
        }).collect())
    }
    
    async fn update_project(&self, id: Uuid, updates: UpdateProjectRequest) -> anyhow::Result<Project> {
        let mut query = String::from("UPDATE projects SET updated_at = $1");
        let mut param_count = 2;
        let _bindings: Vec<String> = vec![];
        
        if updates.name.is_some() {
            query.push_str(&format!(", name = ${}", param_count));
            param_count += 1;
        }
        if updates.description.is_some() {
            query.push_str(&format!(", description = ${}", param_count));
            param_count += 1;
        }
        if updates.retention_days.is_some() {
            query.push_str(&format!(", retention_days = ${}", param_count));
            param_count += 1;
        }
        if updates.sampling_rate.is_some() {
            query.push_str(&format!(", sampling_rate = ${}", param_count));
            param_count += 1;
        }
        
        query.push_str(&format!(" WHERE id = ${} RETURNING *", param_count));
        
        let mut q = sqlx::query(&query).bind(Utc::now());
        
        if let Some(name) = updates.name {
            q = q.bind(name);
        }
        if let Some(desc) = updates.description {
            q = q.bind(desc);
        }
        if let Some(retention) = updates.retention_days {
            q = q.bind(retention);
        }
        if let Some(sampling) = updates.sampling_rate {
            q = q.bind(sampling);
        }
        q = q.bind(id);
        
        let row = q.fetch_one(self.client.pool()).await?;
        Ok(Self::row_to_project(&row))
    }
    
    async fn delete_project(&self, id: Uuid) -> anyhow::Result<()> {
        sqlx::query("DELETE FROM projects WHERE id = $1")
            .bind(id)
            .execute(self.client.pool())
            .await?;
        Ok(())
    }
    
    async fn add_member(&self, project_id: Uuid, user_id: Uuid, req: AddProjectMemberRequest) -> anyhow::Result<ProjectMember> {
        let id = Uuid::new_v4();
        let now = Utc::now();
        
        let row = sqlx::query(
            r#"
            INSERT INTO project_members (
                id, project_id, user_id, role, custom_role_id,
                permissions, is_active, added_by, joined_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            RETURNING *
            "#
        )
        .bind(id)
        .bind(project_id)
        .bind(user_id)
        .bind(&req.role)
        .bind(req.custom_role_id)
        .bind(&req.permissions.unwrap_or_default())
        .bind(true)
        .bind(None::<Uuid>) // added_by - not in request
        .bind(now)
        .fetch_one(self.client.pool())
        .await?;
        
        Ok(Self::row_to_member(&row))
    }
    
    async fn get_member(&self, project_id: Uuid, user_id: Uuid) -> anyhow::Result<Option<ProjectMember>> {
        let row = sqlx::query(
            "SELECT * FROM project_members WHERE project_id = $1 AND user_id = $2"
        )
        .bind(project_id)
        .bind(user_id)
        .fetch_optional(self.client.pool())
        .await?;
        
        Ok(row.as_ref().map(Self::row_to_member))
    }
    
    async fn list_members(&self, project_id: Uuid) -> anyhow::Result<Vec<ProjectMember>> {
        let rows = sqlx::query(
            "SELECT * FROM project_members WHERE project_id = $1 AND is_active = true ORDER BY joined_at"
        )
        .bind(project_id)
        .fetch_all(self.client.pool())
        .await?;
        
        Ok(rows.iter().map(Self::row_to_member).collect())
    }
    
    async fn remove_member(&self, project_id: Uuid, user_id: Uuid) -> anyhow::Result<()> {
        sqlx::query(
            "UPDATE project_members SET is_active = false WHERE project_id = $1 AND user_id = $2"
        )
        .bind(project_id)
        .bind(user_id)
        .execute(self.client.pool())
        .await?;
        
        Ok(())
    }
}
