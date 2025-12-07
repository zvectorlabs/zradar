//! PostgreSQL roles repository implementation

use async_trait::async_trait;
use chrono::Utc;
use sqlx::Row;
use std::sync::Arc;
use uuid::Uuid;

use crate::client::PostgresClient;
use zradar_traits::{
    CreateCustomRoleRequest, CustomRole, PermissionDefinition, RoleRepository,
    UpdateCustomRoleRequest,
};

pub struct PostgresRoleRepository {
    client: Arc<PostgresClient>,
}

impl PostgresRoleRepository {
    pub fn new(client: Arc<PostgresClient>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl RoleRepository for PostgresRoleRepository {
    async fn get_permission_definitions(
        &self,
        scope: Option<&str>,
    ) -> anyhow::Result<Vec<PermissionDefinition>> {
        let query = if let Some(s) = scope {
            sqlx::query("SELECT * FROM permission_definitions WHERE scope = $1 ORDER BY id").bind(s)
        } else {
            sqlx::query("SELECT * FROM permission_definitions ORDER BY id")
        };

        let rows = query.fetch_all(self.client.pool()).await?;

        Ok(rows
            .iter()
            .map(|r| PermissionDefinition {
                id: r.get("id"),
                category: r.get("category"),
                action: r.get("action"),
                name: r.get("name"),
                description: r.get("description"),
                applicable_scopes: r.get("applicable_scopes"),
                risk_level: r.get("risk_level"),
                requires: r.get("requires"),
                is_active: r.get("is_active"),
                created_at: r.get("created_at"),
            })
            .collect())
    }

    async fn get_permission_definition(
        &self,
        id: &str,
    ) -> anyhow::Result<Option<PermissionDefinition>> {
        let row = sqlx::query("SELECT * FROM permission_definitions WHERE id = $1")
            .bind(id)
            .fetch_optional(self.client.pool())
            .await?;

        Ok(row.as_ref().map(|r| PermissionDefinition {
            id: r.get("id"),
            category: r.get("category"),
            action: r.get("action"),
            name: r.get("name"),
            description: r.get("description"),
            applicable_scopes: r.get("applicable_scopes"),
            risk_level: r.get("risk_level"),
            requires: r.get("requires"),
            is_active: r.get("is_active"),
            created_at: r.get("created_at"),
        }))
    }

    async fn create_custom_role(
        &self,
        org_id: Uuid,
        req: CreateCustomRoleRequest,
        created_by: Uuid,
    ) -> anyhow::Result<CustomRole> {
        let id = Uuid::new_v4();
        let now = Utc::now();

        let row = sqlx::query(
            r#"
            INSERT INTO custom_roles (
                id, organization_id, name, description, scope,
                permissions, is_system, color, created_by, created_at, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            RETURNING *
            "#,
        )
        .bind(id)
        .bind(org_id)
        .bind(&req.name)
        .bind(&req.description)
        .bind(&req.scope)
        .bind(&req.permissions)
        .bind(false) // is_system
        .bind(&req.color)
        .bind(created_by)
        .bind(now)
        .bind(now)
        .fetch_one(self.client.pool())
        .await?;

        Ok(CustomRole {
            id: row.get("id"),
            organization_id: row.get("organization_id"),
            name: row.get("name"),
            description: row.get("description"),
            scope: row.get("scope"),
            permissions: row.get("permissions"),
            is_system: row.get("is_system"),
            color: row.get("color"),
            created_by: row.get("created_by"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
        })
    }

    async fn get_custom_role(&self, id: Uuid) -> anyhow::Result<Option<CustomRole>> {
        let row = sqlx::query("SELECT * FROM custom_roles WHERE id = $1")
            .bind(id)
            .fetch_optional(self.client.pool())
            .await?;

        Ok(row.as_ref().map(|r| CustomRole {
            id: r.get("id"),
            organization_id: r.get("organization_id"),
            name: r.get("name"),
            description: r.get("description"),
            scope: r.get("scope"),
            permissions: r.get("permissions"),
            is_system: r.get("is_system"),
            color: r.get("color"),
            created_by: r.get("created_by"),
            created_at: r.get("created_at"),
            updated_at: r.get("updated_at"),
        }))
    }

    async fn list_custom_roles(
        &self,
        org_id: Uuid,
        scope: Option<&str>,
    ) -> anyhow::Result<Vec<CustomRole>> {
        let query = if let Some(s) = scope {
            sqlx::query("SELECT * FROM custom_roles WHERE organization_id = $1 AND scope = $2 ORDER BY name")
                .bind(org_id)
                .bind(s)
        } else {
            sqlx::query("SELECT * FROM custom_roles WHERE organization_id = $1 ORDER BY name")
                .bind(org_id)
        };

        let rows = query.fetch_all(self.client.pool()).await?;

        Ok(rows
            .iter()
            .map(|r| CustomRole {
                id: r.get("id"),
                organization_id: r.get("organization_id"),
                name: r.get("name"),
                description: r.get("description"),
                scope: r.get("scope"),
                permissions: r.get("permissions"),
                is_system: r.get("is_system"),
                color: r.get("color"),
                created_by: r.get("created_by"),
                created_at: r.get("created_at"),
                updated_at: r.get("updated_at"),
            })
            .collect())
    }

    async fn update_custom_role(
        &self,
        id: Uuid,
        updates: UpdateCustomRoleRequest,
    ) -> anyhow::Result<CustomRole> {
        let now = Utc::now();

        let row = sqlx::query(
            r#"
            UPDATE custom_roles 
            SET description = COALESCE($1, description),
                permissions = COALESCE($2, permissions),
                color = COALESCE($3, color),
                updated_at = $4
            WHERE id = $5
            RETURNING *
            "#,
        )
        .bind(updates.description)
        .bind(updates.permissions)
        .bind(updates.color)
        .bind(now)
        .bind(id)
        .fetch_one(self.client.pool())
        .await?;

        Ok(CustomRole {
            id: row.get("id"),
            organization_id: row.get("organization_id"),
            name: row.get("name"),
            description: row.get("description"),
            scope: row.get("scope"),
            permissions: row.get("permissions"),
            is_system: row.get("is_system"),
            color: row.get("color"),
            created_by: row.get("created_by"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
        })
    }

    async fn delete_custom_role(&self, id: Uuid) -> anyhow::Result<()> {
        sqlx::query("DELETE FROM custom_roles WHERE id = $1")
            .bind(id)
            .execute(self.client.pool())
            .await?;
        Ok(())
    }

    async fn get_user_org_permissions(
        &self,
        org_id: Uuid,
        user_id: Uuid,
    ) -> anyhow::Result<Vec<String>> {
        let row = sqlx::query(
            r#"
            SELECT om.role, om.permissions, cr.permissions as custom_permissions
            FROM organization_members om
            LEFT JOIN custom_roles cr ON om.custom_role_id = cr.id
            WHERE om.organization_id = $1 AND om.user_id = $2 AND om.is_active = true
            "#,
        )
        .bind(org_id)
        .bind(user_id)
        .fetch_optional(self.client.pool())
        .await?;

        if let Some(r) = row {
            let mut perms: Vec<String> = r.try_get("permissions").unwrap_or_default();
            if let Ok(custom_perms) = r.try_get::<Vec<String>, _>("custom_permissions") {
                perms.extend(custom_perms);
            }
            Ok(perms)
        } else {
            Ok(vec![])
        }
    }

    async fn get_user_project_permissions(
        &self,
        project_id: Uuid,
        user_id: Uuid,
    ) -> anyhow::Result<Vec<String>> {
        let row = sqlx::query(
            r#"
            SELECT pm.role, pm.permissions, cr.permissions as custom_permissions
            FROM project_members pm
            LEFT JOIN custom_roles cr ON pm.custom_role_id = cr.id
            WHERE pm.project_id = $1 AND pm.user_id = $2 AND pm.is_active = true
            "#,
        )
        .bind(project_id)
        .bind(user_id)
        .fetch_optional(self.client.pool())
        .await?;

        if let Some(r) = row {
            let mut perms: Vec<String> = r.try_get("permissions").unwrap_or_default();
            if let Ok(custom_perms) = r.try_get::<Vec<String>, _>("custom_permissions") {
                perms.extend(custom_perms);
            }
            Ok(perms)
        } else {
            Ok(vec![])
        }
    }
}
