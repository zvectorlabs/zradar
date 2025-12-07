//! PostgreSQL organization repository implementation

use async_trait::async_trait;
use sqlx::FromRow;
use std::sync::Arc;
use uuid::Uuid;

use crate::client::PostgresClient;
use zradar_traits::{
    AddMemberRequest, CreateOrganizationRequest, Organization, OrganizationMember,
    OrganizationRepository, OrganizationWithRole, UpdateOrganizationRequest,
};

#[derive(Debug, Clone, FromRow)]
struct OrganizationRow {
    id: Uuid,
    slug: String,
    name: String,
    description: Option<String>,
    owner_id: Uuid,
    is_active: bool,
    plan: String,
    monthly_span_limit: i64,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>,
    settings: serde_json::Value,
    metadata: serde_json::Value,
}

impl From<OrganizationRow> for Organization {
    fn from(row: OrganizationRow) -> Self {
        Organization {
            id: row.id,
            slug: row.slug,
            name: row.name,
            description: row.description,
            owner_id: row.owner_id,
            is_active: row.is_active,
            plan: row.plan,
            monthly_span_limit: row.monthly_span_limit,
            created_at: row.created_at,
            updated_at: row.updated_at,
            settings: row.settings,
            metadata: row.metadata,
        }
    }
}

#[derive(Debug, Clone, FromRow)]
struct MemberRow {
    id: Uuid,
    organization_id: Uuid,
    user_id: Uuid,
    role: Option<String>,
    custom_role_id: Option<Uuid>,
    permissions: Vec<String>,
    is_active: bool,
    invited_by: Option<Uuid>,
    joined_at: chrono::DateTime<chrono::Utc>,
}

impl From<MemberRow> for OrganizationMember {
    fn from(row: MemberRow) -> Self {
        OrganizationMember {
            id: row.id,
            organization_id: row.organization_id,
            user_id: row.user_id,
            role: row.role,
            custom_role_id: row.custom_role_id,
            permissions: row.permissions,
            is_active: row.is_active,
            invited_by: row.invited_by,
            joined_at: row.joined_at,
        }
    }
}

pub struct PostgresOrganizationRepository {
    client: Arc<PostgresClient>,
}

impl PostgresOrganizationRepository {
    pub fn new(client: Arc<PostgresClient>) -> Self {
        Self { client }
    }

    async fn list_user_memberships(
        &self,
        user_id: Uuid,
    ) -> anyhow::Result<Vec<OrganizationMember>> {
        let rows = sqlx::query_as::<_, MemberRow>(
            r#"
            SELECT om.* FROM organization_members om
            INNER JOIN organizations o ON o.id = om.organization_id
            WHERE om.user_id = $1 AND om.is_active = true AND o.is_active = true
            ORDER BY om.joined_at DESC
            "#,
        )
        .bind(user_id)
        .fetch_all(self.client.pool())
        .await?;

        Ok(rows.into_iter().map(Into::into).collect())
    }
}

#[async_trait]
impl OrganizationRepository for PostgresOrganizationRepository {
    async fn create_org(
        &self,
        owner_id: Uuid,
        req: CreateOrganizationRequest,
    ) -> anyhow::Result<Organization> {
        let mut tx = self.client.pool().begin().await?;

        let row = sqlx::query_as::<_, OrganizationRow>(
            r#"
            INSERT INTO organizations (slug, name, description, owner_id)
            VALUES ($1, $2, $3, $4)
            RETURNING *
            "#,
        )
        .bind(&req.slug)
        .bind(&req.name)
        .bind(&req.description)
        .bind(owner_id)
        .fetch_one(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            INSERT INTO organization_members (organization_id, user_id, role, permissions)
            VALUES ($1, $2, 'owner', ARRAY['*'])
            "#,
        )
        .bind(row.id)
        .bind(owner_id)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        Ok(row.into())
    }

    async fn get_org(&self, id: Uuid) -> anyhow::Result<Option<Organization>> {
        let row = sqlx::query_as::<_, OrganizationRow>("SELECT * FROM organizations WHERE id = $1")
            .bind(id)
            .fetch_optional(self.client.pool())
            .await?;

        Ok(row.map(Into::into))
    }

    async fn get_org_by_slug(&self, slug: &str) -> anyhow::Result<Option<Organization>> {
        let row =
            sqlx::query_as::<_, OrganizationRow>("SELECT * FROM organizations WHERE slug = $1")
                .bind(slug)
                .fetch_optional(self.client.pool())
                .await?;

        Ok(row.map(Into::into))
    }

    async fn list_user_orgs(&self, user_id: Uuid) -> anyhow::Result<Vec<OrganizationWithRole>> {
        let members = self.list_user_memberships(user_id).await?;

        let mut results = Vec::new();
        for member in members {
            if let Some(org) = self.get_org(member.organization_id).await? {
                results.push(OrganizationWithRole {
                    organization: org,
                    member_role: member.role,
                    member_permissions: member.permissions,
                });
            }
        }

        Ok(results)
    }

    async fn update_org(
        &self,
        id: Uuid,
        updates: UpdateOrganizationRequest,
    ) -> anyhow::Result<Organization> {
        let row = sqlx::query_as::<_, OrganizationRow>(
            r#"
            UPDATE organizations
            SET name = COALESCE($2, name),
                description = COALESCE($3, description),
                plan = COALESCE($4, plan),
                monthly_span_limit = COALESCE($5, monthly_span_limit),
                settings = COALESCE($6, settings),
                updated_at = NOW()
            WHERE id = $1
            RETURNING *
            "#,
        )
        .bind(id)
        .bind(updates.name)
        .bind(updates.description)
        .bind(updates.plan)
        .bind(updates.monthly_span_limit)
        .bind(updates.settings)
        .fetch_one(self.client.pool())
        .await?;

        Ok(row.into())
    }

    async fn delete_org(&self, id: Uuid) -> anyhow::Result<()> {
        sqlx::query("DELETE FROM organizations WHERE id = $1")
            .bind(id)
            .execute(self.client.pool())
            .await?;
        Ok(())
    }

    async fn add_member(
        &self,
        org_id: Uuid,
        user_id: Uuid,
        req: AddMemberRequest,
    ) -> anyhow::Result<OrganizationMember> {
        let row = sqlx::query_as::<_, MemberRow>(
            r#"
            INSERT INTO organization_members (organization_id, user_id, role, custom_role_id, permissions)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING *
            "#,
        )
        .bind(org_id)
        .bind(user_id)
        .bind(req.role)
        .bind(req.custom_role_id)
        .bind(req.permissions.unwrap_or_default())
        .fetch_one(self.client.pool())
        .await?;

        Ok(row.into())
    }

    async fn get_member(
        &self,
        org_id: Uuid,
        user_id: Uuid,
    ) -> anyhow::Result<Option<OrganizationMember>> {
        let row = sqlx::query_as::<_, MemberRow>(
            "SELECT * FROM organization_members WHERE organization_id = $1 AND user_id = $2",
        )
        .bind(org_id)
        .bind(user_id)
        .fetch_optional(self.client.pool())
        .await?;

        Ok(row.map(Into::into))
    }

    async fn list_members(&self, org_id: Uuid) -> anyhow::Result<Vec<OrganizationMember>> {
        let rows = sqlx::query_as::<_, MemberRow>(
            "SELECT * FROM organization_members WHERE organization_id = $1 AND is_active = true",
        )
        .bind(org_id)
        .fetch_all(self.client.pool())
        .await?;

        Ok(rows.into_iter().map(Into::into).collect())
    }

    async fn remove_member(&self, org_id: Uuid, user_id: Uuid) -> anyhow::Result<()> {
        sqlx::query("DELETE FROM organization_members WHERE organization_id = $1 AND user_id = $2")
            .bind(org_id)
            .bind(user_id)
            .execute(self.client.pool())
            .await?;
        Ok(())
    }
}
