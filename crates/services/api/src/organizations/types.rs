//! Organization types and DTOs

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

// Re-export domain entities from zradar_traits
pub use zradar_traits::{
    AddMemberRequest, CreateOrganizationRequest, Organization, OrganizationMember,
    OrganizationRepository, OrganizationWithRole, UpdateOrganizationRequest,
};

/// Organization member response with user details
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct OrganizationMemberResponse {
    pub id: Uuid,
    pub organization_id: Uuid,
    pub user_id: Uuid,
    #[schema(example = "user@example.com")]
    pub user_email: Option<String>,
    #[schema(example = "John Doe")]
    pub user_full_name: Option<String>,
    pub role: Option<String>,
    pub custom_role_id: Option<Uuid>,
    pub permissions: Vec<String>,
    pub is_active: bool,
    pub invited_by: Option<Uuid>,
    pub joined_at: DateTime<Utc>,
}

impl From<OrganizationMember> for OrganizationMemberResponse {
    fn from(member: OrganizationMember) -> Self {
        Self {
            id: member.id,
            organization_id: member.organization_id,
            user_id: member.user_id,
            user_email: None,
            user_full_name: None,
            role: member.role,
            custom_role_id: member.custom_role_id,
            permissions: member.permissions,
            is_active: member.is_active,
            invited_by: member.invited_by,
            joined_at: member.joined_at,
        }
    }
}

/// HTTP request to add a member (includes email for lookup)
#[derive(Debug, Deserialize, ToSchema)]
pub struct AddOrganizationMemberRequest {
    #[schema(example = "user@example.com")]
    pub user_email: String,
    #[schema(example = "admin")]
    pub role: Option<String>,
    pub custom_role_id: Option<Uuid>,
    pub permissions: Option<Vec<String>>,
}

impl AddOrganizationMemberRequest {
    /// Convert to trait AddMemberRequest (after resolving user_id)
    pub fn to_add_member_request(&self) -> AddMemberRequest {
        AddMemberRequest {
            role: self.role.clone(),
            custom_role_id: self.custom_role_id,
            permissions: self.permissions.clone(),
        }
    }
}
