//! Organization types and DTOs

use serde::Deserialize;
use utoipa::ToSchema;
use uuid::Uuid;

// Re-export domain entities from zradar_traits
pub use zradar_traits::{
    Organization, OrganizationMember, OrganizationWithRole,
    CreateOrganizationRequest, UpdateOrganizationRequest, AddMemberRequest,
    OrganizationRepository,
};

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

