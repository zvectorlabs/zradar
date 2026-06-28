use axum::{Extension, Router, routing::get};
use std::sync::Arc;
use zradar_traits::AdminAuthorizer;

use super::handlers::{SettingsState, get_workspace_settings, update_workspace_settings};
use crate::http::AuthMode;

pub fn settings_router(
    state: Arc<SettingsState>,
    auth: Arc<dyn AdminAuthorizer>,
    auth_mode: AuthMode,
) -> Router {
    Router::new()
        .route(
            "/api/v1/workspaces/:id/settings",
            get(get_workspace_settings).put(update_workspace_settings),
        )
        .layer(Extension(auth_mode))
        .layer(Extension(auth))
        .with_state(state)
}
