use axum::{Extension, Router, routing::get};
use std::sync::Arc;
use zradar_traits::Authenticator;

use super::handlers::{SettingsState, get_project_settings, update_project_settings};

pub fn settings_router(state: Arc<SettingsState>, auth: Arc<dyn Authenticator>) -> Router {
    Router::new()
        .route(
            "/api/v1/projects/:id/settings",
            get(get_project_settings).put(update_project_settings),
        )
        .layer(Extension(auth))
        .with_state(state)
}
