use functional_tests::*;
use uuid::Uuid;

#[tokio::test]
#[ignore]
async fn test_admin_mutations_create_audit_logs() -> Result<()> {
    let mut session = TestSession::setup().await?;
    let tenant_id = Uuid::nil();
    let actor_project_id = Uuid::new_v4();
    let settings_project_id = Uuid::new_v4();

    session.client.set_project_id(actor_project_id.to_string());

    let settings_response = session
        .client
        .put(
            &format!("/api/v1/projects/{}/settings", settings_project_id),
            &json!({
                "traces_retention_days": 90,
                "metrics_retention_days": 30,
                "logs_retention_days": 30,
                "max_ingestion_rate": 100,
                "file_push_interval_secs": 300,
                "blocked": false,
            }),
        )
        .await?;
    assert_eq!(settings_response.status(), 200);

    let retention_response = session
        .client
        .put(
            "/api/v1/admin/retention/config",
            &json!({
                "org_id": tenant_id,
                "default_days": 14,
                "project_overrides": {},
            }),
        )
        .await?;
    assert_eq!(retention_response.status(), 200);

    let database_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://zradar_test:test_pass_123@localhost:9011/zradar_test".to_string()
    });
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await?;

    let settings_count: (i64,) = sqlx::query_as(
        r#"
        SELECT COUNT(*)
        FROM audit_logs
        WHERE action = 'project_settings.update'
          AND resource_type = 'project_settings'
          AND resource_id = $1
          AND org_id = $2
          AND project_id = $3
        "#,
    )
    .bind(settings_project_id.to_string())
    .bind(tenant_id)
    .bind(settings_project_id)
    .fetch_one(&pool)
    .await?;

    assert_eq!(settings_count.0, 1);

    let retention_count: (i64,) = sqlx::query_as(
        r#"
        SELECT COUNT(*)
        FROM audit_logs
        WHERE action = 'retention_config.update'
          AND resource_type = 'retention_policy'
          AND resource_id = $1
          AND org_id = $2
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(tenant_id)
    .fetch_one(&pool)
    .await?;

    assert!(
        retention_count.0 >= 1,
        "retention update should create at least one audit log for the API-key tenant"
    );

    let audit_response = session
        .client
        .get(&format!(
            "/api/v1/admin/audit-logs?org_id={}&action=project_settings.update&limit=10",
            tenant_id
        ))
        .await?;
    assert_eq!(audit_response.status(), 200);
    let audit_json: Value = audit_response.json().await?;
    let items = audit_json["items"]
        .as_array()
        .expect("audit items must be array");
    assert!(
        items.iter().any(|item| {
            item["action"] == "project_settings.update"
                && item["resource_id"] == settings_project_id.to_string()
        }),
        "audit response should include this test's project_settings.update row"
    );

    println!("✅ Admin mutations create audit logs");
    Ok(())
}
