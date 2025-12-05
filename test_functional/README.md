# zradar Functional Tests

Black-box API testing suite for zradar - 48 tests covering the entire API surface.

## 🚀 Quick Start

```bash
# Run all tests
make functional_tests

# Or directly
./scripts/test-rust-functional.sh
```

## 📊 What's Tested

**48 black-box API tests** (no database queries, API responses only):

| Category | Tests | What |
|----------|-------|------|
| Health | 4 | `/health`, `/health/ready`, `/health/live` |
| Auth | 7 | Login, registration, JWT tokens, permissions |
| Organizations | 7 | CRUD operations, validation, hierarchy |
| Projects | 6 | CRUD operations, org relationships |
| API Keys | 7 | Create, revoke, lifecycle, `zvr_` prefix |
| **OTLP Tracing** | **11** | **Single/multi-span, concurrent, high-volume** |
| E2E Workflows | 6 | Complete flows, multi-tenant, distributed traces |

**Runtime:** ~30-40 seconds

## 🔌 Port Configuration

All test services use **sequential ports 9011-9016** (no conflicts with dev):

```
9011 - PostgreSQL
9012 - ClickHouse HTTP
9013 - ClickHouse Native
9014 - Redis
9015 - Admin API (REST)
9016 - OTLP gRPC
```

## 🧪 Run Specific Tests

```bash
# By category
cargo test --test functional_tests test_health -- --ignored
cargo test --test functional_tests test_auth -- --ignored
cargo test --test functional_tests test_tracing -- --ignored

# Single test with output
cargo test --test functional_tests test_send_single_trace -- --ignored --nocapture
```

## 🛠️ Test Structure

```
test_functional/
├── helpers/              # Reusable utilities
│   ├── api_client.rs    # HTTP REST client
│   ├── grpc_client.rs   # OTLP/gRPC client
│   ├── fixtures.rs      # Test data generators
│   └── test_helpers.rs  # Utilities
└── scenarios/           # Test categories
    ├── test_health.rs
    ├── test_auth.rs
    ├── test_organizations.rs
    ├── test_projects.rs
    ├── test_api_keys.rs
    ├── test_tracing.rs   ⭐ Most important
    └── test_e2e.rs
```

## 💡 Usage Example

```rust
#[tokio::test]
#[ignore]
async fn test_complete_flow() -> Result<()> {
    let ctx = TestContext::new();
    let client = ctx.login_as_admin()?;
    
    // Create org → project → API key
    let org = client.create_organization("my-org", "My Org")?;
    let org_id = parse_uuid_from_json(&org, "id")?;
    
    let project = client.create_project(&org_id, "my-proj", "My Project")?;
    let project_id = parse_uuid_from_json(&project, "id")?;
    
    let api_key = client.create_api_key(&project_id, "key", "Description")?;
    let key_value = helpers::get_string_from_json(&api_key, "key")?;
    
    // Send trace via OTLP
    let otlp_client = OtlpClient::new(ctx.config.grpc_url)
        .with_api_key(key_value.to_string());
    
    otlp_client
        .send_test_trace("my-service", &trace_id, &span_id, "operation")
        .await?;
    
    Ok(())
}
```

## 🔧 Configuration

Environment variables (with defaults):

```bash
TEST_API_URL=http://localhost:9015     # Admin API
TEST_GRPC_URL=http://localhost:9016    # OTLP gRPC
TEST_ADMIN_EMAIL=admin@example.com
TEST_ADMIN_PASSWORD=changeme123
```

## 🐛 Manual Testing

```bash
# 1. Start databases
docker-compose -f docker-compose.test.yml up -d

# 2. Run migrations
DATABASE_URL=postgresql://zradar_test:test_pass_123@localhost:9011/zradar_test \
  cargo sqlx migrate run

# 3. Start server
DATABASE_URL=postgresql://zradar_test:test_pass_123@localhost:9011/zradar_test \
CLICKHOUSE_URL=http://localhost:9012 \
ADMIN_API_PORT=9015 \
OTLP_PORT=9016 \
  cargo run --bin zradar-server &

# 4. Run tests
TEST_API_URL=http://localhost:9015 \
TEST_GRPC_URL=http://localhost:9016 \
  cargo test --test functional_tests -- --ignored --nocapture

# 5. Cleanup
docker-compose -f docker-compose.test.yml down -v
```

## 🎯 Key Helpers

### ApiClient - HTTP REST
```rust
let client = ctx.login_as_admin()?;
let org = client.create_organization("name", "Display Name")?;
let project = client.create_project(&org_id, "name", "Display")?;
let key = client.create_api_key(&proj_id, "name", "desc")?;
```

### OtlpClient - gRPC Tracing
```rust
let otlp = OtlpClient::new(url).with_api_key(key);
otlp.send_test_trace("service", &trace_id, &span_id, "operation").await?;
```

### TestDataGenerator - Unique Data
```rust
let email = TestDataGenerator::email();           // test-123@example.com
let org_name = TestDataGenerator::org_name();     // test-org-123
let trace_id = TestDataGenerator::trace_id();     // [u8; 16]
```

## 🎨 Test Principles

✅ **Black-box** - API responses only, no DB queries  
✅ **Independent** - Each test uses unique data  
✅ **Fast** - In-memory databases (tmpfs)  
✅ **Isolated** - Separate test ports, auto cleanup  
✅ **Type-safe** - Rust compile-time checks  

## 🐞 Troubleshooting

```bash
# Check containers
docker ps --filter "name=zradar-test"

# View logs
docker-compose -f docker-compose.test.yml logs -f

# Kill test processes
lsof -ti:9011-9016 | xargs kill -9

# Emergency cleanup
./scripts/test-cleanup.sh
```

## 📝 Adding Tests

1. Add to appropriate `scenarios/test_*.rs` file
2. Use `TestDataGenerator` for unique data
3. Verify through API responses only (no DB)
4. Mark with `#[test] #[ignore]` or `#[tokio::test] #[ignore]`

```rust
#[test]
#[ignore]
fn test_my_feature() -> Result<()> {
    let ctx = TestContext::new();
    let client = ctx.login_as_admin()?;
    
    // Your test here
    
    Ok(())
}
```

---

**Status:** ✅ Ready to use  
**Command:** `make functional_tests`  
**License:** Apache-2.0
