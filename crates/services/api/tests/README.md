# zradar-control Integration Tests

This directory contains integration tests for the zradar control plane.

## Test Categories

### Simple Authentication Tests (`auth_simple_tests.rs`)
Basic authentication tests that don't require database:
- **Bcrypt Tests:**
  - Password hashing
  - Password verification (correct/incorrect)
  - Hash uniqueness (salt verification)
- **UUID Tests:**
  - UUID generation
  - UUID string conversion
- **API Key Format Tests:**
  - Key generation format
  - Alphanumeric validation

### Unit Tests (in source files)
Core business logic tests:
- **Permission Tests** (`src/permissions.rs`):
  - Wildcard expansion
  - Dependency validation
  - Risk assessment
- **RBAC Tests** (`src/rbac.rs`):
  - Permission checking
  - Hierarchical permissions
- **API Key Tests** (`src/auth/api_key.rs`):
  - Key generation
  - Key hashing

## Running Tests

### Unit Tests Only
```bash
cargo test --lib
```

### Integration Tests (Requires Database)
```bash
# Set up test database
export TEST_DATABASE_URL=postgres://postgres:password@localhost:5432/zradar_test

# Create test database
createdb zradar_test

# Run migrations
sqlx migrate run --database-url $TEST_DATABASE_URL

# Run integration tests
cargo test --test '*' -- --ignored
```

### All Tests
```bash
cargo test --all
```

### Specific Test
```bash
# Run specific RBAC test
cargo test --test rbac_tests test_rbac_organization_owner_permissions -- --ignored

# Run all JWT tests
cargo test jwt -- --nocapture
```

## Test Database Setup

The integration tests require a PostgreSQL database. By default, they look for:

```
postgres://postgres:password@localhost:5432/zradar_test
```

Override with `TEST_DATABASE_URL`:

```bash
export TEST_DATABASE_URL=postgres://user:pass@host:port/dbname
```

### One-Time Setup

```bash
# Create test database
createdb zradar_test

# Run migrations
export DATABASE_URL=postgres://postgres:password@localhost:5432/zradar_test
sqlx migrate run

# Run tests
cargo test --test '*' -- --ignored
```

### Clean Test Database

```bash
# Drop and recreate
dropdb zradar_test
createdb zradar_test
sqlx migrate run --database-url postgres://postgres:password@localhost:5432/zradar_test
```

## Test Organization

Tests are marked with `#[ignore]` if they require database access. This allows you to:

1. Run unit tests quickly without database setup
2. Run integration tests explicitly when database is available

```rust
#[tokio::test]
#[ignore] // Requires database
async fn test_something() {
    // ...
}
```

## Writing New Tests

### Unit Tests
Add directly to the module files:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_something() {
        // Test logic
    }
}
```

### Integration Tests
Add new files to `tests/` directory:

```rust
// tests/my_tests.rs
use zradar_control::*;

#[tokio::test]
#[ignore]
async fn test_feature() {
    // Test with real database
}
```

## Test Coverage

Current test coverage (15 tests passing):

| Module | Unit Tests | Integration Tests | Status |
|--------|-----------|-------------------|---------|
| Permissions | ✅ 9 tests | N/A | Complete |
| RBAC | ✅ 2 tests | 🚧 | Core logic done |
| Auth (Bcrypt) | ✅ 3 tests | N/A | Complete |
| Auth (JWT) | ✅ 1 test | 🚧 | Basic done |
| Auth (API Keys) | ✅ 2 tests | 🚧 | Basic done |
| Storage | ❌ | ❌ | Needs work |
| API Endpoints | ❌ | ❌ | Future |
| Audit Logging | ❌ | ❌ | Future |

## CI/CD Integration

For CI pipelines:

```yaml
# .github/workflows/test.yml
- name: Run unit tests
  run: cargo test --lib

- name: Setup test database
  run: |
    docker run -d -p 5432:5432 \
      -e POSTGRES_PASSWORD=password \
      -e POSTGRES_DB=zradar_test \
      postgres:17

- name: Run integration tests
  env:
    TEST_DATABASE_URL: postgres://postgres:password@localhost:5432/zradar_test
  run: cargo test --test '*' -- --ignored
```

## Performance Tests

Some tests measure performance (e.g., caching). These may be flaky in CI:

- RBAC permission caching
- API key validation caching

Consider:
- Using `#[cfg(not(ci))]` for performance tests
- Increasing thresholds in CI environments
- Using separate benchmark suite for precise measurements

## Troubleshooting

### "Failed to connect to test database"
- Ensure PostgreSQL is running
- Check `TEST_DATABASE_URL` is correct
- Verify database exists: `psql zradar_test`

### "relation does not exist"
- Run migrations: `sqlx migrate run`
- Ensure you're using correct database

### "permission denied"
- Check database user permissions
- Grant required permissions: `GRANT ALL ON DATABASE zradar_test TO your_user;`

### Flaky cache tests
- Cache timing tests may be unreliable
- Consider skipping in CI: `#[cfg_attr(ci, ignore)]`

## Future Tests

Planned test additions:
- [ ] API endpoint tests (with mock HTTP client)
- [ ] Audit log verification tests
- [ ] Permission dependency validation tests
- [ ] Custom role CRUD tests
- [ ] Organization/Project cascade deletion tests
- [ ] Concurrent access tests
- [ ] Rate limiting tests (when implemented)

## Resources

- [Rust Testing Book](https://doc.rust-lang.org/book/ch11-00-testing.html)
- [tokio Testing Guide](https://tokio.rs/tokio/topics/testing)
- [sqlx Testing](https://github.com/launchbadge/sqlx#testing)

