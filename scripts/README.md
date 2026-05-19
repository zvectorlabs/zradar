# zradar Scripts

## Migration Testing

### `test-migrations.sh`

Automated test script for the PostgreSQL migration system.

## Bootstrap

### `bootstrap.sh`

Development environment setup script.

**What it does:**
- Checks for required tools (`psql`, `sqlx-cli`)
- Runs PostgreSQL migrations
- Creates config files from examples

## Best Practices

1. **Keep scripts minimal** - Let the application handle complex logic
2. **Use environment variables** - Make scripts configurable
3. **Clear error messages** - Help debugging
4. **Idempotent operations** - Safe to run multiple times
5. **Single responsibility** - Each script does one thing well

