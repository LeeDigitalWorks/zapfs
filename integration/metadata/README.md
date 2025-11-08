# Metadata Integration Tests

This directory contains integration tests for the metadata service database interface.

## Test Files

- `db_encryption_test.go` - Tests encryption fields using in-memory database (always runs)
- `db_vitess_test.go` - Tests database interface against MySQL/Vitess (requires DB_DSN)

## Running Tests

### With In-Memory Database (Default)

Tests run against the in-memory database by default:

```bash
go test -tags=integration -v ./integration/metadata
```

### With MySQL/Vitess Database

To test against a real MySQL or Vitess database, set the `DB_DSN` environment variable:

```bash
# Using Docker Compose MySQL
export DB_DSN="zapfs:zapfs@tcp(localhost:3306)/zapfs"
go test -tags=integration -v ./integration/metadata -run TestDatabaseInterface

# Using custom MySQL instance
export DB_DSN="user:password@tcp(host:3306)/database"
go test -tags=integration -v ./integration/metadata -run TestDatabaseInterface

# Using Vitess
export DB_DSN="user:password@tcp(vtgate:3306)/keyspace"
go test -tags=integration -v ./integration/metadata -run TestDatabaseInterface
```

### Using Docker Compose

The easiest way to test against MySQL is using the provided Docker Compose setup:

```bash
# Start MySQL (and other services)
cd docker
docker compose up -d mysql

# Wait for MySQL to be ready
docker compose ps mysql

# Run tests against MySQL
export DB_DSN="zapfs:zapfs@tcp(localhost:3306)/zapfs"
cd ../..
go test -tags=integration -v ./integration/metadata -run TestDatabaseInterface

# Cleanup
cd docker
docker compose down
```
