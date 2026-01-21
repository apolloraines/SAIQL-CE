# SAIQL Migration Truth Harness

This directory contains a containerized test suite to verify "Supported Database" (PG/MySQL) migration capabilities.

## Structure
- `docker-compose.yml`: Spins up source (Postgres, MySQL) and target (Postgres, MySQL) databases.
- `run_matrix.sh`: Orchestrates the test (Seed -> Migrate -> Verify).
- `seed_and_verify.py`: Helper script for DB operations.

## Usage

To run the full matrix test (requires Docker):

```bash
cd tests/migration_matrix
docker-compose up --build --abort-on-container-exit
```

## Test Flow
1. **Seed**: Creates `inventory` table in Postgres Source and `users` table in MySQL Source.
2. **Migrate**:
    - Postgres Source -> Postgres Target
    - MySQL Source -> MySQL Target
    - MSSQL Source -> MSSQL Target
3. **Verify**: Checks row counts in generic target databases to ensure complete data transfer.
