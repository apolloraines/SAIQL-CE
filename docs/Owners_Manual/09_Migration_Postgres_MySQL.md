# 🔄 Database Migration Guide

**Goal**: Import data from PostgreSQL or MySQL into SAIQL.

SAIQL includes a migration tool (`tools/db_migrator.py`) that introspects your legacy database schema (tables, PK/FK constraints) and migrates data into SAIQL's native storage using transactional batches.

---

## 1. Prerequisites
Ensure you have the source database credentials and the necessary drivers installed.

```bash
pip install psycopg2-binary  # For PostgreSQL
pip install mysql-connector-python  # For MySQL
```

## 2. The Migration Tool
The tool is located at `tools/db_migrator.py`.

**Usage**:
```bash
python3 tools/db_migrator.py --source "<CONNECTION_URL>" --target-dir "<TARGET_DIR>"
```

### Connection URL Formats
- **PostgreSQL**: `postgresql://user:password@host:port/dbname`
- **MySQL**: `mysql://user:password@host:port/dbname`

## 3. Example: Migrating from PostgreSQL
Migrate a database named `legacy_users` to SAIQL.

```bash
# 1. Stop SAIQL Server (Recommended)
sudo systemctl stop saiql

# 2. Run Migration
python3 tools/db_migrator.py \
    --source "postgresql://admin:secret@localhost:5432/legacy_users" \
    --target-dir "data/"

# 3. Verify Output
# Look for: "Migration completed successfully! ✨"

# 4. Point SAIQL at the migrated DB (default path: data/saiql_store.db)
# Update config/database_config.json to use that path for the sqlite backend.
```

## 4. What Gets Migrated?
- **Tables**: Created with corresponding SAIQL types.
- **Columns**: Mapped to SAIQL types (TEXT, INTEGER, FLOAT, BOOLEAN, TIMESTAMP). Common variants like `int(11)` are handled; uncommon/custom types are stored as TEXT.
- **Constraints**: Primary Keys and Foreign Keys are preserved.
- **Data**: All rows are imported in transactional batches (default 1000 rows per transaction).

## 5. Safety & Performance
- **Transactional**: Data is inserted in batches (default 1000 rows) within transactions.
- **Idempotent**: The tool creates tables `IF NOT EXISTS`.
- **Large Datasets**: For very large tables (>1M rows), ensure you have sufficient disk space for the SQLite target; consider migrating table-by-table if needed.
- **Type Edge Cases**: Complex/custom types (e.g., geometry/JSON/enum) are currently stored as TEXT—review and adjust manually after migration if required.

---

### Troubleshooting
- **Connection Failed**: Check firewall rules and credentials.
- **Missing Driver**: Install `psycopg2-binary` or `mysql-connector-python`.
- **Type Errors**: Some complex custom types (e.g., PostGIS geometry) map to `TEXT`. Update schemas post-migration if you need richer typing.
- **Path Mismatch**: If SAIQL still points to the old database path, update `config/database_config.json` (sqlite backend path) to the migrated `saiql_store.db`.

### Next Step
- **[10_Translation_Layer_Mode.md](./10_Translation_Layer_Mode.md)**: Use SAIQL without migrating data.
