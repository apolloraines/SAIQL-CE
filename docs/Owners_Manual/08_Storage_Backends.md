# Storage Backends

**Goal**: Understand SAIQL CE's database backends and file import capabilities.

SAIQL Community Edition supports 4 storage backends:

---

## Supported Backends

### SQLite (Default)
**Best for**: Development, demos, embedded use.
- **Config**: `type: sqlite`
- **Path**: `data/saiql.db`
- **Pros**: Zero config, single file, bundled with Python.
- **Cons**: Lower concurrency for writes.

### PostgreSQL (Production)
**Best for**: Production workloads, high concurrency.
- **Config**: `type: postgresql`
- **Pros**: Robust, scalable, industry standard.
- **Cons**: Requires external server setup.

### MySQL / MariaDB
**Best for**: Existing MySQL environments.
- **Config**: `type: mysql`
- **Pros**: Widely deployed, familiar to many teams.

### CSV / Excel (File Import)
**Best for**: Quick data import, spreadsheet users.
- **Config**: `type: file`
- **Pros**: Instant adoption, no database setup needed.
- **Use case**: Import existing spreadsheets into SAIQL.

---

## Configuration
Edit `config/database_config.json` to select your backend:

```json
{
    "default_backend": "sqlite",
    "backends": {
        "sqlite": {
            "type": "sqlite",
            "path": "data/saiql.db",
            "timeout": 30
        },
        "postgresql": { ... }
    }
}
```

> After running the migration tool, set the sqlite `path` to the generated `saiql_store.db` in your chosen target directory.

---

### Next Step
- **[09_Migration_Postgres_MySQL.md](./09_Migration_Postgres_MySQL.md)**: Import your existing data.
