# 🗣️ Translation Layer Mode

**Goal**: Use SAIQL as a semantic frontend for existing PostgreSQL/MySQL databases.

In this mode, SAIQL does **not** store data. Instead, it translates your queries (including semantic search) into SQL dialects compatible with your legacy database.

---

## 1. How It Works
1.  **Input**: You send a SAIQL query (e.g., `SELECT * FROM users WHERE active = true`).
2.  **Translation**: SAIQL compiles this into the target dialect (e.g., PostgreSQL SQL).
3.  **Execution**: The generated SQL is executed against the remote database.
4.  **Result**: Data is returned to you.

## 2. Configuration
Edit `config/database_config.json` to point to your legacy database.

```json
{
    "default_backend": "postgresql",
    "backends": {
        "postgresql": {
            "type": "postgresql",
            "host": "legacy-db.example.com",
            "port": 5432,
            "database": "production_db",
            "user": "app_user",
            "password": "${SAIQL_DB_PASSWORD}"
        }
    }
}
```

## 3. Limitations
- **Complex Joins**: Highly complex multi-database joins are not supported in translation mode (data must reside in the same backend).

---

### Verification
Run a query and check the `backend` field in the response.

```bash
curl -X POST "http://localhost:8000/query" ...
```

**Response**:
```json
{
  "success": true,
  "backend": "postgresql",  <-- Confirms translation mode
  "data": [...]
}
```

### Next Step
- **[11_Production_Operations.md](./11_Production_Operations.md)**: Manage your SAIQL server.
