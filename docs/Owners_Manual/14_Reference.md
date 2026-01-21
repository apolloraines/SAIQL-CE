# 📖 Reference

**Goal**: Comprehensive reference for CLI, API, and Configuration.

---

## 1. CLI Reference (`bin/saiql.py`)

| Flag | Description |
| :--- | :--- |
| `--help` | Show help message |
| `--version` | Show version info |
| `--interactive` | Start REPL shell |
| `--query "SQL"` | Execute a single query |
| `--load FILE` | Load a database file |
| `--warm-cache` | Pre-load cache for performance |
| `--debug` | Enable debug logging |

## 2. Environment Variables

| Variable | Default | Description |
| :--- | :--- | :--- |
| `SAIQL_PROFILE` | `dev` | Run mode (`dev`, `test`, `prod`) |
| `SAIQL_HOME` | `.` | Base directory for data |
| `SAIQL_JWT_SECRET` | None | Secret for token signing (Required) |
| `SAIQL_DB_PASSWORD` | None | DB Password (Required for Prod) |
| `SAIQL_API_KEY_SALT` | None | Salt for API keys |

## 3. Configuration Files

### `server_config.json`
```json
{
  "host": "0.0.0.0",
  "port": 8000,
  "debug": false,
  "workers": 4,
  "timeout": 60,
  "cors_origins": ["*"],
  "rate_limit": "100/minute",
  "require_auth": true
}
```

### `database_config.json`
```json
{
  "default_backend": "sqlite",
  "backends": {
    "sqlite": { "type": "sqlite", "path": "data/saiql.db" },
    "postgresql": { "type": "postgresql", ... },
    "mysql": { "type": "mysql", ... }
  }
}
```

## 4. API Endpoints

| Method | Path | Description | Auth |
| :--- | :--- | :--- | :--- |
| `GET` | `/health` | Server status | No |
| `POST` | `/auth/token` | Get JWT token | No |
| `POST` | `/query` | Execute SAIQL query | Yes |
| `POST` | `/transaction` | Execute batch | Yes |
| `GET` | `/metrics` | System metrics | Yes |

---

*End of Manual*
