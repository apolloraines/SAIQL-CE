# ⚙️ Configuration & Profiles

**Goal**: Configure SAIQL securely using profiles and environment variables.

---

## 1. The `secure_config.py` System
SAIQL uses a profile-based configuration system defined in `config/secure_config.py`.

### Available Profiles
Set the `SAIQL_PROFILE` environment variable to switch modes:

| Profile | Description | Default DB | Debug Mode |
| :--- | :--- | :--- | :--- |
| `dev` | Development | SQLite (`saiql.db`) | True |
| `test` | Testing | SQLite (`test.db`) | True |
| `prod` | Production | PostgreSQL (Recommended) | False |

## 2. Environment Variables
Never hardcode secrets. Use these variables:

| Variable | Required | Description | Example |
| :--- | :--- | :--- | :--- |
| `SAIQL_PROFILE` | Yes | Active profile | `prod` |
| `SAIQL_JWT_SECRET` | Yes | Secret for signing tokens | `openssl rand -hex 32` |
| `SAIQL_DB_PASSWORD` | Yes (Prod) | Database password | `strong_password` |
| `SAIQL_HOME` | No | Custom data directory | `/var/lib/saiql` |

## 3. Setting Variables (Production)
For systemd services, use an `EnvironmentFile`.

1. Create the file:
   ```bash
   sudo touch /etc/saiql/saiql.env
   sudo chmod 600 /etc/saiql/saiql.env
   sudo chown saiql:saiql /etc/saiql/saiql.env
   ```

2. Add content:
   ```bash
   SAIQL_PROFILE=prod
   SAIQL_JWT_SECRET=your_generated_secret_here
   SAIQL_DB_PASSWORD=your_db_password
   ```

## 4. Configuration Files
SAIQL also uses JSON files in `config/` for non-sensitive settings:

- `server_config.json`: Port, host, rate limits.
- `database_config.json`: Backend connection details (host, user, db name).

**Example `database_config.json`**:
```json
{
    "default_backend": "postgresql",
    "backends": {
        "postgresql": {
            "type": "postgresql",
            "host": "localhost",
            "port": 5432,
            "database": "saiql_prod",
            "user": "saiql_user",
            "password": "${SAIQL_DB_PASSWORD}" 
        }
    }
}
```
*Note: Use `${VAR}` syntax to reference environment variables in JSON configs.*

---

### Next Step
- **[04_First_Run_and_Healthchecks.md](./04_First_Run_and_Healthchecks.md)**: Verify your configuration.
