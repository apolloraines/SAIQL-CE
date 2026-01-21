# 🆘 Troubleshooting & FAQ

**Goal**: Solve common problems and answer frequent questions.

---

## 1. Installation Failures

### `ModuleNotFoundError: No module named 'psycopg2'`
**Cause**: Missing PostgreSQL driver.
**Fix**:
```bash
pip install psycopg2-binary
```

### `ImportError: attempted relative import...`
**Cause**: Running tests or scripts from the wrong directory.
**Fix**: Always run from the project root (`SAIQL.DEV`).
```bash
cd /path/to/SAIQL.DEV
python3 -m pytest ...
```

## 2. Authentication Failures

### `401 Unauthorized`
**Cause**: Invalid token or wrong secret.
**Fix**:
1. Check `SAIQL_JWT_SECRET` environment variable.
2. Regenerate token: `curl -X POST ... /auth/token`

### `Signature has expired`
**Cause**: Token is too old (default 30 mins).
**Fix**: Get a new token.

## 3. Database Issues

### `locked` (SQLite)
**Cause**: Another process is writing to the DB.
**Fix**: Ensure only one server instance is running.
```bash
sudo lsof data/saiql.db
```

### `connection refused` (Postgres/MySQL)
**Cause**: Database server is down or firewall is blocking.
**Fix**:
1. `sudo systemctl status postgresql`
2. Check `database_config.json` host/port.

## 4. Migration Issues

### `Failed to insert batch...`
**Cause**: Data mismatch or constraint violation.
**Fix**:
1. Check logs for specific error.
2. Ensure source data doesn't violate SAIQL constraints (e.g., NULL in NOT NULL column).

---

## FAQ

**Q: Can I use SAIQL with Docker?**
A: Yes, a Dockerfile is provided in the repo. Build with `docker build -t saiql .`.

**Q: How do I reset the admin password?**
A: Delete the `saiql.db` file (dev) or update the `users` table directly in the backend database (prod).

**Q: Is SAIQL ACID compliant?**
A: Yes, when using SQLite (WAL mode) or PostgreSQL backends.

---

### Next Step
- **[14_Reference.md](./14_Reference.md)**: Full command and config reference.
