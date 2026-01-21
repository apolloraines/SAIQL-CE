# 🏥 First Run & Health Checks

**Goal**: Start the server and verify it is healthy and secure.

---

## 1. Start the Server
If running manually (Dev):
```bash
export SAIQL_PROFILE=dev
export SAIQL_DB_PASSWORD=secure_password
export SAIQL_PORT=8000
export SAIQL_BOOTSTRAP_TEMPLATE=true
python3 saiql_production_server.py
```

If running via Systemd (Prod):
```bash
sudo systemctl start saiql
sudo systemctl status saiql
```

## 2. Health Check Endpoint
Verify the server is up and responding.

```bash
curl http://localhost:8000/health
```

**Expected Output**:
```json
{
  "status": "healthy",
  "version": "0.3.0-alpha",
  "uptime": 12.5,
  "backend": "sqlite" 
}
```

## 3. Verify Authentication
Ensure the server rejects unauthenticated requests (if configured for auth).

```bash
curl -I http://localhost:8000/query -d '{"query": "SELECT 1"}'
```

**Expected Output**:
`HTTP/1.1 401 Unauthorized` (or 403 Forbidden)

## 4. Get an Admin Token
Generate a token to prove auth works. If you did not set `SAIQL_JWT_SECRET`, `saiql_production_server.py` will inject one from `secure_config.py` (or generate a temporary dev secret) before AuthManager initializes.

```bash
curl -X POST "http://localhost:8000/auth/token" \
     -H "Content-Type: application/json" \
     -d '{"username": "admin", "password": "admin_password"}'
```
*Note: Replace `admin_password` with the actual password (check logs on first run for generated password if not set).*

## 5. End-to-End Query Test
Run a simple query to verify the execution engine.

```bash
# Export token for convenience
export TOKEN="your_access_token_here"

curl -X POST "http://localhost:8000/query" \
     -H "Authorization: Bearer $TOKEN" \
     -H "Content-Type: application/json" \
     -d '{"query": "IMAGINE A WORLD WHERE AI IS HELPFUL"}'
```

**Expected Result**:
A JSON response containing query results or a success message.

---

### Troubleshooting
- **Connection Refused**: Is the server running? Check `sudo systemctl status saiql`.
- **401 Unauthorized**: Check `SAIQL_JWT_SECRET` matches between generation and server start.
- **500 Internal Error**: Check logs (`logs/saiql.log` or `journalctl -u saiql`).

### Next Step
- **[05_CLI_Guide.md](./05_CLI_Guide.md)**: Learn to use the CLI.
