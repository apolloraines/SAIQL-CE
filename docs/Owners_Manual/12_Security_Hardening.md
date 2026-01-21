# 🔒 Security Hardening

**Goal**: Secure your SAIQL deployment against unauthorized access.

---

## 1. Least Privilege
Run SAIQL as a dedicated user (`saiql`), never as `root`.
- **User**: `saiql`
- **Group**: `saiql`
- **File Permissions**: `chmod 600` for config files containing secrets.

## 2. Secrets Management
- **Never** commit secrets to git.
- **Use Environment Variables**: `SAIQL_JWT_SECRET`, `SAIQL_DB_PASSWORD`.
- **Rotate Secrets**: Change `SAIQL_JWT_SECRET` periodically and restart the service. Note: This invalidates existing tokens.

## 3. Network Security
- **Bind Address**: Bind to `127.0.0.1` if using a reverse proxy.
- **Firewall**: Allow port 8000 only from trusted sources (or localhost).
- **TLS/SSL**: **Never expose SAIQL directly to the internet without TLS.** Use Nginx or Caddy as a reverse proxy to handle HTTPS.

### Nginx Example
```nginx
server {
    listen 443 ssl;
    server_name saiql.example.com;

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

## 4. Authentication
- **Enable Auth**: Ensure `require_auth` is `true` in `config/server_config.json`.
- **Strong Passwords**: Use long, random passwords for admin accounts.

---

### Next Step
- **[13_Troubleshooting_and_FAQ.md](./13_Troubleshooting_and_FAQ.md)**: Solve common problems.
