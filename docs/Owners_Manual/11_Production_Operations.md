# 🏭 Production Operations

**Goal**: Manage SAIQL as a long-running production service.

---

## 1. Systemd Service
Ensure SAIQL starts on boot and restarts on failure.

### Create Service File
`/etc/systemd/system/saiql.service`

```ini
[Unit]
Description=SAIQL Production Server
After=network.target postgresql.service

[Service]
User=saiql
Group=saiql
WorkingDirectory=/opt/saiql
EnvironmentFile=/etc/saiql/saiql.env
ExecStart=/opt/saiql/venv/bin/python3 saiql_production_server.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

### Enable Service
```bash
sudo systemctl daemon-reload
sudo systemctl enable --now saiql
```

## 2. Logging
Logs are essential for troubleshooting.

- **Access Logs**: `journalctl -u saiql`
- **Application Logs**: Configured in `config/server_config.json` (default: stdout/stderr captured by systemd).

**View Live Logs**:
```bash
sudo journalctl -u saiql -f
```

## 3. Backups
Protect your data.

### SQLite Backend
Stop the service and copy the file.
```bash
sudo systemctl stop saiql
cp /var/lib/saiql/saiql.db /backup/saiql_$(date +%F).db
sudo systemctl start saiql
```

### PostgreSQL Backend
Use `pg_dump`.
```bash
pg_dump -U saiql_user saiql_prod > /backup/saiql_prod_$(date +%F).sql
```

## 4. Upgrades
1.  **Backup Data**: Always backup before upgrading.
2.  **Pull Code**: `git pull origin main`
3.  **Update Deps**: `./venv/bin/pip install -r requirements.txt`
4.  **Restart**: `sudo systemctl restart saiql`

---

### Next Step
- **[12_Security_Hardening.md](./12_Security_Hardening.md)**: Secure your deployment.
