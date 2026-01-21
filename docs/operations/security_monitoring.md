# SAIQL Operational Security & Monitoring Playbook

This note captures the guardrails added during the March 2025 hardening pass. Treat it as the “run first” checklist before pushing a new build.

## Security Verification
- `python scripts/verify_security.py` validates that sensitive files are absent from the repo, `.env.template` exposes all required keys, and the environment or secret store populates them.
- The script also asserts that `docker-compose.yml` is wired to those variables. Run it locally or wire it into CI:
  ```bash
  python scripts/verify_security.py
  ```
- `python scripts/backup_restore.py` exposes `create`, `restore`, and `list` subcommands for managed snapshots of the `data/` directory. Use `--tag` to align with retention policies and schedule the command via cron/systemd.

## Deployment Hygiene
- `python scripts/check_deployment_config.py` audits docker-compose bindings, installer permissions, and requirement file parity. It falls back to textual scanning if non-standard characters appear in YAML.
- Keep `config/.env.template` updated whenever new secrets are introduced.

## Authentication Lifecycle
- The production FastAPI server exposes two admin endpoints:
  - `POST /auth/refresh` – swaps an expiring JWT for a fresh one.
  - `POST /auth/api-keys/{key_id}/rotate` – rotates an API key and returns the new secret (admin role required).
- `AuthManager` now supports token refresh, API key rotation, and external identity providers via `verify_external_token`.

## Logging & Metrics
- `EnterpriseLogger.export_metrics("prometheus")` emits Prometheus-ready gauges derived from the in-memory aggregator.
- This can be scraped along with `/metrics` (powered by `AdvancedPerformanceMonitor`) for unified observability.

## Suggested Workflow
1. Edit code.
2. `python scripts/verify_security.py`
3. `python scripts/check_deployment_config.py`
4. `python scripts/backup_restore.py create --tag nightly`
5. Run tests / lint suite.
6. Update `/home/nova/GPTMem/gpt_memory.saiql` with notable learnings before closing the session.

Lock this checklist into your personal dev ritual; your future self (and any auditors) will thank you.
