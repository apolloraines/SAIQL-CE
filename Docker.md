\# Docker



SAIQL supports a normal Python/venv workflow by default. Docker is an optional path for:

\- Production-style packaging (single artifact, consistent runtime)

\- Running SAIQL in environments where you do not want to manage Python deps on the host

\- CI smoke tests / reproducible deployments



This repo includes a multi-stage production Dockerfile ("SAIQL-Charlie Production Docker Image"). :contentReference\[oaicite:0]{index=0}



---



\## What this Docker image contains



High level:

\- Multi-stage build based on `python:3.11-slim` (builder + production). 

\- Installs Python deps from `requirements-prod.txt` during build. :contentReference\[oaicite:2]{index=2}

\- Creates a non-root `saiql` user and runs as that user. :contentReference\[oaicite:3]{index=3}

\- Copies default config files into `/config/saiql/`. :contentReference\[oaicite:4]{index=4}

\- Exposes ports `8000` and `8001`. :contentReference\[oaicite:5]{index=5}

\- Healthcheck hits `http://localhost:8000/health`. :contentReference\[oaicite:6]{index=6}

\- Declares volumes: `/data`, `/logs`, `/config`. :contentReference\[oaicite:7]{index=7}

\- Default command starts the server: `python -m interface.saiql\_server --host 0.0.0.0 --port 8000`. :contentReference\[oaicite:8]{index=8}



---



\## Build



From repo root:



docker build -t saiql:local \\

&nbsp; --build-arg VERSION=1.0.0 \\

&nbsp; --build-arg BUILD\_DATE="$(date -u +%Y-%m-%dT%H:%M:%SZ)" \\

&nbsp; --build-arg VCS\_REF="$(git rev-parse --short HEAD)" \\

&nbsp; .



The Dockerfile supports `VERSION`, `BUILD\_DATE`, and `VCS\_REF` build args. 



---



\## Run



Minimal run (bind ports + mount volumes):



docker run --rm -it \\

&nbsp; -p 8000:8000 -p 8001:8001 \\

&nbsp; -v "$(pwd)/.docker-data:/data" \\

&nbsp; -v "$(pwd)/.docker-logs:/logs" \\

&nbsp; -v "$(pwd)/.docker-config:/config" \\

&nbsp; saiql:local



Notes:

\- The image declares `/data`, `/logs`, `/config` as volumes, so mounting them is the expected path. :contentReference\[oaicite:10]{index=10}

\- The server listens on `0.0.0.0:8000` inside the container. 



---



\## Environment variables



The image sets these defaults (override with `-e KEY=VALUE`):



\- `SAIQL\_ENV=production`

\- `SAIQL\_HOST=0.0.0.0`

\- `SAIQL\_PORT=8000`

\- `SAIQL\_LOG\_LEVEL=INFO` :contentReference\[oaicite:12]{index=12}



Example override:



docker run --rm -it \\

&nbsp; -e SAIQL\_LOG\_LEVEL=DEBUG \\

&nbsp; -p 8000:8000 \\

&nbsp; saiql:local



---



\## Security notes



\- The container runs as a non-root user (`saiql`) and sets ownership for `/app /data /logs /config`. 

\- Healthcheck requires the server to expose `/health` on port 8000. :contentReference\[oaicite:14]{index=14}



---



\## Docker vs Benchmarks



This Dockerfile is for packaging/running SAIQL itself.



The benchmark harness uses separate docker-compose stacks under `benchmarks/containers/` (Postgres+pgvector and OpenSearch). Those are optional and only required if you want cross-engine "truth" benchmark runs.



---



\## Troubleshooting



\### "docker: command not found"

Docker is not installed on that host. This is expected for venv-only setups.



\### Healthcheck failing

\- Confirm the server is running and serving `GET /health` on port 8000. :contentReference\[oaicite:15]{index=15}



\### Config changes not sticking

Mount `/config` and edit files under your host-mounted config directory. The image copies defaults into `/config/saiql/` at build time. :contentReference\[oaicite:16]{index=16}



