# 🌐 API Guide

**Goal**: Interact with SAIQL programmatically via the REST API.

The API is built with FastAPI and runs on port 8000 by default.

---

## 1. Authentication
Most endpoints require a Bearer token.

### Get Token
**POST** `/auth/token`

```bash
curl -X POST "http://localhost:8000/auth/token" \
     -H "Content-Type: application/json" \
     -d '{"username": "admin", "password": "admin_password"}'
```

**Response**:
```json
{
  "access_token": "eyJhbGciOiJIUzI1...",
  "token_type": "bearer"
}
```

## 2. Execute Query
**POST** `/query`

Execute a SAIQL query.

```bash
curl -X POST "http://localhost:8000/query" \
     -H "Authorization: Bearer $TOKEN" \
     -H "Content-Type: application/json" \
     -d '{"query": "SELECT * FROM users"}'
```

**Response**:
```json
{
  "success": true,
  "data": [
    {"id": 1, "name": "Alice"}
  ],
  "execution_time": 0.002,
  "backend": "sqlite"
}
```

## 3. Transaction Management
Use the transaction manager endpoints:
- **POST** `/transaction/begin?isolation_level=READ_COMMITTED`
- **POST** `/transaction/{tx_id}/commit`
- **POST** `/transaction/{tx_id}/abort` (if added)

Example:
```bash
TX=$(curl -s -X POST "http://localhost:8000/transaction/begin" \
    -H "Authorization: Bearer $TOKEN" | jq -r '.transaction_id')

curl -X POST "http://localhost:8000/query" \
     -H "Authorization: Bearer $TOKEN" \
     -H "Content-Type: application/json" \
     -d '{"query": "INSERT INTO users (name) VALUES (''Alice'')"}'

curl -X POST "http://localhost:8000/transaction/${TX}/commit" \
     -H "Authorization: Bearer $TOKEN"
```

## 4. System Health
**GET** `/health`

Check server status (No auth required).

```bash
curl http://localhost:8000/health
```

## 5. Metrics
**GET** `/metrics`

Get performance statistics.

```bash
curl -H "Authorization: Bearer $TOKEN" http://localhost:8000/metrics
```

---

### API Documentation
When the server is running, visit:
- **Swagger UI**: `http://localhost:8000/docs`
- **ReDoc**: `http://localhost:8000/redoc`

### Other Endpoints
- **POST** `/auth/refresh` – Refresh an access token.
- **POST** `/auth/api-keys/{key_id}/rotate` – Rotate an API key (admin).
- **GET** `/status` – Detailed server status.

### Next Step
- **[07_Query_Language_Basics.md](./07_Query_Language_Basics.md)**: Master the SAIQL syntax.
