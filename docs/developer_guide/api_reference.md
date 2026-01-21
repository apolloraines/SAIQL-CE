# SAIQL CE API Reference

This document provides reference documentation for SAIQL Community Edition APIs including REST endpoints, Python client library, and CLI commands.

## Deployment Modes

SAIQL CE APIs work in Translation Mode:

- **Translation Mode**: SAIQL queries are translated to SQL and executed on your existing database (PostgreSQL, MySQL, SQLite, CSV/Excel)

## Table of Contents
- [REST API](#rest-api)
- [Python Client API](#python-client-api)
- [CLI Reference](#cli-reference)
- [Authentication](#authentication)
- [Error Handling](#error-handling)
- [Response Formats](#response-formats)
- [Rate Limiting](#rate-limiting)

## REST API

### Base URL
```
http://your-saiql-server.com/api/v1
```

### Authentication Headers
```http
Authorization: Bearer <jwt-token>
# OR
X-API-Key: <api-key>
```

### Query Execution

#### Execute Query
Execute a SAIQL query and return results.

**Endpoint**: `POST /query`

**Request Body**:
```json
{
  "query": "*10[users]::name,email>>oQ",
  "parameters": {
    "limit": 10,
    "status": "active"
  },
  "options": {
    "cache_duration": 300,
    "explain": false
  }
}
```

**Response**:
```json
{
  "success": true,
  "data": [
    {
      "name": "John Doe",
      "email": "john@example.com"
    }
  ],
  "metadata": {
    "execution_time_ms": 45,
    "rows_returned": 10,
    "cache_hit": false
  }
}
```

**curl Example**:
```bash
curl -X POST "http://localhost:5433/api/v1/query" \
  -H "Authorization: Bearer your-jwt-token" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "*5[users]::name,email|status=@status",
    "parameters": {"status": "active"}
  }'
```

#### Batch Query Execution
Execute multiple queries in a single request.

**Endpoint**: `POST /query/batch`

**Request Body**:
```json
{
  "queries": [
    {
      "id": "users_query",
      "query": "*10[users]::name,email"
    },
    {
      "id": "orders_query",
      "query": "*5[orders]::id,amount"
    }
  ],
  "options": {
    "parallel_execution": true,
    "fail_fast": false
  }
}
```

**Response**:
```json
{
  "success": true,
  "results": {
    "users_query": {
      "success": true,
      "data": [...],
      "metadata": {...}
    },
    "orders_query": {
      "success": true,
      "data": [...],
      "metadata": {...}
    }
  },
  "execution_summary": {
    "total_time_ms": 123,
    "successful_queries": 2,
    "failed_queries": 0
  }
}
```

### Schema and Metadata

#### Get Schema Information
Retrieve table schemas and metadata.

**Endpoint**: `GET /schema`

**Query Parameters**:
- `table` (optional): Specific table name
- `include_stats` (optional): Include table statistics

**Response**:
```json
{
  "success": true,
  "schema": {
    "users": {
      "columns": {
        "id": {"type": "INTEGER", "primary_key": true},
        "name": {"type": "TEXT", "nullable": false},
        "email": {"type": "TEXT", "unique": true},
        "created_at": {"type": "TIMESTAMP", "default": "now()"}
      },
      "indexes": ["idx_users_email", "idx_users_created_at"],
      "row_count": 15420
    }
  }
}
```

#### Get Symbol Legend
Retrieve SAIQL symbol definitions and usage.

**Endpoint**: `GET /legend`

**Response**:
```json
{
  "success": true,
  "symbols": {
    "*": {
      "name": "Query Prefix",
      "description": "Initiates a SAIQL query",
      "examples": ["*[users]", "*5[products]"]
    },
    ">>": {
      "name": "Pipeline Operator",
      "description": "Pipes query results to output format",
      "examples": [">>oQ", ">>oJ"]
    },
    "::": {
      "name": "Column Separator",
      "description": "Separates table from column list",
      "examples": ["*[users]::name,email"]
    },
    "|": {
      "name": "Filter Operator",
      "description": "Apply WHERE conditions",
      "examples": ["*[users]::*|status='active'"]
    }
  }
}
```

### Health and Status

#### Health Check
Get system health status.

**Endpoint**: `GET /health`

**Response**:
```json
{
  "status": "healthy",
  "timestamp": "2025-08-14T12:00:00Z",
  "version": "0.4.0-ce",
  "components": {
    "database": {"status": "healthy", "response_time_ms": 2},
    "cache": {"status": "healthy", "hit_rate": 0.85}
  },
  "metrics": {
    "queries_per_second": 1250,
    "avg_response_time_ms": 15,
    "memory_usage_mb": 2048
  }
}
```

#### System Statistics
Get detailed system performance statistics.

**Endpoint**: `GET /stats`

**Response**:
```json
{
  "success": true,
  "statistics": {
    "queries": {
      "total_executed": 1000000,
      "successful": 999500,
      "failed": 500,
      "avg_execution_time_ms": 25
    },
    "cache": {
      "hit_rate": 0.85,
      "miss_rate": 0.15,
      "size_mb": 512,
      "evictions": 1250
    }
  }
}
```

## Python Client API

### Installation
```bash
pip install saiql-ce-client
```

### Basic Usage

#### Client Initialization
```python
from core import SAIQLClient

# Basic connection
client = SAIQLClient(host='localhost', port=5433)

# With SSL/TLS
client = SAIQLClient(
    host='saiql-server.com',
    port=5433,
    ssl=True,
    ssl_verify=True
)

# With connection pooling
client = SAIQLClient(
    host='localhost',
    port=5433,
    pool_size=10,
    max_overflow=20,
    pool_timeout=30
)
```

#### Authentication
```python
# API Key authentication
client.authenticate(api_key='your-api-key')

# JWT authentication
client.authenticate_jwt('your-jwt-token')
```

### Query Operations

#### Basic Query Execution
```python
# Simple query
result = client.query("*10[users]::name,email>>oQ")

# Query with parameters
result = client.query(
    "*[users]::name,email|status=@status",
    parameters={'status': 'active'}
)

# Query with options
result = client.query(
    "*100[products]::*",
    options={
        'cache_duration': 300,
        'explain': True
    }
)
```

#### Async Query Execution
```python
import asyncio
from core import AsyncSAIQLClient

async def async_example():
    client = AsyncSAIQLClient('localhost', 5433)
    await client.authenticate(api_key='your-key')

    # Async query
    result = await client.query("*5[users]::name,email")

    # Parallel queries
    tasks = [
        client.query("*10[users]::name"),
        client.query("*10[orders]::amount"),
        client.query("*10[products]::price")
    ]
    results = await asyncio.gather(*tasks)

    await client.close()
```

#### Batch Queries
```python
# Execute multiple queries in batch
queries = [
    {"id": "users", "query": "*10[users]::name,email"},
    {"id": "orders", "query": "*5[orders]::id,amount"},
    {"id": "products", "query": "*5[products]::name,price"}
]

results = client.batch_query(queries, parallel=True)

for query_id, result in results.items():
    print(f"{query_id}: {len(result['data'])} rows")
```

### Advanced Features

#### Streaming Queries
```python
# Stream large result sets
def process_chunk(chunk):
    print(f"Processing {len(chunk)} rows")

client.query_stream(
    "*100000[large_table]::*",
    chunk_size=1000,
    callback=process_chunk
)
```

#### Transaction Management
```python
# Transaction context manager
with client.transaction() as tx:
    tx.execute("*[users]::*|id=1>>UPDATE(name='John Updated')")
    tx.execute("*[orders]::*>>INSERT(user_id=1, amount=99.99)")
    # Automatically commits on success, rolls back on error
```

#### Connection Context Manager
```python
# Automatic connection management
with SAIQLClient('localhost', 5433) as client:
    client.authenticate(api_key='your-key')
    result = client.query("*5[users]::name,email")
    # Connection automatically closed
```

### Configuration and Options

#### Client Configuration
```python
from core import SAIQLClient, ClientConfig

config = ClientConfig(
    timeout=30,
    retry_attempts=3,
    retry_delay=1.0,
    cache_enabled=True,
    debug_logging=False
)

client = SAIQLClient('localhost', 5433, config=config)
```

#### Query Options
```python
# Per-query options
options = {
    'cache_duration': 3600,        # Cache for 1 hour
    'timeout': 60,                 # Query timeout in seconds
    'explain': True,               # Return query execution plan
    'profile': True                # Return performance profile
}

result = client.query("*1000[large_table]::*", options=options)
```

## CLI Reference

### Installation
```bash
# Install SAIQL CE CLI
pip install saiql-ce-cli

# Or use directly from repo
python saiql.py --help
```

### Configuration

#### Set Connection Defaults
```bash
# Configure default connection
saiql config set host localhost
saiql config set port 5433
saiql config set api-key your-api-key

# View configuration
saiql config list

# Test connection
saiql ping
```

### Query Commands

#### Interactive Query Shell
```bash
# Start interactive shell
saiql shell

# In shell:
saiql> *5[users]::name,email>>oQ
saiql> \help           # Show help
saiql> \explain *[users]::*  # Explain query
saiql> \timing on      # Show execution times
saiql> \quit           # Exit shell
```

#### Command Line Queries
```bash
# Execute single query
saiql query "*10[users]::name,email"

# Query with output format
saiql query "*5[products]::name,price" --format json
saiql query "*[orders]::*" --format csv > orders.csv

# Query with parameters
saiql query "*[users]::*|status=@status" --param status=active

# Query with options
saiql query "*1000[data]::*" --timeout 60
```

#### Batch Operations
```bash
# Execute queries from file
saiql batch --file queries.saiql

# Export query results
saiql export --query "*[users]::*" --output users.json --format json
```

### Schema and Metadata Commands

#### Schema Operations
```bash
# Show all tables
saiql tables

# Describe table structure
saiql describe users

# Show table statistics
saiql stats users

# Show indexes
saiql indexes users
```

#### Symbol Reference
```bash
# Show symbol legend
saiql legend

# Search for specific symbols
saiql legend --search "|"

# Show examples for symbol
saiql legend --examples "*"
```

### Administration Commands

#### Backup and Restore
```bash
# Create backup
saiql backup create --output backup_20250814.saiql

# List backups
saiql backup list

# Restore from backup
saiql backup restore --file backup_20250814.saiql
```

#### Monitoring
```bash
# Show system health
saiql health

# Show performance statistics
saiql stats --system

# Monitor queries in real-time
saiql monitor --queries

# Show slow queries
saiql slow-queries --threshold 100ms
```

## Authentication

### JWT Authentication
```http
POST /auth/login
Content-Type: application/json

{
  "username": "user@example.com",
  "password": "secure_password"
}
```

**Response**:
```json
{
  "success": true,
  "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "expires_in": 3600,
  "refresh_token": "refresh_token_here"
}
```

### API Key Management
```http
POST /auth/api-keys
Authorization: Bearer <jwt-token>

{
  "name": "Production API Key",
  "permissions": ["query:read", "query:write"],
  "expires_at": "2025-12-31T23:59:59Z"
}
```

## Error Handling

### Error Response Format
```json
{
  "success": false,
  "error": {
    "code": "QUERY_SYNTAX_ERROR",
    "message": "Invalid SAIQL syntax: missing table specification",
    "details": {
      "line": 1,
      "column": 15,
      "suggestion": "Use *[table_name] format"
    }
  },
  "request_id": "req_1234567890"
}
```

### Common Error Codes

| Code | Description | HTTP Status |
|------|-------------|-------------|
| `AUTHENTICATION_FAILED` | Invalid credentials | 401 |
| `AUTHORIZATION_DENIED` | Insufficient permissions | 403 |
| `QUERY_SYNTAX_ERROR` | Invalid SAIQL syntax | 400 |
| `TABLE_NOT_FOUND` | Referenced table doesn't exist | 404 |
| `TIMEOUT_EXCEEDED` | Query execution timeout | 408 |
| `RATE_LIMIT_EXCEEDED` | Too many requests | 429 |

### Python Client Error Handling
```python
from core.exceptions import (
    SAIQLSyntaxError,
    SAIQLAuthenticationError,
    SAIQLTimeoutError
)

try:
    result = client.query("invalid query syntax")
except SAIQLSyntaxError as e:
    print(f"Syntax error: {e.message}")
    print(f"Suggestion: {e.suggestion}")
except SAIQLAuthenticationError as e:
    print(f"Authentication failed: {e}")
except SAIQLTimeoutError as e:
    print(f"Query timeout: {e}")
```

## Response Formats

### Standard Response
All API responses follow this structure:
```json
{
  "success": boolean,
  "data": any,
  "metadata": {
    "execution_time_ms": number,
    "cache_hit": boolean
  },
  "error": {
    "code": "string",
    "message": "string"
  }
}
```

### Query Result Formats

#### JSON (Default)
```json
{
  "success": true,
  "data": [
    {"id": 1, "name": "John", "email": "john@example.com"},
    {"id": 2, "name": "Jane", "email": "jane@example.com"}
  ]
}
```

#### CSV
```csv
id,name,email
1,John,john@example.com
2,Jane,jane@example.com
```

#### Table Format
```
+----+------+------------------+
| id | name | email            |
+----+------+------------------+
| 1  | John | john@example.com |
| 2  | Jane | jane@example.com |
+----+------+------------------+
```

## Rate Limiting

### Rate Limit Headers
```http
X-RateLimit-Limit: 1000
X-RateLimit-Remaining: 999
X-RateLimit-Reset: 1692014400
X-RateLimit-Window: 3600
```

### Rate Limit Configuration
```python
# Configure rate limits per client
client.set_rate_limit(
    requests_per_minute=1000,
    burst_limit=100,
    concurrent_queries=10
)
```

---

This API reference covers SAIQL Community Edition. For advanced features like natural language queries, semantic search, and distributed execution, see the Full Edition documentation.
