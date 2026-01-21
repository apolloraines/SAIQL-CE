# Server Implementation Migration Notice

## DEPRECATED: interface/saiql_server.py (Flask)
The Flask-based server implementation has been deprecated in favor of the FastAPI implementation.

**Deprecated File**: `interface/saiql_server.py.deprecated`
**Replacement**: `saiql_production_server.py` (FastAPI-based)

## Migration Benefits:
- **Async Performance**: FastAPI provides better async request handling
- **Auto-Documentation**: Built-in OpenAPI/Swagger documentation
- **Type Validation**: Automatic request/response validation
- **Production Ready**: Better production performance and features

## Usage:
```bash
# Use the main FastAPI server
python saiql_production_server.py

# Or via entry point
saiql-server
```

The Flask implementation will be removed in a future version.