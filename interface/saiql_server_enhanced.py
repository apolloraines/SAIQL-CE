#!/usr/bin/env python3
"""
SAIQL-Delta Enhanced Server
=============================

Production-ready FastAPI server with comprehensive features:
- Vector/embedding endpoints
- Health checks and metrics
- OpenAPI documentation
- Prometheus monitoring
- Security hardening
- Performance optimization

Author: Apollo & Claude
Version: 2.0.0
"""

import os
import sys
import logging
import asyncio
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List
from pathlib import Path

# FastAPI and dependencies
from fastapi import FastAPI, HTTPException, Depends, Request, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.openapi.docs import get_swagger_ui_html, get_redoc_html
from fastapi.openapi.utils import get_openapi
import uvicorn
from pydantic import BaseModel

# Monitoring and metrics
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
import time

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Import SAIQL components
try:
    from core.saiql_core import SAIQLParser, QueryComponents
    from core.symbolic_engine import SymbolicEngine, ExecutionResult, ExecutionStatus
    from security.auth_manager import AuthManager
except ImportError as e:
    logging.error(f"Failed to import SAIQL components: {e}")
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=getattr(logging, os.getenv('SAIQL_LOG_LEVEL', 'INFO')),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Prometheus metrics
REQUEST_COUNT = Counter('saiql_requests_total', 'Total HTTP requests', ['method', 'endpoint'])
REQUEST_DURATION = Histogram('saiql_request_duration_seconds', 'HTTP request duration')
ACTIVE_CONNECTIONS = Gauge('saiql_active_connections', 'Active connections')
QUERY_COUNT = Counter('saiql_queries_total', 'Total SAIQL queries executed', ['status'])
QUERY_DURATION = Histogram('saiql_query_duration_seconds', 'SAIQL query execution time')

# Pydantic models
class QueryRequest(BaseModel):
    """SAIQL query request model"""
    query: str
    options: Optional[Dict[str, Any]] = None

class QueryResponse(BaseModel):
    """SAIQL query response model"""
    success: bool
    data: Optional[Any] = None
    error: Optional[str] = None
    execution_time: float
    query_id: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None

class HealthResponse(BaseModel):
    """Health check response model"""
    status: str
    timestamp: str
    version: str
    components: Dict[str, Any]
    uptime: float

class MetricsResponse(BaseModel):
    """Metrics response model"""
    queries_executed: int
    total_execution_time: float
    average_response_time: float
    active_connections: int
    uptime: float

# Global state
app_start_time = time.time()
saiql_parser = None
saiql_engine = None
auth_manager = None

# Security: HTTP Bearer authentication scheme
security = HTTPBearer(auto_error=True)

async def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)) -> str:
    """
    Verify bearer token for authenticated endpoints.

    Returns the user_id if authentication succeeds, raises HTTPException otherwise.
    """
    if auth_manager is None:
        raise HTTPException(status_code=503, detail="Authentication service not available")

    token = credentials.credentials

    # Validate the token using auth_manager
    try:
        auth_result = auth_manager.verify_token(token)
        if not auth_result.success:
            raise HTTPException(
                status_code=401,
                detail=auth_result.error_message or "Invalid or expired token",
                headers={"WWW-Authenticate": "Bearer"}
            )
        return auth_result.user_id or "anonymous"
    except HTTPException:
        raise
    except Exception as e:
        logger.warning(f"Authentication failed: {e}")
        raise HTTPException(
            status_code=401,
            detail="Authentication failed",
            headers={"WWW-Authenticate": "Bearer"}
        )

def create_app() -> FastAPI:
    """Create and configure FastAPI application"""
    
    # Create FastAPI app with custom OpenAPI
    app = FastAPI(
        title="SAIQL-Delta API",
        description="Semantic AI Query Language Database System",
        version="2.0.0",
        docs_url=None,  # Disable default docs for custom implementation
        redoc_url=None,
        openapi_url="/api/openapi.json"
    )
    
    # Security middleware
    app.add_middleware(
        TrustedHostMiddleware, 
        allowed_hosts=["*"]  # Configure properly for production
    )
    
    # CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],  # Configure properly for production
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    # Compression middleware
    app.add_middleware(GZipMiddleware, minimum_size=1000)
    
    # Request tracking middleware
    @app.middleware("http")
    async def track_requests(request: Request, call_next):
        start_time = time.time()
        ACTIVE_CONNECTIONS.inc()
        
        response = await call_next(request)
        
        # Record metrics
        duration = time.time() - start_time
        REQUEST_COUNT.labels(
            method=request.method,
            endpoint=request.url.path
        ).inc()
        REQUEST_DURATION.observe(duration)
        ACTIVE_CONNECTIONS.dec()
        
        return response
    
    return app

# Create application instance
app = create_app()

@app.on_event("startup")
async def startup_event():
    """Initialize application components on startup"""
    global saiql_parser, saiql_engine, auth_manager

    logger.info("Starting SAIQL-Delta Enhanced Server (CE Edition)...")

    try:
        # Initialize SAIQL components
        saiql_parser = SAIQLParser()
        saiql_engine = SymbolicEngine()

        # Initialize auth manager
        auth_manager = AuthManager()

        logger.info("SAIQL-Delta server initialized successfully (CE Edition)")

    except Exception as e:
        logger.error(f"Failed to initialize server: {e}")
        raise

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on server shutdown"""
    logger.info("Shutting down SAIQL-Delta server...")

# Health check endpoints
@app.get("/health", response_model=HealthResponse)
async def health_check():
    """
    Comprehensive health check endpoint

    Returns the health status of all system components.
    """
    try:
        uptime = time.time() - app_start_time

        # Check SAIQL components
        saiql_status = "healthy" if saiql_parser and saiql_engine else "unhealthy"

        # Overall status
        overall_status = "healthy" if saiql_status == "healthy" else "unhealthy"

        return HealthResponse(
            status=overall_status,
            timestamp=datetime.now(timezone.utc).isoformat(),
            version="2.0.0-ce",
            components={
                "saiql_parser": saiql_status,
                "saiql_engine": saiql_status,
                "auth_manager": "healthy" if auth_manager else "unhealthy"
            },
            uptime=uptime
        )

    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return HealthResponse(
            status="unhealthy",
            timestamp=datetime.now(timezone.utc).isoformat(),
            version="2.0.0-ce",
            components={"error": str(e)},
            uptime=time.time() - app_start_time
        )

@app.get("/health/ready")
async def readiness_check():
    """Kubernetes readiness probe"""
    try:
        # Quick check of essential components
        if not (saiql_parser and saiql_engine):
            raise HTTPException(status_code=503, detail="SAIQL components not ready")
        
        return {"status": "ready", "timestamp": datetime.now(timezone.utc).isoformat()}
        
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Not ready: {str(e)}")

@app.get("/health/live")
async def liveness_check():
    """Kubernetes liveness probe"""
    return {"status": "alive", "timestamp": datetime.now(timezone.utc).isoformat()}

# Metrics endpoint
@app.get("/metrics")
async def metrics_endpoint():
    """Prometheus metrics endpoint"""
    try:
        metrics_data = generate_latest()
        return Response(
            content=metrics_data,
            media_type=CONTENT_TYPE_LATEST
        )
    except Exception as e:
        logger.error(f"Metrics generation failed: {e}")
        raise HTTPException(status_code=500, detail="Metrics unavailable")

@app.get("/api/v1/metrics", response_model=MetricsResponse)
async def application_metrics():
    """Application-specific metrics"""
    try:
        uptime = time.time() - app_start_time

        # Get SAIQL engine metrics if available
        saiql_metrics = getattr(saiql_engine, 'get_metrics', lambda: {})()

        return MetricsResponse(
            queries_executed=saiql_metrics.get('queries_executed', 0),
            total_execution_time=saiql_metrics.get('total_execution_time', 0.0),
            average_response_time=saiql_metrics.get('average_response_time', 0.0),
            active_connections=0,
            uptime=uptime
        )

    except Exception as e:
        logger.error(f"Failed to get metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# SAIQL query endpoints
@app.post("/api/v1/query", response_model=QueryResponse)
async def execute_query(request: QueryRequest, user_id: str = Depends(verify_token)):
    """
    Execute a SAIQL query (requires authentication)

    Parses and executes a SAIQL query, returning results in JSON format.
    Supports various query options and performance tracking.
    """
    try:
        start_time = time.time()
        query_id = f"query_{int(start_time * 1000)}"

        logger.info(f"[user={user_id}] Executing SAIQL query: {request.query[:100]}...")
        
        # Parse query
        if not saiql_parser:
            raise HTTPException(status_code=503, detail="SAIQL parser not initialized")
        
        query_components = saiql_parser.parse(request.query)
        
        # Execute query
        if not saiql_engine:
            raise HTTPException(status_code=503, detail="SAIQL engine not initialized")
        
        result = saiql_engine.execute(query_components, request.options or {})
        
        execution_time = time.time() - start_time
        
        # Record metrics
        QUERY_COUNT.labels(status="success").inc()
        QUERY_DURATION.observe(execution_time)
        
        # Format response
        if result.status == ExecutionStatus.SUCCESS:
            return QueryResponse(
                success=True,
                data=result.data,
                execution_time=execution_time,
                query_id=query_id,
                metadata={
                    "rows_affected": result.rows_affected,
                    "query_type": query_components.query_type if hasattr(query_components, 'query_type') else None
                }
            )
        else:
            QUERY_COUNT.labels(status="error").inc()
            return QueryResponse(
                success=False,
                error=result.error_message,
                execution_time=execution_time,
                query_id=query_id
            )
            
    except Exception as e:
        execution_time = time.time() - start_time
        QUERY_COUNT.labels(status="error").inc()
        logger.error(f"Query execution failed: {e}")
        
        return QueryResponse(
            success=False,
            error=str(e),
            execution_time=execution_time,
            query_id=query_id
        )

@app.post("/api/v1/parse")
async def parse_query(request: QueryRequest, user_id: str = Depends(verify_token)):
    """
    Parse a SAIQL query without execution (requires authentication)

    Validates and parses a SAIQL query, returning the parsed structure
    without executing it. Useful for query validation and analysis.
    """
    try:
        if not saiql_parser:
            raise HTTPException(status_code=503, detail="SAIQL parser not initialized")
        
        start_time = time.time()
        query_components = saiql_parser.parse(request.query)
        execution_time = time.time() - start_time
        
        return {
            "success": True,
            "parsed_query": query_components.__dict__ if hasattr(query_components, '__dict__') else str(query_components),
            "parse_time": execution_time,
            "query_valid": True
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "query_valid": False,
            "parse_time": time.time() - start_time
        }

# Documentation endpoints
@app.get("/", response_class=HTMLResponse)
async def root():
    """API documentation root"""
    return """
    <html>
        <head>
            <title>SAIQL-Delta API (CE)</title>
            <style>
                body { font-family: Arial, sans-serif; margin: 40px; }
                .header { color: #0078d4; }
                .endpoint { margin: 20px 0; padding: 10px; border-left: 3px solid #0078d4; }
            </style>
        </head>
        <body>
            <h1 class="header">SAIQL-Delta API Server (Community Edition)</h1>
            <p>Semantic AI Query Language Database System - Version 2.0.0-CE</p>

            <h2>Available Endpoints:</h2>

            <div class="endpoint">
                <h3>Documentation</h3>
                <a href="/docs">Interactive API Documentation (Swagger UI)</a><br>
                <a href="/redoc">Alternative Documentation (ReDoc)</a>
            </div>

            <div class="endpoint">
                <h3>Health and Monitoring</h3>
                <a href="/health">Health Check</a><br>
                <a href="/metrics">Prometheus Metrics</a><br>
                <a href="/api/v1/metrics">Application Metrics</a>
            </div>

            <div class="endpoint">
                <h3>SAIQL Queries</h3>
                <strong>POST</strong> /api/v1/query - Execute SAIQL query<br>
                <strong>POST</strong> /api/v1/parse - Parse SAIQL query
            </div>

            <p><em>For detailed API documentation, visit <a href="/docs">/docs</a></em></p>
        </body>
    </html>
    """

@app.get("/docs", include_in_schema=False)
async def custom_swagger_ui_html():
    """Custom Swagger UI"""
    return get_swagger_ui_html(
        openapi_url="/api/openapi.json",
        title="SAIQL-Delta API Documentation",
        swagger_js_url="https://cdn.jsdelivr.net/npm/swagger-ui-dist@4.15.5/swagger-ui-bundle.js",
        swagger_css_url="https://cdn.jsdelivr.net/npm/swagger-ui-dist@4.15.5/swagger-ui.css",
    )

@app.get("/redoc", include_in_schema=False)
async def redoc_html():
    """ReDoc documentation"""
    return get_redoc_html(
        openapi_url="/api/openapi.json",
        title="SAIQL-Delta API Documentation",
        redoc_js_url="https://cdn.jsdelivr.net/npm/redoc@2.0.0/bundles/redoc.standalone.js",
    )

def custom_openapi():
    """Generate custom OpenAPI schema"""
    if app.openapi_schema:
        return app.openapi_schema

    openapi_schema = get_openapi(
        title="SAIQL-Delta API (CE)",
        version="2.0.0-ce",
        description="""
        ## Semantic AI Query Language Database System - Community Edition

        SAIQL-Delta CE provides database querying with a semantic query language.

        ### Key Features:
        - **Semantic Query Language**: Natural and intuitive query syntax
        - **Production Ready**: Health checks, metrics, and monitoring
        - **Secure**: Authentication and authorization support

        ### Getting Started:
        1. Check system health at `/health`
        2. Try a simple query via `POST /api/v1/query`
        """,
        routes=app.routes,
    )
    
    # Add security schemes
    openapi_schema["components"]["securitySchemes"] = {
        "BearerAuth": {
            "type": "http",
            "scheme": "bearer",
            "bearerFormat": "JWT"
        }
    }
    
    app.openapi_schema = openapi_schema
    return app.openapi_schema

app.openapi = custom_openapi

def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description="SAIQL-Delta Enhanced Server")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind to")
    parser.add_argument("--port", type=int, default=8000, help="Port to bind to")
    parser.add_argument("--workers", type=int, default=1, help="Number of worker processes")
    parser.add_argument("--reload", action="store_true", help="Enable auto-reload for development")
    parser.add_argument("--log-level", default="info", choices=["debug", "info", "warning", "error"])
    
    args = parser.parse_args()
    
    # Configuration
    config = {
        "host": args.host,
        "port": args.port,
        "log_level": args.log_level,
        "access_log": True,
        "reload": args.reload,
    }
    
    if args.workers > 1 and not args.reload:
        config["workers"] = args.workers
    
    logger.info(f"Starting SAIQL-Delta Enhanced Server on {args.host}:{args.port}")
    
    # Run server
    uvicorn.run("interface.saiql_server_enhanced:app", **config)

if __name__ == "__main__":
    main()

