#!/usr/bin/env python3
"""
SAIQL Production Server - Phase 5
=================================

Production-ready SAIQL server optimized for small-scale deployments like Nova.
Built for reliability, performance, and easy operation.

Author: Apollo & Claude  
Version: 5.0.0
"""

import json
import asyncio
import signal
import threading
import time
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime
import uvicorn
from fastapi import FastAPI, HTTPException, Depends, Security
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import logging

# Import SAIQL components
from core.operators import SAIQLOperators
from core.transaction_manager import TransactionManager, IsolationLevel
from core.execution_planner import QueryOptimizer, create_sample_statistics
from core.monitor import AdvancedPerformanceMonitor
from core.logging import logger, LogCategory
from security.auth_manager import AuthManager, AuthResult, UserRole

bearer_scheme = HTTPBearer(auto_error=False)

class ProductionSAIQLServer:
    """Production-ready SAIQL server"""
    
    def __init__(self, config_path: str = "config/production.json"):
        self.config_path = config_path
        self.config = self._load_config()
        self.app = FastAPI(
            title="SAIQL Database Engine",
            description="Semantic AI Query Language - Production Server",
            version="5.0.0",
            docs_url="/docs" if self.config["saiql"].get("debug", False) else None,
            redoc_url="/redoc" if self.config["saiql"].get("debug", False) else None
        )
        
        # Initialize secure config and inject auth secrets into environment
        self._ensure_auth_environment()

        # Initialize SAIQL components
        self.runtime = SAIQLOperators()
        self.transaction_manager = TransactionManager()
        self.query_optimizer = QueryOptimizer()
        self.performance_monitor = AdvancedPerformanceMonitor()
        self.auth_manager = AuthManager()
        self.auth_required = self.config.get("security", {}).get("enable_authentication", True)

        if not self.auth_required:
            logger.warn(
                "Authentication is disabled in production configuration. This should only be used "
                "for controlled testing environments.",
                category=LogCategory.SECURITY
            )
        
        # Server state
        self.shutdown_event = asyncio.Event()
        self.is_running = False
        
        # Setup server
        self._setup_logging()
        self._setup_monitoring()
        self._setup_middleware()
        self._setup_routes()
        self._setup_optimizer()
        
        logger.info("SAIQL Production Server initialized", 
                   category=LogCategory.SYSTEM, version="5.0.0")
    
    def _load_config(self) -> Dict[str, Any]:
        """Load production configuration with env var overrides"""
        import os
        
        config = {}
        try:
            if Path(self.config_path).exists():
                with open(self.config_path, 'r') as f:
                    config = json.load(f)
        except Exception as e:
            logger.warn(f"Failed to load config file: {e}", category=LogCategory.SYSTEM)

        # Default structure if missing
        if "saiql" not in config: config["saiql"] = {}
        if "server" not in config: config["server"] = {}
        if "security" not in config: config["security"] = {}
        if "logging" not in config: config["logging"] = {}

        # Env var overrides (Security Best Practices)
        config["server"]["host"] = os.getenv("SAIQL_HOST", config["server"].get("host", "0.0.0.0"))
        config["server"]["port"] = int(os.getenv("SAIQL_PORT", config["server"].get("port", 5433)))
        
        # Security: Enforce TLS in production unless explicitly disabled via env
        # Default to True for security
        tls_env = os.getenv("SAIQL_TLS_ENABLED", "true").lower()
        config["security"]["tls_enabled"] = tls_env == "true"
        config["security"]["tls_cert"] = os.getenv("SAIQL_TLS_CERT", config["security"].get("tls_cert", "/etc/saiql/server.crt"))
        config["security"]["tls_key"] = os.getenv("SAIQL_TLS_KEY", config["security"].get("tls_key", "/etc/saiql/server.key"))
        
        # Secrets
        secret_key = os.getenv("SAIQL_SECRET_KEY")
        if secret_key:
            config["security"]["secret_key"] = secret_key
        elif config.get("security", {}).get("enable_authentication", True) and not config["security"].get("secret_key"):
             logger.warn("No secret key found in env or config! Generating temporary key.", category=LogCategory.SECURITY)
             import secrets
             config["security"]["secret_key"] = secrets.token_hex(32)

        return config

    def _ensure_auth_environment(self):
        """Ensure JWT secret is available in environment for AuthManager"""
        import os

        # If JWT secret already in environment, we're good
        if os.getenv('SAIQL_JWT_SECRET'):
            return

        # Otherwise, inject from secure config or use the one from _load_config
        try:
            from config.secure_config import get_current_config
            secure_config = get_current_config()
            if secure_config.jwt_secret_key:
                os.environ['SAIQL_JWT_SECRET'] = secure_config.jwt_secret_key
                logger.info("Injected JWT secret from secure config", category=LogCategory.SECURITY)
                return
        except Exception as e:
            logger.warn(f"Could not load secure config: {e}", category=LogCategory.SECURITY)

        # Fallback: use the secret from production config
        secret_key = self.config.get("security", {}).get("secret_key")
        if secret_key:
            os.environ['SAIQL_JWT_SECRET'] = secret_key
            logger.info("Injected JWT secret from production config", category=LogCategory.SECURITY)
        else:
            logger.error("No JWT secret available for AuthManager!", category=LogCategory.SECURITY)

    def _setup_logging(self):
        """Configure production logging"""
        log_config = self.config.get("logging", {})
        
        # Configure uvicorn logging
        logging.getLogger("uvicorn").setLevel(log_config.get("level", "INFO"))
        logging.getLogger("uvicorn.access").setLevel("WARNING")  # Reduce noise
        
        logger.info("Logging configured for production", 
                   category=LogCategory.SYSTEM, configured_level=log_config.get("level"))
    
    def _setup_monitoring(self):
        """Setup performance monitoring"""
        if self.config.get("monitoring", {}).get("enable_performance_profiling", True):
            self.performance_monitor.start_monitoring(interval=
                self.config.get("performance", {}).get("metrics_collection_interval", 10)
            )
            logger.info("Performance monitoring enabled", category=LogCategory.PERFORMANCE)

    def require_authentication(
        self,
        credentials: HTTPAuthorizationCredentials = Security(bearer_scheme)
    ) -> AuthResult:
        """FastAPI dependency enforcing authentication"""
        if not self.auth_required:
            return AuthResult(success=True, metadata={"auth_disabled": True})

        if credentials is None or not credentials.credentials:
            raise HTTPException(status_code=401, detail="Missing bearer token")

        auth_result = self.auth_manager.verify_token(credentials.credentials)
        if not auth_result.success:
            raise HTTPException(status_code=401, detail=auth_result.error_message or "Unauthorized")

        return auth_result
    
    def _setup_middleware(self):
        """Setup FastAPI middleware"""
        # CORS
        cors_origins = self.config.get("security", {}).get("cors_origins", [])
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=cors_origins,
            allow_credentials=True,
            allow_methods=["GET", "POST", "PUT", "DELETE"],
            allow_headers=["*"],
        )
        
        # Custom middleware for request logging
        @self.app.middleware("http")
        async def log_requests(request, call_next):
            start_time = time.time()
            
            with logger.context(
                method=request.method,
                url=str(request.url),
                client_ip=request.client.host if request.client else "unknown"
            ):
                logger.info("Request received", category=LogCategory.API)
                
                response = await call_next(request)
                
                duration = time.time() - start_time
                logger.info("Request completed", category=LogCategory.API,
                           status_code=response.status_code, duration_ms=duration * 1000)
                
                return response
    
    def _setup_routes(self):
        """Setup API routes"""
        
        # Health check endpoint
        @self.app.get("/health")
        async def health_check():
            """Health check endpoint for load balancers"""
            try:
                # Quick health check
                result = self.runtime.execute_operator("*", 1, "health_check")
                
                return {
                    "status": "healthy",
                    "version": "5.0.0",
                    "timestamp": datetime.now().isoformat(),
                    "uptime": time.time() - self.performance_monitor.system_monitor.start_time.timestamp(),
                    "performance": self.performance_monitor.get_performance_summary()
                }
            except Exception as e:
                logger.error("Health check failed", category=LogCategory.ERROR, exception=e)
                raise HTTPException(status_code=503, detail="Service unhealthy")
        
        # Metrics endpoint for monitoring
        @self.app.get("/metrics")
        async def get_metrics(auth_result: AuthResult = Depends(self.require_authentication)):
            """Prometheus-compatible metrics endpoint"""
            try:
                if self.auth_required and (
                    not auth_result.user or not auth_result.user.has_permission("read")
                ):
                    raise HTTPException(status_code=403, detail="Insufficient permissions")
                return self.performance_monitor.export_metrics("prometheus")
            except Exception as e:
                logger.error("Metrics export failed", category=LogCategory.ERROR, exception=e)
                raise HTTPException(status_code=500, detail="Metrics unavailable")
        
        # Authentication lifecycle endpoints
        @self.app.post("/auth/token")
        async def login_for_access_token(
            credentials: dict
        ):
            """Login to get access token"""
            username = credentials.get("username")
            password = credentials.get("password")
            
            if not username or not password:
                raise HTTPException(status_code=400, detail="Username and password required")
                
            user = self.auth_manager.authenticate_user(username, password)
            if not user:
                raise HTTPException(status_code=401, detail="Invalid credentials")
                
            token = self.auth_manager.create_token(user.user_id)
            
            return {
                "access_token": token,
                "token_type": "bearer",
                "expires_in": self.config.get("jwt", {}).get("expiry_hours", 24) * 3600
            }

        @self.app.post("/auth/refresh")
        async def refresh_token(
            credentials: HTTPAuthorizationCredentials = Security(bearer_scheme)
        ):
            """Refresh an access token"""
            if credentials is None or not credentials.credentials:
                raise HTTPException(status_code=401, detail="Missing bearer token")

            result = self.auth_manager.refresh_token(credentials.credentials)
            if not result.success:
                raise HTTPException(status_code=401, detail=result.error_message or "Token refresh failed")

            return {
                "status": "refreshed",
                "token": result.metadata.get("token"),
                "expires_at": result.token_expires_at.isoformat() if result.token_expires_at else None
            }
        
        @self.app.post("/auth/api-keys/{key_id}/rotate")
        async def rotate_api_key(
            key_id: str,
            payload: Dict[str, Any],
            auth_result: AuthResult = Depends(self.require_authentication)
        ):
            """Rotate an API key (admin only)"""
            if self.auth_required and (
                not auth_result.user or UserRole.ADMIN not in auth_result.roles
            ):
                raise HTTPException(status_code=403, detail="Admin privileges required for key rotation")

            expires_days = payload.get("expires_days")
            try:
                new_key, secret = self.auth_manager.rotate_api_key(key_id, expires_days=expires_days)
            except ValueError as exc:
                raise HTTPException(status_code=404, detail=str(exc))

            return {
                "status": "rotated",
                "new_key_id": new_key.key_id,
                "secret": secret,
                "expires_at": new_key.expires_at.isoformat() if new_key.expires_at else None,
                "roles": [role.value for role in new_key.roles]
            }
        
        # Query execution endpoint
        @self.app.post("/query")
        async def execute_query(
            query_request: dict,
            auth_result: AuthResult = Depends(self.require_authentication)
        ):
            """Execute SAIQL query"""
            try:
                if self.auth_required and (
                    not auth_result.user or not auth_result.user.has_permission("execute")
                ):
                    raise HTTPException(status_code=403, detail="Insufficient permissions")

                query_id = f"query_{int(time.time() * 1000)}"
                
                with logger.context(query_id=query_id):
                    logger.info("Query execution started", category=LogCategory.QUERY,
                               query_type=query_request.get("operation", "unknown"))
                    
                    with self.performance_monitor.profile_query(query_id, str(query_request)) as profile:
                        # Execute query using runtime in thread pool to avoid blocking event loop
                        operation = query_request.get("operation", "*")
                        params = query_request.get("parameters", [])
                        
                        # Run synchronous runtime method in executor
                        loop = asyncio.get_running_loop()
                        result = await loop.run_in_executor(
                            None,  # Use default executor
                            self.runtime.execute_operator,
                            operation,
                            *params
                        )
                        
                        # Update profile
                        profile.estimated_cost = 10.0
                        profile.actual_rows = 1
                        profile.cache_hits = 1
                        
                        logger.info("Query executed successfully", category=LogCategory.QUERY,
                                   execution_time=profile.execution_time)
                        
                        return {
                            "query_id": query_id,
                            "status": "success",
                            "result": result,
                            "execution_time": profile.execution_time,
                            "timestamp": datetime.now().isoformat()
                        }
                        
            except Exception as e:
                logger.error("Query execution failed", category=LogCategory.QUERY, exception=e)
                raise HTTPException(status_code=400, detail=f"Query failed: {str(e)}")
        
        # Transaction management endpoints
        @self.app.post("/transaction/begin")
        async def begin_transaction(
            isolation_level: str = "READ_COMMITTED",
            auth_result: AuthResult = Depends(self.require_authentication)
        ):
            """Begin a new transaction"""
            try:
                if self.auth_required and (
                    not auth_result.user or not auth_result.user.has_permission("write")
                ):
                    raise HTTPException(status_code=403, detail="Insufficient permissions")

                isolation = IsolationLevel[isolation_level]
                tx_id = self.transaction_manager.begin_transaction(isolation)
                
                logger.info("Transaction started", category=LogCategory.TRANSACTION,
                           transaction_id=tx_id, isolation_level=isolation_level)
                
                return {"transaction_id": tx_id, "isolation_level": isolation_level}
                
            except Exception as e:
                logger.error("Transaction begin failed", category=LogCategory.TRANSACTION, exception=e)
                raise HTTPException(status_code=400, detail=f"Transaction failed: {str(e)}")
        
        @self.app.post("/transaction/{tx_id}/commit")
        async def commit_transaction(
            tx_id: str,
            auth_result: AuthResult = Depends(self.require_authentication)
        ):
            """Commit a transaction"""
            try:
                if self.auth_required and (
                    not auth_result.user or not auth_result.user.has_permission("write")
                ):
                    raise HTTPException(status_code=403, detail="Insufficient permissions")

                success = self.transaction_manager.commit_transaction(tx_id)
                
                if success:
                    logger.info("Transaction committed", category=LogCategory.TRANSACTION,
                               transaction_id=tx_id)
                    return {"status": "committed", "transaction_id": tx_id}
                else:
                    logger.warn("Transaction commit failed", category=LogCategory.TRANSACTION,
                               transaction_id=tx_id)
                    raise HTTPException(status_code=400, detail="Transaction commit failed")
                
            except Exception as e:
                logger.error("Transaction commit error", category=LogCategory.TRANSACTION, exception=e)
                raise HTTPException(status_code=400, detail=f"Commit failed: {str(e)}")
        
        # Performance and monitoring endpoints
        @self.app.get("/performance")
        async def get_performance(auth_result: AuthResult = Depends(self.require_authentication)):
            """Get performance statistics"""
            try:
                if self.auth_required and (
                    not auth_result.user or not auth_result.user.has_permission("read")
                ):
                    raise HTTPException(status_code=403, detail="Insufficient permissions")
                return self.performance_monitor.get_performance_summary()
            except Exception as e:
                logger.error("Performance data retrieval failed", category=LogCategory.ERROR, exception=e)
                raise HTTPException(status_code=500, detail="Performance data unavailable")
        
        @self.app.get("/status")
        async def get_server_status(auth_result: AuthResult = Depends(self.require_authentication)):
            """Get detailed server status"""
            try:
                if self.auth_required and (
                    not auth_result.user or not auth_result.user.has_permission("read")
                ):
                    raise HTTPException(status_code=403, detail="Insufficient permissions")

                tx_stats = self.transaction_manager.get_transaction_stats()
                perf_summary = self.performance_monitor.get_performance_summary()
                
                return {
                    "server": {
                        "version": "5.0.0",
                        "status": "running",
                        "uptime": time.time() - self.performance_monitor.system_monitor.start_time.timestamp()
                    },
                    "transactions": tx_stats,
                    "performance": perf_summary,
                    "timestamp": datetime.now().isoformat()
                }
                
            except Exception as e:
                logger.error("Status retrieval failed", category=LogCategory.ERROR, exception=e)
                raise HTTPException(status_code=500, detail="Status unavailable")
    
    def _setup_optimizer(self):
        """Setup query optimizer with sample statistics"""
        try:
            stats = create_sample_statistics()
            self.query_optimizer.load_statistics(stats)
            logger.info("Query optimizer configured", category=LogCategory.SYSTEM)
        except Exception as e:
            logger.error("Query optimizer setup failed", category=LogCategory.ERROR, exception=e)
    
    def _setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown"""
        def signal_handler(signum, frame):
            logger.info("Shutdown signal received", category=LogCategory.SYSTEM, signal=signum)
            asyncio.create_task(self.shutdown())
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    
    async def shutdown(self):
        """Graceful shutdown"""
        logger.info("SAIQL server shutting down...", category=LogCategory.SYSTEM)
        
        self.is_running = False
        self.shutdown_event.set()
        
        # Stop monitoring
        self.performance_monitor.stop_monitoring()
        
        # Clean up resources
        logger.info("SAIQL server shutdown complete", category=LogCategory.SYSTEM)
    
    def start(self):
        """Start the production server"""
        logger.info("Starting SAIQL Production Server", category=LogCategory.SYSTEM)

        server_config = self.config.get("server", {})
        security_config = self.config.get("security", {})

        self.is_running = True
        self._setup_signal_handlers()

        # Build uvicorn configuration
        uvicorn_config = {
            "host": server_config.get("host", "0.0.0.0"),
            "port": server_config.get("port", 5432),
            "workers": 1,  # Single worker for simplicity
            "log_level": self.config.get("logging", {}).get("level", "info").lower(),
            "access_log": False,  # We handle access logging via middleware
            "reload": False,  # Never reload in production
        }

        # Apply TLS configuration if enabled
        if security_config.get("tls_enabled", False):
            tls_cert = security_config.get("tls_cert")
            tls_key = security_config.get("tls_key")

            if not tls_cert or not tls_key:
                logger.error(
                    "TLS is enabled but tls_cert or tls_key not configured",
                    category=LogCategory.SECURITY
                )
                raise ValueError("TLS enabled but certificate/key paths not configured")

            cert_path = Path(tls_cert)
            key_path = Path(tls_key)

            if not cert_path.exists():
                logger.error(
                    f"TLS certificate file not found: {tls_cert}",
                    category=LogCategory.SECURITY
                )
                raise FileNotFoundError(f"TLS certificate not found: {tls_cert}")

            if not key_path.exists():
                logger.error(
                    f"TLS key file not found: {tls_key}",
                    category=LogCategory.SECURITY
                )
                raise FileNotFoundError(f"TLS key not found: {tls_key}")

            uvicorn_config["ssl_certfile"] = str(cert_path)
            uvicorn_config["ssl_keyfile"] = str(key_path)

            logger.info(
                "TLS enabled for production server",
                category=LogCategory.SECURITY,
                cert_path=tls_cert
            )
        else:
            logger.warn(
                "TLS is DISABLED - connections will be unencrypted. "
                "This is NOT recommended for production!",
                category=LogCategory.SECURITY
            )

        # Start uvicorn server
        uvicorn.run(self.app, **uvicorn_config)

def main():
    """Main entry point for production server"""
    import sys
    
    config_path = sys.argv[1] if len(sys.argv) > 1 else "config/production.json"
    
    try:
        server = ProductionSAIQLServer(config_path)
        server.start()
    except KeyboardInterrupt:
        print("\n🛑 SAIQL server stopped by user")
    except Exception as e:
        print(f"❌ SAIQL server failed to start: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
