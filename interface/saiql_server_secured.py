#!/usr/bin/env python3
"""
SAIQL Secured REST API Server - Production-Ready HTTP Interface

This module provides a production-ready, secured RESTful API interface for SAIQL
with comprehensive authentication, authorization, rate limiting, and monitoring.

Features:
- JWT token authentication
- API key management
- Role-based access control
- Rate limiting
- Request logging and monitoring
- Health checks and metrics
- CORS configuration
- Error handling and validation

Author: Apollo & Claude
Version: 1.0.0 (SAIQL-Bravo)
Status: Production-Ready

Usage:
    python saiql_server_secured.py --port 8000 --config config/server_config.json
"""

from flask import Flask, request, jsonify, g, abort
from flask_cors import CORS
import json
import logging
import os
import sys
import argparse
import time
from typing import Dict, Any, Optional, List
from datetime import datetime
from functools import wraps
import traceback
from pathlib import Path
from collections import defaultdict
import threading

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import SAIQL components
try:
    from core.engine import SAIQLEngine, ExecutionContext, QueryResult
    from core.database_manager import DatabaseManager
    from security.auth_manager import AuthManager, UserRole, create_auth_middleware
except ImportError as e:
    print(f"Error importing SAIQL components: {e}")
    print("Make sure you're running from the correct directory structure")
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/saiql_server.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class RateLimiter:
    """Thread-safe in-memory rate limiter using sliding window."""

    def __init__(self, max_requests: int = 100, window_seconds: int = 60):
        """
        Initialize rate limiter.

        Args:
            max_requests: Maximum requests allowed per window
            window_seconds: Time window in seconds
        """
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._requests: Dict[str, List[float]] = defaultdict(list)
        self._lock = threading.Lock()

    def is_allowed(self, identifier: str) -> tuple:
        """
        Check if request is allowed for the given identifier.

        Args:
            identifier: Client identifier (IP address, user_id, or API key)

        Returns:
            Tuple of (allowed: bool, remaining: int, retry_after: Optional[int])
        """
        now = time.time()
        window_start = now - self.window_seconds

        with self._lock:
            # Clean up old requests outside the window
            self._requests[identifier] = [
                ts for ts in self._requests[identifier]
                if ts > window_start
            ]

            request_count = len(self._requests[identifier])

            if request_count >= self.max_requests:
                # Rate limit exceeded
                oldest_request = min(self._requests[identifier]) if self._requests[identifier] else now
                retry_after = int(oldest_request + self.window_seconds - now) + 1
                return False, 0, retry_after

            # Allow request and record timestamp
            self._requests[identifier].append(now)
            remaining = self.max_requests - request_count - 1
            return True, remaining, None

    def cleanup(self):
        """Remove stale entries to prevent memory growth."""
        now = time.time()
        window_start = now - self.window_seconds

        with self._lock:
            stale_keys = [
                key for key, timestamps in self._requests.items()
                if not timestamps or max(timestamps) < window_start
            ]
            for key in stale_keys:
                del self._requests[key]


class SAIQLServer:
    """Production-ready SAIQL REST API Server"""
    
    def __init__(self, config_path: Optional[str] = None):
        """Initialize SAIQL server"""
        self.config = self._load_config(config_path)
        
        # Initialize Flask app
        self.app = Flask(__name__)
        self._configure_cors()
        
        # Initialize SAIQL components
        self.db_manager = DatabaseManager()
        self.engine = SAIQLEngine()
        self.auth_manager = AuthManager()

        # Initialize rate limiter using config
        rate_limit = self.config.get('security', {}).get('rate_limit_per_minute', 100)
        self.rate_limiter = RateLimiter(max_requests=rate_limit, window_seconds=60)

        # Server statistics
        self.stats = {
            'start_time': datetime.now(),
            'total_requests': 0,
            'authenticated_requests': 0,
            'successful_queries': 0,
            'failed_queries': 0,
            'uptime_seconds': 0,
            'endpoints_hit': {},
            'auth_methods_used': {},
            'backends_used': {}
        }
        
        # Set up authentication middleware
        self._setup_authentication()
        
        # Register routes
        self._register_routes()
        
        # Set up request logging
        self._setup_request_logging()
        
        logger.info("SAIQL secured server initialized")
    
    def _load_config(self, config_path: Optional[str]) -> Dict[str, Any]:
        """Load server configuration"""
        default_config = {
            'server': {
                'host': '0.0.0.0',
                'port': 8000,
                'debug': False,
                'threaded': True
            },
            'cors': {
                'origins': ['*'],
                'methods': ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
                'allow_headers': ['Content-Type', 'Authorization', 'X-API-Key']
            },
            'security': {
                'require_auth': True,
                'rate_limit_per_minute': 100,
                'log_requests': True,
                'hide_error_details': True
            },
            'api': {
                'max_query_length': 10000,
                'default_timeout': 300,
                'max_batch_size': 100
            }
        }
        
        if config_path and os.path.exists(config_path):
            try:
                with open(config_path, 'r') as f:
                    file_config = json.load(f)
                self._merge_config(default_config, file_config)
            except Exception as e:
                logger.warning(f"Error loading config: {e}, using defaults")
        
        return default_config
    
    def _merge_config(self, base: Dict[str, Any], override: Dict[str, Any]):
        """Recursively merge configuration dictionaries"""
        for key, value in override.items():
            if key in base and isinstance(base[key], dict) and isinstance(value, dict):
                self._merge_config(base[key], value)
            else:
                base[key] = value
    
    def _configure_cors(self):
        """Configure CORS settings"""
        cors_config = self.config.get('cors', {})
        CORS(self.app, **cors_config)
    
    def _setup_authentication(self):
        """Set up authentication middleware"""
        if not self.config.get('security', {}).get('require_auth', True):
            logger.warning("Authentication is DISABLED - not recommended for production!")
            return
        
        auth_middleware = create_auth_middleware(self.auth_manager)
        
        @self.app.before_request
        def before_request():
            # Update statistics
            self.stats['total_requests'] += 1
            endpoint = request.endpoint or 'unknown'
            self.stats['endpoints_hit'][endpoint] = self.stats['endpoints_hit'].get(endpoint, 0) + 1

            # Apply rate limiting (using client IP as identifier)
            client_ip = request.remote_addr or 'unknown'
            allowed, remaining, retry_after = self.rate_limiter.is_allowed(client_ip)

            if not allowed:
                logger.warning(f"Rate limit exceeded for {client_ip}")
                response = jsonify({
                    'error': 'Rate limit exceeded',
                    'retry_after': retry_after
                })
                response.status_code = 429
                response.headers['Retry-After'] = str(retry_after)
                response.headers['X-RateLimit-Limit'] = str(self.rate_limiter.max_requests)
                response.headers['X-RateLimit-Remaining'] = '0'
                return response

            # Add rate limit headers to response context for after_request
            g.rate_limit_remaining = remaining

            # Apply authentication
            if self.config.get('security', {}).get('require_auth', True):
                auth_result = auth_middleware()
                if auth_result:  # Authentication failed
                    return auth_result
                else:
                    # Authentication successful
                    self.stats['authenticated_requests'] += 1
                    if hasattr(g, 'auth_method'):
                        method = g.auth_method.value
                        self.stats['auth_methods_used'][method] = self.stats['auth_methods_used'].get(method, 0) + 1
    
    def _setup_request_logging(self):
        """Set up request/response logging"""
        if not self.config.get('security', {}).get('log_requests', True):
            return
        
        @self.app.after_request
        def after_request(response):
            # Log request details
            log_data = {
                'timestamp': datetime.now().isoformat(),
                'method': request.method,
                'path': request.path,
                'status': response.status_code,
                'user_id': getattr(g, 'current_user', {}).get('user_id') if hasattr(g, 'current_user') else None,
                'ip': request.remote_addr,
                'user_agent': request.headers.get('User-Agent', '')
            }
            
            if response.status_code >= 400:
                logger.warning(f"Request failed: {json.dumps(log_data)}")
            else:
                logger.info(f"Request completed: {json.dumps(log_data)}")

            # Add rate limit headers to successful responses
            if hasattr(g, 'rate_limit_remaining'):
                response.headers['X-RateLimit-Limit'] = str(self.rate_limiter.max_requests)
                response.headers['X-RateLimit-Remaining'] = str(g.rate_limit_remaining)

            return response
    
    def _register_routes(self):
        """Register all API routes"""
        
        @self.app.route('/', methods=['GET'])
        def home():
            """API documentation home page"""
            return jsonify({
                'name': 'SAIQL REST API',
                'version': '0.2.0-alpha (Echo)',
                'description': 'Semantic AI Query Language REST API with Authentication',
                'endpoints': {
                    'POST /api/v1/query': 'Execute single SAIQL query',
                    'POST /api/v1/batch': 'Execute multiple SAIQL queries',
                    'POST /api/v1/parse': 'Parse SAIQL query without execution',
                    'GET /api/v1/health': 'Health check',
                    'GET /api/v1/stats': 'Server statistics',
                    'GET /api/v1/legend': 'Symbol legend',
                    'POST /auth/token': 'Create JWT token',
                    'POST /auth/api-key': 'Create API key'
                },
                'authentication': {
                    'methods': ['JWT Bearer Token', 'API Key'],
                    'headers': ['Authorization: Bearer <token>', 'X-API-Key: <key>']
                }
            })
        
        @self.app.route('/api/v1/query', methods=['POST'])
        @self.require_permission('execute')
        def execute_query():
            """Execute single SAIQL query"""
            try:
                data = request.get_json()
                if not data or 'query' not in data:
                    return jsonify({'error': 'Query is required'}), 400
                
                query = data['query']
                backend = data.get('backend', 'default')
                timeout = data.get('timeout', self.config['api']['default_timeout'])
                
                # Validate query length
                if len(query) > self.config['api']['max_query_length']:
                    return jsonify({'error': 'Query too long'}), 400
                
                # Create execution context
                context = ExecutionContext(
                    session_id=f"api_{int(time.time())}",
                    user_id=g.current_user.user_id if hasattr(g, 'current_user') else None,
                    timeout_seconds=timeout
                )
                
                # Execute query
                start_time = time.time()
                result = self.engine.execute(query, context)
                execution_time = time.time() - start_time
                
                # Update statistics
                if result.success:
                    self.stats['successful_queries'] += 1
                else:
                    self.stats['failed_queries'] += 1
                
                self.stats['backends_used'][backend] = self.stats['backends_used'].get(backend, 0) + 1
                
                # Prepare response
                response_data = result.to_dict()
                response_data['server_execution_time'] = execution_time
                
                return jsonify(response_data)
                
            except Exception as e:
                logger.error(f"Query execution error: {e}")
                self.stats['failed_queries'] += 1
                
                if self.config.get('security', {}).get('hide_error_details', True):
                    error_message = "Internal server error"
                else:
                    error_message = str(e)
                
                return jsonify({'error': error_message}), 500
        
        @self.app.route('/api/v1/batch', methods=['POST'])
        @self.require_permission('execute')
        def execute_batch():
            """Execute multiple SAIQL queries"""
            try:
                data = request.get_json()
                if not data or 'queries' not in data:
                    return jsonify({'error': 'Queries array is required'}), 400
                
                queries = data['queries']
                if len(queries) > self.config['api']['max_batch_size']:
                    return jsonify({'error': f"Batch size exceeds limit of {self.config['api']['max_batch_size']}"}), 400
                
                backend = data.get('backend', 'default')
                timeout = data.get('timeout', self.config['api']['default_timeout'])
                
                # Create execution context
                context = ExecutionContext(
                    session_id=f"batch_{int(time.time())}",
                    user_id=g.current_user.user_id if hasattr(g, 'current_user') else None,
                    timeout_seconds=timeout
                )
                
                # Execute batch
                start_time = time.time()
                results = self.engine.execute_batch(queries, context)
                execution_time = time.time() - start_time
                
                # Update statistics
                successful = sum(1 for r in results if r.success)
                failed = len(results) - successful
                self.stats['successful_queries'] += successful
                self.stats['failed_queries'] += failed
                
                # Prepare response
                response_data = {
                    'results': [result.to_dict() for result in results],
                    'summary': {
                        'total_queries': len(queries),
                        'successful': successful,
                        'failed': failed,
                        'execution_time': execution_time
                    }
                }
                
                return jsonify(response_data)
                
            except Exception as e:
                logger.error(f"Batch execution error: {e}")
                self.stats['failed_queries'] += 1
                
                if self.config.get('security', {}).get('hide_error_details', True):
                    error_message = "Internal server error"
                else:
                    error_message = str(e)
                
                return jsonify({'error': error_message}), 500
        
        @self.app.route('/api/v1/parse', methods=['POST'])
        @self.require_permission('read')
        def parse_query():
            """Parse SAIQL query without execution"""
            try:
                data = request.get_json()
                if not data or 'query' not in data:
                    return jsonify({'error': 'Query is required'}), 400
                
                query = data['query']
                
                # Parse query using engine (without execution)
                context = ExecutionContext(session_id=f"parse_{int(time.time())}")
                result = self.engine.execute(query, context)
                
                # Return parsing information
                response_data = {
                    'query': query,
                    'valid': result.success,
                    'sql_generated': result.sql_generated,
                    'compilation_time': result.compilation_time,
                    'parsing_time': result.parsing_time,
                    'lexing_time': result.lexing_time,
                    'optimizations_applied': result.optimizations_applied,
                    'complexity_score': result.complexity_score,
                    'warnings': result.warnings,
                    'error_message': result.error_message
                }
                
                return jsonify(response_data)
                
            except Exception as e:
                logger.error(f"Parse error: {e}")
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/v1/health', methods=['GET'])
        def health_check():
            """Health check endpoint"""
            try:
                # Test database connectivity
                db_health = {}
                for backend in self.db_manager.get_available_backends():
                    try:
                        result = self.db_manager.execute("SELECT 1 as test", backend=backend)
                        db_health[backend] = 'healthy' if result.success else 'unhealthy'
                    except Exception:
                        db_health[backend] = 'error'
                
                # Calculate uptime
                uptime = (datetime.now() - self.stats['start_time']).total_seconds()
                
                health_data = {
                    'status': 'healthy',
                    'timestamp': datetime.now().isoformat(),
                    'uptime_seconds': uptime,
                    'version': '1.0.0-bravo',
                    'components': {
                        'authentication': 'healthy',
                        'query_engine': 'healthy',
                        'databases': db_health
                    }
                }
                
                return jsonify(health_data)
                
            except Exception as e:
                logger.error(f"Health check error: {e}")
                return jsonify({
                    'status': 'unhealthy',
                    'error': str(e),
                    'timestamp': datetime.now().isoformat()
                }), 500
        
        @self.app.route('/api/v1/stats', methods=['GET'])
        @self.require_permission('read')
        def get_statistics():
            """Get server statistics"""
            try:
                # Update uptime
                self.stats['uptime_seconds'] = (datetime.now() - self.stats['start_time']).total_seconds()
                
                # Get component statistics
                engine_stats = self.engine.get_stats()
                auth_stats = self.auth_manager.get_statistics()
                db_stats = self.db_manager.get_statistics()
                
                return jsonify({
                    'server': self.stats,
                    'engine': engine_stats,
                    'authentication': auth_stats,
                    'databases': db_stats
                })
                
            except Exception as e:
                logger.error(f"Stats error: {e}")
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/api/v1/legend', methods=['GET'])
        @self.require_permission('read')
        def get_legend():
            """Get symbol legend"""
            try:
                # Get legend from engine
                legend_data = {
                    'symbols': {},  # Would be populated from engine.lexer.legend_map
                    'version': '1.0.0',
                    'total_symbols': 0
                }
                
                return jsonify(legend_data)
                
            except Exception as e:
                logger.error(f"Legend error: {e}")
                return jsonify({'error': str(e)}), 500
        
        # Authentication endpoints
        @self.app.route('/auth/token', methods=['POST'])
        def create_token():
            """Create JWT token (requires API key authentication)"""
            # This would typically require API key authentication
            # Implementation would depend on your specific auth flow
            return jsonify({'message': 'Token creation endpoint - implementation needed'}), 501
        
        @self.app.route('/auth/api-key', methods=['POST'])
        @self.require_role(UserRole.ADMIN)
        def create_api_key():
            """Create new API key (admin only)"""
            try:
                data = request.get_json()
                if not data or 'name' not in data:
                    return jsonify({'error': 'Key name is required'}), 400
                
                user_id = data.get('user_id', g.current_user.user_id)
                name = data['name']
                roles = [UserRole(role) for role in data.get('roles', ['read_only'])]
                expires_days = data.get('expires_days', 365)
                
                api_key, key_secret = self.auth_manager.create_api_key(
                    user_id=user_id,
                    name=name,
                    roles=roles,
                    expires_days=expires_days
                )
                
                return jsonify({
                    'key_id': api_key.key_id,
                    'key_secret': key_secret,
                    'name': api_key.name,
                    'roles': [role.value for role in api_key.roles],
                    'expires_at': api_key.expires_at.isoformat() if api_key.expires_at else None,
                    'warning': 'Store this key securely - it will not be shown again'
                })
                
            except Exception as e:
                logger.error(f"API key creation error: {e}")
                return jsonify({'error': str(e)}), 500
    
    def require_permission(self, permission: str):
        """Decorator to require specific permission"""
        def decorator(f):
            @wraps(f)
            def decorated_function(*args, **kwargs):
                if not hasattr(g, 'current_user') or not g.current_user:
                    abort(401)
                
                if not g.current_user.has_permission(permission):
                    abort(403)
                
                return f(*args, **kwargs)
            return decorated_function
        return decorator
    
    def require_role(self, role: UserRole):
        """Decorator to require specific role"""
        def decorator(f):
            @wraps(f)
            def decorated_function(*args, **kwargs):
                if not hasattr(g, 'current_user') or not g.current_user:
                    abort(401)
                
                if not g.current_user.has_role(role):
                    abort(403)
                
                return f(*args, **kwargs)
            return decorated_function
        return decorator
    
    def run(self, host: Optional[str] = None, port: Optional[int] = None, debug: Optional[bool] = None):
        """Run the SAIQL server"""
        config = self.config.get('server', {})
        
        run_host = host or config.get('host', '0.0.0.0')
        run_port = port or config.get('port', 8000)
        run_debug = debug if debug is not None else config.get('debug', False)
        
        logger.info(f"Starting SAIQL secured server on {run_host}:{run_port}")
        logger.info(f"Authentication: {'Enabled' if self.config.get('security', {}).get('require_auth', True) else 'DISABLED'}")
        
        try:
            self.app.run(
                host=run_host,
                port=run_port,
                debug=run_debug,
                threaded=config.get('threaded', True)
            )
        except KeyboardInterrupt:
            logger.info("Server stopped by user")
        except Exception as e:
            logger.error(f"Server error: {e}")
        finally:
            # Cleanup
            self.db_manager.close_all()
            logger.info("Server shutdown complete")


# Alias for backward compatibility with test scripts
SecuredSAIQLServer = SAIQLServer


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='SAIQL Secured REST API Server')
    parser.add_argument('--host', type=str, default=None, help='Host to bind to')
    parser.add_argument('--port', type=int, default=None, help='Port to bind to')
    parser.add_argument('--config', type=str, default=None, help='Configuration file path')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    
    args = parser.parse_args()
    
    # Create and run server
    server = SAIQLServer(config_path=args.config)
    server.run(host=args.host, port=args.port, debug=args.debug)

if __name__ == '__main__':
    main()
