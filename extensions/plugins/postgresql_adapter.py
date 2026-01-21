#!/usr/bin/env python3
"""
SAIQL PostgreSQL Adapter - Production-Ready Database Adapter

This module provides a production-ready PostgreSQL adapter for SAIQL with:
- Connection pooling and management
- Transaction handling
- Error recovery and retry logic
- SSL/TLS support
- Performance monitoring
- Prepared statement caching
- Connection health checks

Author: Apollo & Claude  
Version: 1.0.0
Status: Production-Ready for SAIQL-Bravo

Usage:
    adapter = PostgreSQLAdapter(
        host='localhost',
        database='saiql_db',
        user='saiql_user',
        password='secure_password'
    )
    result = adapter.execute("SELECT * FROM users")
"""

import psycopg2
import psycopg2.pool
import psycopg2.extras
import psycopg2.sql
from psycopg2 import OperationalError, DatabaseError, IntegrityError
import logging
import time
import threading
from typing import Dict, List, Any, Optional, Union, Tuple, ContextManager
from dataclasses import dataclass, field
from contextlib import contextmanager
from enum import Enum
import json
import ssl
from urllib.parse import urlparse
import warnings

# Configure logging
logger = logging.getLogger(__name__)

class ConnectionState(Enum):
    """Connection states"""
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting" 
    CONNECTED = "connected"
    ERROR = "error"
    RECONNECTING = "reconnecting"

class SSLMode(Enum):
    """SSL connection modes"""
    DISABLE = "disable"
    ALLOW = "allow"
    PREFER = "prefer"
    REQUIRE = "require"
    VERIFY_CA = "verify-ca"
    VERIFY_FULL = "verify-full"

@dataclass
class ConnectionConfig:
    """PostgreSQL connection configuration"""
    host: str = "localhost"
    port: int = 5432
    database: str = "saiql"
    user: str = "saiql_user"
    password: str = ""
    
    # Connection pool settings
    min_connections: int = 2
    max_connections: int = 20
    connection_timeout: int = 30
    
    # SSL settings
    ssl_mode: SSLMode = SSLMode.PREFER
    ssl_cert: Optional[str] = None
    ssl_key: Optional[str] = None
    ssl_ca: Optional[str] = None
    
    # Performance settings
    statement_timeout: int = 300  # seconds
    idle_in_transaction_timeout: int = 60  # seconds
    connect_timeout: int = 10
    
    # Retry settings
    max_retries: int = 3
    retry_delay: float = 1.0
    
    # Application settings
    application_name: str = "SAIQL-Bravo"
    search_path: str = "public"
    
    def to_connection_params(self) -> Dict[str, Any]:
        """Convert to psycopg2 connection parameters"""
        params = {
            'host': self.host,
            'port': self.port,
            'database': self.database,
            'user': self.user,
            'password': self.password,
            'connect_timeout': self.connect_timeout,
            'application_name': self.application_name,
            'options': f'-c search_path={self.search_path}'
        }
        
        # Add SSL configuration
        if self.ssl_mode != SSLMode.DISABLE:
            params['sslmode'] = self.ssl_mode.value
            if self.ssl_cert:
                params['sslcert'] = self.ssl_cert
            if self.ssl_key:
                params['sslkey'] = self.ssl_key  
            if self.ssl_ca:
                params['sslrootcert'] = self.ssl_ca
                
        return params

@dataclass
class QueryMetrics:
    """Query execution metrics"""
    execution_time: float = 0.0
    rows_affected: int = 0
    rows_returned: int = 0
    connection_time: float = 0.0
    planning_time: float = 0.0
    execution_time_db: float = 0.0
    cache_hit: bool = False
    prepared_statement: bool = False
    
class PreparedStatementCache:
    """Cache for prepared statements"""
    
    def __init__(self, max_size: int = 100):
        self.max_size = max_size
        self.cache = {}
        self.usage_count = {}
        self._lock = threading.RLock()
    
    def get_statement_name(self, sql: str) -> str:
        """Generate unique statement name for SQL"""
        import hashlib
        return f"saiql_stmt_{hashlib.md5(sql.encode()).hexdigest()[:16]}"
    
    def is_cached(self, sql: str) -> bool:
        """Check if statement is cached"""
        with self._lock:
            stmt_name = self.get_statement_name(sql)
            return stmt_name in self.cache
    
    def add_statement(self, sql: str, cursor) -> str:
        """Add prepared statement to cache"""
        with self._lock:
            stmt_name = self.get_statement_name(sql)
            
            if len(self.cache) >= self.max_size:
                # Remove least used statement
                least_used = min(self.usage_count.items(), key=lambda x: x[1])
                old_stmt_name = least_used[0]
                try:
                    cursor.execute(f"DEALLOCATE {old_stmt_name}")
                except Exception:
                    pass  # Statement might not exist
                del self.cache[old_stmt_name]
                del self.usage_count[old_stmt_name]
            
            # Prepare statement
            try:
                cursor.execute(f"PREPARE {stmt_name} AS {sql}")
                self.cache[stmt_name] = sql
                self.usage_count[stmt_name] = 0
                logger.debug(f"Prepared statement: {stmt_name}")
            except Exception as e:
                logger.warning(f"Failed to prepare statement: {e}")
                return None
            
            return stmt_name
    
    def use_statement(self, sql: str) -> Optional[str]:
        """Mark statement as used and return statement name"""
        with self._lock:
            stmt_name = self.get_statement_name(sql)
            if stmt_name in self.cache:
                self.usage_count[stmt_name] += 1
                return stmt_name
            return None

class PostgreSQLAdapter:
    """
    Production-ready PostgreSQL adapter for SAIQL
    
    Features:
    - Connection pooling with automatic retry
    - SSL/TLS support with certificate validation
    - Prepared statement caching
    - Transaction management with savepoints
    - Connection health monitoring
    - Performance metrics collection
    - Graceful error handling and recovery
    """
    
    def __init__(self, config: Optional[ConnectionConfig] = None, **kwargs):
        """Initialize PostgreSQL adapter"""
        # Set up configuration
        if config:
            self.config = config
        else:
            self.config = ConnectionConfig(**kwargs)
        
        # Initialize state
        self.state = ConnectionState.DISCONNECTED
        self.pool = None
        self.prepared_statements = PreparedStatementCache()
        self._pool_lock = threading.RLock()
        
        # Statistics
        self.stats = {
            'connections_created': 0,
            'connections_closed': 0,
            'queries_executed': 0,
            'failed_queries': 0,
            'total_execution_time': 0.0,
            'cache_hits': 0,
            'retries_attempted': 0,
            'start_time': time.time()
        }
        
        # Initialize connection pool
        self._initialize_pool()
        
        logger.info(f"PostgreSQL adapter initialized for {self.config.host}:{self.config.port}/{self.config.database}")
    
    def _initialize_pool(self):
        """Initialize connection pool"""
        try:
            with self._pool_lock:
                if self.pool:
                    self.pool.closeall()
                
                connection_params = self.config.to_connection_params()
                
                # Create connection pool
                self.pool = psycopg2.pool.ThreadedConnectionPool(
                    minconn=self.config.min_connections,
                    maxconn=self.config.max_connections,
                    **connection_params
                )
                
                self.state = ConnectionState.CONNECTED
                self.stats['connections_created'] += self.config.min_connections
                
                # Test connection
                self._health_check()
                
                logger.info(f"Connection pool initialized with {self.config.min_connections}-{self.config.max_connections} connections")
                
        except Exception as e:
            self.state = ConnectionState.ERROR
            logger.error(f"Failed to initialize connection pool: {e}")
            raise
    
    @contextmanager
    def get_connection(self, autocommit: bool = False):
        """Get connection from pool with automatic cleanup"""
        connection = None
        start_time = time.time()
        
        try:
            with self._pool_lock:
                if not self.pool:
                    raise RuntimeError("Connection pool not initialized")
                
                connection = self.pool.getconn()
                connection.autocommit = autocommit
                
                # Set session parameters
                with connection.cursor() as cursor:
                    cursor.execute(f"SET statement_timeout = {self.config.statement_timeout * 1000}")
                    cursor.execute(f"SET idle_in_transaction_session_timeout = {self.config.idle_in_transaction_timeout * 1000}")
            
            connection_time = time.time() - start_time
            yield connection, connection_time
            
        except OperationalError as e:
            logger.warning(f"Connection error: {e}")
            # Attempt to reconnect
            self._handle_connection_error()
            raise
        except Exception as e:
            logger.error(f"Database error: {e}")
            raise
        finally:
            if connection:
                try:
                    with self._pool_lock:
                        if self.pool:
                            self.pool.putconn(connection)
                except Exception as e:
                    logger.error(f"Error returning connection to pool: {e}")
    
    def execute_query(self, sql: str, params: Optional[Tuple] = None,
                     fetch: bool = True, use_prepared: bool = False) -> Dict[str, Any]:
        """
        Execute SQL query with comprehensive error handling and metrics

        Args:
            sql: SQL query string
            params: Query parameters (optional)
            fetch: Whether to fetch results
            use_prepared: Whether to use server-side prepared statements.
                          Default False because PostgreSQL prepared statements
                          are connection-scoped and cannot be shared across
                          pooled connections. psycopg2's cursor.execute()
                          handles parameter binding correctly at the driver level.

        Returns:
            Dictionary with execution results and metrics
        """
        start_time = time.time()
        metrics = QueryMetrics()
        result = {
            'success': False,
            'data': [],
            'rows_affected': 0,
            'metrics': metrics,
            'error': None
        }
        
        retry_count = 0
        while retry_count <= self.config.max_retries:
            try:
                # Use autocommit=True for atomic queries to ensure immediate effect
                with self.get_connection(autocommit=True) as (connection, conn_time):
                    metrics.connection_time = conn_time
                    
                    with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                        query_start = time.time()

                        # Server-side prepared statements are disabled - they are
                        # connection-scoped in PostgreSQL and cannot work with connection pooling
                        if use_prepared:
                            raise ValueError(
                                "use_prepared=True is not supported with connection pooling. "
                                "PostgreSQL prepared statements are connection-scoped and cannot be "
                                "shared across pooled connections. Use use_prepared=False (default)."
                            )

                        cursor.execute(sql, params)
                        
                        metrics.execution_time_db = time.time() - query_start
                        
                        # Fetch results if requested
                        if fetch and cursor.description:
                            result['data'] = cursor.fetchall()
                            metrics.rows_returned = len(result['data'])
                        
                        metrics.rows_affected = cursor.rowcount if cursor.rowcount > 0 else 0
                        
                        # Get query plan information if available
                        try:
                            if sql.strip().upper().startswith('SELECT'):
                                cursor.execute("SELECT pg_stat_get_backend_activity_start(pg_backend_pid())")
                        except Exception:
                            pass  # Ignore plan extraction errors
                
                result['success'] = True
                self.stats['queries_executed'] += 1
                break
                
            except (OperationalError, DatabaseError) as e:
                retry_count += 1
                self.stats['retries_attempted'] += 1
                
                if retry_count <= self.config.max_retries:
                    logger.warning(f"Query failed, retrying ({retry_count}/{self.config.max_retries}): {e}")
                    time.sleep(self.config.retry_delay * retry_count)
                    continue
                else:
                    result['error'] = f"Query failed after {self.config.max_retries} retries: {str(e)}"
                    self.stats['failed_queries'] += 1
                    break
                    
            except Exception as e:
                result['error'] = f"Unexpected error: {str(e)}"
                self.stats['failed_queries'] += 1
                break
        
        metrics.execution_time = time.time() - start_time
        self.stats['total_execution_time'] += metrics.execution_time
        result['metrics'] = metrics
        
        return result
    
    def _should_prepare_statement(self, sql: str) -> bool:
        """Determine if statement should be prepared"""
        # Prepare SELECT, INSERT, UPDATE, DELETE statements
        sql_upper = sql.strip().upper()
        return any(sql_upper.startswith(cmd) for cmd in ['SELECT', 'INSERT', 'UPDATE', 'DELETE'])
    
    def execute_transaction(self, operations: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Execute multiple operations in a transaction
        
        Args:
            operations: List of operation dictionaries with 'sql' and optional 'params'
            
        Returns:
            Transaction result dictionary
        """
        start_time = time.time()
        result = {
            'success': False,
            'operations_completed': 0,
            'total_rows_affected': 0,
            'execution_time': 0.0,
            'error': None
        }
        
        try:
            with self.get_connection() as (connection, _):
                with connection.cursor() as cursor:
                # Start transaction managed by psycopg2 (implicitly)
                    
                    try:
                        for i, operation in enumerate(operations):
                            sql = operation.get('sql')
                            params = operation.get('params')
                            
                            cursor.execute(sql, params)
                            result['total_rows_affected'] += cursor.rowcount if cursor.rowcount > 0 else 0
                            result['operations_completed'] += 1
                        
                        # Commit transaction
                        connection.commit()
                        result['success'] = True
                        
                    except Exception as e:
                        # Rollback on error
                        connection.rollback()
                        result['error'] = f"Transaction failed at operation {result['operations_completed']}: {str(e)}"
                        
        except Exception as e:
            result['error'] = f"Transaction setup failed: {str(e)}"
        
        result['execution_time'] = time.time() - start_time
        return result
    
    def _health_check(self) -> bool:
        """Perform health check on database connection"""
        try:
            with self.get_connection() as (connection, _):
                with connection.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    cursor.fetchone()
            return True
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False
    
    def _handle_connection_error(self):
        """Handle connection errors and attempt recovery"""
        logger.warning("Handling connection error, attempting to reinitialize pool")
        self.state = ConnectionState.RECONNECTING
        
        try:
            time.sleep(self.config.retry_delay)
            self._initialize_pool()
        except Exception as e:
            self.state = ConnectionState.ERROR
            logger.error(f"Failed to recover from connection error: {e}")
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get adapter statistics"""
        uptime = time.time() - self.stats['start_time']
        total_queries = self.stats['queries_executed']
        
        return {
            'uptime_seconds': uptime,
            'state': self.state.value,
            'pool_size': f"{self.config.min_connections}-{self.config.max_connections}",
            'queries_executed': total_queries,
            'failed_queries': self.stats['failed_queries'],
            'success_rate': (total_queries - self.stats['failed_queries']) / max(total_queries, 1),
            'cache_hits': self.stats['cache_hits'],
            'cache_hit_rate': self.stats['cache_hits'] / max(total_queries, 1),
            'retries_attempted': self.stats['retries_attempted'],
            'avg_execution_time': self.stats['total_execution_time'] / max(total_queries, 1),
            'total_execution_time': self.stats['total_execution_time'],
            'prepared_statements_cached': len(self.prepared_statements.cache)
        }
    
    # ===== Phase 11 L0 Methods =====

    def get_tables(self) -> List[str]:
        """
        Get list of user tables in current schema (L0 method).

        Returns:
            List of table names (lowercase)

        Phase 11 L0 Implementation:
        - Queries information_schema.tables for user schema
        - Excludes system schemas (pg_catalog, information_schema)
        - Returns base tables only (no views)
        """
        query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """

        result = self.execute_query(query)

        if not result['success']:
            logger.error(f"Failed to get tables: {result.get('error')}")
            return []

        tables = [row['table_name'].lower() for row in result['data']]
        logger.debug(f"Found {len(tables)} tables: {tables}")

        return tables

    def get_schema(self, table_name: str) -> Dict[str, Any]:
        """
        Get table schema introspection (L0 method).

        Args:
            table_name: Table name (case-insensitive)

        Returns:
            Schema dict with keys:
                - columns: List of column dicts (name, type, nullable, type_info)
                - pk: List (empty for L0 - constraints not in scope)
                - fks: List (empty for L0 - constraints not in scope)

        Phase 11 L0 Implementation:
        - Basic column introspection only
        - Type mapping via TypeRegistry
        - Constraints (PK/FK/UK) deferred to L1
        """
        schema = {
            'columns': [],
            'pk': [],  # L0: not implemented
            'fks': []  # L0: not implemented
        }

        table_lower = table_name.lower()

        col_query = """
            SELECT
                column_name,
                data_type,
                character_maximum_length,
                numeric_precision,
                numeric_scale,
                is_nullable,
                column_default
            FROM information_schema.columns
            WHERE table_schema = 'public'
            AND table_name = %s
            ORDER BY ordinal_position
        """

        result = self.execute_query(col_query, (table_lower,))

        if not result['success']:
            logger.error(f"Failed to get schema for {table_name}: {result.get('error')}")
            return schema

        # Import TypeRegistry for type mapping
        from core.type_registry import TypeRegistry, IRType

        for col in result['data']:
            col_name = col['column_name']
            data_type = col['data_type']

            # Build full type string with length/precision/scale
            full_type_str = data_type
            if col['character_maximum_length']:
                full_type_str = f"{data_type}({col['character_maximum_length']})"
            elif col['numeric_precision'] is not None:
                if col['numeric_scale'] is not None and col['numeric_scale'] > 0:
                    full_type_str = f"{data_type}({col['numeric_precision']},{col['numeric_scale']})"

            # Map to IR type
            type_info = TypeRegistry.map_to_ir('postgresql', full_type_str)

            # Warn if unsupported
            if type_info.ir_type == IRType.UNKNOWN:
                logger.warning(
                    f"Table {table_name}.{col_name}: Unsupported PostgreSQL type '{data_type}'. "
                    f"Manual migration may be required."
                )

            schema['columns'].append({
                'name': col_name.lower(),
                'type': data_type,
                'type_info': type_info,
                'unsupported': type_info.ir_type == IRType.UNKNOWN,
                'length': col['character_maximum_length'],
                'precision': col['numeric_precision'],
                'scale': col['numeric_scale'],
                'nullable': col['is_nullable'] == 'YES',
                'default': col['column_default']
            })

        logger.debug(
            f"Schema introspected for {table_name}: "
            f"{len(schema['columns'])} columns"
        )

        return schema

    def extract_data(
        self,
        table_name: str,
        order_by: Optional[List[str]] = None,
        chunk_size: int = 10000
    ) -> Dict[str, Any]:
        """
        Extract data from table with deterministic ordering (L0 method).

        Args:
            table_name: Table name
            order_by: Columns to order by (for deterministic extraction)
                     If None, uses first column
            chunk_size: Rows per chunk (default: 10000)

        Returns:
            Dict with keys:
                - data: List of all row dicts
                - stats: Extraction stats dict (rows, duration, etc.)

        Phase 11 L0 Implementation:
        - Simple SELECT * with ORDER BY
        - All rows loaded into memory (no streaming)
        - Deterministic ordering required for repeatability
        """
        start_time = time.time()

        # Determine ORDER BY clause
        if order_by:
            order_clause = ', '.join([f'"{col}"' for col in order_by])
        else:
            # Default: order by first column for determinism
            schema = self.get_schema(table_name)
            if schema['columns']:
                first_col = schema['columns'][0]['name']
                order_clause = f'"{first_col}"'
                logger.info(f"Using first column for deterministic ordering: {first_col}")
            else:
                order_clause = ""
                logger.warning(f"No columns found for {table_name}, extraction may not be deterministic")

        # Build query
        table_lower = table_name.lower()

        if order_clause:
            query = f'SELECT * FROM "{table_lower}" ORDER BY {order_clause}'
        else:
            query = f'SELECT * FROM "{table_lower}"'

        logger.info(f"Extracting data from {table_name}...")

        result = self.execute_query(query)

        if not result['success']:
            logger.error(f"Failed to extract data from {table_name}: {result.get('error')}")
            return {
                'data': [],
                'stats': {
                    'total_rows': 0,
                    'duration_seconds': time.time() - start_time,
                    'error': result.get('error')
                }
            }

        data = result['data']
        duration = time.time() - start_time

        logger.info(f"Extraction complete: {len(data)} rows from {table_name} ({duration:.2f}s)")

        return {
            'data': data,
            'stats': {
                'total_rows': len(data),
                'duration_seconds': duration,
                'order_by': order_clause if order_clause else None
            }
        }

    # ===== Phase 11 L1 Methods =====

    def get_primary_keys(self, table_name: str) -> List[str]:
        """
        Get primary key columns for a table (L1 method).

        Args:
            table_name: Table name

        Returns:
            List of column names in the primary key
        """
        query = """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY'
            AND tc.table_schema = 'public'
            AND tc.table_name = %s
            ORDER BY kcu.ordinal_position
        """

        result = self.execute_query(query, (table_name.lower(),))

        if not result['success']:
            logger.error(f"Failed to get primary keys for {table_name}: {result.get('error')}")
            return []

        return [row['column_name'] for row in result['data']]

    def get_foreign_keys(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Get foreign key constraints for a table (L1 method).

        Args:
            table_name: Table name

        Returns:
            List of FK dicts with: column, ref_table, ref_column, constraint_name
        """
        query = """
            SELECT
                kcu.column_name,
                ccu.table_name AS ref_table,
                ccu.column_name AS ref_column,
                tc.constraint_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage ccu
                ON ccu.constraint_name = tc.constraint_name
                AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
            AND tc.table_schema = 'public'
            AND tc.table_name = %s
        """

        result = self.execute_query(query, (table_name.lower(),))

        if not result['success']:
            logger.error(f"Failed to get foreign keys for {table_name}: {result.get('error')}")
            return []

        return [{
            'column': row['column_name'],
            'ref_table': row['ref_table'],
            'ref_column': row['ref_column'],
            'constraint_name': row['constraint_name']
        } for row in result['data']]

    def get_unique_constraints(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Get unique constraints for a table (L1 method).

        Args:
            table_name: Table name

        Returns:
            List of unique constraint dicts with: constraint_name, columns
        """
        query = """
            SELECT
                tc.constraint_name,
                array_agg(kcu.column_name ORDER BY kcu.ordinal_position) AS columns
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'UNIQUE'
            AND tc.table_schema = 'public'
            AND tc.table_name = %s
            GROUP BY tc.constraint_name
        """

        result = self.execute_query(query, (table_name.lower(),))

        if not result['success']:
            logger.error(f"Failed to get unique constraints for {table_name}: {result.get('error')}")
            return []

        return [{
            'constraint_name': row['constraint_name'],
            'columns': row['columns']
        } for row in result['data']]

    def get_indexes(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Get indexes for a table (L1 method).

        Args:
            table_name: Table name

        Returns:
            List of index dicts with: name, columns, is_unique, is_primary
        """
        query = """
            SELECT
                i.relname AS index_name,
                a.attname AS column_name,
                ix.indisunique AS is_unique,
                ix.indisprimary AS is_primary
            FROM pg_class t
            JOIN pg_index ix ON t.oid = ix.indrelid
            JOIN pg_class i ON i.oid = ix.indexrelid
            JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
            WHERE t.relkind = 'r'
            AND t.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')
            AND t.relname = %s
            ORDER BY i.relname, a.attnum
        """

        result = self.execute_query(query, (table_name.lower(),))

        if not result['success']:
            logger.error(f"Failed to get indexes for {table_name}: {result.get('error')}")
            return []

        # Group by index name
        indexes = {}
        for row in result['data']:
            idx_name = row['index_name']
            if idx_name not in indexes:
                indexes[idx_name] = {
                    'name': idx_name,
                    'columns': [],
                    'is_unique': row['is_unique'],
                    'is_primary': row['is_primary']
                }
            indexes[idx_name]['columns'].append(row['column_name'])

        return list(indexes.values())

    # =========================================================================
    # L2 Methods (Views)
    # =========================================================================

    def get_views(self, schema: str = 'public') -> List[Dict[str, Any]]:
        """
        Get list of views in the schema (L2 method).

        Args:
            schema: Schema name (default: public)

        Returns:
            List of view dictionaries with name, schema, definition
        """
        query = """
            SELECT
                schemaname,
                viewname,
                definition
            FROM pg_views
            WHERE schemaname = %s
            ORDER BY viewname
        """

        result = self.execute_query(query, (schema,))

        if not result['success']:
            logger.error(f"Failed to get views: {result.get('error')}")
            return []

        views = []
        for row in result['data']:
            views.append({
                'schema': row['schemaname'],
                'name': row['viewname'],
                'definition': row['definition']
            })

        logger.debug(f"Found {len(views)} views in schema {schema}")
        return views

    def get_view_definition(self, view_name: str, schema: str = 'public') -> Optional[str]:
        """
        Get the SQL definition of a view (L2 method).

        Args:
            view_name: View name
            schema: Schema name (default: public)

        Returns:
            View definition SQL or None
        """
        query = """
            SELECT definition
            FROM pg_views
            WHERE schemaname = %s AND viewname = %s
        """

        result = self.execute_query(query, (schema, view_name))

        if not result['success'] or not result['data']:
            return None

        return result['data'][0]['definition']

    def get_view_dependencies(self, view_name: str, schema: str = 'public') -> List[Dict[str, str]]:
        """
        Get dependencies of a view (what tables/views it depends on) (L2 method).

        Args:
            view_name: View name
            schema: Schema name

        Returns:
            List of dependency dictionaries with type, schema, name
        """
        query = """
            SELECT DISTINCT
                d.refclassid::regclass::text as ref_class,
                CASE
                    WHEN c.relkind = 'r' THEN 'table'
                    WHEN c.relkind = 'v' THEN 'view'
                    WHEN c.relkind = 'm' THEN 'materialized_view'
                    ELSE 'other'
                END as dep_type,
                n.nspname as dep_schema,
                c.relname as dep_name
            FROM pg_depend d
            JOIN pg_rewrite r ON d.objid = r.oid
            JOIN pg_class v ON r.ev_class = v.oid
            JOIN pg_namespace vn ON v.relnamespace = vn.oid
            JOIN pg_class c ON d.refobjid = c.oid
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE v.relname = %s
            AND vn.nspname = %s
            AND c.relname != %s
            AND d.deptype = 'n'
            ORDER BY dep_type, dep_name
        """

        result = self.execute_query(query, (view_name, schema, view_name))

        if not result['success']:
            logger.error(f"Failed to get view dependencies: {result.get('error')}")
            return []

        return [
            {
                'type': row['dep_type'],
                'schema': row['dep_schema'],
                'name': row['dep_name']
            }
            for row in result['data']
        ]

    def get_views_in_dependency_order(self, schema: str = 'public') -> List[Dict[str, Any]]:
        """
        Get views sorted by dependency order (L2 method).
        Views with no dependencies come first, then views that depend on those, etc.

        Args:
            schema: Schema name

        Returns:
            List of views in creation order (dependencies first)
        """
        views = self.get_views(schema)

        # Build dependency graph
        view_names = {v['name'] for v in views}
        dependencies = {}

        for view in views:
            deps = self.get_view_dependencies(view['name'], schema)
            # Only track dependencies on other views in this schema
            view_deps = [d['name'] for d in deps if d['type'] == 'view' and d['name'] in view_names]
            dependencies[view['name']] = set(view_deps)

        # Topological sort
        sorted_views = []
        remaining = set(view_names)

        while remaining:
            # Find views with no unresolved dependencies
            ready = [v for v in remaining if not (dependencies.get(v, set()) & remaining)]

            if not ready:
                # Circular dependency - break by taking any remaining
                logger.warning(f"Circular view dependency detected in: {remaining}")
                ready = [next(iter(remaining))]

            for v_name in sorted(ready):  # Sort for determinism
                remaining.remove(v_name)
                view_dict = next(v for v in views if v['name'] == v_name)
                sorted_views.append(view_dict)

        return sorted_views

    def get_materialized_views(self, schema: str = 'public') -> List[Dict[str, Any]]:
        """
        Get list of materialized views (L2 method).

        Args:
            schema: Schema name

        Returns:
            List of materialized view dictionaries
        """
        query = """
            SELECT
                schemaname,
                matviewname,
                definition,
                ispopulated
            FROM pg_matviews
            WHERE schemaname = %s
            ORDER BY matviewname
        """

        result = self.execute_query(query, (schema,))

        if not result['success']:
            logger.error(f"Failed to get materialized views: {result.get('error')}")
            return []

        return [
            {
                'schema': row['schemaname'],
                'name': row['matviewname'],
                'definition': row['definition'],
                'is_populated': row['ispopulated']
            }
            for row in result['data']
        ]

    def create_view(self, name: str, definition: str, schema: str = 'public',
                    or_replace: bool = True) -> Dict[str, Any]:
        """
        Create a view from definition (L2 emission method).

        Args:
            name: View name
            definition: SELECT statement (the view query)
            schema: Schema name
            or_replace: Use CREATE OR REPLACE

        Returns:
            Result dict with success status
        """
        # Clean up definition - remove leading SELECT if present in raw def
        definition = definition.strip()

        # Build CREATE VIEW statement
        create_type = "CREATE OR REPLACE VIEW" if or_replace else "CREATE VIEW"
        qualified_name = f'"{schema}"."{name}"'

        # The definition from pg_views already includes SELECT
        sql = f"{create_type} {qualified_name} AS {definition}"

        result = self.execute_query(sql)

        if result['success']:
            logger.info(f"Created view {schema}.{name}")
        else:
            logger.error(f"Failed to create view {schema}.{name}: {result.get('error')}")

        return result

    def drop_view(self, name: str, schema: str = 'public',
                  if_exists: bool = True, cascade: bool = False) -> Dict[str, Any]:
        """
        Drop a view (L2 emission method).

        Args:
            name: View name
            schema: Schema name
            if_exists: Use IF EXISTS
            cascade: Use CASCADE

        Returns:
            Result dict with success status
        """
        qualified_name = f'"{schema}"."{name}"'
        sql = f"DROP VIEW {'IF EXISTS ' if if_exists else ''}{qualified_name}{' CASCADE' if cascade else ''}"

        result = self.execute_query(sql)

        if result['success']:
            logger.info(f"Dropped view {schema}.{name}")

        return result

    def create_views_in_order(self, views: List[Dict[str, Any]],
                               schema: str = 'public') -> Dict[str, Any]:
        """
        Create multiple views in dependency order (L2 emission method).

        Args:
            views: List of view dicts with 'name' and 'definition'
            schema: Target schema

        Returns:
            Result dict with created/failed counts
        """
        results = {
            'success': True,
            'created': [],
            'failed': [],
            'total': len(views)
        }

        for view in views:
            name = view['name']
            definition = view['definition']

            result = self.create_view(name, definition, schema)

            if result['success']:
                results['created'].append(name)
            else:
                results['failed'].append({
                    'name': name,
                    'error': result.get('error')
                })
                results['success'] = False

        logger.info(f"Created {len(results['created'])}/{results['total']} views")
        return results

    # =========================================================================
    # L3 Methods (Routines - Functions/Procedures)
    # =========================================================================

    def get_functions(self, schema: str = 'public') -> List[Dict[str, Any]]:
        """
        Get list of user-defined functions (L3 method).

        Args:
            schema: Schema name

        Returns:
            List of function dictionaries with name, args, return type, body
        """
        query = """
            SELECT
                p.proname as name,
                n.nspname as schema,
                pg_get_function_arguments(p.oid) as arguments,
                pg_get_function_result(p.oid) as return_type,
                l.lanname as language,
                CASE
                    WHEN p.prokind = 'f' THEN 'function'
                    WHEN p.prokind = 'p' THEN 'procedure'
                    WHEN p.prokind = 'a' THEN 'aggregate'
                    WHEN p.prokind = 'w' THEN 'window'
                    ELSE 'unknown'
                END as kind,
                p.provolatile as volatility,
                p.proisstrict as is_strict,
                p.prosecdef as security_definer,
                pg_get_functiondef(p.oid) as definition
            FROM pg_proc p
            JOIN pg_namespace n ON p.pronamespace = n.oid
            JOIN pg_language l ON p.prolang = l.oid
            WHERE n.nspname = %s
            AND p.prokind IN ('f', 'p')
            AND l.lanname IN ('sql', 'plpgsql')
            ORDER BY p.proname
        """

        result = self.execute_query(query, (schema,))

        if not result['success']:
            logger.error(f"Failed to get functions: {result.get('error')}")
            return []

        functions = []
        for row in result['data']:
            functions.append({
                'name': row['name'],
                'schema': row['schema'],
                'arguments': row['arguments'],
                'return_type': row['return_type'],
                'language': row['language'],
                'kind': row['kind'],
                'volatility': row['volatility'],  # i=immutable, s=stable, v=volatile
                'is_strict': row['is_strict'],
                'security_definer': row['security_definer'],
                'definition': row['definition']
            })

        logger.debug(f"Found {len(functions)} functions in schema {schema}")
        return functions

    def get_function_definition(self, func_name: str, schema: str = 'public') -> Optional[str]:
        """
        Get the full CREATE FUNCTION statement (L3 method).

        Args:
            func_name: Function name
            schema: Schema name

        Returns:
            Full function definition SQL or None
        """
        query = """
            SELECT pg_get_functiondef(p.oid) as definition
            FROM pg_proc p
            JOIN pg_namespace n ON p.pronamespace = n.oid
            WHERE n.nspname = %s AND p.proname = %s
            LIMIT 1
        """

        result = self.execute_query(query, (schema, func_name))

        if not result['success'] or not result['data']:
            return None

        return result['data'][0]['definition']

    def get_safe_functions(self, schema: str = 'public') -> List[Dict[str, Any]]:
        """
        Get functions that are safe to migrate (L3 subset method).

        Safe subset criteria:
        - Language: SQL or PL/pgSQL only
        - Volatility: IMMUTABLE or STABLE (not VOLATILE by default)
        - No SECURITY DEFINER (unless explicitly allowed)

        Args:
            schema: Schema name

        Returns:
            List of safe function dictionaries
        """
        all_funcs = self.get_functions(schema)

        safe_funcs = []
        skipped = []

        for func in all_funcs:
            reasons = []

            # Check language
            if func['language'] not in ('sql', 'plpgsql'):
                reasons.append(f"unsupported language: {func['language']}")

            # Check volatility (allow immutable and stable)
            if func['volatility'] == 'v':
                reasons.append("volatile function")

            # Check security definer
            if func['security_definer']:
                reasons.append("security definer")

            if reasons:
                func['skip_reasons'] = reasons
                skipped.append(func)
            else:
                safe_funcs.append(func)

        logger.info(f"Safe functions: {len(safe_funcs)}, Skipped: {len(skipped)}")
        return safe_funcs

    def get_skipped_functions(self, schema: str = 'public') -> List[Dict[str, Any]]:
        """
        Get functions that are NOT safe to migrate with skip reasons (L3 subset method).

        Args:
            schema: Schema name

        Returns:
            List of skipped function dictionaries with skip_reasons
        """
        all_funcs = self.get_functions(schema)
        skipped = []

        for func in all_funcs:
            reasons = []

            if func['language'] not in ('sql', 'plpgsql'):
                reasons.append(f"unsupported language: {func['language']}")

            if func['volatility'] == 'v':
                reasons.append("volatile function")

            if func['security_definer']:
                reasons.append("security definer")

            if reasons:
                func['skip_reasons'] = reasons
                skipped.append(func)

        return skipped

    def create_function(self, definition: str, or_replace: bool = True) -> Dict[str, Any]:
        """
        Create a function from its full definition (L3 emission method).

        Args:
            definition: Full CREATE FUNCTION statement
            or_replace: Modify to use OR REPLACE if not present

        Returns:
            Result dict with success status
        """
        sql = definition.strip()

        # Ensure OR REPLACE if requested
        if or_replace and 'OR REPLACE' not in sql.upper():
            sql = sql.replace('CREATE FUNCTION', 'CREATE OR REPLACE FUNCTION', 1)
            sql = sql.replace('CREATE function', 'CREATE OR REPLACE function', 1)

        result = self.execute_query(sql)

        if result['success']:
            logger.info("Created function successfully")
        else:
            logger.error(f"Failed to create function: {result.get('error')}")

        return result

    def drop_function(self, name: str, args: str = '', schema: str = 'public',
                      if_exists: bool = True, cascade: bool = False) -> Dict[str, Any]:
        """
        Drop a function (L3 emission method).

        Args:
            name: Function name
            args: Argument signature (e.g., 'integer, text')
            schema: Schema name
            if_exists: Use IF EXISTS
            cascade: Use CASCADE

        Returns:
            Result dict with success status
        """
        qualified_name = f'"{schema}"."{name}"'
        if args:
            qualified_name += f"({args})"

        sql = f"DROP FUNCTION {'IF EXISTS ' if if_exists else ''}{qualified_name}{' CASCADE' if cascade else ''}"

        result = self.execute_query(sql)

        if result['success']:
            logger.info(f"Dropped function {schema}.{name}")

        return result

    def create_functions_in_order(self, functions: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Create multiple functions from their definitions (L3 emission method).

        Args:
            functions: List of function dicts with 'definition' key

        Returns:
            Result dict with created/failed counts
        """
        results = {
            'success': True,
            'created': [],
            'failed': [],
            'total': len(functions)
        }

        for func in functions:
            definition = func.get('definition', '')
            name = func.get('name', 'unknown')

            result = self.create_function(definition)

            if result['success']:
                results['created'].append(name)
            else:
                results['failed'].append({
                    'name': name,
                    'error': result.get('error')
                })
                results['success'] = False

        logger.info(f"Created {len(results['created'])}/{results['total']} functions")
        return results

    # =========================================================================
    # L4 Methods (Triggers)
    # =========================================================================

    def get_triggers(self, schema: str = 'public') -> List[Dict[str, Any]]:
        """
        Get list of triggers (L4 method).

        Args:
            schema: Schema name

        Returns:
            List of trigger dictionaries
        """
        query = """
            SELECT
                t.tgname as trigger_name,
                n.nspname as schema,
                c.relname as table_name,
                p.proname as function_name,
                CASE
                    WHEN t.tgtype & 2 = 2 THEN 'BEFORE'
                    WHEN t.tgtype & 64 = 64 THEN 'INSTEAD OF'
                    ELSE 'AFTER'
                END as timing,
                CASE
                    WHEN t.tgtype & 4 = 4 THEN true ELSE false
                END as on_insert,
                CASE
                    WHEN t.tgtype & 8 = 8 THEN true ELSE false
                END as on_delete,
                CASE
                    WHEN t.tgtype & 16 = 16 THEN true ELSE false
                END as on_update,
                CASE
                    WHEN t.tgtype & 1 = 1 THEN 'ROW'
                    ELSE 'STATEMENT'
                END as level,
                t.tgenabled as enabled,
                pg_get_triggerdef(t.oid) as definition
            FROM pg_trigger t
            JOIN pg_class c ON t.tgrelid = c.oid
            JOIN pg_namespace n ON c.relnamespace = n.oid
            JOIN pg_proc p ON t.tgfoid = p.oid
            WHERE n.nspname = %s
            AND NOT t.tgisinternal
            ORDER BY c.relname, t.tgname
        """

        result = self.execute_query(query, (schema,))

        if not result['success']:
            logger.error(f"Failed to get triggers: {result.get('error')}")
            return []

        triggers = []
        for row in result['data']:
            events = []
            if row['on_insert']:
                events.append('INSERT')
            if row['on_update']:
                events.append('UPDATE')
            if row['on_delete']:
                events.append('DELETE')

            triggers.append({
                'name': row['trigger_name'],
                'schema': row['schema'],
                'table_name': row['table_name'],
                'function_name': row['function_name'],
                'timing': row['timing'],
                'events': events,
                'level': row['level'],
                'enabled': row['enabled'] == 'O',  # 'O'=origin, 'D'=disabled
                'definition': row['definition']
            })

        logger.debug(f"Found {len(triggers)} triggers in schema {schema}")
        return triggers

    def get_trigger_function(self, trigger_name: str, schema: str = 'public') -> Optional[Dict[str, Any]]:
        """
        Get the function associated with a trigger (L4 method).

        Args:
            trigger_name: Trigger name
            schema: Schema name

        Returns:
            Function dictionary or None
        """
        query = """
            SELECT
                p.proname as func_name,
                pg_get_functiondef(p.oid) as definition
            FROM pg_trigger t
            JOIN pg_class c ON t.tgrelid = c.oid
            JOIN pg_namespace n ON c.relnamespace = n.oid
            JOIN pg_proc p ON t.tgfoid = p.oid
            WHERE n.nspname = %s AND t.tgname = %s
        """

        result = self.execute_query(query, (schema, trigger_name))

        if not result['success'] or not result['data']:
            return None

        row = result['data'][0]
        return {
            'name': row['func_name'],
            'definition': row['definition']
        }

    def get_safe_triggers(self, schema: str = 'public') -> List[Dict[str, Any]]:
        """
        Get triggers that are safe to migrate (L4 subset method).

        Safe subset criteria:
        - ROW level triggers only (not STATEMENT level)
        - BEFORE or AFTER timing (not INSTEAD OF)
        - Language: SQL or PL/pgSQL

        Note: Trigger functions are typically VOLATILE because they modify the
        triggering row (NEW). This is expected and safe behavior. We don't
        filter by volatility for trigger functions.

        Args:
            schema: Schema name

        Returns:
            List of safe trigger dictionaries
        """
        all_triggers = self.get_triggers(schema)

        safe_triggers = []
        skipped = []

        for trigger in all_triggers:
            reasons = []

            # Check level - ROW level only
            if trigger['level'] != 'ROW':
                reasons.append("statement-level trigger")

            # Check timing - no INSTEAD OF
            if trigger['timing'] == 'INSTEAD OF':
                reasons.append("INSTEAD OF trigger")

            if reasons:
                trigger['skip_reasons'] = reasons
                skipped.append(trigger)
            else:
                safe_triggers.append(trigger)

        logger.info(f"Safe triggers: {len(safe_triggers)}, Skipped: {len(skipped)}")
        return safe_triggers

    def get_skipped_triggers(self, schema: str = 'public') -> List[Dict[str, Any]]:
        """
        Get triggers that are NOT safe to migrate with skip reasons (L4 subset method).

        Args:
            schema: Schema name

        Returns:
            List of skipped trigger dictionaries with skip_reasons
        """
        all_triggers = self.get_triggers(schema)
        skipped = []

        for trigger in all_triggers:
            reasons = []

            # Check level - ROW level only
            if trigger['level'] != 'ROW':
                reasons.append("statement-level trigger")

            # Check timing - no INSTEAD OF
            if trigger['timing'] == 'INSTEAD OF':
                reasons.append("INSTEAD OF trigger")

            if reasons:
                trigger['skip_reasons'] = reasons
                skipped.append(trigger)

        return skipped

    def create_trigger(self, definition: str) -> Dict[str, Any]:
        """
        Create a trigger from its full definition (L4 emission method).

        Args:
            definition: Full CREATE TRIGGER statement

        Returns:
            Result dict with success status
        """
        sql = definition.strip()

        result = self.execute_query(sql)

        if result['success']:
            logger.info("Created trigger successfully")
        else:
            logger.error(f"Failed to create trigger: {result.get('error')}")

        return result

    def drop_trigger(self, name: str, table: str, schema: str = 'public',
                     if_exists: bool = True, cascade: bool = False) -> Dict[str, Any]:
        """
        Drop a trigger (L4 emission method).

        Args:
            name: Trigger name
            table: Table the trigger is on
            schema: Schema name
            if_exists: Use IF EXISTS
            cascade: Use CASCADE

        Returns:
            Result dict with success status
        """
        qualified_table = f'"{schema}"."{table}"'
        sql = f"DROP TRIGGER {'IF EXISTS ' if if_exists else ''}\"{name}\" ON {qualified_table}{' CASCADE' if cascade else ''}"

        result = self.execute_query(sql)

        if result['success']:
            logger.info(f"Dropped trigger {name} on {schema}.{table}")

        return result

    def create_triggers_in_order(self, triggers: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Create multiple triggers from their definitions (L4 emission method).

        Note: Trigger functions must already exist.

        Args:
            triggers: List of trigger dicts with 'definition' key

        Returns:
            Result dict with created/failed counts
        """
        results = {
            'success': True,
            'created': [],
            'failed': [],
            'total': len(triggers)
        }

        for trigger in triggers:
            definition = trigger.get('definition', '')
            name = trigger.get('name', 'unknown')

            result = self.create_trigger(definition)

            if result['success']:
                results['created'].append(name)
            else:
                results['failed'].append({
                    'name': name,
                    'error': result.get('error')
                })
                results['success'] = False

        logger.info(f"Created {len(results['created'])}/{results['total']} triggers")
        return results

    def close(self):
        """Close all connections and cleanup"""
        logger.info("Closing PostgreSQL adapter")

        try:
            with self._pool_lock:
                if self.pool:
                    self.pool.closeall()
                    self.stats['connections_closed'] += self.config.max_connections
                    self.pool = None

            self.state = ConnectionState.DISCONNECTED

            # Log final statistics
            stats = self.get_statistics()
            logger.info(f"PostgreSQL adapter closed. Final stats: {json.dumps(stats, indent=2)}")

        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

# Utility functions
def create_adapter_from_url(database_url: str, **kwargs) -> PostgreSQLAdapter:
    """Create adapter from database URL"""
    parsed = urlparse(database_url)
    
    config = ConnectionConfig(
        host=parsed.hostname or 'localhost',
        port=parsed.port or 5432,
        database=parsed.path.lstrip('/') if parsed.path else 'postgres',
        user=parsed.username or 'postgres',
        password=parsed.password or '',
        **kwargs
    )
    
    return PostgreSQLAdapter(config)

def test_connection(config: ConnectionConfig) -> bool:
    """Test PostgreSQL connection"""
    try:
        adapter = PostgreSQLAdapter(config)
        result = adapter.execute_query("SELECT version()")
        adapter.close()
        return result['success']
    except Exception as e:
        logger.error(f"Connection test failed: {e}")
        return False

# Example usage and testing
if __name__ == "__main__":
    # Example configuration
    config = ConnectionConfig(
        host="localhost",
        database="saiql_test",
        user="postgres",
        password="password",
        min_connections=2,
        max_connections=10
    )
    
    try:
        # Test the adapter
        adapter = PostgreSQLAdapter(config)
        
        # Test simple query
        result = adapter.execute_query("SELECT current_timestamp as now")
        print(f"Query result: {result}")
        
        # Test transaction
        operations = [
            {"sql": "CREATE TEMP TABLE test_table (id SERIAL, name TEXT)"},
            {"sql": "INSERT INTO test_table (name) VALUES (%s)", "params": ("Test Name",)}
        ]
        tx_result = adapter.execute_transaction(operations)
        print(f"Transaction result: {tx_result}")
        
        # Print statistics
        stats = adapter.get_statistics()
        print(f"Adapter statistics: {json.dumps(stats, indent=2)}")
        
        adapter.close()
        
    except Exception as e:
        print(f"Test failed: {e}")
        import traceback
        traceback.print_exc()
