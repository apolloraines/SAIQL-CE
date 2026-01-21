#!/usr/bin/env python3
"""
SAIQL MySQL Adapter - Production-Ready Database Adapter

This module provides a production-ready MySQL adapter for SAIQL with:
- Connection pooling and management
- Transaction handling
- Error recovery and retry logic
- SSL/TLS support
- Performance monitoring
- Prepared statement caching
- Connection health checks

Author: Apollo & Claude  
Version: 1.0.0
Status: Production-Ready for SAIQL-Delta

Usage:
    adapter = MySQLAdapter(
        host='localhost',
        database='saiql_db',
        user='saiql_user',
        password='secure_password'
    )
    result = adapter.execute("SELECT * FROM users")
"""

import pymysql
import pymysql.cursors
from pymysql import OperationalError, DatabaseError, IntegrityError, MySQLError
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
import queue
import re

# Configure logging
logger = logging.getLogger(__name__)


def strip_definer(sql: str) -> str:
    """
    Strip DEFINER clause from SQL statement for portability.

    Per collab rules, DEFINER clauses should be stripped when emitting
    views, routines, and triggers to avoid permission issues.

    Args:
        sql: SQL statement potentially containing DEFINER clause

    Returns:
        SQL statement with DEFINER clause removed
    """
    # Pattern matches: DEFINER=`user`@`host` or DEFINER='user'@'host'
    # Also handles: DEFINER = `user`@`host` (with spaces)
    pattern = r'\s*DEFINER\s*=\s*[`\']?[\w%]+[`\']?\s*@\s*[`\']?[\w%.]+[`\']?\s*'
    return re.sub(pattern, ' ', sql, flags=re.IGNORECASE).strip()

class ConnectionState(Enum):
    """Connection states"""
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting" 
    CONNECTED = "connected"
    ERROR = "error"
    RECONNECTING = "reconnecting"

class SSLMode(Enum):
    """SSL connection modes"""
    DISABLED = "DISABLED"
    PREFERRED = "PREFERRED"
    REQUIRED = "REQUIRED"
    VERIFY_CA = "VERIFY_CA"
    VERIFY_IDENTITY = "VERIFY_IDENTITY"

@dataclass
class ConnectionConfig:
    """MySQL connection configuration"""
    host: str = "localhost"
    port: int = 3306
    database: str = "saiql"
    user: str = "saiql_user"
    password: str = ""
    
    # Connection pool settings
    min_connections: int = 2
    max_connections: int = 20
    connection_timeout: int = 30
    
    # SSL settings
    ssl_mode: SSLMode = SSLMode.PREFERRED
    ssl_cert: Optional[str] = None
    ssl_key: Optional[str] = None
    ssl_ca: Optional[str] = None
    ssl_capath: Optional[str] = None
    ssl_cipher: Optional[str] = None
    
    # Performance settings
    connect_timeout: int = 10
    read_timeout: int = 30
    write_timeout: int = 30
    
    # Retry settings
    max_retries: int = 3
    retry_delay: float = 1.0
    
    # MySQL specific settings
    charset: str = "utf8mb4"
    sql_mode: str = "STRICT_TRANS_TABLES,NO_ZERO_DATE,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO"
    autocommit: bool = False
    
    def to_connection_params(self) -> Dict[str, Any]:
        """Convert to PyMySQL connection parameters"""
        params = {
            'host': self.host,
            'port': self.port,
            'database': self.database,
            'user': self.user,
            'password': self.password,
            'connect_timeout': self.connect_timeout,
            'read_timeout': self.read_timeout,
            'write_timeout': self.write_timeout,
            'charset': self.charset,
            'autocommit': self.autocommit,
            'cursorclass': pymysql.cursors.DictCursor
        }
        
        # Add SSL configuration
        if self.ssl_mode != SSLMode.DISABLED:
            ssl_context = {}
            if self.ssl_mode == SSLMode.REQUIRED:
                ssl_context['check_hostname'] = False
                ssl_context['verify_mode'] = ssl.CERT_NONE
            elif self.ssl_mode == SSLMode.VERIFY_CA:
                ssl_context['check_hostname'] = False
                ssl_context['verify_mode'] = ssl.CERT_REQUIRED
            elif self.ssl_mode == SSLMode.VERIFY_IDENTITY:
                ssl_context['check_hostname'] = True
                ssl_context['verify_mode'] = ssl.CERT_REQUIRED
                
            if self.ssl_ca:
                ssl_context['ca'] = self.ssl_ca
            if self.ssl_cert:
                ssl_context['cert'] = self.ssl_cert
            if self.ssl_key:
                ssl_context['key'] = self.ssl_key
            if self.ssl_capath:
                ssl_context['capath'] = self.ssl_capath
            if self.ssl_cipher:
                ssl_context['cipher'] = self.ssl_cipher
                
            if ssl_context:
                params['ssl'] = ssl_context
                
        return params

@dataclass
class QueryMetrics:
    """Query execution metrics"""
    execution_time: float = 0.0
    rows_affected: int = 0
    rows_returned: int = 0
    connection_time: float = 0.0
    cache_hit: bool = False
    prepared_statement: bool = False

class ConnectionPool:
    """Thread-safe MySQL connection pool"""
    
    def __init__(self, config: ConnectionConfig):
        self.config = config
        self.pool = queue.Queue(maxsize=config.max_connections)
        self.created_connections = 0
        self._lock = threading.RLock()
        
        # Pre-populate pool with minimum connections
        for _ in range(config.min_connections):
            conn = self._create_connection()
            if conn:
                self.pool.put(conn)
    
    def _create_connection(self):
        """Create a new MySQL connection"""
        try:
            connection_params = self.config.to_connection_params()
            conn = pymysql.connect(**connection_params)
            
            # Set session variables
            with conn.cursor() as cursor:
                cursor.execute(f"SET sql_mode = '{self.config.sql_mode}'")
                cursor.execute("SET time_zone = '+00:00'")  # Use UTC
                cursor.execute("SET SESSION wait_timeout = 3600")  # 1 hour
                cursor.execute("SET SESSION interactive_timeout = 3600")
            
            conn.commit()
            self.created_connections += 1
            logger.debug(f"Created new MySQL connection ({self.created_connections} total)")
            return conn
            
        except Exception as e:
            logger.error(f"Failed to create MySQL connection: {e}")
            return None
    
    def get_connection(self, timeout: int = 30):
        """Get connection from pool"""
        try:
            # Try to get connection from pool
            try:
                conn = self.pool.get(timeout=timeout)
                # Test connection
                conn.ping(reconnect=True)
                return conn
            except queue.Empty:
                # Pool is empty, create new connection if under limit
                with self._lock:
                    if self.created_connections < self.config.max_connections:
                        conn = self._create_connection()
                        if conn:
                            return conn
                        
                # Still no connection available
                raise RuntimeError("No connections available and max pool size reached")
                
        except Exception as e:
            logger.error(f"Failed to get connection from pool: {e}")
            raise
    
    def return_connection(self, conn):
        """Return connection to pool"""
        try:
            if conn and conn.open:
                self.pool.put_nowait(conn)
            else:
                # Connection is closed, create replacement if needed
                with self._lock:
                    if self.pool.qsize() < self.config.min_connections:
                        new_conn = self._create_connection()
                        if new_conn:
                            self.pool.put_nowait(new_conn)
        except queue.Full:
            # Pool is full, close connection
            try:
                conn.close()
            except:
                pass
        except Exception as e:
            logger.warning(f"Error returning connection to pool: {e}")
    
    def close_all(self):
        """Close all connections in pool"""
        while not self.pool.empty():
            try:
                conn = self.pool.get_nowait()
                conn.close()
            except queue.Empty:
                break
            except Exception as e:
                logger.warning(f"Error closing pooled connection: {e}")

class PreparedStatementCache:
    """Cache for prepared statements"""
    
    def __init__(self, max_size: int = 100):
        self.max_size = max_size
        self.cache = {}
        self.usage_count = {}
        self._lock = threading.RLock()
    
    def get_statement_id(self, sql: str) -> str:
        """Generate unique statement ID for SQL"""
        import hashlib
        return f"saiql_stmt_{hashlib.md5(sql.encode()).hexdigest()[:16]}"
    
    def is_cached(self, sql: str) -> bool:
        """Check if statement is cached"""
        with self._lock:
            stmt_id = self.get_statement_id(sql)
            return stmt_id in self.cache
    
    def add_statement(self, sql: str, cursor) -> str:
        """Add prepared statement to cache"""
        with self._lock:
            stmt_id = self.get_statement_id(sql)
            
            if len(self.cache) >= self.max_size:
                # Remove least used statement
                least_used = min(self.usage_count.items(), key=lambda x: x[1])
                old_stmt_id = least_used[0]
                try:
                    cursor.execute(f"DEALLOCATE PREPARE {old_stmt_id}")
                except Exception:
                    pass  # Statement might not exist
                del self.cache[old_stmt_id]
                del self.usage_count[old_stmt_id]
            
            # Prepare statement
            try:
                cursor.execute(f"PREPARE {stmt_id} FROM %s", (sql,))
                self.cache[stmt_id] = sql
                self.usage_count[stmt_id] = 0
                logger.debug(f"Prepared statement: {stmt_id}")
            except Exception as e:
                logger.warning(f"Failed to prepare statement: {e}")
                return None
            
            return stmt_id
    
    def use_statement(self, sql: str) -> Optional[str]:
        """Mark statement as used and return statement ID"""
        with self._lock:
            stmt_id = self.get_statement_id(sql)
            if stmt_id in self.cache:
                self.usage_count[stmt_id] += 1
                return stmt_id
            return None

class MySQLAdapter:
    """
    Production-ready MySQL adapter for SAIQL
    
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
        """Initialize MySQL adapter"""
        # Set up configuration
        if config:
            self.config = config
        else:
            self.config = ConnectionConfig(**kwargs)
        
        # Initialize state
        self.state = ConnectionState.DISCONNECTED
        self.pool = None
        self.prepared_statements = PreparedStatementCache()
        
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
        
        logger.info(f"MySQL adapter initialized for {self.config.host}:{self.config.port}/{self.config.database}")
    
    def _initialize_pool(self):
        """Initialize connection pool"""
        try:
            if self.pool:
                self.pool.close_all()
            
            self.pool = ConnectionPool(self.config)
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
    def get_connection(self):
        """Get connection from pool with automatic cleanup"""
        connection = None
        start_time = time.time()
        
        try:
            if not self.pool:
                raise RuntimeError("Connection pool not initialized")
            
            connection = self.pool.get_connection(timeout=self.config.connection_timeout)
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
                    self.pool.return_connection(connection)
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
                          Default False because MySQL prepared statements are
                          connection-scoped and cannot be shared across pooled
                          connections. PyMySQL's cursor.execute() handles
                          parameter binding correctly at the driver level.

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
                with self.get_connection() as (connection, conn_time):
                    metrics.connection_time = conn_time
                    
                    with connection.cursor() as cursor:
                        query_start = time.time()

                        # Server-side prepared statements are disabled - they are
                        # connection-scoped in MySQL and cannot work with connection pooling
                        if use_prepared:
                            raise ValueError(
                                "use_prepared=True is not supported with connection pooling. "
                                "MySQL prepared statements are connection-scoped and cannot be "
                                "shared across pooled connections. Use use_prepared=False (default)."
                            )

                        cursor.execute(sql, params)
                        
                        metrics.execution_time_db = time.time() - query_start
                        
                        # Fetch results if requested
                        if fetch and cursor.description:
                            result['data'] = cursor.fetchall()
                            metrics.rows_returned = len(result['data'])
                        
                        metrics.rows_affected = cursor.rowcount if cursor.rowcount > 0 else 0

                    # Commit DML statements when autocommit is disabled
                    if not self.config.autocommit and self._is_dml_statement(sql):
                        connection.commit()

                result['success'] = True
                self.stats['queries_executed'] += 1
                break
                
            except IntegrityError as e:
                # Integrity errors (constraint violations) should NOT be retried
                result['error'] = f"Integrity error: {str(e)}"
                self.stats['failed_queries'] += 1
                break

            except (OperationalError, DatabaseError, MySQLError) as e:
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

    def _is_dml_statement(self, sql: str) -> bool:
        """Check if SQL is a DML statement that modifies data (needs commit)."""
        sql_upper = sql.strip().upper()
        return any(sql_upper.startswith(cmd) for cmd in ['INSERT', 'UPDATE', 'DELETE', 'REPLACE'])
    
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
                connection.begin()
                
                try:
                    with connection.cursor() as cursor:
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
        Get list of user tables in current database (L0 method).

        Returns:
            List of table names (lowercase)
        """
        query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = DATABASE()
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """

        result = self.execute_query(query)

        if not result['success']:
            logger.error(f"Failed to get tables: {result.get('error')}")
            return []

        tables = [(row.get('table_name') or row.get('TABLE_NAME', '')).lower() for row in result['data']]
        logger.debug(f"Found {len(tables)} tables: {tables}")

        return tables

    def get_schema(self, table_name: str) -> Dict[str, Any]:
        """
        Get table schema introspection (L0 method).

        Args:
            table_name: Table name

        Returns:
            Schema dict with columns, pk, fks
        """
        schema = {
            'columns': [],
            'pk': [],
            'fks': []
        }

        col_query = """
            SELECT
                COLUMN_NAME,
                DATA_TYPE,
                CHARACTER_MAXIMUM_LENGTH,
                NUMERIC_PRECISION,
                NUMERIC_SCALE,
                IS_NULLABLE,
                COLUMN_DEFAULT
            FROM information_schema.columns
            WHERE table_schema = DATABASE()
            AND table_name = %s
            ORDER BY ORDINAL_POSITION
        """

        result = self.execute_query(col_query, (table_name,))

        if not result['success']:
            logger.error(f"Failed to get schema for {table_name}: {result.get('error')}")
            return schema

        from core.type_registry import TypeRegistry, IRType

        for col in result['data']:
            col_name = col.get('column_name') or col.get('COLUMN_NAME')
            data_type = col.get('data_type') or col.get('DATA_TYPE')
            char_len = col.get('character_maximum_length') or col.get('CHARACTER_MAXIMUM_LENGTH')
            num_prec = col.get('numeric_precision') or col.get('NUMERIC_PRECISION')
            num_scale = col.get('numeric_scale') or col.get('NUMERIC_SCALE')
            is_nullable = col.get('is_nullable') or col.get('IS_NULLABLE')
            col_default = col.get('column_default') or col.get('COLUMN_DEFAULT')

            full_type_str = data_type
            if char_len:
                full_type_str = f"{data_type}({char_len})"
            elif num_prec is not None:
                if num_scale is not None and num_scale > 0:
                    full_type_str = f"{data_type}({num_prec},{num_scale})"

            type_info = TypeRegistry.map_to_ir('mysql', full_type_str)

            if type_info.ir_type == IRType.UNKNOWN:
                logger.warning(f"Table {table_name}.{col_name}: Unsupported MySQL type '{data_type}'.")

            schema['columns'].append({
                'name': col_name.lower(),
                'type': data_type,
                'type_info': type_info,
                'unsupported': type_info.ir_type == IRType.UNKNOWN,
                'length': char_len,
                'precision': num_prec,
                'scale': num_scale,
                'nullable': is_nullable == 'YES',
                'default': col_default
            })

        return schema

    def extract_data(
        self,
        table_name: str,
        order_by: Optional[List[str]] = None,
        chunk_size: int = 10000
    ) -> Dict[str, Any]:
        """
        Extract data from table with deterministic ordering (L0 method).
        """
        start_time = time.time()

        if order_by:
            order_clause = ', '.join([f'`{col}`' for col in order_by])
        else:
            schema = self.get_schema(table_name)
            if schema['columns']:
                first_col = schema['columns'][0]['name']
                order_clause = f'`{first_col}`'
            else:
                order_clause = ""

        if order_clause:
            query = f'SELECT * FROM `{table_name}` ORDER BY {order_clause}'
        else:
            query = f'SELECT * FROM `{table_name}`'

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
        """Get primary key columns for a table (L1 method)."""
        query = """
            SELECT COLUMN_NAME
            FROM information_schema.key_column_usage
            WHERE table_schema = DATABASE()
            AND table_name = %s
            AND CONSTRAINT_NAME = 'PRIMARY'
            ORDER BY ORDINAL_POSITION
        """

        result = self.execute_query(query, (table_name,))

        if not result['success']:
            logger.error(f"Failed to get primary keys for {table_name}: {result.get('error')}")
            return []

        return [row.get('column_name') or row.get('COLUMN_NAME') for row in result['data']]

    def get_foreign_keys(self, table_name: str) -> List[Dict[str, Any]]:
        """Get foreign key constraints for a table (L1 method)."""
        query = """
            SELECT
                kcu.COLUMN_NAME,
                kcu.REFERENCED_TABLE_NAME AS ref_table,
                kcu.REFERENCED_COLUMN_NAME AS ref_column,
                kcu.CONSTRAINT_NAME
            FROM information_schema.key_column_usage kcu
            WHERE kcu.table_schema = DATABASE()
            AND kcu.table_name = %s
            AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
        """

        result = self.execute_query(query, (table_name,))

        if not result['success']:
            logger.error(f"Failed to get foreign keys for {table_name}: {result.get('error')}")
            return []

        return [{
            'column': row.get('column_name') or row.get('COLUMN_NAME'),
            'ref_table': row.get('ref_table') or row.get('REFERENCED_TABLE_NAME'),
            'ref_column': row.get('ref_column') or row.get('REFERENCED_COLUMN_NAME'),
            'constraint_name': row.get('constraint_name') or row.get('CONSTRAINT_NAME')
        } for row in result['data']]

    def get_unique_constraints(self, table_name: str) -> List[Dict[str, Any]]:
        """Get unique constraints for a table (L1 method)."""
        query = """
            SELECT
                tc.CONSTRAINT_NAME,
                GROUP_CONCAT(kcu.COLUMN_NAME ORDER BY kcu.ORDINAL_POSITION) AS columns
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                AND tc.table_schema = kcu.table_schema
                AND tc.table_name = kcu.table_name
            WHERE tc.CONSTRAINT_TYPE = 'UNIQUE'
            AND tc.table_schema = DATABASE()
            AND tc.table_name = %s
            GROUP BY tc.CONSTRAINT_NAME
        """

        result = self.execute_query(query, (table_name,))

        if not result['success']:
            logger.error(f"Failed to get unique constraints for {table_name}: {result.get('error')}")
            return []

        return [{
            'constraint_name': row.get('constraint_name') or row.get('CONSTRAINT_NAME'),
            'columns': (row.get('columns') or row.get('COLUMNS', '')).split(',')
        } for row in result['data']]

    def get_indexes(self, table_name: str) -> List[Dict[str, Any]]:
        """Get indexes for a table (L1 method)."""
        query = """
            SELECT
                INDEX_NAME,
                GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) AS columns,
                NOT NON_UNIQUE AS is_unique
            FROM information_schema.statistics
            WHERE table_schema = DATABASE()
            AND table_name = %s
            GROUP BY INDEX_NAME, NON_UNIQUE
            ORDER BY INDEX_NAME
        """

        result = self.execute_query(query, (table_name,))

        if not result['success']:
            logger.error(f"Failed to get indexes for {table_name}: {result.get('error')}")
            return []

        return [{
            'name': row.get('index_name') or row.get('INDEX_NAME'),
            'columns': (row.get('columns') or row.get('COLUMNS', '')).split(','),
            'is_unique': bool(row.get('is_unique') or row.get('IS_UNIQUE')),
            'is_primary': (row.get('index_name') or row.get('INDEX_NAME')) == 'PRIMARY'
        } for row in result['data']]

    # =========================================================================
    # L2 Methods (Views)
    # =========================================================================

    def get_views(self, database: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get list of views in the database (L2 method).

        Args:
            database: Database name (default: current database)

        Returns:
            List of view dictionaries with name, definition, definer, security_type
        """
        db_clause = "= %s" if database else "= DATABASE()"
        params = (database,) if database else ()

        query = f"""
            SELECT
                TABLE_NAME as view_name,
                VIEW_DEFINITION as definition,
                DEFINER as definer,
                SECURITY_TYPE as security_type,
                CHARACTER_SET_CLIENT as charset,
                COLLATION_CONNECTION as collation
            FROM information_schema.VIEWS
            WHERE TABLE_SCHEMA {db_clause}
            ORDER BY TABLE_NAME
        """

        result = self.execute_query(query, params)

        if not result['success']:
            logger.error(f"Failed to get views: {result.get('error')}")
            return []

        views = []
        for row in result['data']:
            views.append({
                'name': row.get('view_name') or row.get('VIEW_NAME'),
                'schema': database or self.config.database,
                'database': database or self.config.database,
                'definition': row.get('definition') or row.get('VIEW_DEFINITION'),
                'definer': row.get('definer') or row.get('DEFINER'),
                'security_type': row.get('security_type') or row.get('SECURITY_TYPE'),
                'charset': row.get('charset') or row.get('CHARACTER_SET_CLIENT'),
                'collation': row.get('collation') or row.get('COLLATION_CONNECTION')
            })

        logger.debug(f"Found {len(views)} views")
        return views

    def get_view_definition(self, view_name: str, database: Optional[str] = None) -> Optional[str]:
        """
        Get the SQL definition of a view (L2 method).

        Args:
            view_name: View name
            database: Database name (default: current database)

        Returns:
            View definition SQL or None
        """
        db_clause = "= %s" if database else "= DATABASE()"
        params = (view_name, database) if database else (view_name,)

        query = f"""
            SELECT VIEW_DEFINITION as definition
            FROM information_schema.VIEWS
            WHERE TABLE_NAME = %s AND TABLE_SCHEMA {db_clause}
        """

        result = self.execute_query(query, params)

        if not result['success'] or not result['data']:
            return None

        return result['data'][0].get('definition') or result['data'][0].get('VIEW_DEFINITION')

    def get_view_dependencies(self, view_name: str, database: Optional[str] = None) -> List[Dict[str, str]]:
        """
        Get dependencies of a view (L2 method).
        MySQL doesn't have native dependency tracking, so we parse from definition.

        Args:
            view_name: View name
            database: Database name

        Returns:
            List of dependency dictionaries with type, name
        """
        definition = self.get_view_definition(view_name, database)
        if not definition:
            return []

        # Get list of all tables and views to check against
        tables = set(self.get_tables())
        views = {v['name'] for v in self.get_views(database)}

        deps = []
        # Simple parsing - look for FROM and JOIN clauses
        import re
        # Match table/view names after FROM or JOIN
        # Handle: parentheses, fully qualified names like `schema`.`table`, aliases
        pattern = r'(?:FROM|JOIN)\s*\(?(?:`?[\w]+`?\.)?`?(\w+)`?'
        matches = re.findall(pattern, definition, re.IGNORECASE)

        for match in matches:
            if match != view_name:  # Don't include self
                if match in views:
                    deps.append({'type': 'view', 'name': match})
                elif match in tables:
                    deps.append({'type': 'table', 'name': match})

        return deps

    def get_views_in_dependency_order(self, database: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get views sorted by dependency order (L2 method).

        Args:
            database: Database name

        Returns:
            List of views in creation order (dependencies first)
        """
        views = self.get_views(database)

        # Build dependency graph
        view_names = {v['name'] for v in views}
        dependencies = {}

        for view in views:
            deps = self.get_view_dependencies(view['name'], database)
            # Only track dependencies on other views
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

    def create_view(self, name: str, definition: str, database: Optional[str] = None,
                    or_replace: bool = True, definer: Optional[str] = None) -> Dict[str, Any]:
        """
        Create a view from definition (L2 emission method).

        Args:
            name: View name
            definition: SELECT statement
            database: Database name
            or_replace: Use CREATE OR REPLACE
            definer: Optional DEFINER clause (default: strip for portability)

        Returns:
            Result dict with success status
        """
        definition = definition.strip()

        # Build CREATE VIEW statement
        create_type = "CREATE OR REPLACE" if or_replace else "CREATE"
        qualified_name = f"`{database}`.`{name}`" if database else f"`{name}`"

        # Strip DEFINER for portability by default
        if definer:
            sql = f"{create_type} DEFINER={definer} VIEW {qualified_name} AS {definition}"
        else:
            sql = f"{create_type} VIEW {qualified_name} AS {definition}"

        result = self.execute_query(sql)

        if result['success']:
            logger.info(f"Created view {name}")
        else:
            logger.error(f"Failed to create view {name}: {result.get('error')}")

        return result

    def drop_view(self, name: str, database: Optional[str] = None,
                  if_exists: bool = True) -> Dict[str, Any]:
        """
        Drop a view (L2 emission method).

        Args:
            name: View name
            database: Database name
            if_exists: Use IF EXISTS

        Returns:
            Result dict with success status
        """
        qualified_name = f"`{database}`.`{name}`" if database else f"`{name}`"
        sql = f"DROP VIEW {'IF EXISTS ' if if_exists else ''}{qualified_name}"

        result = self.execute_query(sql)

        if result['success']:
            logger.info(f"Dropped view {name}")

        return result

    def create_views_in_order(self, views: List[Dict[str, Any]],
                               database: Optional[str] = None) -> Dict[str, Any]:
        """
        Create multiple views in dependency order (L2 emission method).

        Args:
            views: List of view dicts with 'name' and 'definition'
            database: Target database

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

            result = self.create_view(name, definition, database)

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
    # L3 Methods (Routines - Stored Procedures/Functions)
    # =========================================================================

    def get_routines(self, database: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get list of stored routines (L3 method).

        Args:
            database: Database name (default: current database)

        Returns:
            List of routine dictionaries
        """
        db_clause = "= %s" if database else "= DATABASE()"
        params = (database,) if database else ()

        query = f"""
            SELECT
                ROUTINE_NAME as name,
                ROUTINE_SCHEMA as `schema`,
                ROUTINE_TYPE as routine_type,
                DATA_TYPE as return_type,
                DTD_IDENTIFIER as return_full_type,
                ROUTINE_DEFINITION as body,
                IS_DETERMINISTIC as is_deterministic,
                SQL_DATA_ACCESS as data_access,
                SECURITY_TYPE as security_type,
                DEFINER as definer,
                PARAMETER_STYLE as param_style,
                SQL_MODE as sql_mode
            FROM information_schema.ROUTINES
            WHERE ROUTINE_SCHEMA {db_clause}
            ORDER BY ROUTINE_TYPE, ROUTINE_NAME
        """

        result = self.execute_query(query, params)

        if not result['success']:
            logger.error(f"Failed to get routines: {result.get('error')}")
            return []

        routines = []
        for row in result['data']:
            security = row.get('security_type') or row.get('SECURITY_TYPE')
            routines.append({
                'name': row.get('name') or row.get('ROUTINE_NAME'),
                'schema': row.get('schema') or row.get('ROUTINE_SCHEMA'),
                'type': row.get('routine_type') or row.get('ROUTINE_TYPE'),
                'return_type': row.get('return_type') or row.get('DATA_TYPE'),
                'return_full_type': row.get('return_full_type') or row.get('DTD_IDENTIFIER'),
                'body': row.get('body') or row.get('ROUTINE_DEFINITION'),
                'is_deterministic': (row.get('is_deterministic') or row.get('IS_DETERMINISTIC')) == 'YES',
                'data_access': row.get('data_access') or row.get('SQL_DATA_ACCESS'),
                'security_type': security,
                'sql_security': security,  # Alias for compatibility
                'definer': row.get('definer') or row.get('DEFINER'),
                'sql_mode': row.get('sql_mode') or row.get('SQL_MODE')
            })

        logger.debug(f"Found {len(routines)} routines")
        return routines

    def get_routine_definition(self, routine_name: str, routine_type: str = 'FUNCTION',
                                database: Optional[str] = None) -> Optional[str]:
        """
        Get the full CREATE statement for a routine (L3 method).

        Args:
            routine_name: Routine name
            routine_type: 'FUNCTION' or 'PROCEDURE'
            database: Database name

        Returns:
            Full CREATE statement or None
        """
        qualified_name = f"`{database}`.`{routine_name}`" if database else f"`{routine_name}`"

        query = f"SHOW CREATE {routine_type} {qualified_name}"

        result = self.execute_query(query)

        if not result['success'] or not result['data']:
            return None

        row = result['data'][0]
        # Column name varies: 'Create Function' or 'Create Procedure'
        for key in row:
            if 'create' in key.lower():
                return row[key]

        return None

    def get_routine_parameters(self, routine_name: str, database: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get parameters for a routine (L3 method).

        Args:
            routine_name: Routine name
            database: Database name

        Returns:
            List of parameter dictionaries
        """
        db_clause = "= %s" if database else "= DATABASE()"
        params = (routine_name, database) if database else (routine_name,)

        query = f"""
            SELECT
                PARAMETER_NAME as name,
                PARAMETER_MODE as mode,
                DATA_TYPE as data_type,
                DTD_IDENTIFIER as full_type,
                ORDINAL_POSITION as position
            FROM information_schema.PARAMETERS
            WHERE SPECIFIC_NAME = %s AND SPECIFIC_SCHEMA {db_clause}
            ORDER BY ORDINAL_POSITION
        """

        result = self.execute_query(query, params)

        if not result['success']:
            return []

        return [{
            'name': row.get('name') or row.get('PARAMETER_NAME'),
            'mode': row.get('mode') or row.get('PARAMETER_MODE'),
            'data_type': row.get('data_type') or row.get('DATA_TYPE'),
            'full_type': row.get('full_type') or row.get('DTD_IDENTIFIER'),
            'position': row.get('position') or row.get('ORDINAL_POSITION')
        } for row in result['data']]

    def get_safe_routines(self, database: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get routines that are safe to migrate (L3 subset method).

        Safe subset criteria:
        - IS_DETERMINISTIC = YES (functions only; procedures exempt)
        - SQL_DATA_ACCESS in (NO SQL, CONTAINS SQL, READS SQL DATA)
        - Not MODIFIES SQL DATA
        - SQL_SECURITY = INVOKER (DEFINER is privilege escalation risk)
        - No dynamic SQL patterns (PREPARE/EXECUTE)

        Args:
            database: Database name

        Returns:
            List of safe routine dictionaries
        """
        all_routines = self.get_routines(database)

        safe_routines = []
        skipped = []

        for routine in all_routines:
            reasons = []

            # Check deterministic - but READS SQL DATA procedures are OK
            # (Procedures cannot be DETERMINISTIC in MySQL, only functions)
            is_read_only = routine['data_access'] in ('READS SQL DATA', 'NO SQL', 'CONTAINS SQL')
            is_procedure = routine['type'] == 'PROCEDURE'

            if not routine['is_deterministic'] and not (is_procedure and is_read_only):
                reasons.append("not deterministic")

            # Check data access - MODIFIES SQL DATA is risky
            if routine['data_access'] == 'MODIFIES SQL DATA':
                reasons.append("modifies SQL data")

            # Check SQL SECURITY - DEFINER is privilege escalation risk
            sql_security = routine.get('sql_security') or routine.get('security_type') or ''
            if sql_security.upper() == 'DEFINER':
                reasons.append("SQL SECURITY DEFINER (privilege escalation risk)")

            # Check for dynamic SQL patterns in body
            body = routine.get('body') or ''
            if 'PREPARE' in body.upper() or 'EXECUTE' in body.upper():
                reasons.append("contains dynamic SQL")

            if reasons:
                routine['skip_reasons'] = reasons
                skipped.append(routine)
            else:
                safe_routines.append(routine)

        logger.info(f"Safe routines: {len(safe_routines)}, Skipped: {len(skipped)}")
        return safe_routines

    def get_skipped_routines(self, database: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get routines that are NOT safe to migrate with skip reasons (L3 subset method).

        Args:
            database: Database name

        Returns:
            List of skipped routine dictionaries with skip_reasons
        """
        all_routines = self.get_routines(database)
        skipped = []

        for routine in all_routines:
            reasons = []

            # Check deterministic - but READS SQL DATA procedures are OK
            is_read_only = routine['data_access'] in ('READS SQL DATA', 'NO SQL', 'CONTAINS SQL')
            is_procedure = routine['type'] == 'PROCEDURE'

            if not routine['is_deterministic'] and not (is_procedure and is_read_only):
                reasons.append("not deterministic")

            if routine['data_access'] == 'MODIFIES SQL DATA':
                reasons.append("modifies SQL data")

            # Check SQL SECURITY - DEFINER is privilege escalation risk
            sql_security = routine.get('sql_security') or routine.get('security_type') or ''
            if sql_security.upper() == 'DEFINER':
                reasons.append("SQL SECURITY DEFINER (privilege escalation risk)")

            body = routine.get('body') or ''
            if 'PREPARE' in body.upper() or 'EXECUTE' in body.upper():
                reasons.append("contains dynamic SQL")

            if reasons:
                routine['skip_reasons'] = reasons
                skipped.append(routine)

        return skipped

    def create_routine(self, definition: str) -> Dict[str, Any]:
        """
        Create a routine from its full definition (L3 emission method).

        DEFINER clause is stripped for portability per collab rules.

        Args:
            definition: Full CREATE FUNCTION/PROCEDURE statement

        Returns:
            Result dict with success status
        """
        # Strip DEFINER clause for portability
        sql = strip_definer(definition.strip())

        result = self.execute_query(sql)

        if result['success']:
            logger.info("Created routine successfully (DEFINER stripped)")
        else:
            logger.error(f"Failed to create routine: {result.get('error')}")

        return result

    def drop_routine(self, name: str, routine_type: str = 'FUNCTION',
                     database: Optional[str] = None, if_exists: bool = True) -> Dict[str, Any]:
        """
        Drop a routine (L3 emission method).

        Args:
            name: Routine name
            routine_type: 'FUNCTION' or 'PROCEDURE'
            database: Database name
            if_exists: Use IF EXISTS

        Returns:
            Result dict with success status
        """
        qualified_name = f"`{database}`.`{name}`" if database else f"`{name}`"
        sql = f"DROP {routine_type} {'IF EXISTS ' if if_exists else ''}{qualified_name}"

        result = self.execute_query(sql)

        if result['success']:
            logger.info(f"Dropped {routine_type.lower()} {name}")

        return result

    def create_routines_in_order(self, routines: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Create multiple routines from their definitions (L3 emission method).

        Args:
            routines: List of routine dicts with 'definition' key

        Returns:
            Result dict with created/failed counts
        """
        results = {
            'success': True,
            'created': [],
            'failed': [],
            'total': len(routines)
        }

        for routine in routines:
            definition = routine.get('definition', '')
            name = routine.get('name', 'unknown')

            result = self.create_routine(definition)

            if result['success']:
                results['created'].append(name)
            else:
                results['failed'].append({
                    'name': name,
                    'error': result.get('error')
                })
                results['success'] = False

        logger.info(f"Created {len(results['created'])}/{results['total']} routines")
        return results

    # =========================================================================
    # L4 Methods (Triggers)
    # =========================================================================

    def get_triggers(self, database: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get list of triggers (L4 method).

        Args:
            database: Database name (default: current database)

        Returns:
            List of trigger dictionaries
        """
        db_clause = "= %s" if database else "= DATABASE()"
        params = (database,) if database else ()

        query = f"""
            SELECT
                TRIGGER_NAME as name,
                TRIGGER_SCHEMA as `schema`,
                EVENT_MANIPULATION as event,
                EVENT_OBJECT_TABLE as table_name,
                ACTION_TIMING as timing,
                ACTION_STATEMENT as body,
                ACTION_ORIENTATION as orientation,
                DEFINER as definer,
                SQL_MODE as sql_mode
            FROM information_schema.TRIGGERS
            WHERE TRIGGER_SCHEMA {db_clause}
            ORDER BY EVENT_OBJECT_TABLE, ACTION_TIMING, EVENT_MANIPULATION
        """

        result = self.execute_query(query, params)

        if not result['success']:
            logger.error(f"Failed to get triggers: {result.get('error')}")
            return []

        triggers = []
        for row in result['data']:
            body = row.get('body') or row.get('ACTION_STATEMENT')
            triggers.append({
                'name': row.get('name') or row.get('TRIGGER_NAME'),
                'schema': row.get('schema') or row.get('TRIGGER_SCHEMA'),
                'event': row.get('event') or row.get('EVENT_MANIPULATION'),
                'table_name': row.get('table_name') or row.get('EVENT_OBJECT_TABLE'),
                'timing': row.get('timing') or row.get('ACTION_TIMING'),
                'body': body,
                'statement': body,  # Alias for compatibility
                'orientation': row.get('orientation') or row.get('ACTION_ORIENTATION'),
                'definer': row.get('definer') or row.get('DEFINER'),
                'sql_mode': row.get('sql_mode') or row.get('SQL_MODE')
            })

        logger.debug(f"Found {len(triggers)} triggers")
        return triggers

    def get_trigger_definition(self, trigger_name: str, database: Optional[str] = None) -> Optional[str]:
        """
        Get the full CREATE TRIGGER statement (L4 method).

        Args:
            trigger_name: Trigger name
            database: Database name

        Returns:
            Full CREATE TRIGGER statement or None
        """
        qualified_name = f"`{database}`.`{trigger_name}`" if database else f"`{trigger_name}`"

        query = f"SHOW CREATE TRIGGER {qualified_name}"

        result = self.execute_query(query)

        if not result['success'] or not result['data']:
            return None

        row = result['data'][0]
        # Column is 'SQL Original Statement'
        if 'SQL Original Statement' in row:
            return row['SQL Original Statement']
        # Fallback: look for column containing 'statement'
        for key in row:
            if 'statement' in key.lower():
                return row[key]

        return None

    def get_safe_triggers(self, database: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get triggers that are safe to migrate (L4 subset method).

        Safe subset criteria:
        - ROW level triggers (orientation = ROW)
        - BEFORE or AFTER timing
        - No dynamic SQL in body
        - No calls to unsafe routines

        Args:
            database: Database name

        Returns:
            List of safe trigger dictionaries
        """
        all_triggers = self.get_triggers(database)

        safe_triggers = []
        skipped = []

        for trigger in all_triggers:
            reasons = []

            # Check orientation (MySQL triggers are always ROW level, but verify)
            if trigger.get('orientation') and trigger['orientation'] != 'ROW':
                reasons.append("not row-level trigger")

            # AFTER triggers may have side effects - skip by default per L4 rules
            if trigger.get('timing') == 'AFTER':
                reasons.append("AFTER trigger (may have side effects)")

            # Check for dynamic SQL patterns in body
            body = trigger.get('body') or ''
            if 'PREPARE' in body.upper() or 'EXECUTE IMMEDIATE' in body.upper():
                reasons.append("contains dynamic SQL")

            # Check for calls to potentially unsafe functions
            unsafe_patterns = ['SLEEP(', 'BENCHMARK(', 'LOAD_FILE(', 'INTO OUTFILE', 'INTO DUMPFILE']
            for pattern in unsafe_patterns:
                if pattern in body.upper():
                    reasons.append(f"contains unsafe pattern: {pattern}")
                    break

            if reasons:
                trigger['skip_reasons'] = reasons
                skipped.append(trigger)
            else:
                safe_triggers.append(trigger)

        logger.info(f"Safe triggers: {len(safe_triggers)}, Skipped: {len(skipped)}")
        return safe_triggers

    def get_skipped_triggers(self, database: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get triggers that are NOT safe to migrate with skip reasons (L4 subset method).

        Args:
            database: Database name

        Returns:
            List of skipped trigger dictionaries with skip_reasons
        """
        all_triggers = self.get_triggers(database)
        skipped = []

        for trigger in all_triggers:
            reasons = []

            if trigger.get('orientation') and trigger['orientation'] != 'ROW':
                reasons.append("not row-level trigger")

            # AFTER triggers may have side effects - skip by default per L4 rules
            if trigger.get('timing') == 'AFTER':
                reasons.append("AFTER trigger (may have side effects)")

            body = trigger.get('body') or ''
            if 'PREPARE' in body.upper() or 'EXECUTE IMMEDIATE' in body.upper():
                reasons.append("contains dynamic SQL")

            unsafe_patterns = ['SLEEP(', 'BENCHMARK(', 'LOAD_FILE(', 'INTO OUTFILE', 'INTO DUMPFILE']
            for pattern in unsafe_patterns:
                if pattern in body.upper():
                    reasons.append(f"contains unsafe pattern: {pattern}")
                    break

            if reasons:
                trigger['skip_reasons'] = reasons
                skipped.append(trigger)

        return skipped

    def create_trigger(self, definition: str) -> Dict[str, Any]:
        """
        Create a trigger from its full definition (L4 emission method).

        DEFINER clause is stripped for portability per collab rules.

        Args:
            definition: Full CREATE TRIGGER statement

        Returns:
            Result dict with success status
        """
        # Strip DEFINER clause for portability
        sql = strip_definer(definition.strip())

        result = self.execute_query(sql)

        if result['success']:
            logger.info("Created trigger successfully (DEFINER stripped)")
        else:
            logger.error(f"Failed to create trigger: {result.get('error')}")

        return result

    def drop_trigger(self, name: str, database: Optional[str] = None,
                     if_exists: bool = True) -> Dict[str, Any]:
        """
        Drop a trigger (L4 emission method).

        Args:
            name: Trigger name
            database: Database name
            if_exists: Use IF EXISTS

        Returns:
            Result dict with success status
        """
        qualified_name = f"`{database}`.`{name}`" if database else f"`{name}`"
        sql = f"DROP TRIGGER {'IF EXISTS ' if if_exists else ''}{qualified_name}"

        result = self.execute_query(sql)

        if result['success']:
            logger.info(f"Dropped trigger {name}")

        return result

    def create_triggers_in_order(self, triggers: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Create multiple triggers from their definitions (L4 emission method).

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
        logger.info("Closing MySQL adapter")

        try:
            if self.pool:
                self.pool.close_all()
                self.stats['connections_closed'] += self.config.max_connections
                self.pool = None

            self.state = ConnectionState.DISCONNECTED

            # Log final statistics
            stats = self.get_statistics()
            logger.info(f"MySQL adapter closed. Final stats: {json.dumps(stats, indent=2)}")

        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

# Utility functions
def create_adapter_from_url(database_url: str, **kwargs) -> MySQLAdapter:
    """Create adapter from database URL"""
    parsed = urlparse(database_url)
    
    config = ConnectionConfig(
        host=parsed.hostname or 'localhost',
        port=parsed.port or 3306,
        database=parsed.path.lstrip('/') if parsed.path else 'mysql',
        user=parsed.username or 'root',
        password=parsed.password or '',
        **kwargs
    )
    
    return MySQLAdapter(config)

def test_connection(config: ConnectionConfig) -> bool:
    """Test MySQL connection"""
    try:
        adapter = MySQLAdapter(config)
        result = adapter.execute_query("SELECT VERSION() as version")
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
        user="root",
        password="password",
        min_connections=2,
        max_connections=10
    )
    
    try:
        # Test the adapter
        adapter = MySQLAdapter(config)
        
        # Test simple query
        result = adapter.execute_query("SELECT NOW() as current_time")
        print(f"Query result: {result}")
        
        # Test transaction
        operations = [
            {"sql": "CREATE TEMPORARY TABLE test_table (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255))"},
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
