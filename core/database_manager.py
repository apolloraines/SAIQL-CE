#!/usr/bin/env python3
"""
SAIQL Database Manager - Multi-Backend Database Support

This module provides a unified interface for multiple database backends,
managing connections, adapters, and providing a consistent API for SAIQL operations.

Supported backends:
- SQLite (built-in)
- PostgreSQL (production-ready)
- MySQL (extensible)

Author: Apollo & Claude
Version: 1.0.0
Status: Production-Ready for SAIQL-Bravo

Usage:
    manager = DatabaseManager()
    result = manager.execute("SELECT * FROM users", backend="postgresql")
"""

import json
import logging
import os
import sqlite3
from typing import Dict, List, Any, Optional, Union, Type
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
import threading
import time
from contextlib import contextmanager

# Configure logging
logger = logging.getLogger(__name__)

class BackendType(Enum):
    """Supported database backend types (CE Edition)"""
    SQLITE = "sqlite"
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    FILE = "file"  # CSV/Excel support

@dataclass
class DatabaseResult:
    """Unified database result object"""
    success: bool
    data: List[Dict[str, Any]]
    rows_affected: int
    execution_time: float
    backend: str
    sql_executed: str
    error_message: Optional[str] = None
    metadata: Dict[str, Any] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'success': self.success,
            'data': self.data,
            'rows_affected': self.rows_affected,
            'execution_time': self.execution_time,
            'backend': self.backend,
            'sql_executed': self.sql_executed,
            'error_message': self.error_message,
            'metadata': self.metadata or {}
        }

class DatabaseAdapter:
    """Base class for database adapters"""
    
    def __init__(self, config: Dict[str, Any], firewall: Optional[Any] = None):
        self.config = config
        self.backend_type = config.get('type', 'unknown')
        self.firewall = firewall
        
    def execute_query(self, sql: str, params: Optional[tuple] = None) -> DatabaseResult:
        """Execute a query - to be implemented by subclasses"""
        raise NotImplementedError("Subclasses must implement execute_query")
        
    def execute_transaction(self, operations: List[Dict[str, Any]]) -> DatabaseResult:
        """Execute transaction - to be implemented by subclasses"""
        raise NotImplementedError("Subclasses must implement execute_transaction")
        
    def close(self):
        """Close connections - to be implemented by subclasses"""
        pass
        
    def get_statistics(self) -> Dict[str, Any]:
        """Get adapter statistics - to be implemented by subclasses"""
        return {}

class SQLiteAdapter(DatabaseAdapter):
    """SQLite database adapter"""
    
    def __init__(self, config: Dict[str, Any], firewall: Optional[Any] = None):
        super().__init__(config, firewall)
        self.db_path = config.get('path', 'data/lore_store.db')
        self.timeout = config.get('timeout', 30)
        self._lock = threading.RLock()
        self._ensure_database_exists()

        # QIPI not available in Community Edition
        self.qipi_index = None
        self.use_qipi = False

        # Statistics
        self.stats = {
            'queries_executed': 0,
            'failed_queries': 0,
            'total_execution_time': 0.0,
            'qipi_hits': 0,
            'qipi_misses': 0,
            'start_time': time.time()
        }
        
    def _ensure_database_exists(self):
        """Create database and directory if they don't exist"""
        dirname = os.path.dirname(self.db_path)
        if dirname:
            os.makedirs(dirname, exist_ok=True)

        # Create database with initial schema
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA cache_size=2000")

            # Only create demo tables if explicitly enabled in config
            # This prevents schema drift in production databases
            if self.config.get('create_demo_tables', False):
                conn.executescript("""
                    CREATE TABLE IF NOT EXISTS users (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT NOT NULL,
                        email TEXT UNIQUE,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );

                    CREATE TABLE IF NOT EXISTS orders (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id INTEGER,
                        product TEXT NOT NULL,
                        price DECIMAL(10,2),
                        quantity INTEGER DEFAULT 1,
                        order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (user_id) REFERENCES users (id)
                    );

                    CREATE TABLE IF NOT EXISTS products (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT NOT NULL,
                        price DECIMAL(10,2),
                        category TEXT,
                        stock INTEGER DEFAULT 0
                    );
                """)
    
    def execute_query(self, sql: str, params: Optional[tuple] = None) -> DatabaseResult:
        """Execute SQLite query"""
        start_time = time.time()
        
        try:
            with self._lock:
                with sqlite3.connect(self.db_path, timeout=self.timeout) as conn:
                    conn.row_factory = sqlite3.Row  # Enable column access by name
                    cursor = conn.cursor()
                    
                    if params:
                        cursor.execute(sql, params)
                    else:
                        cursor.execute(sql)
                    
                    # Fetch results if available
                    data = []
                    if cursor.description:
                        rows = cursor.fetchall()
                        data = [dict(row) for row in rows]
                        # For SELECT, rowcount is -1 in sqlite3; use len(data) instead
                        rows_affected = len(data)
                    else:
                        rows_affected = cursor.rowcount
                    
                    conn.commit()
            
            execution_time = time.time() - start_time
            self.stats['queries_executed'] += 1
            self.stats['total_execution_time'] += execution_time
            
            return DatabaseResult(
                success=True,
                data=data,
                rows_affected=rows_affected,
                execution_time=execution_time,
                backend="sqlite",
                sql_executed=sql
            )
            
        except Exception as e:
            execution_time = time.time() - start_time
            self.stats['failed_queries'] += 1
            
            return DatabaseResult(
                success=False,
                data=[],
                rows_affected=0,
                execution_time=execution_time,
                backend="sqlite",
                sql_executed=sql,
                error_message=str(e)
            )
    
    def execute_transaction(self, operations: List[Dict[str, Any]]) -> DatabaseResult:
        """Execute SQLite transaction"""
        start_time = time.time()
        
        try:
            with self._lock:
                with sqlite3.connect(self.db_path, timeout=self.timeout) as conn:
                    cursor = conn.cursor()
                    total_rows_affected = 0
                    
                    cursor.execute("BEGIN")
                    
                    try:
                        for operation in operations:
                            sql = operation.get('sql')
                            params = operation.get('params')
                            
                            if params:
                                cursor.execute(sql, params)
                            else:
                                cursor.execute(sql)
                            
                            total_rows_affected += cursor.rowcount
                        
                        cursor.execute("COMMIT")
                        
                    except Exception as e:
                        cursor.execute("ROLLBACK")
                        raise e
            
            execution_time = time.time() - start_time
            self.stats['queries_executed'] += len(operations)
            self.stats['total_execution_time'] += execution_time
            
            return DatabaseResult(
                success=True,
                data=[],
                rows_affected=total_rows_affected,
                execution_time=execution_time,
                backend="sqlite",
                sql_executed=f"Transaction with {len(operations)} operations"
            )
            
        except Exception as e:
            execution_time = time.time() - start_time
            self.stats['failed_queries'] += 1
            
            return DatabaseResult(
                success=False,
                data=[],
                rows_affected=0,
                execution_time=execution_time,
                backend="sqlite",
                sql_executed=f"Transaction with {len(operations)} operations",
                error_message=str(e)
            )
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get SQLite adapter statistics"""
        uptime = time.time() - self.stats['start_time']
        total_queries = self.stats['queries_executed']
        
        return {
            'backend': 'sqlite',
            'database_path': self.db_path,
            'uptime_seconds': uptime,
            'queries_executed': total_queries,
            'failed_queries': self.stats['failed_queries'],
            'success_rate': (total_queries - self.stats['failed_queries']) / max(total_queries, 1),
            'avg_execution_time': self.stats['total_execution_time'] / max(total_queries, 1),
            'total_execution_time': self.stats['total_execution_time']
        }

class PostgreSQLAdapterWrapper(DatabaseAdapter):
    """Wrapper for PostgreSQL adapter to match DatabaseAdapter interface"""
    
    def __init__(self, config: Dict[str, Any], firewall: Optional[Any] = None):
        super().__init__(config, firewall)
        
        # Import PostgreSQL adapter
        try:
            from extensions.plugins.postgresql_adapter import PostgreSQLAdapter, ConnectionConfig
            
            # Convert config to ConnectionConfig
            pg_config = ConnectionConfig(
                host=config.get('host', 'localhost'),
                port=config.get('port', 5432),
                database=config.get('database', 'saiql'),
                user=config.get('user', 'postgres'),
                password=config.get('password', ''),
                min_connections=config.get('min_connections', 2),
                max_connections=config.get('max_connections', 20),
                connection_timeout=config.get('connection_timeout', 30),
                statement_timeout=config.get('statement_timeout', 300),
                idle_in_transaction_timeout=config.get('idle_in_transaction_timeout', 60),
                max_retries=config.get('max_retries', 3),
                retry_delay=config.get('retry_delay', 1.0),
                application_name=config.get('application_name', 'SAIQL-Bravo')
            )
            
            self.adapter = PostgreSQLAdapter(pg_config)
            
        except ImportError as e:
            logger.error(f"PostgreSQL adapter not available: {e}")
            raise
        except Exception as e:
            logger.error(f"Failed to initialize PostgreSQL adapter: {e}")
            raise
    
    def execute_query(self, sql: str, params: Optional[tuple] = None) -> DatabaseResult:
        """Execute PostgreSQL query"""
        result = self.adapter.execute_query(sql, params)
        
        return DatabaseResult(
            success=result['success'],
            data=result['data'],
            rows_affected=result['rows_affected'],
            execution_time=result['metrics'].execution_time,
            backend="postgresql",
            sql_executed=sql,
            error_message=result.get('error'),
            metadata={
                'connection_time': result['metrics'].connection_time,
                'cache_hit': result['metrics'].cache_hit,
                'prepared_statement': result['metrics'].prepared_statement
            }
        )
    
    def execute_transaction(self, operations: List[Dict[str, Any]]) -> DatabaseResult:
        """Execute PostgreSQL transaction"""
        result = self.adapter.execute_transaction(operations)
        
        return DatabaseResult(
            success=result['success'],
            data=[],
            rows_affected=result['total_rows_affected'],
            execution_time=result['execution_time'],
            backend="postgresql",
            sql_executed=f"Transaction with {len(operations)} operations",
            error_message=result.get('error'),
            metadata={
                'operations_completed': result['operations_completed']
            }
        )
    
    def close(self):
        """Close PostgreSQL connections"""
        self.adapter.close()
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get PostgreSQL adapter statistics"""
        return self.adapter.get_statistics()

class MySQLAdapterWrapper(DatabaseAdapter):
    """Wrapper for MySQL adapter to match DatabaseAdapter interface"""
    
    def __init__(self, config: Dict[str, Any], firewall: Optional[Any] = None):
        super().__init__(config, firewall)
        
        # Import MySQL adapter
        try:
            from extensions.plugins.mysql_adapter import MySQLAdapter, ConnectionConfig
            
            # Convert config to ConnectionConfig
            mysql_config = ConnectionConfig(
                host=config.get('host', 'localhost'),
                port=config.get('port', 3306),
                database=config.get('database', 'saiql'),
                user=config.get('user', 'root'),
                password=config.get('password', ''),
                min_connections=config.get('min_connections', 2),
                max_connections=config.get('max_connections', 20),
                connection_timeout=config.get('connection_timeout', 30),
                connect_timeout=config.get('connect_timeout', 10),
                read_timeout=config.get('read_timeout', 30),
                write_timeout=config.get('write_timeout', 30),
                max_retries=config.get('max_retries', 3),
                retry_delay=config.get('retry_delay', 1.0),
                charset=config.get('charset', 'utf8mb4'),
                sql_mode=config.get('sql_mode', 'STRICT_TRANS_TABLES,NO_ZERO_DATE,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO'),
                autocommit=config.get('autocommit', False)
            )
            
            self.adapter = MySQLAdapter(mysql_config)
            logger.info(f"MySQL adapter initialized for {mysql_config.host}:{mysql_config.port}/{mysql_config.database}")
            
        except ImportError as e:
            logger.error(f"Failed to import MySQL adapter: {e}")
            raise RuntimeError("MySQL adapter not available. Install PyMySQL: pip install PyMySQL")
        except Exception as e:
            logger.error(f"Failed to initialize MySQL adapter: {e}")
            raise
    
    def execute_query(self, sql: str, params: Optional[tuple] = None) -> DatabaseResult:
        """Execute query using MySQL adapter"""
        try:
            start_time = time.time()
            result = self.adapter.execute_query(sql, params, fetch=True)
            execution_time = time.time() - start_time
            
            return DatabaseResult(
                success=result['success'],
                data=result['data'],
                rows_affected=result['rows_affected'],
                execution_time=execution_time,
                backend="mysql",
                sql_executed=sql,
                error_message=result.get('error')
            )
            
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"MySQL query failed: {e}")
            return DatabaseResult(
                success=False,
                data=[],
                rows_affected=0,
                execution_time=execution_time,
                backend="mysql",
                sql_executed=sql,
                error_message=str(e)
            )
    
    def execute_transaction(self, operations: List[Dict[str, Any]]) -> DatabaseResult:
        """Execute transaction using MySQL adapter"""
        try:
            start_time = time.time()
            result = self.adapter.execute_transaction(operations)
            execution_time = time.time() - start_time
            
            return DatabaseResult(
                success=result['success'],
                data=[],
                rows_affected=result['total_rows_affected'],
                execution_time=execution_time,
                backend="mysql",
                sql_executed=f"Transaction with {len(operations)} operations",
                error_message=result.get('error')
            )
            
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"MySQL transaction failed: {e}")
            return DatabaseResult(
                success=False,
                data=[],
                rows_affected=0,
                execution_time=execution_time,
                backend="mysql",
                sql_executed=f"Transaction with {len(operations)} operations",
                error_message=str(e)
            )
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get MySQL adapter statistics"""
        return self.adapter.get_statistics()

class OracleAdapterWrapper(DatabaseAdapter):
    """Wrapper for Oracle adapter to match DatabaseAdapter interface"""
    
    def __init__(self, config: Dict[str, Any], firewall: Optional[Any] = None):
        super().__init__(config, firewall)
        
        # Import Oracle adapter
        try:
            from extensions.plugins.oracle_adapter import OracleAdapter
            self.adapter = OracleAdapter(config)
            logger.info(f"Oracle adapter initialized for {config.get('host')}:{config.get('port')}/{config.get('database')}")
            
        except ImportError as e:
            logger.error(f"Failed to import Oracle adapter: {e}")
            raise RuntimeError("Oracle adapter not available. Install oracledb: pip install oracledb")
        except Exception as e:
            logger.error(f"Failed to initialize Oracle adapter: {e}")
            raise
    
    def execute_query(self, sql: str, params: Optional[tuple] = None) -> DatabaseResult:
        """Execute query using Oracle adapter"""
        try:
            start_time = time.time()
            # Oracle params might need conversion if tuple? oracledb handles list/tuple/dict.
            # BaseAdapter signature expects params.
            result = self.adapter.execute_query(sql, params)
            execution_time = time.time() - start_time
            
            # Helper to shape result (OracleAdapter returns list of dicts or rowcount int)
            data = []
            rows_affected = 0
            if isinstance(result, list):
                data = result
                rows_affected = len(data) # Not strictly true for Select but ok
            else:
                rows_affected = result
            
            return DatabaseResult(
                success=True,
                data=data,
                rows_affected=rows_affected,
                execution_time=execution_time,
                backend="oracle",
                sql_executed=sql
            )
            
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"Oracle query failed: {e}")
            return DatabaseResult(
                success=False,
                data=[],
                rows_affected=0,
                execution_time=execution_time,
                backend="oracle",
                sql_executed=sql,
                error_message=str(e)
            )
    
    def execute_transaction(self, operations: List[Dict[str, Any]]) -> DatabaseResult:
        """Execute transaction using Oracle adapter"""
        try:
            start_time = time.time()
            success = self.adapter.execute_transaction(operations)
            execution_time = time.time() - start_time
            
            if not success:
                raise Exception("Transaction reported failure")

            return DatabaseResult(
                success=True,
                data=[],
                rows_affected=len(operations),
                execution_time=execution_time,
                backend="oracle",
                sql_executed=f"Transaction with {len(operations)} operations"
            )
            
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"Oracle transaction failed: {e}")
            return DatabaseResult(
                success=False,
                data=[],
                rows_affected=0,
                execution_time=execution_time,
                backend="oracle",
                sql_executed=f"Transaction with {len(operations)} operations",
                error_message=str(e)
            )
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get Oracle adapter statistics"""
        return self.adapter.get_statistics()

class MSSQLAdapterWrapper(DatabaseAdapter):
    """Wrapper for MSSQL adapter to match DatabaseAdapter interface"""
    
    def __init__(self, config: Dict[str, Any], firewall: Optional[Any] = None):
        super().__init__(config, firewall)
        
        # Import MSSQL adapter
        try:
            from extensions.plugins.mssql_adapter import MSSQLAdapter
            self.adapter = MSSQLAdapter(config)
            logger.info(f"MSSQL adapter initialized for {config.get('host')}:{config.get('port')}/{config.get('database')}")
            
        except ImportError as e:
            logger.error(f"Failed to import MSSQL adapter: {e}")
            raise RuntimeError("MSSQL adapter not available.")
        except Exception as e:
            logger.error(f"Failed to initialize MSSQL adapter: {e}")
            raise
    
    def execute_query(self, sql: str, params: Optional[tuple] = None) -> DatabaseResult:
        """Execute query using MSSQL adapter"""
        try:
            start_time = time.time()
            result = self.adapter.execute_query(sql, params, fetch=True)
            execution_time = time.time() - start_time
            
            return DatabaseResult(
                success=result['success'],
                data=result['data'],
                rows_affected=result['rows_affected'],
                execution_time=execution_time,
                backend="mssql",
                sql_executed=sql,
                error_message=result.get('error')
            )
            
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"MSSQL query failed: {e}")
            return DatabaseResult(
                success=False,
                data=[],
                rows_affected=0,
                execution_time=execution_time,
                backend="mssql",
                sql_executed=sql,
                error_message=str(e)
            )
    
    def execute_transaction(self, operations: List[Dict[str, Any]]) -> DatabaseResult:
        """Execute transaction using MSSQL adapter"""
        try:
            start_time = time.time()
            result = self.adapter.execute_transaction(operations)
            execution_time = time.time() - start_time
            
            return DatabaseResult(
                success=result['success'],
                data=[],
                rows_affected=result['total_rows_affected'],
                execution_time=execution_time,
                backend="mssql",
                sql_executed=f"Transaction with {len(operations)} operations",
                error_message=result.get('error')
            )
            
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"MSSQL transaction failed: {e}")
            return DatabaseResult(
                success=False,
                data=[],
                rows_affected=0,
                execution_time=execution_time,
                backend="mssql",
                sql_executed=f"Transaction with {len(operations)} operations",
                error_message=str(e)
            )
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get MSSQL adapter statistics"""
        return self.adapter.get_statistics()

class BigQueryAdapterWrapper(DatabaseAdapter):
    """Wrapper for BigQuery adapter to match DatabaseAdapter interface"""

    def __init__(self, config: Dict[str, Any], firewall: Optional[Any] = None):
        super().__init__(config, firewall)

        # Import BigQuery adapter
        try:
            from extensions.plugins.bigquery_adapter import BigQueryAdapter, BigQueryConfig, AuthMethod

            # Determine auth method
            auth_method = AuthMethod.APPLICATION_DEFAULT
            if config.get('credentials_path') or config.get('credentials_json'):
                auth_method = AuthMethod.SERVICE_ACCOUNT

            # Convert config to BigQueryConfig
            bq_config = BigQueryConfig(
                project_id=config.get('project_id', ''),
                auth_method=auth_method,
                credentials_path=config.get('credentials_path'),
                credentials_json=config.get('credentials_json'),
                default_dataset=config.get('default_dataset'),
                location=config.get('location', 'US'),
                query_timeout=config.get('query_timeout', 300),
                use_legacy_sql=config.get('use_legacy_sql', False),
                max_retries=config.get('max_retries', 3),
                retry_delay=config.get('retry_delay', 1.0),
                maximum_bytes_billed=config.get('maximum_bytes_billed'),
                application_name=config.get('application_name', 'SAIQL-BigQuery')
            )

            self.adapter = BigQueryAdapter(bq_config)
            logger.info(f"BigQuery adapter initialized for project: {bq_config.project_id}")

        except ImportError as e:
            logger.error(f"Failed to import BigQuery adapter: {e}")
            raise RuntimeError("BigQuery adapter not available. Install: pip install google-cloud-bigquery")
        except Exception as e:
            logger.error(f"Failed to initialize BigQuery adapter: {e}")
            raise

    def execute_query(self, sql: str, params: Optional[tuple] = None) -> DatabaseResult:
        """Execute query using BigQuery adapter"""
        try:
            # Convert tuple params to dict if needed (BigQuery uses named params)
            param_dict = None
            if params:
                param_dict = {f"param{i}": v for i, v in enumerate(params)}

            result = self.adapter.execute_query(sql, param_dict)

            return DatabaseResult(
                success=result.success,
                data=result.data,
                rows_affected=result.rows_affected,
                execution_time=result.execution_time,
                backend="bigquery",
                sql_executed=sql,
                error_message=result.error_message,
                metadata={
                    'job_id': result.job_id,
                    'bytes_processed': result.metrics.bytes_processed,
                    'bytes_billed': result.metrics.bytes_billed,
                    'cache_hit': result.metrics.cache_hit,
                }
            )

        except Exception as e:
            logger.error(f"BigQuery query failed: {e}")
            return DatabaseResult(
                success=False,
                data=[],
                rows_affected=0,
                execution_time=0.0,
                backend="bigquery",
                sql_executed=sql,
                error_message=str(e)
            )

    def execute_transaction(self, operations: List[Dict[str, Any]]) -> DatabaseResult:
        """
        Execute transaction using BigQuery adapter.

        Note: BigQuery doesn't support traditional transactions.
        This executes operations sequentially.
        """
        start_time = time.time()
        total_rows = 0

        try:
            for op in operations:
                sql = op.get('sql')
                params = op.get('params')

                param_dict = None
                if params:
                    param_dict = {f"param{i}": v for i, v in enumerate(params)}

                result = self.adapter.execute_query(sql, param_dict)
                if not result.success:
                    raise Exception(f"Operation failed: {result.error_message}")

                total_rows += result.rows_affected

            return DatabaseResult(
                success=True,
                data=[],
                rows_affected=total_rows,
                execution_time=time.time() - start_time,
                backend="bigquery",
                sql_executed=f"Batch with {len(operations)} operations"
            )

        except Exception as e:
            logger.error(f"BigQuery batch operation failed: {e}")
            return DatabaseResult(
                success=False,
                data=[],
                rows_affected=0,
                execution_time=time.time() - start_time,
                backend="bigquery",
                sql_executed=f"Batch with {len(operations)} operations",
                error_message=str(e)
            )

    def close(self):
        """Close BigQuery connection"""
        if hasattr(self, 'adapter'):
            self.adapter.close()

    def get_statistics(self) -> Dict[str, Any]:
        """Get BigQuery adapter statistics"""
        return self.adapter.get_statistics()

class DatabaseManager:
    """
    Multi-backend database manager for SAIQL
    
    Provides a unified interface for multiple database backends with:
    - Automatic backend selection
    - Connection management
    - Configuration-driven setup
    - Performance monitoring
    - Error handling and recovery
    """
    
    def __init__(self, config_path: Optional[str] = None, firewall: Optional[Any] = None, config: Optional[Dict[str, Any]] = None):
        """Initialize database manager"""
        if config:
            self.config = config
        else:
            self.config = self._load_config(config_path)
        self.firewall = firewall
        self.adapters = {}
        self.default_backend = self.config.get('default_backend', 'sqlite')
        self._lock = threading.RLock()
        
        # Initialize default backend
        self._initialize_backend(self.default_backend)
        
        logger.info(f"Database manager initialized with default backend: {self.default_backend}")
    
    def _load_config(self, config_path: Optional[str]) -> Dict[str, Any]:
        """Load database configuration"""
        if config_path is None:
            config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'database_config.json')
        
        if not os.path.exists(config_path):
            logger.warning(f"Config file not found: {config_path}, using defaults")
            return self._get_default_config()
        
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
                
            # Resolve environment variables
            config = self._resolve_environment_variables(config)
            return config
            
        except Exception as e:
            logger.error(f"Error loading config: {e}, using defaults")
            return self._get_default_config()
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Get default configuration"""
        return {
            'default_backend': 'sqlite',
            'backends': {
                'sqlite': {
                    'type': 'sqlite',
                    'path': 'data/lore_store.db',
                    'timeout': 30
                }
            }
        }
    
    def _resolve_environment_variables(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Resolve environment variables in configuration"""
        import re
        
        def resolve_value(value):
            if isinstance(value, str):
                # Handle ${VAR} and ${VAR:default} patterns
                pattern = r'\$\{([^}:]+)(?::([^}]*))?\}'
                
                def replace_env_var(match):
                    var_name = match.group(1)
                    default_value = match.group(2) if match.group(2) is not None else ''
                    return os.environ.get(var_name, default_value)
                
                return re.sub(pattern, replace_env_var, value)
            elif isinstance(value, dict):
                return {k: resolve_value(v) for k, v in value.items()}
            elif isinstance(value, list):
                return [resolve_value(item) for item in value]
            else:
                return value
        
        return resolve_value(config)
    
    def _initialize_backend(self, backend_name: str):
        """Initialize a database backend"""
        if backend_name in self.adapters:
            return  # Already initialized
        
        backend_config = self.config.get('backends', {}).get(backend_name)
        if not backend_config:
            raise ValueError(f"No configuration found for backend: {backend_name}")
        
        backend_type = backend_config.get('type')
        
        try:
            with self._lock:
                if backend_type == 'sqlite':
                    self.adapters[backend_name] = SQLiteAdapter(backend_config, firewall=self.firewall)
                elif backend_type == 'postgresql':
                    self.adapters[backend_name] = PostgreSQLAdapterWrapper(backend_config, firewall=self.firewall)
                elif backend_type == 'mysql':
                    self.adapters[backend_name] = MySQLAdapterWrapper(backend_config, firewall=self.firewall)
                elif backend_type == 'mssql':
                    self.adapters[backend_name] = MSSQLAdapterWrapper(backend_config, firewall=self.firewall)
                elif backend_type == 'oracle':
                    self.adapters[backend_name] = OracleAdapterWrapper(backend_config, firewall=self.firewall)
                elif backend_type == 'bigquery':
                    self.adapters[backend_name] = BigQueryAdapterWrapper(backend_config, firewall=self.firewall)
                else:
                    raise ValueError(f"Unsupported backend type: {backend_type}")
                
                logger.info(f"Initialized backend: {backend_name} ({backend_type})")
                
        except Exception as e:
            logger.error(f"Failed to initialize backend {backend_name}: {e}")
            raise
    
    def execute_query(self, sql: str, params: Optional[tuple] = None, 
               backend: Optional[str] = None) -> DatabaseResult:
        """
        Execute SQL query on specified backend
        
        Args:
            sql: SQL query string
            params: Query parameters (optional)
            backend: Backend name (uses default if not specified)
            
        Returns:
            DatabaseResult with execution details
        """
        backend_name = backend or self.default_backend
        
        # Initialize backend if not already done
        if backend_name not in self.adapters:
            self._initialize_backend(backend_name)
        
        adapter = self.adapters[backend_name]
        return adapter.execute_query(sql, params)

    # Alias for convenience (matches docstring example)
    def execute(self, sql: str, params: Optional[tuple] = None,
                backend: Optional[str] = None) -> DatabaseResult:
        """Alias for execute_query() - provided for convenience."""
        return self.execute_query(sql, params, backend)

    def execute_transaction(self, operations: List[Dict[str, Any]], 
                           backend: Optional[str] = None) -> DatabaseResult:
        """
        Execute transaction on specified backend
        
        Args:
            operations: List of operation dictionaries
            backend: Backend name (uses default if not specified)
            
        Returns:
            DatabaseResult with transaction details
        """
        backend_name = backend or self.default_backend
        
        # Initialize backend if not already done
        if backend_name not in self.adapters:
            self._initialize_backend(backend_name)
        
        adapter = self.adapters[backend_name]
        return adapter.execute_transaction(operations)
    
    def get_available_backends(self) -> List[str]:
        """Get list of configured backends"""
        return list(self.config.get('backends', {}).keys())
    
    # Keys to redact from backend config to prevent credential leakage
    _SECRET_KEYS = {'password', 'passwd', 'pwd', 'secret', 'token', 'api_key',
                    'apikey', 'auth', 'credential', 'credentials', 'credentials_json',
                    'credentials_path', 'key', 'private_key', 'master_key'}

    def get_backend_info(self, backend: Optional[str] = None) -> Dict[str, Any]:
        """Get information about a backend (secrets redacted)"""
        backend_name = backend or self.default_backend
        backend_config = self.config.get('backends', {}).get(backend_name, {})

        # Redact all secret keys, not just password
        safe_config = {}
        for k, v in backend_config.items():
            if k.lower() in self._SECRET_KEYS or any(s in k.lower() for s in ('secret', 'credential', 'token', 'key', 'password')):
                safe_config[k] = '***REDACTED***'
            else:
                safe_config[k] = v

        info = {
            'name': backend_name,
            'type': backend_config.get('type', 'unknown'),
            'initialized': backend_name in self.adapters,
            'config': safe_config
        }

        if backend_name in self.adapters:
            info['statistics'] = self.adapters[backend_name].get_statistics()

        return info
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get statistics for all backends"""
        stats = {
            'default_backend': self.default_backend,
            'available_backends': self.get_available_backends(),
            'initialized_backends': list(self.adapters.keys()),
            'backend_stats': {}
        }
        
        for backend_name, adapter in self.adapters.items():
            stats['backend_stats'][backend_name] = adapter.get_statistics()
        
        return stats
    
    def close_all(self):
        """Close all database connections"""
        logger.info("Closing all database connections")
        
        with self._lock:
            for backend_name, adapter in self.adapters.items():
                try:
                    adapter.close()
                    logger.info(f"Closed backend: {backend_name}")
                except Exception as e:
                    logger.error(f"Error closing backend {backend_name}: {e}")
            
            self.adapters.clear()

# Example usage and testing
if __name__ == "__main__":
    try:
        # Initialize database manager
        manager = DatabaseManager()
        
        print("Database Manager Test")
        print("=" * 50)
        
        # Show available backends
        backends = manager.get_available_backends()
        print(f"Available backends: {backends}")
        
        # Test query on default backend
        result = manager.execute("SELECT 'Database Manager Test' as message")
        print(f"Test query result: {result.to_dict()}")
        
        # Show backend info
        for backend in backends:
            info = manager.get_backend_info(backend)
            print(f"\nBackend info for {backend}:")
            print(json.dumps(info, indent=2))
        
        # Show statistics
        stats = manager.get_statistics()
        print(f"\nManager statistics:")
        print(json.dumps(stats, indent=2))
        
        manager.close_all()
        
    except Exception as e:
        print(f"Test failed: {e}")
        import traceback
        traceback.print_exc()
