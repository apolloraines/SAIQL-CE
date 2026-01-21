#!/usr/bin/env python3
"""
SAIQL Symbolic Engine - Query Execution Engine

This module executes parsed SAIQL queries against databases, handling connections,
transactions, results, and errors. It bridges the gap between symbolic notation
and actual database operations.

Author: Apollo & Claude
Version: 1.0.0
Status: Foundation Phase
"""

import sqlite3
import json
import logging
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict
from contextlib import contextmanager
from enum import Enum
import time
import os

# Import our parser components
from . import QueryComponents, QueryType
# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ExecutionStatus(Enum):
    """Query execution status"""
    SUCCESS = "success"
    ERROR = "error"
    WARNING = "warning"
    TIMEOUT = "timeout"

@dataclass
class ExecutionResult:
    """Results from query execution"""
    status: ExecutionStatus
    data: List[Dict[str, Any]]
    rows_affected: int
    execution_time: float
    sql_executed: str
    saiql_query: str
    error_message: Optional[str] = None
    metadata: Dict[str, Any] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert result to dictionary"""
        return asdict(self)

    def to_json(self) -> str:
        """Convert result to JSON string"""
        return json.dumps(self.to_dict(), indent=2, default=str)

class DatabaseConnection:
    """Manages database connections and operations"""

    def __init__(self, db_path: str = "data/lore_store.db", create_demo_tables: bool = False):
        self.db_path = db_path
        self._persistent_conn = None  # Persistent connection for transactions
        self.transaction_active = False
        self._ensure_database_exists(create_demo_tables)

    def _get_persistent_connection(self):
        """Get or create persistent connection for transactions"""
        if self._persistent_conn is None:
            self._persistent_conn = sqlite3.connect(self.db_path)
            self._persistent_conn.row_factory = sqlite3.Row
        return self._persistent_conn

    def close_persistent_connection(self):
        """Close persistent connection"""
        if self._persistent_conn:
            self._persistent_conn.close()
            self._persistent_conn = None
            self.transaction_active = False

    def _ensure_database_exists(self, create_demo_tables: bool = False):
        """Create database directory if needed. Optionally create demo tables."""
        # Only create directory if db_path has a directory component
        db_dir = os.path.dirname(self.db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)

        # Skip demo table creation unless explicitly requested
        if not create_demo_tables:
            return

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            # Create sample tables for testing (only when create_demo_tables=True)
            cursor.executescript("""
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
                
                CREATE TABLE IF NOT EXISTS trading_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    timestamp DATETIME,
                    open_price DECIMAL(15,8),
                    close_price DECIMAL(15,8),
                    high_price DECIMAL(15,8),
                    low_price DECIMAL(15,8),
                    volume DECIMAL(20,2),
                    rsi DECIMAL(5,2),
                    macd DECIMAL(10,6)
                );
            """)
            
            # Insert sample data if tables are empty
            cursor.execute("SELECT COUNT(*) FROM users")
            if cursor.fetchone()[0] == 0:
                cursor.executescript("""
                    INSERT INTO users (name, email) VALUES 
                        ('Apollo', 'apollo@loretoken.com'),
                        ('Claude', 'claude@anthropic.com'),
                        ('Alice', 'alice@example.com'),
                        ('Bob', 'bob@example.com');
                    
                    INSERT INTO products (name, price, category, stock) VALUES
                        ('Laptop', 999.99, 'Electronics', 50),
                        ('Mouse', 29.99, 'Electronics', 200),
                        ('Keyboard', 79.99, 'Electronics', 150),
                        ('Monitor', 299.99, 'Electronics', 75);
                    
                    INSERT INTO orders (user_id, product, price, quantity) VALUES
                        (1, 'Laptop', 999.99, 1),
                        (1, 'Mouse', 29.99, 2),
                        (2, 'Keyboard', 79.99, 1),
                        (3, 'Monitor', 299.99, 1);
                        
                    INSERT INTO trading_data (symbol, timestamp, open_price, close_price, high_price, low_price, volume, rsi, macd) VALUES
                        ('BTC', '2025-01-01 09:00:00', 50000.00, 51000.00, 51500.00, 49800.00, 1000000.00, 65.5, 0.125),
                        ('ETH', '2025-01-01 09:00:00', 3000.00, 3100.00, 3150.00, 2980.00, 500000.00, 70.2, 0.089),
                        ('BTC', '2025-01-01 10:00:00', 51000.00, 50800.00, 51200.00, 50600.00, 800000.00, 62.3, 0.105);
                """)
            
            conn.commit()
            logger.info(f"Database initialized: {self.db_path}")

    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row  # Enable column access by name
        try:
            yield conn
        finally:
            conn.close()

    def execute_query(self, sql: str, params: Optional[Tuple] = None) -> Tuple[List[Dict], int]:
        """Execute SQL query and return results"""
        # Use persistent connection if transaction is active, otherwise use context manager
        if self.transaction_active:
            conn = self._get_persistent_connection()
            cursor = conn.cursor()

            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)

            # Handle different query types
            if sql.strip().upper().startswith(('SELECT', 'WITH')):
                rows = cursor.fetchall()
                data = [dict(row) for row in rows]
                return data, len(data)
            else:
                # Don't auto-commit during transaction
                return [], cursor.rowcount
        else:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                if params:
                    cursor.execute(sql, params)
                else:
                    cursor.execute(sql)

                # Handle different query types
                if sql.strip().upper().startswith(('SELECT', 'WITH')):
                    rows = cursor.fetchall()
                    data = [dict(row) for row in rows]
                    return data, len(data)
                else:
                    conn.commit()
                    return [], cursor.rowcount

    def begin_transaction(self):
        """Begin a transaction"""
        conn = self._get_persistent_connection()
        conn.execute("BEGIN TRANSACTION")
        self.transaction_active = True

    def commit_transaction(self):
        """Commit the active transaction"""
        if self._persistent_conn and self.transaction_active:
            self._persistent_conn.commit()
            self.transaction_active = False

    def rollback_transaction(self):
        """Rollback the active transaction"""
        if self._persistent_conn and self.transaction_active:
            self._persistent_conn.rollback()
            self.transaction_active = False

class SymbolicEngine:
    """
    SAIQL Symbolic Execution Engine
    
    Executes parsed SAIQL queries against databases
    """
    
    def __init__(self, db_path: str = "data/lore_store.db"):
        self.db = DatabaseConnection(db_path)
        self.query_cache = {}
        self.execution_stats = {
            'total_queries': 0,
            'successful_queries': 0,
            'failed_queries': 0,
            'avg_execution_time': 0.0
        }
        
    def execute(self, components: QueryComponents, debug: bool = False) -> ExecutionResult:
        """
        Execute a parsed SAIQL query
        
        Args:
            components: Parsed query components from SAIQL parser
            debug: Enable debug output
            
        Returns:
            ExecutionResult with query results and metadata
        """
        start_time = time.time()
        
        if debug:
            logger.info(f"Executing SAIQL: {getattr(components, 'raw_query', '')}")
            logger.info(f"Query type: {getattr(components, 'query_type', 'UNKNOWN')}")
            logger.info(f"SQL translation: {getattr(components, 'sql_translation', '')}")
        
        try:
            # Route to appropriate execution method
            if components.query_type == QueryType.SELECT:
                result = self._execute_select(components, debug)
            elif components.query_type == QueryType.INSERT:
                result = self._execute_insert(components, debug)
            elif components.query_type == QueryType.UPDATE:
                result = self._execute_update(components, debug)
            elif components.query_type == QueryType.JOIN:
                result = self._execute_join(components, debug)
            elif components.query_type == QueryType.AGGREGATE:
                result = self._execute_aggregate(components, debug)
            elif components.query_type == QueryType.TRANSACTION:
                result = self._execute_transaction(components, debug)
            else:
                result = self._execute_generic(components, debug)
                
            # Calculate execution time
            execution_time = time.time() - start_time
            result.execution_time = execution_time
            
            # Update stats
            self._update_stats(result, execution_time)
            
            if debug:
                logger.info(f"Execution completed in {execution_time:.4f}s")
                logger.info(f"Rows affected/returned: {result.rows_affected}")
                
            return result
            
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"Query execution failed: {e}")
            
            error_result = ExecutionResult(
                status=ExecutionStatus.ERROR,
                data=[],
                rows_affected=0,
                execution_time=execution_time,
                sql_executed=getattr(components, 'sql_translation', ''),
                saiql_query=getattr(components, 'raw_query', ''),
                error_message=str(e)
            )
            
            self._update_stats(error_result, execution_time)
            return error_result

    def _execute_select(self, components: QueryComponents, debug: bool) -> ExecutionResult:
        """Execute SELECT query"""
        sql = getattr(components, 'sql_translation', '') or ''

        # Build SQL from components if translation not provided
        tables = getattr(components, 'tables', []) or getattr(components, 'from_tables', []) or []
        columns = getattr(components, 'columns', []) or getattr(components, 'select_fields', []) or []

        if not sql or sql == "-- SQL translation not available":
            if tables:
                col_str = ', '.join(columns) if columns else '*'
                sql = f"SELECT {col_str} FROM {tables[0]}"
            else:
                sql = "SELECT 1"  # Fallback

        data, row_count = self.db.execute_query(sql)

        return ExecutionResult(
            status=ExecutionStatus.SUCCESS,
            data=data,
            rows_affected=row_count,
            execution_time=0,  # Will be set by caller
            sql_executed=sql,
            saiql_query=getattr(components, 'raw_query', ''),
            metadata={'query_type': 'select', 'tables': tables}
        )

    def _execute_insert(self, components: QueryComponents, debug: bool) -> ExecutionResult:
        """Execute INSERT query"""
        tables = getattr(components, 'tables', []) or getattr(components, 'from_tables', []) or []
        if not tables:
            raise ValueError("No table specified for INSERT")

        table = tables[0]

        # Use sql_translation if available
        sql = getattr(components, 'sql_translation', '') or ''
        if sql and sql != "-- SQL translation not available":
            pass  # Use the provided SQL
        else:
            # CE scaffolding: INSERT requires explicit values in real usage
            # This is a placeholder that will fail gracefully for tables with required columns
            raise ValueError(f"INSERT into '{table}' requires explicit column values (CE scaffolding limitation)")

        data, row_count = self.db.execute_query(sql)

        return ExecutionResult(
            status=ExecutionStatus.SUCCESS,
            data=data,
            rows_affected=row_count,
            execution_time=0,
            sql_executed=sql,
            saiql_query=getattr(components, 'raw_query', ''),
            metadata={'query_type': 'insert', 'table': table}
        )

    def _execute_update(self, components: QueryComponents, debug: bool) -> ExecutionResult:
        """Execute UPDATE query"""
        tables = getattr(components, 'tables', []) or getattr(components, 'from_tables', []) or []
        if not tables:
            raise ValueError("No table specified for UPDATE")

        table = tables[0]

        # Use sql_translation if available
        sql = getattr(components, 'sql_translation', '') or ''
        if sql and sql != "-- SQL translation not available":
            pass  # Use the provided SQL
        else:
            # CE scaffolding: UPDATE requires explicit SET clause and WHERE condition
            # Refusing to execute without proper SQL to avoid full-table updates
            raise ValueError(f"UPDATE on '{table}' requires explicit SET and WHERE clauses (CE scaffolding limitation)")

        data, row_count = self.db.execute_query(sql)

        return ExecutionResult(
            status=ExecutionStatus.SUCCESS,
            data=data,
            rows_affected=row_count,
            execution_time=0,
            sql_executed=sql,
            saiql_query=getattr(components, 'raw_query', ''),
            metadata={'query_type': 'update', 'table': table}
        )

    def _execute_join(self, components: QueryComponents, debug: bool) -> ExecutionResult:
        """Execute JOIN query"""
        tables = getattr(components, 'tables', []) or getattr(components, 'from_tables', []) or []
        if len(tables) < 2:
            raise ValueError("JOIN requires at least 2 tables")

        # Use the pre-generated SQL translation if available
        sql = getattr(components, 'sql_translation', '') or ''
        if sql and sql != "-- SQL translation not available":
            pass  # Use the provided SQL
        else:
            # Try to infer join condition from components.joins
            joins = getattr(components, 'joins', []) or []
            table1, table2 = tables[0], tables[1]

            if joins and len(joins) > 0:
                # Use join info if available
                join_info = joins[0]
                join_col = getattr(join_info, 'column', None) or 'id'
                sql = f"SELECT * FROM {table1} t1 JOIN {table2} t2 ON t1.{join_col} = t2.{join_col} LIMIT 100"
            else:
                # CE scaffolding: require explicit join condition
                raise ValueError(f"JOIN between '{table1}' and '{table2}' requires explicit join condition (CE scaffolding limitation)")

        data, row_count = self.db.execute_query(sql)

        return ExecutionResult(
            status=ExecutionStatus.SUCCESS,
            data=data,
            rows_affected=row_count,
            execution_time=0,
            sql_executed=sql,
            saiql_query=getattr(components, 'raw_query', ''),
            metadata={'query_type': 'join', 'tables': tables}
        )

    def _execute_aggregate(self, components: QueryComponents, debug: bool) -> ExecutionResult:
        """Execute aggregate functions"""
        tables = getattr(components, 'tables', []) or getattr(components, 'from_tables', []) or []
        if not tables:
            raise ValueError("No table specified for aggregation")

        table = tables[0]

        # Use sql_translation if available
        sql = getattr(components, 'sql_translation', '') or ''
        agg_func = "COUNT"

        if sql and sql != "-- SQL translation not available":
            pass  # Use the provided SQL
        else:
            # Determine aggregate type from tokens or aggregations
            tokens = getattr(components, 'tokens', []) or []
            aggregations = getattr(components, 'aggregations', []) or []

            # Mapping from semantic names to SQL aggregate functions
            agg_func_map = {
                'countFunction': 'COUNT',
                'sumFunction': 'SUM',
                'averageFunction': 'AVG',
                'minimumFunction': 'MIN',
                'maximumFunction': 'MAX',
            }

            for token in tokens:
                # Check semantic, semantic_info, and semantic_meaning (lexer uses semantic_meaning)
                semantic = (getattr(token, 'semantic', None) or
                           getattr(token, 'semantic_info', None) or
                           getattr(token, 'semantic_meaning', None))
                sql_hint = getattr(token, 'sql_hint', None)
                if semantic in agg_func_map:
                    agg_func = sql_hint or agg_func_map[semantic]
                    break

            # Check aggregations list
            for agg in aggregations:
                if hasattr(agg, 'function'):
                    agg_func = agg.function.upper()
                    break

            sql = f"SELECT {agg_func}(*) as result FROM {table}"

        data, row_count = self.db.execute_query(sql)

        return ExecutionResult(
            status=ExecutionStatus.SUCCESS,
            data=data,
            rows_affected=row_count,
            execution_time=0,
            sql_executed=sql,
            saiql_query=getattr(components, 'raw_query', ''),
            metadata={'query_type': 'aggregate', 'function': agg_func}
        )

    def _execute_transaction(self, components: QueryComponents, debug: bool) -> ExecutionResult:
        """Execute transaction commands using persistent connection"""
        # Determine transaction type from tokens
        trans_type = "BEGIN"
        tokens = getattr(components, 'tokens', []) or []

        # Mapping from semantic names to SQL transaction commands
        trans_type_map = {
            'beginTransaction': 'BEGIN',
            'commitTransaction': 'COMMIT',
            'rollbackTransaction': 'ROLLBACK',
        }

        for token in tokens:
            # Check semantic, semantic_info, and semantic_meaning (lexer uses semantic_meaning)
            semantic = (getattr(token, 'semantic', None) or
                       getattr(token, 'semantic_info', None) or
                       getattr(token, 'semantic_meaning', None))
            sql_hint = getattr(token, 'sql_hint', None)
            if semantic in trans_type_map:
                trans_type = sql_hint or trans_type_map[semantic]
                break

        # Use DatabaseConnection's transaction methods for proper transaction handling
        if trans_type in ("BEGIN", "begin"):
            self.db.begin_transaction()
            sql = "BEGIN TRANSACTION"
        elif trans_type in ("COMMIT", "commit"):
            self.db.commit_transaction()
            sql = "COMMIT"
        elif trans_type in ("ROLLBACK", "rollback"):
            self.db.rollback_transaction()
            sql = "ROLLBACK"
        else:
            self.db.begin_transaction()
            sql = "BEGIN TRANSACTION"

        return ExecutionResult(
            status=ExecutionStatus.SUCCESS,
            data=[],
            rows_affected=0,
            execution_time=0,
            sql_executed=sql,
            saiql_query=getattr(components, 'raw_query', ''),
            metadata={'query_type': 'transaction', 'operation': trans_type}
        )

    def _execute_generic(self, components: QueryComponents, debug: bool) -> ExecutionResult:
        """Execute generic/unknown query types"""
        # Try to execute the SQL translation directly
        sql = getattr(components, 'sql_translation', '') or ''

        if not sql or sql == "-- SQL translation not available":
            query_type = getattr(components, 'query_type', 'UNKNOWN')
            raise ValueError(f"Cannot execute unknown query type: {query_type}")

        data, row_count = self.db.execute_query(sql)

        return ExecutionResult(
            status=ExecutionStatus.SUCCESS,
            data=data,
            rows_affected=row_count,
            execution_time=0,
            sql_executed=sql,
            saiql_query=getattr(components, 'raw_query', ''),
            metadata={'query_type': 'generic'}
        )

    def _update_stats(self, result: ExecutionResult, execution_time: float):
        """Update execution statistics"""
        self.execution_stats['total_queries'] += 1
        
        if result.status == ExecutionStatus.SUCCESS:
            self.execution_stats['successful_queries'] += 1
        else:
            self.execution_stats['failed_queries'] += 1
            
        # Update average execution time
        total = self.execution_stats['total_queries']
        current_avg = self.execution_stats['avg_execution_time']
        self.execution_stats['avg_execution_time'] = ((current_avg * (total - 1)) + execution_time) / total

    def get_stats(self) -> Dict[str, Any]:
        """Get execution statistics"""
        return self.execution_stats.copy()

    def clear_cache(self):
        """Clear query cache"""
        self.query_cache.clear()
        logger.info("Query cache cleared")

def main():
    """Test the symbolic engine with sample queries"""
    from . import QueryComponents, QueryType

    # Initialize engine
    engine = SymbolicEngine()

    print("SAIQL Symbolic Engine Test Results")
    print("=" * 60)

    # Test 1: SELECT query
    print("\nTest 1: SELECT query")
    components = QueryComponents()
    components.query_type = QueryType.SELECT
    components.tables = ['users']
    components.columns = ['name', 'email']
    components.raw_query = "*[users]::name,email"

    try:
        result = engine.execute(components, debug=False)
        print(f"   Status: {result.status.value}")
        print(f"   SQL: {result.sql_executed}")
        print(f"   Rows: {result.rows_affected}")
        if result.data:
            print(f"   Sample: {result.data[0]}")
    except Exception as e:
        print(f"   Error: {e}")

    # Test 2: AGGREGATE query
    print("\nTest 2: AGGREGATE query")
    components2 = QueryComponents()
    components2.query_type = QueryType.AGGREGATE
    components2.tables = ['users']
    components2.raw_query = "*COUNT[users]"

    try:
        result = engine.execute(components2, debug=False)
        print(f"   Status: {result.status.value}")
        print(f"   SQL: {result.sql_executed}")
        print(f"   Result: {result.data}")
    except Exception as e:
        print(f"   Error: {e}")

    # Test 3: Transaction (BEGIN)
    print("\nTest 3: BEGIN TRANSACTION")
    components3 = QueryComponents()
    components3.query_type = QueryType.TRANSACTION
    components3.raw_query = "$BEGIN"

    try:
        result = engine.execute(components3, debug=False)
        print(f"   Status: {result.status.value}")
        print(f"   SQL: {result.sql_executed}")
    except Exception as e:
        print(f"   Error: {e}")

    # Test 4: COMMIT
    print("\nTest 4: COMMIT TRANSACTION")
    components4 = QueryComponents()
    components4.query_type = QueryType.TRANSACTION
    components4.raw_query = "$COMMIT"
    from . import ParsedToken
    components4.tokens = [ParsedToken('transaction', 'commit', 'commitTransaction', 'COMMIT')]

    try:
        result = engine.execute(components4, debug=False)
        print(f"   Status: {result.status.value}")
        print(f"   SQL: {result.sql_executed}")
    except Exception as e:
        print(f"   Error: {e}")

    # Show execution stats
    print("\nExecution Statistics:")
    stats = engine.get_stats()
    for key, value in stats.items():
        print(f"   {key}: {value}")

if __name__ == "__main__":
    main()
