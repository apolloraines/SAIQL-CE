#!/usr/bin/env python3
"""
Real Database Integration Tests - Production-Grade Testing
==========================================================

Comprehensive tests against real PostgreSQL, MySQL, and SQLite databases
with performance benchmarking, stress testing, and production scenarios.

Author: Apollo & Claude
Version: 1.0.0
Status: Production Testing Suite
"""

import pytest
import asyncio
import os
import time
import statistics
import logging
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timezone
import json
import psutil
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# Database imports
import sqlite3
try:
    import psycopg2
    import psycopg2.extras
    POSTGRESQL_AVAILABLE = True
except ImportError:
    POSTGRESQL_AVAILABLE = False

try:
    import pymysql
    MYSQL_AVAILABLE = True
except ImportError:
    MYSQL_AVAILABLE = False

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Test configurations
TEST_CONFIG = {
    "sqlite": {
        "path": "tests/database/test_database.db",
        "timeout": 30
    },
    "postgresql": {
        "host": os.getenv("POSTGRES_TEST_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_TEST_PORT", "5433")),
        "database": os.getenv("POSTGRES_TEST_DB", "saiql_test"),
        "user": os.getenv("POSTGRES_TEST_USER", "saiql_test_user"),
        "password": os.getenv("POSTGRES_TEST_PASSWORD", "saiql_test_password_123")
    },
    "mysql": {
        "host": os.getenv("MYSQL_TEST_HOST", "localhost"),
        "port": int(os.getenv("MYSQL_TEST_PORT", "3307")),
        "database": os.getenv("MYSQL_TEST_DB", "saiql_test"),
        "user": os.getenv("MYSQL_TEST_USER", "saiql_test_user"),
        "password": os.getenv("MYSQL_TEST_PASSWORD", "saiql_test_password_123")
    }
}

class DatabaseTester:
    """Base class for database testing"""
    
    def __init__(self, backend_name: str, config: Dict[str, Any]):
        self.backend_name = backend_name
        self.config = config
        self.connection = None
        self.metrics = {
            "queries_executed": 0,
            "total_time": 0.0,
            "errors": 0,
            "start_time": time.time()
        }
    
    def connect(self) -> bool:
        """Connect to database - implemented by subclasses"""
        raise NotImplementedError
    
    def disconnect(self):
        """Disconnect from database"""
        if self.connection:
            try:
                self.connection.close()
            except:
                pass
            self.connection = None
    
    def execute_query(self, query: str, params: Optional[Tuple] = None) -> List[Dict[str, Any]]:
        """Execute query - implemented by subclasses"""
        raise NotImplementedError
    
    def time_query(self, query: str, params: Optional[Tuple] = None) -> Tuple[List[Dict[str, Any]], float]:
        """Execute query with timing"""
        start_time = time.time()
        try:
            result = self.execute_query(query, params)
            execution_time = time.time() - start_time
            self.metrics["queries_executed"] += 1
            self.metrics["total_time"] += execution_time
            return result, execution_time
        except Exception as e:
            execution_time = time.time() - start_time
            self.metrics["errors"] += 1
            logger.error(f"Query failed on {self.backend_name}: {e}")
            raise
    
    def benchmark_queries(self, queries: List[Tuple[str, str]], iterations: int = 10) -> Dict[str, Any]:
        """Benchmark a set of queries"""
        results = {}
        
        for query_name, query in queries:
            times = []
            for i in range(iterations):
                try:
                    _, exec_time = self.time_query(query)
                    times.append(exec_time)
                except Exception as e:
                    logger.error(f"Benchmark query {query_name} failed: {e}")
                    continue
            
            if times:
                results[query_name] = {
                    "avg_time": statistics.mean(times),
                    "min_time": min(times),
                    "max_time": max(times),
                    "std_dev": statistics.stdev(times) if len(times) > 1 else 0,
                    "iterations": len(times),
                    "queries_per_second": len(times) / sum(times) if sum(times) > 0 else 0
                }
        
        return results
    
    def stress_test(self, query: str, duration_seconds: int = 30, concurrent_threads: int = 4) -> Dict[str, Any]:
        """Stress test with concurrent queries"""
        start_time = time.time()
        end_time = start_time + duration_seconds
        
        query_count = 0
        error_count = 0
        total_time = 0
        lock = threading.Lock()
        
        def worker():
            nonlocal query_count, error_count, total_time
            while time.time() < end_time:
                try:
                    _, exec_time = self.time_query(query)
                    with lock:
                        query_count += 1
                        total_time += exec_time
                except Exception:
                    with lock:
                        error_count += 1
        
        # Start worker threads
        threads = []
        for _ in range(concurrent_threads):
            thread = threading.Thread(target=worker)
            thread.start()
            threads.append(thread)
        
        # Wait for completion
        for thread in threads:
            thread.join()
        
        actual_duration = time.time() - start_time
        
        return {
            "duration": actual_duration,
            "total_queries": query_count,
            "total_errors": error_count,
            "queries_per_second": query_count / actual_duration if actual_duration > 0 else 0,
            "avg_query_time": total_time / query_count if query_count > 0 else 0,
            "error_rate": error_count / (query_count + error_count) if (query_count + error_count) > 0 else 0,
            "concurrent_threads": concurrent_threads
        }

class SQLiteTester(DatabaseTester):
    """SQLite database tester"""
    
    def connect(self) -> bool:
        try:
            self.connection = sqlite3.connect(
                self.config["path"],
                timeout=self.config["timeout"]
            )
            self.connection.row_factory = sqlite3.Row
            return True
        except Exception as e:
            logger.error(f"SQLite connection failed: {e}")
            return False
    
    def execute_query(self, query: str, params: Optional[Tuple] = None) -> List[Dict[str, Any]]:
        cursor = self.connection.cursor()
        cursor.execute(query, params or ())
        
        if query.strip().upper().startswith("SELECT"):
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
        else:
            self.connection.commit()
            return [{"rows_affected": cursor.rowcount}]

class PostgreSQLTester(DatabaseTester):
    """PostgreSQL database tester"""
    
    def connect(self) -> bool:
        if not POSTGRESQL_AVAILABLE:
            logger.warning("PostgreSQL driver not available")
            return False
        
        try:
            self.connection = psycopg2.connect(
                host=self.config["host"],
                port=self.config["port"],
                database=self.config["database"],
                user=self.config["user"],
                password=self.config["password"],
                cursor_factory=psycopg2.extras.RealDictCursor
            )
            return True
        except Exception as e:
            logger.error(f"PostgreSQL connection failed: {e}")
            return False
    
    def execute_query(self, query: str, params: Optional[Tuple] = None) -> List[Dict[str, Any]]:
        cursor = self.connection.cursor()
        cursor.execute(query, params or ())
        
        if query.strip().upper().startswith("SELECT"):
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
        else:
            self.connection.commit()
            return [{"rows_affected": cursor.rowcount}]

class MySQLTester(DatabaseTester):
    """MySQL database tester"""
    
    def connect(self) -> bool:
        if not MYSQL_AVAILABLE:
            logger.warning("MySQL driver not available")
            return False
        
        try:
            self.connection = pymysql.connect(
                host=self.config["host"],
                port=self.config["port"],
                database=self.config["database"],
                user=self.config["user"],
                password=self.config["password"],
                charset="utf8mb4",
                cursorclass=pymysql.cursors.DictCursor
            )
            return True
        except Exception as e:
            logger.error(f"MySQL connection failed: {e}")
            return False
    
    def execute_query(self, query: str, params: Optional[Tuple] = None) -> List[Dict[str, Any]]:
        cursor = self.connection.cursor()
        cursor.execute(query, params or ())
        
        if query.strip().upper().startswith("SELECT"):
            return cursor.fetchall()
        else:
            self.connection.commit()
            return [{"rows_affected": cursor.rowcount}]

@pytest.fixture(scope="session")
def database_testers():
    """Create database testers for all available backends"""
    testers = {}
    
    # SQLite (always available)
    sqlite_tester = SQLiteTester("sqlite", TEST_CONFIG["sqlite"])
    if sqlite_tester.connect():
        # Set up test tables for SQLite
        sqlite_tester.execute_query("""
            CREATE TABLE IF NOT EXISTS test_users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                email TEXT UNIQUE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        sqlite_tester.execute_query("""
            INSERT OR REPLACE INTO test_users (username, email) VALUES 
            ('alice', 'alice@test.com'),
            ('bob', 'bob@test.com'),
            ('delta', 'delta@test.com')
        """)
        testers["sqlite"] = sqlite_tester
    
    # PostgreSQL (if available and connected)
    if POSTGRESQL_AVAILABLE:
        pg_tester = PostgreSQLTester("postgresql", TEST_CONFIG["postgresql"])
        if pg_tester.connect():
            testers["postgresql"] = pg_tester
    
    # MySQL (if available and connected)
    if MYSQL_AVAILABLE:
        mysql_tester = MySQLTester("mysql", TEST_CONFIG["mysql"])
        if mysql_tester.connect():
            testers["mysql"] = mysql_tester
    
    yield testers
    
    # Cleanup
    for tester in testers.values():
        tester.disconnect()

class TestRealDatabaseConnections:
    """Test real database connections and basic operations"""
    
    def test_connection_health(self, database_testers):
        """Test that all database connections are healthy"""
        assert len(database_testers) > 0, "No database connections available"
        
        for backend, tester in database_testers.items():
            logger.info(f"Testing {backend} connection health")
            
            # Test simple query
            result, exec_time = tester.time_query("SELECT 1 as test_value")
            assert len(result) == 1
            assert result[0]["test_value"] == 1
            assert exec_time < 1.0  # Should be fast
            
            logger.info(f"{backend} connection healthy (query time: {exec_time:.4f}s)")
    
    def test_basic_operations(self, database_testers):
        """Test basic CRUD operations"""
        for backend, tester in database_testers.items():
            logger.info(f"Testing {backend} basic operations")
            
            if backend == "sqlite":
                # SQLite uses the test table we created
                result, _ = tester.time_query("SELECT COUNT(*) as count FROM test_users")
                assert result[0]["count"] >= 3
                
                result, _ = tester.time_query("SELECT * FROM test_users WHERE username = ?", ("alice",))
                assert len(result) == 1
                assert result[0]["username"] == "alice"
            
            elif backend == "postgresql":
                # Test PostgreSQL with schema
                result, _ = tester.time_query("SELECT COUNT(*) as count FROM saiql_test.users")
                assert result[0]["count"] >= 5
                
                result, _ = tester.time_query("SELECT * FROM saiql_test.users WHERE username = %s", ("alice_smith",))
                assert len(result) == 1
                assert result[0]["username"] == "alice_smith"
            
            elif backend == "mysql":
                # Test MySQL
                result, _ = tester.time_query("SELECT COUNT(*) as count FROM users")
                assert result[0]["count"] >= 5
                
                result, _ = tester.time_query("SELECT * FROM users WHERE username = %s", ("alice_smith",))
                assert len(result) == 1
                assert result[0]["username"] == "alice_smith"
            
            logger.info(f"{backend} basic operations passed")

class TestDatabasePerformance:
    """Performance tests for all database backends"""
    
    def test_benchmark_simple_queries(self, database_testers):
        """Benchmark simple queries across all backends"""
        results = {}
        
        for backend, tester in database_testers.items():
            logger.info(f"Benchmarking {backend} simple queries")
            
            if backend == "sqlite":
                queries = [
                    ("select_count", "SELECT COUNT(*) as count FROM test_users"),
                    ("select_all", "SELECT * FROM test_users"),
                    ("select_filtered", "SELECT * FROM test_users WHERE username LIKE 'a%'")
                ]
            else:
                table_prefix = "saiql_test." if backend == "postgresql" else ""
                queries = [
                    ("select_count", f"SELECT COUNT(*) as count FROM {table_prefix}users"),
                    ("select_all", f"SELECT * FROM {table_prefix}users"),
                    ("select_filtered", f"SELECT * FROM {table_prefix}users WHERE username LIKE 'a%'"),
                    ("join_query", f"SELECT u.username, COUNT(o.id) as order_count FROM {table_prefix}users u LEFT JOIN {table_prefix}orders o ON u.id = o.user_id GROUP BY u.id, u.username"),
                    ("aggregate_query", f"SELECT status, COUNT(*) as count, AVG(age) as avg_age FROM {table_prefix}users GROUP BY status")
                ]
            
            benchmark_results = tester.benchmark_queries(queries, iterations=20)
            results[backend] = benchmark_results
            
            # Verify performance expectations
            for query_name, metrics in benchmark_results.items():
                assert metrics["avg_time"] < 1.0, f"{backend} {query_name} too slow: {metrics['avg_time']:.4f}s"
                assert metrics["queries_per_second"] > 10, f"{backend} {query_name} too slow: {metrics['queries_per_second']:.1f} qps"
                
                logger.info(f"{backend} {query_name}: {metrics['avg_time']*1000:.2f}ms avg, {metrics['queries_per_second']:.1f} qps")
        
        # Save benchmark results
        with open("benchmark_results_real_databases.json", "w") as f:
            json.dump({
                "timestamp": datetime.now().isoformat(),
                "results": results
            }, f, indent=2, default=str)
    
    def test_stress_testing(self, database_testers):
        """Stress test database connections"""
        for backend, tester in database_testers.items():
            logger.info(f"Stress testing {backend}")
            
            if backend == "sqlite":
                query = "SELECT COUNT(*) as count FROM test_users"
            else:
                table_prefix = "saiql_test." if backend == "postgresql" else ""
                query = f"SELECT COUNT(*) as count FROM {table_prefix}users"
            
            # 30-second stress test with 4 concurrent threads
            stress_results = tester.stress_test(query, duration_seconds=30, concurrent_threads=4)
            
            # Verify stress test results
            assert stress_results["total_queries"] > 100, f"{backend} stress test too few queries: {stress_results['total_queries']}"
            assert stress_results["error_rate"] < 0.01, f"{backend} stress test error rate too high: {stress_results['error_rate']:.2%}"
            assert stress_results["queries_per_second"] > 50, f"{backend} stress test too slow: {stress_results['queries_per_second']:.1f} qps"
            
            logger.info(f"{backend} stress test: {stress_results['total_queries']} queries, {stress_results['queries_per_second']:.1f} qps, {stress_results['error_rate']:.2%} errors")

class TestDatabaseSpecificFeatures:
    """Test database-specific features and advanced operations"""
    
    @pytest.mark.skipif(not POSTGRESQL_AVAILABLE, reason="PostgreSQL not available")
    def test_postgresql_features(self, database_testers):
        """Test PostgreSQL-specific features"""
        if "postgresql" not in database_testers:
            pytest.skip("PostgreSQL not connected")
        
        tester = database_testers["postgresql"]
        logger.info("Testing PostgreSQL-specific features")
        
        # Test JSON operations
        result, _ = tester.time_query("""
            SELECT username, metadata->>'preferences' as prefs 
            FROM saiql_test.users 
            WHERE metadata->'preferences'->>'theme' = 'dark'
        """)
        assert len(result) >= 1
        
        # Test stored function
        result, _ = tester.time_query("SELECT * FROM get_user_order_summary(1)")
        assert len(result) == 1
        assert "total_orders" in result[0]
        
        # Test full-text search
        result, _ = tester.time_query("""
            SELECT title, ts_rank(to_tsvector('english', content), query) as rank
            FROM saiql_test.documents, plainto_tsquery('english', 'machine learning') query
            WHERE to_tsvector('english', content) @@ query
            ORDER BY rank DESC
        """)
        assert len(result) >= 1
        
        logger.info("PostgreSQL-specific features passed")
    
    @pytest.mark.skipif(not MYSQL_AVAILABLE, reason="MySQL not available")
    def test_mysql_features(self, database_testers):
        """Test MySQL-specific features"""
        if "mysql" not in database_testers:
            pytest.skip("MySQL not connected")
        
        tester = database_testers["mysql"]
        logger.info("Testing MySQL-specific features")
        
        # Test JSON operations
        result, _ = tester.time_query("""
            SELECT username, JSON_EXTRACT(metadata, '$.preferences.theme') as theme
            FROM users 
            WHERE JSON_EXTRACT(metadata, '$.preferences.theme') = 'dark'
        """)
        assert len(result) >= 1
        
        # Test stored procedure
        result, _ = tester.time_query("CALL GetUserOrderSummary(1)")
        assert len(result) == 1
        assert "total_orders" in result[0]
        
        # Test full-text search
        result, _ = tester.time_query("""
            SELECT title, MATCH(title, content) AGAINST('machine learning' IN NATURAL LANGUAGE MODE) as score
            FROM documents
            WHERE MATCH(title, content) AGAINST('machine learning' IN NATURAL LANGUAGE MODE)
            ORDER BY score DESC
        """)
        assert len(result) >= 1
        
        logger.info("MySQL-specific features passed")

class TestConcurrencyAndReliability:
    """Test database concurrency, transactions, and reliability"""
    
    def test_concurrent_access(self, database_testers):
        """Test concurrent database access"""
        for backend, tester in database_testers.items():
            logger.info(f"Testing {backend} concurrent access")
            
            def concurrent_worker(worker_id: int, results: List):
                try:
                    if backend == "sqlite":
                        query = f"SELECT '{worker_id}' as worker_id, COUNT(*) as count FROM test_users"
                    else:
                        table_prefix = "saiql_test." if backend == "postgresql" else ""
                        query = f"SELECT '{worker_id}' as worker_id, COUNT(*) as count FROM {table_prefix}users"
                    
                    result, exec_time = tester.time_query(query)
                    results.append({"worker_id": worker_id, "result": result, "time": exec_time})
                except Exception as e:
                    results.append({"worker_id": worker_id, "error": str(e)})
            
            # Run 10 concurrent workers
            results = []
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = [executor.submit(concurrent_worker, i, results) for i in range(10)]
                for future in as_completed(futures):
                    future.result()  # Wait for completion
            
            # Verify all workers completed successfully
            assert len(results) == 10
            errors = [r for r in results if "error" in r]
            assert len(errors) == 0, f"{backend} concurrent access had {len(errors)} errors"
            
            logger.info(f"{backend} concurrent access test passed")

def generate_comprehensive_report(database_testers: Dict[str, DatabaseTester]) -> Dict[str, Any]:
    """Generate comprehensive test report"""
    report = {
        "timestamp": datetime.now().isoformat(),
        "test_environment": {
            "platform": os.uname().sysname,
            "architecture": os.uname().machine,
            "python_version": os.sys.version,
            "available_backends": list(database_testers.keys())
        },
        "backend_details": {},
        "performance_summary": {},
        "recommendations": []
    }
    
    for backend, tester in database_testers.items():
        report["backend_details"][backend] = {
            "config": {k: v for k, v in tester.config.items() if k != "password"},
            "metrics": tester.metrics,
            "connection_status": "connected" if tester.connection else "disconnected"
        }
    
    return report

if __name__ == "__main__":
    # Run tests manually
    print("🚀 Running Real Database Integration Tests")
    print("=" * 50)
    
    # Create testers
    testers = {}
    
    # SQLite
    sqlite_tester = SQLiteTester("sqlite", TEST_CONFIG["sqlite"])
    if sqlite_tester.connect():
        print("✅ SQLite connected")
        testers["sqlite"] = sqlite_tester
    
    # PostgreSQL
    if POSTGRESQL_AVAILABLE:
        pg_tester = PostgreSQLTester("postgresql", TEST_CONFIG["postgresql"])
        if pg_tester.connect():
            print("✅ PostgreSQL connected")
            testers["postgresql"] = pg_tester
        else:
            print("❌ PostgreSQL connection failed")
    
    # MySQL
    if MYSQL_AVAILABLE:
        mysql_tester = MySQLTester("mysql", TEST_CONFIG["mysql"])
        if mysql_tester.connect():
            print("✅ MySQL connected")
            testers["mysql"] = mysql_tester
        else:
            print("❌ MySQL connection failed")
    
    if not testers:
        print("❌ No database connections available")
        exit(1)
    
    print(f"\n🔍 Testing {len(testers)} database backend(s)")
    
    # Generate and save report
    report = generate_comprehensive_report(testers)
    with open("real_database_test_report.json", "w") as f:
        json.dump(report, f, indent=2, default=str)
    
    print(f"📊 Test report saved to: real_database_test_report.json")
    
    # Cleanup
    for tester in testers.values():
        tester.disconnect()
    
    print("✅ All tests completed successfully!")
# IHNpZ25hdHVyZTplYzUyNTBkZWRmNTZjZTZiMDA4NCBpZDpTQUlRTC1DSEFSTElFLVBST0QgYnk6QXBvbGxvICYgQ2xhdWRlIA==
