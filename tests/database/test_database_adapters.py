#!/usr/bin/env python3
"""
Comprehensive Database Adapter Testing Suite
============================================

Tests for all database adapters (PostgreSQL, MySQL, SQLite) with:
- Connection testing
- Query execution validation
- Performance benchmarking
- Error handling
- Security testing
- Cross-database compatibility

Author: Apollo & Claude
Version: 1.0.0
"""

import pytest
import asyncio
import os
import time
import random
import string
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
import logging

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

# Test configuration
TEST_DATABASE_CONFIG = {
    "sqlite": {
        "path": ":memory:",  # Use in-memory database for tests
        "timeout": 30
    },
    "postgresql": {
        "host": os.getenv("PG_TEST_HOST", "localhost"),
        "port": int(os.getenv("PG_TEST_PORT", "5432")),
        "database": os.getenv("PG_TEST_DATABASE", "saiql_test"),
        "user": os.getenv("PG_TEST_USER", "saiql_test"),
        "password": os.getenv("PG_TEST_PASSWORD", "test_password"),
        "ssl_mode": "prefer"
    },
    "mysql": {
        "host": os.getenv("MYSQL_TEST_HOST", "localhost"),
        "port": int(os.getenv("MYSQL_TEST_PORT", "3306")),
        "database": os.getenv("MYSQL_TEST_DATABASE", "saiql_test"),
        "user": os.getenv("MYSQL_TEST_USER", "saiql_test"),
        "password": os.getenv("MYSQL_TEST_PASSWORD", "test_password"),
        "charset": "utf8mb4"
    }
}

class DatabaseTester:
    """Base class for database testing"""
    
    def __init__(self, db_type: str, config: Dict[str, Any]):
        self.db_type = db_type
        self.config = config
        self.connection = None
        self.logger = logging.getLogger(f"{__name__}.{db_type}")
    
    def connect(self):
        """Connect to database"""
        raise NotImplementedError
    
    def disconnect(self):
        """Disconnect from database"""
        if self.connection:
            self.connection.close()
            self.connection = None
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """Execute a query and return results"""
        raise NotImplementedError
    
    def create_test_table(self, table_name: str):
        """Create a test table"""
        raise NotImplementedError
    
    def drop_test_table(self, table_name: str):
        """Drop a test table"""
        try:
            self.execute_query(f"DROP TABLE IF EXISTS {table_name}")
        except Exception as e:
            self.logger.warning(f"Failed to drop table {table_name}: {e}")

class SQLiteTester(DatabaseTester):
    """SQLite database tester"""
    
    def connect(self):
        """Connect to SQLite database"""
        try:
            self.connection = sqlite3.connect(
                self.config["path"],
                timeout=self.config.get("timeout", 30)
            )
            self.connection.row_factory = sqlite3.Row
            return True
        except Exception as e:
            self.logger.error(f"SQLite connection failed: {e}")
            return False
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """Execute SQLite query"""
        cursor = self.connection.cursor()
        cursor.execute(query, params or ())
        
        if query.strip().upper().startswith("SELECT"):
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
        else:
            self.connection.commit()
            return [{"rows_affected": cursor.rowcount}]
    
    def create_test_table(self, table_name: str):
        """Create SQLite test table"""
        query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            value INTEGER,
            data TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        self.execute_query(query)

class PostgreSQLTester(DatabaseTester):
    """PostgreSQL database tester"""
    
    def connect(self):
        """Connect to PostgreSQL database"""
        if not POSTGRESQL_AVAILABLE:
            self.logger.warning("PostgreSQL driver not available")
            return False
        
        try:
            self.connection = psycopg2.connect(
                host=self.config["host"],
                port=self.config["port"],
                database=self.config["database"],
                user=self.config["user"],
                password=self.config["password"],
                sslmode=self.config.get("ssl_mode", "prefer")
            )
            return True
        except Exception as e:
            self.logger.error(f"PostgreSQL connection failed: {e}")
            return False
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """Execute PostgreSQL query"""
        cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute(query, params or ())
        
        if query.strip().upper().startswith("SELECT"):
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
        else:
            self.connection.commit()
            return [{"rows_affected": cursor.rowcount}]
    
    def create_test_table(self, table_name: str):
        """Create PostgreSQL test table"""
        query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            value INTEGER,
            data TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        self.execute_query(query)

class MySQLTester(DatabaseTester):
    """MySQL database tester"""
    
    def connect(self):
        """Connect to MySQL database"""
        if not MYSQL_AVAILABLE:
            self.logger.warning("MySQL driver not available")
            return False
        
        try:
            self.connection = pymysql.connect(
                host=self.config["host"],
                port=self.config["port"],
                database=self.config["database"],
                user=self.config["user"],
                password=self.config["password"],
                charset=self.config.get("charset", "utf8mb4"),
                cursorclass=pymysql.cursors.DictCursor
            )
            return True
        except Exception as e:
            self.logger.error(f"MySQL connection failed: {e}")
            return False
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """Execute MySQL query"""
        cursor = self.connection.cursor()
        cursor.execute(query, params or ())
        
        if query.strip().upper().startswith("SELECT"):
            return cursor.fetchall()
        else:
            self.connection.commit()
            return [{"rows_affected": cursor.rowcount}]
    
    def create_test_table(self, table_name: str):
        """Create MySQL test table"""
        query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            value INT,
            data TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        self.execute_query(query)

# Test fixtures
@pytest.fixture(scope="session")
def database_testers():
    """Create database testers for all available databases"""
    testers = {}
    
    # SQLite (always available)
    testers["sqlite"] = SQLiteTester("sqlite", TEST_DATABASE_CONFIG["sqlite"])
    
    # PostgreSQL (if available)
    if POSTGRESQL_AVAILABLE:
        testers["postgresql"] = PostgreSQLTester("postgresql", TEST_DATABASE_CONFIG["postgresql"])
    
    # MySQL (if available)
    if MYSQL_AVAILABLE:
        testers["mysql"] = MySQLTester("mysql", TEST_DATABASE_CONFIG["mysql"])
    
    return testers

@pytest.fixture
def connected_testers(database_testers):
    """Connect all database testers"""
    connected = {}
    
    for db_type, tester in database_testers.items():
        if tester.connect():
            connected[db_type] = tester
        else:
            pytest.skip(f"{db_type} database not available")
    
    yield connected
    
    # Cleanup
    for tester in connected.values():
        tester.disconnect()

# Connection Tests
class TestDatabaseConnections:
    """Test database connection functionality"""
    
    def test_sqlite_connection(self, database_testers):
        """Test SQLite connection"""
        tester = database_testers["sqlite"]
        assert tester.connect()
        assert tester.connection is not None
        tester.disconnect()
    
    @pytest.mark.skipif(not POSTGRESQL_AVAILABLE, reason="PostgreSQL not available")
    def test_postgresql_connection(self, database_testers):
        """Test PostgreSQL connection"""
        tester = database_testers["postgresql"]
        connected = tester.connect()
        if connected:
            assert tester.connection is not None
            tester.disconnect()
        else:
            pytest.skip("PostgreSQL server not available")
    
    @pytest.mark.skipif(not MYSQL_AVAILABLE, reason="MySQL not available")
    def test_mysql_connection(self, database_testers):
        """Test MySQL connection"""
        tester = database_testers["mysql"]
        connected = tester.connect()
        if connected:
            assert tester.connection is not None
            tester.disconnect()
        else:
            pytest.skip("MySQL server not available")

# Query Execution Tests
class TestQueryExecution:
    """Test query execution across databases"""
    
    def test_basic_queries(self, connected_testers):
        """Test basic SQL queries"""
        for db_type, tester in connected_testers.items():
            # Test simple SELECT
            if db_type == "sqlite":
                result = tester.execute_query("SELECT 1 as test_value")
            elif db_type == "postgresql":
                result = tester.execute_query("SELECT 1 as test_value")
            elif db_type == "mysql":
                result = tester.execute_query("SELECT 1 as test_value")
            
            assert len(result) == 1
            assert result[0]["test_value"] == 1
    
    def test_table_operations(self, connected_testers):
        """Test table creation, insertion, and querying"""
        for db_type, tester in connected_testers.items():
            table_name = f"test_table_{db_type}_{int(time.time())}"
            
            try:
                # Create table
                tester.create_test_table(table_name)
                
                # Insert data
                insert_query = f"INSERT INTO {table_name} (name, value, data) VALUES (%s, %s, %s)"
                if db_type == "sqlite":
                    insert_query = insert_query.replace("%s", "?")
                
                test_data = [
                    ("Test 1", 100, "Sample data 1"),
                    ("Test 2", 200, "Sample data 2"),
                    ("Test 3", 300, "Sample data 3")
                ]
                
                for data in test_data:
                    tester.execute_query(insert_query, data)
                
                # Query data
                select_query = f"SELECT * FROM {table_name} ORDER BY id"
                results = tester.execute_query(select_query)
                
                assert len(results) == 3
                assert results[0]["name"] == "Test 1"
                assert results[1]["value"] == 200
                
            finally:
                # Cleanup
                tester.drop_test_table(table_name)
    
    def test_parameterized_queries(self, connected_testers):
        """Test parameterized queries to prevent SQL injection"""
        for db_type, tester in connected_testers.items():
            table_name = f"param_test_{db_type}_{int(time.time())}"
            
            try:
                tester.create_test_table(table_name)
                
                # Insert with parameters
                insert_query = f"INSERT INTO {table_name} (name, value) VALUES (%s, %s)"
                if db_type == "sqlite":
                    insert_query = insert_query.replace("%s", "?")
                
                tester.execute_query(insert_query, ("Param Test", 42))
                
                # Select with parameters
                select_query = f"SELECT * FROM {table_name} WHERE name = %s"
                if db_type == "sqlite":
                    select_query = select_query.replace("%s", "?")
                
                results = tester.execute_query(select_query, ("Param Test",))
                
                assert len(results) == 1
                assert results[0]["value"] == 42
                
            finally:
                tester.drop_test_table(table_name)

# Performance Tests
class TestDatabasePerformance:
    """Test database performance characteristics"""
    
    def test_bulk_insert_performance(self, connected_testers):
        """Test bulk insert performance"""
        for db_type, tester in connected_testers.items():
            table_name = f"perf_test_{db_type}_{int(time.time())}"
            
            try:
                tester.create_test_table(table_name)
                
                # Generate test data
                test_records = 1000
                start_time = time.time()
                
                insert_query = f"INSERT INTO {table_name} (name, value, data) VALUES (%s, %s, %s)"
                if db_type == "sqlite":
                    insert_query = insert_query.replace("%s", "?")
                
                for i in range(test_records):
                    name = f"Record_{i}"
                    value = random.randint(1, 1000)
                    data = ''.join(random.choices(string.ascii_letters, k=50))
                    
                    tester.execute_query(insert_query, (name, value, data))
                
                insert_time = time.time() - start_time
                
                # Verify count
                count_result = tester.execute_query(f"SELECT COUNT(*) as count FROM {table_name}")
                assert count_result[0]["count"] == test_records
                
                # Log performance
                records_per_second = test_records / insert_time
                print(f"{db_type} bulk insert: {records_per_second:.2f} records/second")
                
                # Performance threshold (very lenient for testing)
                assert records_per_second > 10  # At least 10 records/second
                
            finally:
                tester.drop_test_table(table_name)
    
    def test_query_performance(self, connected_testers):
        """Test query performance with indexed and non-indexed columns"""
        for db_type, tester in connected_testers.items():
            table_name = f"query_perf_{db_type}_{int(time.time())}"
            
            try:
                tester.create_test_table(table_name)
                
                # Insert test data
                insert_query = f"INSERT INTO {table_name} (name, value, data) VALUES (%s, %s, %s)"
                if db_type == "sqlite":
                    insert_query = insert_query.replace("%s", "?")
                
                # Insert 1000 records
                for i in range(1000):
                    tester.execute_query(insert_query, (f"Record_{i}", i, f"Data_{i}"))
                
                # Test query performance
                start_time = time.time()
                results = tester.execute_query(f"SELECT * FROM {table_name} WHERE value > 500")
                query_time = time.time() - start_time
                
                assert len(results) == 499  # 501-999 = 499 records
                print(f"{db_type} query time: {query_time:.4f} seconds")
                
                # Performance threshold
                assert query_time < 1.0  # Should complete within 1 second
                
            finally:
                tester.drop_test_table(table_name)

# Error Handling Tests
class TestErrorHandling:
    """Test database error handling"""
    
    def test_invalid_query_handling(self, connected_testers):
        """Test handling of invalid SQL queries"""
        for db_type, tester in connected_testers.items():
            with pytest.raises(Exception):
                tester.execute_query("INVALID SQL STATEMENT")
    
    def test_constraint_violation_handling(self, connected_testers):
        """Test handling of constraint violations"""
        for db_type, tester in connected_testers.items():
            table_name = f"constraint_test_{db_type}_{int(time.time())}"
            
            try:
                # Create table with unique constraint
                if db_type == "sqlite":
                    create_query = f"""
                    CREATE TABLE {table_name} (
                        id INTEGER PRIMARY KEY,
                        unique_value TEXT UNIQUE NOT NULL
                    )
                    """
                elif db_type == "postgresql":
                    create_query = f"""
                    CREATE TABLE {table_name} (
                        id SERIAL PRIMARY KEY,
                        unique_value VARCHAR(255) UNIQUE NOT NULL
                    )
                    """
                elif db_type == "mysql":
                    create_query = f"""
                    CREATE TABLE {table_name} (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        unique_value VARCHAR(255) UNIQUE NOT NULL
                    )
                    """
                
                tester.execute_query(create_query)
                
                # Insert first record
                insert_query = f"INSERT INTO {table_name} (unique_value) VALUES (%s)"
                if db_type == "sqlite":
                    insert_query = insert_query.replace("%s", "?")
                
                tester.execute_query(insert_query, ("unique_test",))
                
                # Try to insert duplicate (should fail)
                with pytest.raises(Exception):
                    tester.execute_query(insert_query, ("unique_test",))
                    
            finally:
                tester.drop_test_table(table_name)

# Cross-Database Compatibility Tests
class TestCrossDatabaseCompatibility:
    """Test compatibility across different database systems"""
    
    def test_data_type_compatibility(self, connected_testers):
        """Test that common data types work across databases"""
        compatible_data = [
            ("String Value", "VARCHAR/TEXT"),
            (42, "INTEGER"),
            (3.14159, "FLOAT/REAL"),
            (True, "BOOLEAN"),
            (datetime.now(), "TIMESTAMP")
        ]
        
        for db_type, tester in connected_testers.items():
            table_name = f"compat_test_{db_type}_{int(time.time())}"
            
            try:
                # Create compatible table
                if db_type == "sqlite":
                    create_query = f"""
                    CREATE TABLE {table_name} (
                        id INTEGER PRIMARY KEY,
                        str_val TEXT,
                        int_val INTEGER,
                        float_val REAL,
                        bool_val INTEGER,
                        time_val TIMESTAMP
                    )
                    """
                elif db_type == "postgresql":
                    create_query = f"""
                    CREATE TABLE {table_name} (
                        id SERIAL PRIMARY KEY,
                        str_val VARCHAR(255),
                        int_val INTEGER,
                        float_val FLOAT,
                        bool_val BOOLEAN,
                        time_val TIMESTAMP
                    )
                    """
                elif db_type == "mysql":
                    create_query = f"""
                    CREATE TABLE {table_name} (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        str_val VARCHAR(255),
                        int_val INT,
                        float_val FLOAT,
                        bool_val BOOLEAN,
                        time_val TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                
                tester.execute_query(create_query)
                
                # Insert compatible data
                insert_query = f"""
                INSERT INTO {table_name} (str_val, int_val, float_val, bool_val, time_val) 
                VALUES (%s, %s, %s, %s, %s)
                """
                if db_type == "sqlite":
                    insert_query = insert_query.replace("%s", "?")
                
                test_timestamp = datetime.now().replace(microsecond=0)  # Remove microseconds for compatibility
                tester.execute_query(insert_query, (
                    "Test String",
                    123,
                    45.67,
                    True,
                    test_timestamp
                ))
                
                # Verify data
                results = tester.execute_query(f"SELECT * FROM {table_name}")
                assert len(results) == 1
                
                row = results[0]
                assert row["str_val"] == "Test String"
                assert row["int_val"] == 123
                assert abs(row["float_val"] - 45.67) < 0.01  # Float comparison with tolerance
                
            finally:
                tester.drop_test_table(table_name)

# Integration Test Runner
def run_database_tests():
    """Run all database tests"""
    import subprocess
    import sys
    
    result = subprocess.run([
        sys.executable, "-m", "pytest", 
        __file__, 
        "-v", 
        "--tb=short",
        "--capture=no"
    ])
    
    return result.returncode == 0

if __name__ == "__main__":
    # Run tests if executed directly
    success = run_database_tests()
    exit(0 if success else 1)

# // c2lnbmF0dXJlOjdhM2JkZjc1YjVjZjJlMTEgaWQ6U0FJUUwtQ0hBUkxJRS12MS4wIGJ5OkFwb2xsbyAmIENsYXVkZSA=
# IHNpZ25hdHVyZTplYzUyNTBkZWRmNTZjZTZiMDA4NCBpZDpTQUlRTC1DSEFSTElFLVBST0QgYnk6QXBvbGxvICYgQ2xhdWRlIA==
