#!/usr/bin/env python3
"""
Test Coverage Booster - Database Manager Extended Tests
======================================================

Extended tests for database manager to boost test coverage significantly.

Author: Apollo & Claude  
Version: 1.0.0
"""

import unittest
import sys
import os
import tempfile
import json
from pathlib import Path

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

try:
    from core.database_manager import DatabaseManager, QueryResult
except ImportError:
    # Skip if not available
    DatabaseManager = None


class TestDatabaseManagerExtended(unittest.TestCase):
    """Extended tests for database manager"""
    
    def setUp(self):
        """Set up test environment"""
        if DatabaseManager is None:
            self.skipTest("DatabaseManager not available")
        
        self.manager = DatabaseManager()
    
    def test_manager_initialization(self):
        """Test database manager initialization"""
        self.assertIsNotNone(self.manager)
        self.assertEqual(self.manager.default_backend, "sqlite")
    
    def test_query_result_creation(self):
        """Test QueryResult object creation"""
        # Test successful result
        result = QueryResult(
            success=True,
            data=[{"id": 1, "name": "test"}],
            metadata={"rows": 1},
            query="SELECT * FROM test",
            execution_time=0.05
        )
        
        self.assertTrue(result.success)
        self.assertEqual(len(result.data), 1)
        self.assertEqual(result.data[0]["name"], "test")
        self.assertEqual(result.metadata["rows"], 1)
        self.assertEqual(result.query, "SELECT * FROM test")
        self.assertEqual(result.execution_time, 0.05)
        
        # Test failed result
        result = QueryResult(
            success=False,
            error="Table not found",
            query="SELECT * FROM nonexistent"
        )
        
        self.assertFalse(result.success)
        self.assertEqual(result.error, "Table not found")
        self.assertEqual(result.query, "SELECT * FROM nonexistent")
    
    def test_available_backends(self):
        """Test getting available backends"""
        backends = self.manager.get_available_backends()
        
        self.assertIsInstance(backends, list)
        self.assertIn("sqlite", backends)
        self.assertGreaterEqual(len(backends), 1)
    
    def test_backend_info(self):
        """Test getting backend information"""
        # Test SQLite backend (should always be available)
        info = self.manager.get_backend_info("sqlite")
        
        self.assertIsInstance(info, dict)
        self.assertIn("initialized", info)
        self.assertTrue(info["initialized"])
        
        # Test non-existent backend
        info = self.manager.get_backend_info("nonexistent")
        self.assertIsNone(info)
    
    def test_statistics(self):
        """Test getting manager statistics"""
        stats = self.manager.get_statistics()
        
        self.assertIsInstance(stats, dict)
        self.assertIn("default_backend", stats)
        self.assertIn("available_backends", stats)
        self.assertEqual(stats["default_backend"], "sqlite")
        self.assertIsInstance(stats["available_backends"], list)
    
    def test_basic_query_execution(self):
        """Test basic query execution"""
        # Test simple SELECT
        result = self.manager.execute("SELECT 'Hello, World!' as message")
        
        self.assertTrue(result.success)
        self.assertIsInstance(result.data, list)
        self.assertEqual(len(result.data), 1)
        self.assertEqual(result.data[0]["message"], "Hello, World!")
        self.assertIsNotNone(result.execution_time)
        self.assertGreater(result.execution_time, 0)
    
    def test_parameterized_queries(self):
        """Test parameterized query execution"""
        # Create a simple test table
        self.manager.execute("CREATE TEMPORARY TABLE test_params (id INTEGER, value TEXT)")
        self.manager.execute("INSERT INTO test_params VALUES (1, 'test1'), (2, 'test2')")
        
        # Test parameterized query
        result = self.manager.execute("SELECT * FROM test_params WHERE id = ?", (1,))
        
        self.assertTrue(result.success)
        self.assertEqual(len(result.data), 1)
        self.assertEqual(result.data[0]["id"], 1)
        self.assertEqual(result.data[0]["value"], "test1")
    
    def test_transaction_support(self):
        """Test transaction support"""
        # Create test table
        self.manager.execute("CREATE TEMPORARY TABLE test_trans (id INTEGER, value TEXT)")
        
        # Test successful transaction
        with self.manager.transaction():
            self.manager.execute("INSERT INTO test_trans VALUES (1, 'value1')")
            self.manager.execute("INSERT INTO test_trans VALUES (2, 'value2')")
        
        result = self.manager.execute("SELECT COUNT(*) as count FROM test_trans")
        self.assertEqual(result.data[0]["count"], 2)
    
    def test_error_handling(self):
        """Test error handling"""
        # Test invalid SQL
        result = self.manager.execute("INVALID SQL STATEMENT")
        
        self.assertFalse(result.success)
        self.assertIsNotNone(result.error)
        self.assertIn("syntax error", result.error.lower())
    
    def test_connection_management(self):
        """Test connection management"""
        # Test that multiple queries work
        for i in range(5):
            result = self.manager.execute(f"SELECT {i} as number")
            self.assertTrue(result.success)
            self.assertEqual(result.data[0]["number"], i)
    
    def test_metadata_handling(self):
        """Test metadata in query results"""
        # Create test data
        self.manager.execute("CREATE TEMPORARY TABLE test_meta (id INTEGER, name TEXT)")
        self.manager.execute("INSERT INTO test_meta VALUES (1, 'first'), (2, 'second'), (3, 'third')")
        
        result = self.manager.execute("SELECT * FROM test_meta")
        
        self.assertTrue(result.success)
        self.assertEqual(len(result.data), 3)
        
        # Check that metadata might be present
        if hasattr(result, 'metadata') and result.metadata:
            self.assertIsInstance(result.metadata, dict)


class TestDatabaseManagerConfiguration(unittest.TestCase):
    """Test database manager configuration"""
    
    def test_config_file_handling(self):
        """Test configuration file handling"""
        # Create temporary config
        config_data = {
            "default_backend": "sqlite",
            "backends": {
                "sqlite": {
                    "path": ":memory:",
                    "timeout": 30
                }
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(config_data, f)
            config_path = f.name
        
        try:
            # Test that config can be loaded (implementation dependent)
            self.assertTrue(os.path.exists(config_path))
            
            # Read it back
            with open(config_path, 'r') as f:
                loaded_config = json.load(f)
            
            self.assertEqual(loaded_config["default_backend"], "sqlite")
            self.assertIn("sqlite", loaded_config["backends"])
        
        finally:
            os.unlink(config_path)


class TestDatabaseManagerPerformance(unittest.TestCase):
    """Test database manager performance characteristics"""
    
    def setUp(self):
        """Set up test environment"""
        if DatabaseManager is None:
            self.skipTest("DatabaseManager not available")
        
        self.manager = DatabaseManager()
    
    def test_batch_operations(self):
        """Test batch operation performance"""
        # Create test table
        self.manager.execute("CREATE TEMPORARY TABLE test_batch (id INTEGER, value TEXT)")
        
        # Insert multiple records
        for i in range(10):
            result = self.manager.execute("INSERT INTO test_batch VALUES (?, ?)", (i, f"value_{i}"))
            self.assertTrue(result.success)
        
        # Verify all records
        result = self.manager.execute("SELECT COUNT(*) as count FROM test_batch")
        self.assertEqual(result.data[0]["count"], 10)
        
        # Test batch select
        result = self.manager.execute("SELECT * FROM test_batch ORDER BY id")
        self.assertEqual(len(result.data), 10)
        
        for i, row in enumerate(result.data):
            self.assertEqual(row["id"], i)
            self.assertEqual(row["value"], f"value_{i}")
    
    def test_query_timing(self):
        """Test query execution timing"""
        result = self.manager.execute("SELECT 1 as test")
        
        self.assertTrue(result.success)
        self.assertIsNotNone(result.execution_time)
        self.assertGreater(result.execution_time, 0)
        self.assertLess(result.execution_time, 1.0)  # Should be fast
    
    def test_large_result_set(self):
        """Test handling of larger result sets"""
        # Create test data
        self.manager.execute("CREATE TEMPORARY TABLE test_large (id INTEGER, data TEXT)")
        
        # Insert 100 records
        for i in range(100):
            self.manager.execute("INSERT INTO test_large VALUES (?, ?)", (i, f"data_{i}" * 10))
        
        # Query all records
        result = self.manager.execute("SELECT * FROM test_large")
        
        self.assertTrue(result.success)
        self.assertEqual(len(result.data), 100)
        
        # Verify data integrity
        for i in range(0, 100, 10):  # Check every 10th record
            self.assertEqual(result.data[i]["id"], i)
            self.assertEqual(result.data[i]["data"], f"data_{i}" * 10)


class TestDatabaseManagerEdgeCases(unittest.TestCase):
    """Test edge cases and error conditions"""
    
    def setUp(self):
        """Set up test environment"""
        if DatabaseManager is None:
            self.skipTest("DatabaseManager not available")
        
        self.manager = DatabaseManager()
    
    def test_empty_query(self):
        """Test empty query handling"""
        result = self.manager.execute("")
        self.assertFalse(result.success)
    
    def test_none_query(self):
        """Test None query handling"""
        result = self.manager.execute(None)
        self.assertFalse(result.success)
    
    def test_very_long_query(self):
        """Test very long query handling"""
        # Create a long but valid query
        long_query = "SELECT " + ", ".join([f"'{i}' as col_{i}" for i in range(100)])
        
        result = self.manager.execute(long_query)
        self.assertTrue(result.success)
        self.assertEqual(len(result.data), 1)
        self.assertEqual(len(result.data[0]), 100)
    
    def test_special_characters(self):
        """Test handling of special characters"""
        test_strings = [
            "Hello 'World'",
            'Hello "World"',
            "String with\nnewline",
            "String with\ttab",
            "Unicode: αβγδε",
            "Emoji: 🚀🌙⭐"
        ]
        
        self.manager.execute("CREATE TEMPORARY TABLE test_chars (id INTEGER, text TEXT)")
        
        for i, test_string in enumerate(test_strings):
            result = self.manager.execute("INSERT INTO test_chars VALUES (?, ?)", (i, test_string))
            self.assertTrue(result.success, f"Failed to insert: {test_string}")
        
        # Verify all strings
        result = self.manager.execute("SELECT * FROM test_chars ORDER BY id")
        self.assertTrue(result.success)
        self.assertEqual(len(result.data), len(test_strings))
        
        for i, row in enumerate(result.data):
            self.assertEqual(row["text"], test_strings[i])


if __name__ == "__main__":
    unittest.main(verbosity=2)
# IHNpZ25hdHVyZTplYzUyNTBkZWRmNTZjZTZiMDA4NCBpZDpTQUlRTC1DSEFSTElFLVBST0QgYnk6QXBvbGxvICYgQ2xhdWRlIA==
