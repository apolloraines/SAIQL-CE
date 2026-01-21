#!/usr/bin/env python3
"""
Integration Test for DB Migrator
"""

import unittest
import os
import sys
import shutil
import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, patch

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.resolve()))

from tools.db_migrator import DBMigrator

class TestDBMigrator(unittest.TestCase):
    
    def setUp(self):
        self.test_dir = Path("/tmp/saiql_migration_test")
        if self.test_dir.exists():
            shutil.rmtree(self.test_dir)
        self.test_dir.mkdir()
        
        self.source_url = "postgresql://test:test@localhost/testdb"
        
    def tearDown(self):
        if self.test_dir.exists():
            shutil.rmtree(self.test_dir)

    def test_migration_flow(self):
        """Test the full migration flow with mocked Postgres source"""
        
        # Create a mock for psycopg2
        mock_psycopg2 = MagicMock()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        
        mock_psycopg2.connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        # Mock cursor results
        # 1. get_tables -> ['migrated_users']
        # 2. get_schema (columns) -> id, name
        # 3. get_schema (pk) -> id
        # 4. get_schema (fks) -> []
        # 5. migrate_data (SELECT *) -> rows
        mock_cursor.fetchall.side_effect = [
            [('migrated_users',)],  # get_tables
            [('id', 'integer', 'NO'), ('name', 'text', 'NO')], # get_schema (columns)
            [('id',)], # get_schema (pk)
            [], # get_schema (fks)
            [(1, 'Alice'), (2, 'Bob')] # migrate_data (SELECT *)
        ]
        
        # Patch sys.modules to inject mock psycopg2
        with patch.dict(sys.modules, {'psycopg2': mock_psycopg2}):
            # Re-import to ensure it picks up the mock if it was already imported
            if 'tools.db_migrator' in sys.modules:
                del sys.modules['tools.db_migrator']
            from tools.db_migrator import DBMigrator
            
            # Initialize migrator
            migrator = DBMigrator(self.source_url, str(self.test_dir))
            
            # Run migration
            migrator.run()
        
        # Verify target SQLite database
        target_db = self.test_dir / "saiql_store.db"
        self.assertTrue(target_db.exists())
        
        # Check data in target
        conn = sqlite3.connect(target_db)
        cursor = conn.cursor()
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        self.assertIn(('migrated_users',), tables)
        
        cursor.execute("SELECT * FROM migrated_users ORDER BY id")
        rows = cursor.fetchall()
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0], (1, 'Alice'))
        self.assertEqual(rows[1], (2, 'Bob'))
        
        conn.close()
        print("Migration test passed! Data successfully migrated to SAIQL.")

    def test_mysql_migration_flow(self):
        """Test the full migration flow with mocked MySQL source"""
        
        # Create a mock for mysql.connector
        mock_mysql = MagicMock()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        
        mock_mysql.connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        # Mock cursor results
        # 1. get_tables -> ['mysql_users']
        # 2. get_schema (columns) -> id, name, PRI
        # 3. get_schema (fks) -> []
        # 4. migrate_data (SELECT *) -> rows
        mock_cursor.fetchall.side_effect = [
            [('mysql_users',)],  # get_tables
            [('id', 'int(11)', 'NO', 'PRI'), ('name', 'varchar(255)', 'NO', '')], # get_schema (DESCRIBE)
            [], # get_schema (fks)
            [(1, 'Alice'), (2, 'Bob')] # migrate_data (SELECT *)
        ]
        
        # Patch sys.modules to inject mock mysql.connector
        # We need to mock 'mysql' package as well
        mock_mysql_pkg = MagicMock()
        mock_mysql_pkg.connector = mock_mysql
        
        with patch.dict(sys.modules, {'mysql': mock_mysql_pkg, 'mysql.connector': mock_mysql}):
            # Re-import to ensure it picks up the mock if it was already imported
            if 'tools.db_migrator' in sys.modules:
                del sys.modules['tools.db_migrator']
            from tools.db_migrator import DBMigrator
            
            # Initialize migrator
            mysql_url = "mysql://test:test@localhost/testdb"
            migrator = DBMigrator(mysql_url, str(self.test_dir))
            
            # Run migration
            migrator.run()
        
        # Verify target SQLite database
        target_db = self.test_dir / "saiql_store.db"
        self.assertTrue(target_db.exists())
        
        # Check data in target
        conn = sqlite3.connect(target_db)
        cursor = conn.cursor()
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        self.assertIn(('mysql_users',), tables)
        
        cursor.execute("SELECT * FROM mysql_users ORDER BY id")
        rows = cursor.fetchall()
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0], (1, 'Alice'))
        self.assertEqual(rows[1], (2, 'Bob'))
        
        conn.close()
        print("MySQL Migration test passed!")

if __name__ == '__main__':
    unittest.main()
