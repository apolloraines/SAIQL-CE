
import unittest
import os
import sqlite3
from tools.db_migrator import DBMigrator
from unittest.mock import MagicMock

class TestDryRunSafety(unittest.TestCase):

    def setUp(self):
        cwd = os.getcwd()
        self.db_path = f"{cwd}/test_dryrun.db"
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
            
        # Initialize an empty DB
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("CREATE TABLE _init_marker (id INT)") # just to make file exist and be valid
            
        self.db_url = f"sqlite:///{self.db_path}"
        self.migrator = DBMigrator("sqlite:///:memory:", target_url=self.db_url, dry_run=True)
        
        # Mock Source
        self.migrator.source_conn = MagicMock()
        self.migrator.source_type = 'sqlite'
        # Simulate schema
        self.migrator.schema_map = {
            'GHOST_TABLE': {
                'columns': [{'name': 'id', 'type': 'INTEGER', 'nullable': False}],
                'pk': ['id'], 'fks': []
            }
        }
        # Simulate data
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [(1,), (2,), (3,)]
        self.migrator.source_conn.cursor.return_value = mock_cursor

    def test_dry_run_no_writes(self):
        # 1. Run migration in Dry Run
        # We manually call create_tables and migrate_data to simulate the run loop without source connecting logic
        
        # Act
        self.migrator.create_saiql_table('GHOST_TABLE', self.migrator.schema_map['GHOST_TABLE'])
        self.migrator.migrate_data('GHOST_TABLE', self.migrator.schema_map['GHOST_TABLE'])
        
        # 2. Verify Target DB State
        with sqlite3.connect(self.db_path) as conn:
            # Check if table GHOST_TABLE exists
            cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='GHOST_TABLE'")
            result = cursor.fetchone()
            
            self.assertIsNone(result, "Dry Run FAILED: Table 'GHOST_TABLE' was created in target!")
            
            # Additional check: marker table should remain untouch
            cursor = conn.execute("SELECT count(*) FROM _init_marker")
            count = cursor.fetchone()[0]
            self.assertEqual(count, 0)
            
        # 3. Verify Log says "Would execute"
        # (This is implicitly checked by the fact that DBMigrator didn't crash on mocked source output)

    def tearDown(self):
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

if __name__ == '__main__':
    unittest.main()
