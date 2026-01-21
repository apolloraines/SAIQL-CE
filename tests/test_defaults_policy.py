
import unittest
import os
import sqlite3
from tools.db_migrator import DBMigrator
from unittest.mock import MagicMock

class TestDefaultsPolicy(unittest.TestCase):

    def setUp(self):
        cwd = os.getcwd()
        self.source_db = f"{cwd}/test_defaults_src.db"
        self.target_db = f"{cwd}/test_defaults_tgt.db"
        
        if os.path.exists(self.source_db): os.remove(self.source_db)
        if os.path.exists(self.target_db): os.remove(self.target_db)
            
        # 1. Create Source with Defaults
        with sqlite3.connect(self.source_db) as conn:
            conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, active INTEGER DEFAULT 1, role TEXT DEFAULT 'user')")
            conn.execute("INSERT INTO users (id) VALUES (1)")
            
        self.migrator = DBMigrator(f"sqlite:///{self.source_db}", target_url=f"sqlite:///{self.target_db}", dry_run=True)
        self.migrator.connect_source()
        self.migrator.db_manager.execute_query("SELECT 1", backend="target") # Init target too

    def test_updates_warn_on_defaults(self):
        # Trigger introspection (which we patched to capture defaults)
        single_schema = self.migrator.get_schema('users')
        self.migrator.schema_map = {'users': single_schema}
        
        print(f"DEBUG SCHEMA: {self.migrator.schema_map}")

        # Run preflight
        self.migrator.preflight_check(['users'])
        
        # Verify warnings
        warnings = self.migrator.report['warnings']
        
        # Expect warning for 'active'
        found_active = any("DEFERRED: Default value '1' for users.active" in w for w in warnings)
        # Expect warning for 'role'
        found_role = any("DEFERRED: Default value ''user''" in w or "Default value 'user'" in w for w in warnings)
        
        self.assertTrue(found_active, f"Missing warning for default integer. Got: {warnings}")
        self.assertTrue(found_role, f"Missing warning for default string. Got: {warnings}")

    def tearDown(self):
        if os.path.exists(self.source_db): os.remove(self.source_db)
        if os.path.exists(self.target_db): os.remove(self.target_db)

if __name__ == '__main__':
    unittest.main()
