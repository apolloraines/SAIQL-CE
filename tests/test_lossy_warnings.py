
import unittest
from unittest.mock import MagicMock
from tools.db_migrator import DBMigrator

class TestLossyWarnings(unittest.TestCase):

    def setUp(self):
        # File DB for stability
        import os
        cwd = os.getcwd()
        db_path = f"{cwd}/test_lossy.db"
        if os.path.exists(db_path): os.remove(db_path)
            
        self.db_url = f"sqlite:///{db_path}"
        self.migrator = DBMigrator("oracle://user:pass@host/db", target_url=self.db_url, dry_run=True)
        # Mock connection to avoid real Oracle driver
        self.migrator.source_conn = MagicMock()
        self.migrator.db_manager = MagicMock()

    def test_oracle_number_to_sqlite(self):
        # Simulate an Oracle Schema with NUMBER column
        self.migrator.schema_map = {
            'FINANCIALS': {
                'columns': [
                    {'name': 'ID', 'type': 'NUMBER', 'nullable': False},
                    {'name': 'AMT', 'type': 'NUMBER(38,2)', 'nullable': True}
                ],
                'pk': ['ID'], 'fks': []
            }
        }
        
        # Run preflight
        self.migrator.preflight_check(['FINANCIALS'])
        
        # Assert warnings
        warnings = self.migrator.report['warnings']
        found_lossy = any("LOSSY TYPE" in w and "Precision loss" in w for w in warnings)
        
        self.assertTrue(found_lossy, f"Expected lossy warning for Oracle NUMBER -> SQLite. Got: {warnings}")

    def test_timestamptz_to_sqlite(self):
        # Simulate Postgres TS w/ TZ
        # Re-init as Postgres source
        self.migrator.source_type = "postgresql" 
        self.migrator.schema_map = {
            'EVENTS': {
                'columns': [
                    {'name': 'TS', 'type': 'TIMESTAMP WITH TIME ZONE', 'nullable': True}
                ],
                'pk': [], 'fks': []
            }
        }
        
        self.migrator.preflight_check(['EVENTS'])
        
        warnings = self.migrator.report['warnings']
        found_tz_loss = any("Timezone loss" in w for w in warnings)
        
        self.assertTrue(found_tz_loss, f"Expected TZ loss warning for PG TIMESTAMPTZ -> SQLite. Got: {warnings}")

    def test_mssql_datetimeoffset_to_sqlite(self):
        # Simulate MSSQL source with DATETIMEOFFSET
        self.migrator.source_type = "mssql"
        self.migrator.schema_map = {
            'AUDIT_LOG': {
                'columns': [
                    {'name': 'CreatedAt', 'type': 'datetimeoffset', 'nullable': False}
                ],
                'pk': [], 'fks': []
            }
        }
        
        self.migrator.preflight_check(['AUDIT_LOG'])
        
        warnings = self.migrator.report['warnings']
        found_mssql_loss = any("mssql datetimeoffset" in w.lower() and "timezone loss" in w.lower() for w in warnings)
        
        self.assertTrue(found_mssql_loss, f"Expected lossy warning for MSSQL DATETIMEOFFSET -> SQLite. Got: {warnings}")

    def tearDown(self):
        import os
        if os.path.exists("test_lossy.db"):
            os.remove("test_lossy.db")

    def test_oracle_empty_string_warning(self):
        # Oracle VARCHAR2 -> Postgres (Semantic mismatch)
        self.migrator.source_type = "oracle"
        self.migrator.target_config = {'type': 'postgresql'} # Override target
        self.migrator.schema_map = {
            'USERS': {
                'columns': [
                    {'name': 'BIO', 'type': 'VARCHAR2(100)', 'nullable': True}
                ],
                'pk': [], 'fks': []
            }
        }
        
        self.migrator.preflight_check(['USERS'])
        
        warnings = self.migrator.report['warnings']
        found_semantic = any("Semantic change" in w and "empty string" in w for w in warnings)
        
        self.assertTrue(found_semantic, f"Expected semantic warning for Oracle VARCHAR2. Got: {warnings}")

    def test_oracle_timestamptz_to_mysql(self):
        # Oracle TIMESTAMP WITH TIME ZONE -> MySQL (TZ loss)
        self.migrator.source_type = "oracle"
        self.migrator.target_config = {'type': 'mysql'}
        self.migrator.schema_map = {
            'LOGS': {
                'columns': [
                    {'name': 'TS', 'type': 'TIMESTAMP WITH TIME ZONE', 'nullable': True}
                ],
                'pk': [], 'fks': []
            }
        }
        
        self.migrator.preflight_check(['LOGS'])
        
        warnings = self.migrator.report['warnings']
        found_tz_loss = any("Timezone loss" in w for w in warnings)
        self.assertTrue(found_tz_loss, f"Expected TZ loss warning. Got: {warnings}")

if __name__ == '__main__':
    unittest.main()
