
import unittest
from unittest.mock import MagicMock, patch
from tools.db_migrator import DBMigrator

class TestDataSemantics(unittest.TestCase):

    def setUp(self):
        # Setup basic migrator using file DB to avoid URL parsing issues with :memory:
        import os
        if os.path.exists("test_semantics.db"):
            os.remove("test_semantics.db")
            
        if os.path.exists("test_state.json"):
            os.remove("test_state.json")
            
        # path needs to be absolute usually in these tools? URL scheme: sqlite:///abs/path
        cwd = os.getcwd()
        db_path = f"{cwd}/test_semantics.db"
        
        self.migrator = DBMigrator(f"sqlite:///{db_path}", target_url=f"sqlite:///{db_path}", dry_run=False, checkpoint_file="test_state.json")
        # Mock connections immediately after init to prevent actual usage
        self.migrator.source_conn = MagicMock()
        self.migrator.db_manager = MagicMock()
        self.migrator.target_config = {'type': 'sqlite'}

    def tearDown(self):
        import os
        if os.path.exists("test_semantics.db"):
            os.remove("test_semantics.db")
        if os.path.exists("test_state.json"):
            os.remove("test_state.json")

    def test_oracle_empty_string_behavior(self):
        # Oracle returns None for empty strings
        # We simulate source returning None
        mock_cursor = MagicMock()
        # count query returns (1,)
        mock_cursor.fetchone.return_value = (1,)
        # fetchmany returns [(None,)] then []
        mock_cursor.fetchmany.side_effect = [[(None,)], []]
        self.migrator.source_conn.cursor.return_value = mock_cursor

        schema = {'columns': [{'name': 'col1', 'type': 'TEXT', 'nullable': True}], 'pk': [], 'fks': []}
        
        # We need to capture what is sent to db_manager
        # migrate_data calls execute_transaction(operations)
        # operations = [{'sql': ..., 'params': row}]
        
        self.migrator.migrate_data('test_table', schema)
        
        # Inspect the call
        args, kwargs = self.migrator.db_manager.execute_transaction.call_args
        operations = args[0]
        params = operations[0]['params']
        
        self.assertIsNone(params[0], "Oracle NULL (None) should migrate as None")

    def test_binary_data_path(self):
        # Simulate source returning bytes
        binary_blob = b'\xDE\xAD\xBE\xEF'
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (1,)
        mock_cursor.fetchmany.side_effect = [[(binary_blob,)], []]
        self.migrator.source_conn.cursor.return_value = mock_cursor

        schema = {'columns': [{'name': 'val', 'type': 'BYTEA', 'nullable': True}], 'pk': [], 'fks': []}
        
        self.migrator.migrate_data('bin_table', schema)
        
        args, kwargs = self.migrator.db_manager.execute_transaction.call_args
        params = args[0][0]['params']
        
        self.assertEqual(params[0], binary_blob, "Binary bytes should be preserved exactly")

    def test_unicode_path(self):
        # Simulate Emoji
        text = "Hello 🌍"
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (1,)
        mock_cursor.fetchmany.side_effect = [[(text,)], []]
        self.migrator.source_conn.cursor.return_value = mock_cursor

        schema = {'columns': [{'name': 'txt', 'type': 'TEXT', 'nullable': True}], 'pk': [], 'fks': []}
        
        self.migrator.migrate_data('utf8_table', schema)
        
        args, kwargs = self.migrator.db_manager.execute_transaction.call_args
        params = args[0][0]['params']
        
        self.assertEqual(params[0], text, "Unicode text should be preserved")

if __name__ == '__main__':
    unittest.main()
