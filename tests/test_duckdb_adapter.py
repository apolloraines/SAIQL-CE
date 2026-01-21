import unittest
from unittest.mock import MagicMock, patch
from tools.db_migrator import DBMigrator

class TestDuckDBAdapter(unittest.TestCase):
    def setUp(self):
        # Use memory DuckDB for source, file SQLite for target (stability)
        import os
        import sys
        
        # Mock duckdb_engine module
        sys.modules["duckdb_engine"] = MagicMock()
        
        self.db_path = "/tmp/test_duckdb_target.db"
        if os.path.exists(self.db_path): os.remove(self.db_path)
        
        self.migrator = DBMigrator("duckdb:///:memory:", target_url=f"sqlite:///{self.db_path}", dry_run=True)
        self.migrator.source_conn = MagicMock()

    def test_duckdb_connection_handling(self):
        """Verify DuckDB connection parameter parsing"""
        # Mock create_engine to return a mock connection
        with patch("sqlalchemy.create_engine") as mock_create:
            mock_engine = MagicMock()
            mock_connection = MagicMock()
            mock_engine.connect.return_value.connection = mock_connection
            mock_create.return_value = mock_engine
            
            # Connect
            self.migrator.connect_source()
            
            # Check if logic proceeded to set source_conn
            self.assertEqual(self.migrator.source_conn, mock_connection)
            self.assertIn("duckdb", self.migrator.source_type)

    def test_duckdb_introspection(self):
        """Verify SHOW TABLES introspection via mocked cursor"""
        # Mock create_engine and connection
        with patch("sqlalchemy.create_engine") as mock_create:
            mock_connection = MagicMock()
            mock_cursor = MagicMock()
            
            # Setup cursor mock for SHOW TABLES
            mock_cursor.fetchall.return_value = [("analytics_sales",), ("analytics_users",)]
            mock_connection.cursor.return_value = mock_cursor
            
            # Wire up engine
            mock_create.return_value.connect.return_value.connection = mock_connection
            
            # Connect
            self.migrator.connect_source()
            
            # Run get_tables
            tables = self.migrator.get_tables()
            
            # Verify
            mock_cursor.execute.assert_called_with("SHOW TABLES")
            self.assertEqual(tables, ["analytics_sales", "analytics_users"])

if __name__ == '__main__':
    unittest.main()
