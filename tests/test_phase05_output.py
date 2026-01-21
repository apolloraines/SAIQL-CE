import pytest
import os
import csv
import json
import shutil
from pathlib import Path
from unittest.mock import MagicMock, patch
from tools.db_migrator import DBMigrator

# Set up test paths
TEST_ARTIFACTS_DIR = Path("./test_phase05_artifacts")

@pytest.fixture
def clean_artifacts():
    if TEST_ARTIFACTS_DIR.exists():
        shutil.rmtree(TEST_ARTIFACTS_DIR)
    yield
    if TEST_ARTIFACTS_DIR.exists():
        shutil.rmtree(TEST_ARTIFACTS_DIR)

class TestOutputMode:
    
    def test_output_mode_files_initialization(self):
        """Verify DBMigrator initializes correctly in files mode without target"""
        migrator = DBMigrator(
            source_url="sqlite:///:memory:",
            output_mode="files",
            output_dir=str(TEST_ARTIFACTS_DIR)
        )
        assert migrator.output_mode == "files"
        # specific dummy target check
        assert migrator.target_config['path'] == ":memory:"

    def test_ddl_generation(self, clean_artifacts):
        """Verify DDL file generation matches schema"""
        migrator = DBMigrator(
            source_url="sqlite:///:memory:",
            output_mode="files",
            output_dir=str(TEST_ARTIFACTS_DIR)
        )
        
        schema = {
            "columns": [
                {"name": "id", "type": "INTEGER", "nullable": False},
                {"name": "name", "type": "TEXT", "nullable": True}
            ],
            "pk": ["id"],
            "fks": []
        }
        
        migrator._write_ddl_file("users", schema)
        
        expected_path = TEST_ARTIFACTS_DIR / "schema.sql"
        assert expected_path.exists()
        
        content = expected_path.read_text()
        assert "CREATE TABLE users" in content
        assert "id INTEGER NOT NULL" in content
        assert "PRIMARY KEY (id)" in content

    @patch("tools.db_migrator.DBMigrator._get_row_count")
    def test_csv_generation_deterministic(self, mock_count, clean_artifacts):
        """Verify CSV generation includes ORDER BY for determinism"""
        mock_count.return_value = 2
        
        # Mock source connection
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        
        # Mock fetchmany to return data then empty
        mock_cursor.fetchmany.side_effect = [
            [(1, "Alice"), (2, "Bob")],
            []
        ]
        
        migrator = DBMigrator(
            source_url="sqlite:///:memory:",
            output_mode="files",
            output_dir=str(TEST_ARTIFACTS_DIR)
        )
        migrator.source_conn = mock_conn
        migrator.source_type = "sqlite" # for quoting
        
        schema = {
            "columns": [
                {"name": "id", "type": "INTEGER", "nullable": False},
                {"name": "name", "type": "TEXT", "nullable": True}
            ],
            "pk": ["id"], # Has PK, should trigger ORDER BY
            "fks": []
        }
        
        migrator._write_csv_file("users", schema)
        
        # Verify SQL query contained ORDER BY
        call_args = mock_cursor.execute.call_args[0][0]
        assert "ORDER BY" in call_args
        assert '"id"' in call_args
        
        # Verify CSV content
        csv_path = TEST_ARTIFACTS_DIR / "data" / "users.csv"
        assert csv_path.exists()
        
        with open(csv_path, "r") as f:
            reader = csv.reader(f)
            header = next(reader)
            assert header == ["id", "name"]
            row1 = next(reader)
            assert row1 == ["1", "Alice"]

    def test_integration_sqlite_to_files(self, clean_artifacts):
        """End-to-end integration test: SQLite -> Files"""
        # Create a real temporary SQLite source
        import sqlite3
        source_db_path = os.path.abspath("test_source_integration.db")
        conn = sqlite3.connect(source_db_path)
        conn.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT)")
        conn.execute("INSERT INTO products VALUES (101, 'Widget')")
        conn.commit()
        conn.close()
        
        try:
            # Run migrator
            migrator = DBMigrator(
                source_url=f"sqlite:///{source_db_path}",
                output_mode="files",
                output_dir=str(TEST_ARTIFACTS_DIR)
            )
            migrator.run()
            
            migrator.run()
            
            # Verify Run Folder
            run_dir = Path(migrator.run_dir)
            output_dir = run_dir / "output"
            
            assert (output_dir / "schema.sql").exists()
            assert (output_dir / "data").exists()
            assert (output_dir / "data" / "products.csv").exists()
            
            # Verify Contents
            with open(output_dir / "schema.sql", "r") as f:
                content = f.read()
                assert "CREATE TABLE products" in content
                
            with open(output_dir / "data" / "products.csv", "r") as f:
                content = f.read()
                assert "101,Widget" in content
                
        finally:
            if os.path.exists(source_db_path):
                os.remove(source_db_path)

    def test_integration_file_source_to_db(self, clean_artifacts):
        """Integration test: File Source (CSV) -> SQLite DB"""
        # Setup source directory and CSV
        source_dir = TEST_ARTIFACTS_DIR / "source_data"
        source_dir.mkdir(parents=True, exist_ok=True)
        csv_path = source_dir / "employees.csv"
        with open(csv_path, "w") as f:
            f.write("id,name\n1,Alice\n2,Bob")
            
        # Target DB
        target_db_path = TEST_ARTIFACTS_DIR / "target.db"
        
        migrator = DBMigrator(
            source_url=f"file://{source_dir.absolute()}",
            target_url=f"sqlite:///{target_db_path.absolute()}",
            output_mode="db",
            checkpoint_file=str(TEST_ARTIFACTS_DIR / "checkpoint.json")
        )
        migrator.run()
        
        # Verify
        import sqlite3
        conn = sqlite3.connect(target_db_path)
        rows = conn.execute("SELECT * FROM employees").fetchall()
        conn.close()
        
        # Check data (id, name)
        # Note: FileAdapter infers types, probably TEXT or INTEGER
        # Alice is row 0
        assert (1, 'Alice') in rows or (1, 'Alice') in [(r[0], r[1]) for r in rows]
        assert len(rows) == 2

