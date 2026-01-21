
import unittest
from core.type_registry import TypeRegistry, IRType, TypeInfo

class TestTypeRegistryMatrix(unittest.TestCase):
    
    def assertMapping(self, dialect, source_type, expected_ir):
        info = TypeRegistry.map_to_ir(dialect, source_type)
        self.assertEqual(info.ir_type, expected_ir, f"{dialect}: {source_type} -> {info.ir_type} (Expected {expected_ir})")

    def assertReverseMapping(self, dialect, ir_type, expected_target_str):
        # Create a simple TypeInfo for the IR type
        info = TypeInfo(ir_type)
        target = TypeRegistry.map_from_ir(dialect, info)
        # Check if expected target string is in the result (substring or exact)
        # We normalize casing for check
        self.assertTrue(expected_target_str.lower() in target.lower(), f"{dialect}: {ir_type} -> {target} (Expected ~ {expected_target_str})")

    # --- POSTGRES ---
    def test_postgres_matrix(self):
        self.assertMapping('postgres', 'boolean', IRType.BOOLEAN)
        self.assertMapping('postgres', 'uuid', IRType.UUID)
        self.assertMapping('postgres', 'jsonb', IRType.JSONB)
        self.assertMapping('postgres', 'timestamp with time zone', IRType.TIMESTAMP_TZ)
        self.assertMapping('postgres', 'bytea', IRType.BYTEA)
        self.assertMapping('postgres', 'numeric', IRType.DECIMAL)

    # --- MYSQL ---
    def test_mysql_matrix(self):
        # Known Gaps to be fixed or verified
        self.assertMapping('mysql', 'tinyint(1)', IRType.BOOLEAN) 
        self.assertMapping('mysql', 'datetime', IRType.TIMESTAMP)
        # Gaps Fixed:
        self.assertMapping('mysql', 'binary', IRType.BYTEA)
        self.assertMapping('mysql', 'varbinary', IRType.BYTEA)
        self.assertMapping('mysql', 'json', IRType.JSON)
        
        # Test reverse
        self.assertReverseMapping('mysql', IRType.BOOLEAN, 'tinyint(1)')
        self.assertReverseMapping('mysql', IRType.JSON, 'json')

    # --- SQLITE ---
    def test_sqlite_matrix(self):
        # SQLite is tricky as it uses affinity, but we inspect "declared type" string
        self.assertMapping('sqlite', 'integer', IRType.INTEGER)
        self.assertMapping('sqlite', 'text', IRType.TEXT)
        self.assertMapping('sqlite', 'blob', IRType.BYTEA)
        # Gaps Fixed:
        self.assertMapping('sqlite', 'boolean', IRType.BOOLEAN)
        self.assertMapping('sqlite', 'datetime', IRType.TIMESTAMP)

    # --- ORACLE ---
    def test_oracle_matrix(self):
        self.assertMapping('oracle', 'NUMBER', IRType.DECIMAL)
        self.assertMapping('oracle', 'DATE', IRType.TIMESTAMP) # Oracle Date has time
        self.assertMapping('oracle', 'TIMESTAMP WITH TIME ZONE', IRType.TIMESTAMP_TZ)
        self.assertMapping('oracle', 'BLOB', IRType.BYTEA)
        # self.assertMapping('oracle', 'CLOB', IRType.TEXT)

    # --- MSSQL ---
    def test_mssql_matrix(self):
        self.assertMapping('mssql', 'bit', IRType.BOOLEAN)
        self.assertMapping('mssql', 'datetimeoffset', IRType.TIMESTAMP_TZ)
        self.assertMapping('mssql', 'uniqueidentifier', IRType.UUID)
        self.assertMapping('mssql', 'money', IRType.DECIMAL)
        self.assertMapping('mssql', 'nvarchar', IRType.VARCHAR)
        
        # Test reverse
        self.assertReverseMapping('mssql', IRType.BOOLEAN, 'BIT')
        self.assertReverseMapping('mssql', IRType.TIMESTAMP_TZ, 'DATETIMEOFFSET')
        self.assertReverseMapping('mssql', IRType.UUID, 'UNIQUEIDENTIFIER')
        self.assertReverseMapping('mssql', IRType.TEXT, 'NVARCHAR(MAX)')

if __name__ == '__main__':
    unittest.main()
