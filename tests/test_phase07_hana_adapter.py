#!/usr/bin/env python3
"""
SAIQL Phase 07: HANA Adapter Unit Tests

Tests HANA adapter functionality with mocked connections (no real HANA required).

Unit tests use mocking and should run without hdbcli installed.
Integration tests require hdbcli and a real HANA instance.

Author: Claude (Phase 07 Primary Builder)
Status: Development
"""

import pytest
import logging
from unittest.mock import Mock, MagicMock, patch, MagicMock as MockModule
from typing import Dict, Any
import sys

logger = logging.getLogger(__name__)

# Mock hdbcli if not available so adapter can be imported
if 'hdbcli' not in sys.modules:
    sys.modules['hdbcli'] = MockModule()
    sys.modules['hdbcli.dbapi'] = MockModule()

# Now import adapter (will work even without hdbcli)
from extensions.plugins.hana_adapter import HANAAdapter, HANA_AVAILABLE


class TestHANAAdapterConfig:
    """Test connection config parsing and validation."""

    def test_valid_config_minimal(self):
        """Test minimal valid config (host, user, password)."""
        config = {
            'host': 'hana-test.example.com',
            'user': 'test_user',
            'password': 'test_password'
        }

        adapter = HANAAdapter(config)

        assert adapter.host == 'hana-test.example.com'
        assert adapter.user == 'test_user'
        assert adapter.password == 'test_password'
        assert adapter.port == 30015  # Default
        assert adapter.database == 'SYSTEMDB'  # Default
        assert adapter.encrypt == False  # Default

    def test_valid_config_full(self):
        """Test full config with all options."""
        config = {
            'host': 'hana-prod.example.com',
            'port': 30041,
            'database': 'TENANTDB',
            'user': 'migration_user',
            'password': 'secure_password',
            'encrypt': True,
            'sslValidateCertificate': False
        }

        adapter = HANAAdapter(config)

        assert adapter.host == 'hana-prod.example.com'
        assert adapter.port == 30041
        assert adapter.database == 'TENANTDB'
        assert adapter.user == 'migration_user'
        assert adapter.password == 'secure_password'
        assert adapter.encrypt == True
        assert adapter.ssl_validate == False

    def test_missing_host_raises_error(self):
        """Test that missing host raises ValueError."""
        config = {
            'user': 'test_user',
            'password': 'test_password'
        }

        with pytest.raises(ValueError, match="requires 'host'"):
            HANAAdapter(config)

    def test_missing_user_raises_error(self):
        """Test that missing user raises ValueError."""
        config = {
            'host': 'hana-test.example.com',
            'password': 'test_password'
        }

        with pytest.raises(ValueError, match="requires 'user'"):
            HANAAdapter(config)

    def test_missing_password_raises_error(self):
        """Test that missing password raises ValueError."""
        config = {
            'host': 'hana-test.example.com',
            'user': 'test_user'
        }

        with pytest.raises(ValueError, match="requires 'password'"):
            HANAAdapter(config)


class TestHANAAdapterSecretRedaction:
    """Test that secrets never appear in logs."""

    def test_password_not_in_log_messages(self, caplog):
        """Test that password never appears in logs."""
        config = {
            'host': 'hana-test.example.com',
            'user': 'test_user',
            'password': 'SUPER_SECRET_PASSWORD_12345'
        }

        with caplog.at_level(logging.INFO):
            adapter = HANAAdapter(config)

        # Check all log messages
        for record in caplog.records:
            assert 'SUPER_SECRET_PASSWORD_12345' not in record.message, \
                f"Password leaked in log: {record.message}"

    def test_connection_log_safe(self, caplog):
        """Test that connection log shows user@host but not password."""
        config = {
            'host': 'hana-test.example.com',
            'user': 'test_user',
            'password': 'secret123'
        }

        with caplog.at_level(logging.INFO):
            adapter = HANAAdapter(config)

        # Should log user@host:port/database
        log_messages = " ".join([r.message for r in caplog.records])
        assert 'test_user@hana-test.example.com:30015/SYSTEMDB' in log_messages
        assert 'secret123' not in log_messages


class TestHANAAdapterIntrospection:
    """Test schema introspection methods (mocked)."""

    @patch('extensions.plugins.hana_adapter.dbapi')
    def test_get_tables_returns_list(self, mock_dbapi):
        """Test get_tables() returns list of table names."""
        # Mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_dbapi.connect.return_value = mock_conn

        # Mock query result
        mock_cursor.description = [('TABLE_NAME',)]
        mock_cursor.fetchall.return_value = [
            ('USERS',),
            ('ORDERS',),
            ('PRODUCTS',)
        ]

        config = {
            'host': 'hana-test.example.com',
            'user': 'test_user',
            'password': 'test_password'
        }

        adapter = HANAAdapter(config)
        tables = adapter.get_tables()

        assert len(tables) == 3
        assert 'users' in tables
        assert 'orders' in tables
        assert 'products' in tables

    @patch('extensions.plugins.hana_adapter.dbapi')
    def test_get_schema_structure(self, mock_dbapi):
        """Test get_schema() returns correct structure."""
        # Mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_dbapi.connect.return_value = mock_conn

        # Mock column query
        def mock_execute(query, params=None):
            if 'TABLE_COLUMNS' in query:
                mock_cursor.description = [
                    ('COLUMN_NAME',), ('DATA_TYPE_NAME',), ('LENGTH',),
                    ('SCALE',), ('IS_NULLABLE',), ('DEFAULT_VALUE',), ('GENERATION_TYPE',)
                ]
                mock_cursor.fetchall.return_value = [
                    ('ID', 'INTEGER', 10, None, 'FALSE', None, 'NONE'),
                    ('NAME', 'NVARCHAR', 100, None, 'TRUE', None, 'NONE'),
                ]
            elif 'IS_UNIQUE_KEY' in query:
                # Check UK before PK since UK query contains both strings
                mock_cursor.description = [('CONSTRAINT_NAME',), ('COLUMN_NAME',)]
                mock_cursor.fetchall.return_value = []  # No UK constraints in mock
            elif 'IS_PRIMARY_KEY' in query:
                mock_cursor.description = [('COLUMN_NAME',)]
                mock_cursor.fetchall.return_value = [('ID',)]
            elif 'REFERENTIAL_CONSTRAINTS' in query:
                mock_cursor.description = [('COLUMN_NAME',), ('REFERENCED_TABLE_NAME',), ('REFERENCED_COLUMN_NAME',)]
                mock_cursor.fetchall.return_value = []
            elif 'INDEXES' in query:
                mock_cursor.description = [('INDEX_NAME',), ('CONSTRAINT',)]
                mock_cursor.fetchall.return_value = []

        mock_cursor.execute.side_effect = mock_execute

        config = {
            'host': 'hana-test.example.com',
            'user': 'test_user',
            'password': 'test_password'
        }

        adapter = HANAAdapter(config)
        schema = adapter.get_schema('users')

        assert 'columns' in schema
        assert 'pk' in schema
        assert 'unique_constraints' in schema
        assert 'fks' in schema
        assert 'indexes' in schema
        assert len(schema['columns']) == 2
        assert schema['pk'] == ['id']
        assert schema['unique_constraints'] == []  # Mock returns no UKs

        # Check that columns have type_info and unsupported flag
        assert 'type_info' in schema['columns'][0]
        assert 'unsupported' in schema['columns'][0]
        # INTEGER and NVARCHAR are supported types
        assert schema['columns'][0]['unsupported'] == False
        assert schema['columns'][1]['unsupported'] == False

    @patch('extensions.plugins.hana_adapter.dbapi')
    def test_get_schema_flags_unsupported_types(self, mock_dbapi, caplog):
        """Test that get_schema() flags unsupported types."""
        # Mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_dbapi.connect.return_value = mock_conn

        # Mock column query with unsupported type
        def mock_execute(query, params=None):
            if 'TABLE_COLUMNS' in query:
                mock_cursor.description = [
                    ('COLUMN_NAME',), ('DATA_TYPE_NAME',), ('LENGTH',),
                    ('SCALE',), ('IS_NULLABLE',), ('DEFAULT_VALUE',), ('GENERATION_TYPE',)
                ]
                mock_cursor.fetchall.return_value = [
                    ('ID', 'INTEGER', 10, None, 'FALSE', None, 'NONE'),
                    ('LOCATION', 'ST_POINT', None, None, 'TRUE', None, 'NONE'),  # Unsupported spatial type
                ]
            elif 'IS_UNIQUE_KEY' in query:
                mock_cursor.description = [('CONSTRAINT_NAME',), ('COLUMN_NAME',)]
                mock_cursor.fetchall.return_value = []
            elif 'IS_PRIMARY_KEY' in query:
                mock_cursor.description = [('COLUMN_NAME',)]
                mock_cursor.fetchall.return_value = [('ID',)]
            elif 'REFERENTIAL_CONSTRAINTS' in query:
                mock_cursor.description = [('COLUMN_NAME',), ('REFERENCED_TABLE_NAME',), ('REFERENCED_COLUMN_NAME',)]
                mock_cursor.fetchall.return_value = []
            elif 'INDEXES' in query:
                mock_cursor.description = [('INDEX_NAME',), ('CONSTRAINT',)]
                mock_cursor.fetchall.return_value = []

        mock_cursor.execute.side_effect = mock_execute

        config = {
            'host': 'hana-test.example.com',
            'user': 'test_user',
            'password': 'test_password',
            'strict_types': False  # Disable exception on unsupported types for this test
        }

        adapter = HANAAdapter(config)

        with caplog.at_level(logging.ERROR):
            schema = adapter.get_schema('locations')

        # Check that unsupported type was flagged
        assert len(schema['columns']) == 2
        assert schema['columns'][0]['unsupported'] == False  # INTEGER is supported
        assert schema['columns'][1]['unsupported'] == True  # ST_POINT is unsupported

        # Check that error was logged
        assert any('Unsupported HANA type' in record.message and 'ST_POINT' in record.message
                   for record in caplog.records)

    @patch('extensions.plugins.hana_adapter.dbapi')
    def test_get_schema_raises_on_unsupported_types_strict_mode(self, mock_dbapi):
        """Test that get_schema() raises TypeError on unsupported types in strict mode."""
        # Mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_dbapi.connect.return_value = mock_conn

        # Mock column query with unsupported type
        def mock_execute(query, params=None):
            if 'TABLE_COLUMNS' in query:
                mock_cursor.description = [
                    ('COLUMN_NAME',), ('DATA_TYPE_NAME',), ('LENGTH',),
                    ('SCALE',), ('IS_NULLABLE',), ('DEFAULT_VALUE',), ('GENERATION_TYPE',)
                ]
                mock_cursor.fetchall.return_value = [
                    ('LOCATION', 'ST_POINT', None, None, 'TRUE', None, 'NONE'),  # Unsupported
                ]
            elif 'IS_UNIQUE_KEY' in query:
                mock_cursor.description = [('CONSTRAINT_NAME',), ('COLUMN_NAME',)]
                mock_cursor.fetchall.return_value = []
            elif 'IS_PRIMARY_KEY' in query:
                mock_cursor.description = [('COLUMN_NAME',)]
                mock_cursor.fetchall.return_value = []
            elif 'REFERENTIAL_CONSTRAINTS' in query:
                mock_cursor.description = [('COLUMN_NAME',), ('REFERENCED_TABLE_NAME',), ('REFERENCED_COLUMN_NAME',)]
                mock_cursor.fetchall.return_value = []
            elif 'INDEXES' in query:
                mock_cursor.description = [('INDEX_NAME',), ('CONSTRAINT',)]
                mock_cursor.fetchall.return_value = []

        mock_cursor.execute.side_effect = mock_execute

        config = {
            'host': 'hana-test.example.com',
            'user': 'test_user',
            'password': 'test_password',
            'strict_types': True  # Default: raise exception on unsupported types
        }

        adapter = HANAAdapter(config)

        # Should raise TypeError on unsupported type
        with pytest.raises(TypeError, match="Unsupported HANA type 'ST_POINT'"):
            adapter.get_schema('locations')


class TestHANAAdapterDataExtraction:
    """Test data extraction logic."""

    @patch('extensions.plugins.hana_adapter.dbapi')
    def test_extract_data_with_pk_ordering(self, mock_dbapi):
        """Test extract_data() uses PK for ordering."""
        # Mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_dbapi.connect.return_value = mock_conn

        # Mock get_schema to return PK
        def mock_execute(query, params=None):
            if 'TABLE_COLUMNS' in query:
                mock_cursor.description = [
                    ('COLUMN_NAME',), ('DATA_TYPE_NAME',), ('LENGTH',),
                    ('SCALE',), ('IS_NULLABLE',), ('DEFAULT_VALUE',), ('GENERATION_TYPE',)
                ]
                mock_cursor.fetchall.return_value = [
                    ('ID', 'INTEGER', 10, None, 'FALSE', None, 'NONE'),
                ]
            elif 'IS_UNIQUE_KEY' in query:
                # Check UK before PK since UK query contains both strings
                mock_cursor.description = [('CONSTRAINT_NAME',), ('COLUMN_NAME',)]
                mock_cursor.fetchall.return_value = []  # No UK constraints in mock
            elif 'IS_PRIMARY_KEY' in query:
                mock_cursor.description = [('COLUMN_NAME',)]
                mock_cursor.fetchall.return_value = [('ID',)]
            elif 'REFERENTIAL_CONSTRAINTS' in query:
                mock_cursor.description = [('COLUMN_NAME',), ('REFERENCED_TABLE_NAME',), ('REFERENCED_COLUMN_NAME',)]
                mock_cursor.fetchall.return_value = []
            elif 'INDEXES' in query:
                mock_cursor.description = [('INDEX_NAME',), ('CONSTRAINT',)]
                mock_cursor.fetchall.return_value = []
            elif 'SELECT * FROM USERS ORDER BY' in query.upper():
                # Data extraction query
                mock_cursor.description = [('ID',), ('NAME',)]
                mock_cursor.fetchall.return_value = [
                    (1, 'Alice'),
                    (2, 'Bob'),
                ]

        mock_cursor.execute.side_effect = mock_execute

        config = {
            'host': 'hana-test.example.com',
            'user': 'test_user',
            'password': 'test_password'
        }

        adapter = HANAAdapter(config)
        result = adapter.extract_data('users')

        # Check return structure
        assert 'data' in result
        assert 'stats' in result

        # Check data
        data = result['data']
        assert len(data) == 2
        assert data[0]['id'] == 1
        assert data[0]['name'] == 'Alice'

        # Check stats
        stats = result['stats']
        assert stats['table_name'] == 'users'
        assert stats['total_rows'] == 2
        assert stats['total_chunks'] == 1
        assert stats['duration_seconds'] is not None


class TestHANAAdapterContextManager:
    """Test context manager support."""

    @patch('extensions.plugins.hana_adapter.dbapi')
    def test_context_manager_connects_and_closes(self, mock_dbapi):
        """Test that context manager connects on entry and closes on exit."""
        mock_conn = MagicMock()
        mock_dbapi.connect.return_value = mock_conn

        config = {
            'host': 'hana-test.example.com',
            'user': 'test_user',
            'password': 'test_password'
        }

        with HANAAdapter(config) as adapter:
            assert adapter.connection is not None

        # Connection should be closed after exit
        mock_conn.close.assert_called_once()


class TestHANAAdapterDeterminism:
    """Test deterministic behavior (Phase 07 requirement)."""

    @patch('extensions.plugins.hana_adapter.dbapi')
    def test_get_tables_ordering_deterministic(self, mock_dbapi, caplog):
        """Test that get_tables() always returns tables in same order."""
        # Mock connection
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_dbapi.connect.return_value = mock_conn

        # Mock result (should be ordered by TABLE_NAME due to ORDER BY in query)
        mock_cursor.description = [('TABLE_NAME',)]
        mock_cursor.fetchall.return_value = [
            ('AARDVARK',),
            ('BEAR',),
            ('CAT',),
        ]

        config = {
            'host': 'hana-test.example.com',
            'user': 'test_user',
            'password': 'test_password'
        }

        adapter = HANAAdapter(config)

        # Call twice
        tables1 = adapter.get_tables()
        tables2 = adapter.get_tables()

        # Should be identical
        assert tables1 == tables2
        assert tables1 == ['aardvark', 'bear', 'cat']


class TestHANATypeMappingCorrectness:
    """Test HANA type mapping correctness (Phase 07 requirement)."""

    def test_exact_mappings(self):
        """Test exact mappings (no data loss)."""
        from core.type_registry import TypeRegistry, IRType

        # Exact numeric mappings
        assert TypeRegistry.map_to_ir('hana', 'INTEGER').ir_type == IRType.INTEGER
        assert TypeRegistry.map_to_ir('hana', 'BIGINT').ir_type == IRType.BIGINT
        assert TypeRegistry.map_to_ir('hana', 'SMALLINT').ir_type == IRType.SMALLINT
        assert TypeRegistry.map_to_ir('hana', 'TINYINT').ir_type == IRType.SMALLINT  # Upcast
        assert TypeRegistry.map_to_ir('hana', 'REAL').ir_type == IRType.REAL
        assert TypeRegistry.map_to_ir('hana', 'DOUBLE').ir_type == IRType.DOUBLE
        assert TypeRegistry.map_to_ir('hana', 'BOOLEAN').ir_type == IRType.BOOLEAN

        # Exact string mappings
        assert TypeRegistry.map_to_ir('hana', 'VARCHAR').ir_type == IRType.VARCHAR
        assert TypeRegistry.map_to_ir('hana', 'NVARCHAR').ir_type == IRType.VARCHAR
        assert TypeRegistry.map_to_ir('hana', 'CHAR').ir_type == IRType.CHAR
        assert TypeRegistry.map_to_ir('hana', 'NCHAR').ir_type == IRType.CHAR
        assert TypeRegistry.map_to_ir('hana', 'CLOB').ir_type == IRType.TEXT
        assert TypeRegistry.map_to_ir('hana', 'NCLOB').ir_type == IRType.TEXT

        # Exact date/time mappings
        assert TypeRegistry.map_to_ir('hana', 'DATE').ir_type == IRType.DATE
        assert TypeRegistry.map_to_ir('hana', 'TIME').ir_type == IRType.TIME
        assert TypeRegistry.map_to_ir('hana', 'TIMESTAMP').ir_type == IRType.TIMESTAMP

        # Exact binary mappings
        assert TypeRegistry.map_to_ir('hana', 'BINARY').ir_type == IRType.BYTEA
        assert TypeRegistry.map_to_ir('hana', 'VARBINARY').ir_type == IRType.BYTEA

    def test_lossy_mappings(self):
        """Test lossy mappings (documented data loss)."""
        from core.type_registry import TypeRegistry, IRType

        # SMALLDECIMAL -> DECIMAL (lossy: precision limit)
        result = TypeRegistry.map_to_ir('hana', 'SMALLDECIMAL')
        assert result.ir_type == IRType.DECIMAL
        # Fixed precision of 16,0 for SMALLDECIMAL

        # SECONDDATE -> TIMESTAMP (lossy: sub-second truncation)
        assert TypeRegistry.map_to_ir('hana', 'SECONDDATE').ir_type == IRType.TIMESTAMP

        # BLOB -> BYTEA (lossy: size limit)
        assert TypeRegistry.map_to_ir('hana', 'BLOB').ir_type == IRType.BYTEA

        # DECIMAL with precision/scale
        result = TypeRegistry.map_to_ir('hana', 'DECIMAL')
        assert result.ir_type == IRType.DECIMAL

    def test_partial_support_mappings(self):
        """Test partial support mappings (functional loss)."""
        from core.type_registry import TypeRegistry, IRType

        # SHORTTEXT -> VARCHAR (loses fuzzy search capability)
        assert TypeRegistry.map_to_ir('hana', 'SHORTTEXT').ir_type == IRType.VARCHAR

    def test_unsupported_types_flagged(self):
        """Test that unsupported types are flagged (return UNKNOWN or raise error)."""
        from core.type_registry import TypeRegistry, IRType

        # Spatial types (unsupported in Phase 07)
        assert TypeRegistry.map_to_ir('hana', 'ST_GEOMETRY').ir_type == IRType.UNKNOWN
        assert TypeRegistry.map_to_ir('hana', 'ST_POINT').ir_type == IRType.UNKNOWN

        # HANA-specific types (unsupported)
        assert TypeRegistry.map_to_ir('hana', 'ALPHANUM').ir_type == IRType.UNKNOWN

        # Binary text types (unsupported)
        assert TypeRegistry.map_to_ir('hana', 'BINTEXT').ir_type == IRType.UNKNOWN

        # Full-text types (unsupported)
        # Note: HANA 'TEXT' is a full-text search type, different from IR TEXT type
        assert TypeRegistry.map_to_ir('hana', 'TEXT').ir_type == IRType.UNKNOWN

        # Array/Table types would also return UNKNOWN if encountered (no specific type names)

    def test_type_mapping_determinism(self):
        """Test that type mappings are deterministic (same input -> same output)."""
        from core.type_registry import TypeRegistry

        # Call twice with same input
        result1 = TypeRegistry.map_to_ir('hana', 'INTEGER')
        result2 = TypeRegistry.map_to_ir('hana', 'INTEGER')

        assert result1.ir_type == result2.ir_type
        assert result1.precision == result2.precision
        assert result1.scale == result2.scale

    def test_case_insensitivity(self):
        """Test that type mapping is case-insensitive."""
        from core.type_registry import TypeRegistry, IRType

        # HANA stores types uppercase, but mapping should be case-insensitive
        assert TypeRegistry.map_to_ir('hana', 'INTEGER').ir_type == IRType.INTEGER
        assert TypeRegistry.map_to_ir('hana', 'integer').ir_type == IRType.INTEGER
        assert TypeRegistry.map_to_ir('hana', 'Integer').ir_type == IRType.INTEGER


# Integration test placeholder (requires real HANA instance)
@pytest.mark.integration
@pytest.mark.skip(reason="Requires real HANA instance")
class TestHANAAdapterIntegration:
    """Integration tests requiring real HANA connection."""

    def test_real_hana_connection(self):
        """
        Test connection to real HANA instance.

        Prerequisites:
        - HANA instance running
        - Environment variables set:
          - HANA_HOST
          - HANA_PORT
          - HANA_USER
          - HANA_PASSWORD
          - HANA_DATABASE
        """
        import os

        config = {
            'host': os.environ['HANA_HOST'],
            'port': int(os.environ.get('HANA_PORT', 30015)),
            'database': os.environ.get('HANA_DATABASE', 'SYSTEMDB'),
            'user': os.environ['HANA_USER'],
            'password': os.environ['HANA_PASSWORD']
        }

        with HANAAdapter(config) as adapter:
            tables = adapter.get_tables()
            assert isinstance(tables, list)

            if tables:
                # Test schema introspection on first table
                schema = adapter.get_schema(tables[0])
                assert 'columns' in schema
                assert 'pk' in schema
