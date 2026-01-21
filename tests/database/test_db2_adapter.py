#!/usr/bin/env python3
"""
Test Suite for IBM Db2 Adapter (L0-L4)

Environment variables required for live tests:
- DB2_DATABASE: Database name
- DB2_HOSTNAME: Host address
- DB2_PORT: Port number (default: 50000)
- DB2_USERNAME: Username
- DB2_PASSWORD: Password
- DB2_SCHEMA: Default schema (optional)
- DB2_TEST_SCHEMA: Dedicated test schema for L4 checksum tests

Run tests:
    # Unit tests only (no connection required)
    pytest tests/database/test_db2_adapter.py -v -k "not credentials"

    # Full tests (requires Db2 connection)
    DB2_DATABASE=testdb DB2_USERNAME=user DB2_PASSWORD=pass \
        pytest tests/database/test_db2_adapter.py -v
"""

import pytest
import json
import hashlib
import tempfile
import os
import sys
from pathlib import Path
from datetime import datetime

# Try to import the adapter
try:
    from extensions.plugins.db2_adapter import (
        Db2Adapter, Db2Config, Db2Result, Db2Error,
        Db2ConnectionError, Db2AuthError, Db2QueryError,
        ConnectionState, DB2_AVAILABLE
    )
    ADAPTER_AVAILABLE = True
except ImportError as e:
    ADAPTER_AVAILABLE = False
    DB2_AVAILABLE = False
    print(f"Db2 adapter not available: {e}")


# Test configuration from environment
def get_test_config() -> 'Db2Config':
    """Get test configuration from environment variables"""
    database = os.environ.get('DB2_DATABASE')
    hostname = os.environ.get('DB2_HOSTNAME', 'localhost')
    port = int(os.environ.get('DB2_PORT', '50000'))
    username = os.environ.get('DB2_USERNAME')
    password = os.environ.get('DB2_PASSWORD')
    schema = os.environ.get('DB2_SCHEMA')

    if not database or not username:
        return None

    return Db2Config(
        database=database,
        hostname=hostname,
        port=port,
        username=username,
        password=password or '',
        schema=schema,
        query_timeout=60,
        max_retries=2,
    )


def get_test_schema() -> str:
    """
    Get dedicated test schema from environment.

    Cost-safety rule: L4 tests that run checksums MUST use a dedicated small
    test schema, not arbitrary schemas. Set DB2_TEST_SCHEMA to a schema with
    small tables.
    """
    return os.environ.get('DB2_TEST_SCHEMA')


# Skip conditions
skip_no_db2 = pytest.mark.skipif(
    not DB2_AVAILABLE,
    reason="ibm_db not installed"
)

skip_no_adapter = pytest.mark.skipif(
    not ADAPTER_AVAILABLE,
    reason="Db2 adapter not available"
)

skip_no_credentials = pytest.mark.skipif(
    get_test_config() is None,
    reason="Db2 credentials not configured (set DB2_DATABASE, DB2_USERNAME, DB2_PASSWORD)"
)

skip_no_test_schema = pytest.mark.skipif(
    get_test_schema() is None,
    reason="L4 checksum tests require dedicated test schema (set DB2_TEST_SCHEMA)"
)


# =============================================================================
# L0 TESTS: CONNECTIVITY
# =============================================================================

class TestL0Connectivity:
    """L0: Connectivity and basic query execution tests"""

    @skip_no_adapter
    def test_config_creation(self):
        """Test Db2Config creation"""
        config = Db2Config(
            database='testdb',
            hostname='localhost',
            port=50000,
            username='testuser',
            password='testpass'
        )

        assert config.database == 'testdb'
        assert config.hostname == 'localhost'
        assert config.port == 50000
        assert config.username == 'testuser'
        assert config.password == 'testpass'
        assert config.protocol == 'TCPIP'

    @skip_no_adapter
    def test_connection_string_generation(self):
        """Test connection string generation"""
        config = Db2Config(
            database='testdb',
            hostname='localhost',
            port=50000,
            username='testuser',
            password='testpass'
        )

        conn_str = config.get_connection_string()

        assert 'DATABASE=testdb' in conn_str
        assert 'HOSTNAME=localhost' in conn_str
        assert 'PORT=50000' in conn_str
        assert 'UID=testuser' in conn_str
        assert 'PWD=testpass' in conn_str

    @skip_no_adapter
    def test_driver_not_installed_error(self):
        """Test error when ibm_db not installed"""
        if DB2_AVAILABLE:
            pytest.skip("ibm_db is installed")

        with pytest.raises(Db2Error) as exc:
            Db2Adapter(Db2Config(
                database='test',
                username='user',
                password='pass'
            ))

        assert 'ibm_db not installed' in str(exc.value)

    @skip_no_adapter
    @skip_no_credentials
    def test_connection_success(self):
        """L0: Test successful connection"""
        config = get_test_config()
        adapter = Db2Adapter(config)

        assert adapter.is_connected()
        assert adapter.state == ConnectionState.CONNECTED

        adapter.close()
        assert adapter.state == ConnectionState.DISCONNECTED

    @skip_no_adapter
    @skip_no_credentials
    def test_trivial_query(self):
        """L0: Test trivial query (SELECT 1)"""
        config = get_test_config()
        adapter = Db2Adapter(config)

        result = adapter.execute_query("SELECT 1 FROM SYSIBM.SYSDUMMY1")

        assert result.success
        assert len(result.data) == 1
        assert result.execution_time > 0

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_query_with_multiple_rows(self):
        """L0: Test query returning multiple rows"""
        config = get_test_config()
        adapter = Db2Adapter(config)

        result = adapter.execute_query("""
            SELECT TABNAME FROM SYSCAT.TABLES
            WHERE TABSCHEMA = 'SYSIBM'
            FETCH FIRST 5 ROWS ONLY
        """)

        assert result.success
        assert len(result.data) <= 5

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_error_invalid_query(self):
        """L0: Test error handling for invalid query"""
        config = get_test_config()
        adapter = Db2Adapter(config)

        result = adapter.execute_query("SELECT * FROM NONEXISTENT_TABLE_XYZ123")

        assert not result.success
        assert result.error_message is not None

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_error_invalid_syntax(self):
        """L0: Test error handling for syntax error"""
        config = get_test_config()
        adapter = Db2Adapter(config)

        result = adapter.execute_query("SELECTT * FROM DUAL")

        assert not result.success
        assert result.error_message is not None

        adapter.close()

    @skip_no_adapter
    def test_error_bad_credentials(self):
        """L0: Test error handling for bad credentials"""
        if not DB2_AVAILABLE:
            pytest.skip("ibm_db not installed")

        config = Db2Config(
            database='testdb',
            hostname='localhost',
            port=50000,
            username='invalid_user_xyz',
            password='invalid_password_xyz'
        )

        with pytest.raises((Db2AuthError, Db2ConnectionError)):
            Db2Adapter(config)

    @skip_no_adapter
    @skip_no_credentials
    def test_statistics(self):
        """L0: Test adapter statistics"""
        config = get_test_config()
        adapter = Db2Adapter(config)

        # Execute a few queries
        adapter.execute_query("SELECT 1 FROM SYSIBM.SYSDUMMY1")
        adapter.execute_query("SELECT 2 FROM SYSIBM.SYSDUMMY1")

        stats = adapter.get_statistics()

        assert stats['backend'] == 'db2'
        assert stats['queries_executed'] >= 2
        assert stats['state'] == 'connected'
        assert 'adapter_version' in stats

        adapter.close()


# =============================================================================
# L1 TESTS: INTROSPECTION
# =============================================================================

class TestL1Introspection:
    """L1: Introspection tests"""

    @skip_no_adapter
    @skip_no_credentials
    def test_list_schemas(self):
        """L1: Test schema listing"""
        config = get_test_config()
        adapter = Db2Adapter(config)

        schemas = adapter.list_schemas()

        assert isinstance(schemas, list)
        # Should have at least some schemas
        for schema in schemas:
            assert 'schema_name' in schema
            assert 'owner' in schema

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_list_tables(self):
        """L1: Test table listing"""
        config = get_test_config()
        adapter = Db2Adapter(config)

        # List tables in SYSCAT schema (always exists)
        tables = adapter.list_tables('SYSCAT')

        assert isinstance(tables, list)
        assert len(tables) > 0

        for table in tables:
            assert 'table_name' in table
            assert 'type' in table
            assert table['type'] in ('TABLE', 'VIEW')

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_describe_table(self):
        """L1: Test table description"""
        config = get_test_config()
        adapter = Db2Adapter(config)

        # Describe a system table
        desc = adapter.describe_table('TABLES', 'SYSCAT')

        assert desc['table_name'] == 'TABLES'
        assert desc['schema'] == 'SYSCAT'
        assert 'columns' in desc
        assert len(desc['columns']) > 0

        # Check column structure
        col = desc['columns'][0]
        assert 'name' in col
        assert 'type' in col
        assert 'nullable' in col

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_list_indexes(self):
        """L1: Test index listing"""
        config = get_test_config()
        adapter = Db2Adapter(config)

        # Get indexes on a system table
        indexes = adapter.list_indexes('TABLES', 'SYSCAT')

        assert isinstance(indexes, list)
        # System tables usually have indexes
        if indexes:
            idx = indexes[0]
            assert 'name' in idx
            assert 'unique' in idx
            assert 'columns' in idx

        adapter.close()


# =============================================================================
# L2 TESTS: IR READ/WRITE
# =============================================================================

class TestL2IRReadWrite:
    """L2: IR Read/Write tests"""

    @skip_no_adapter
    @skip_no_credentials
    def test_extract_schema_ir(self):
        """L2: Test schema IR extraction"""
        config = get_test_config()
        adapter = Db2Adapter(config)

        # Extract schema IR for SYSCAT (always has tables)
        schema_ir = adapter.extract_schema_ir('SYSCAT')

        assert 'schema' in schema_ir
        assert 'database' in schema_ir
        assert 'tables' in schema_ir
        assert 'views' in schema_ir
        assert 'extraction_timestamp' in schema_ir
        assert 'adapter_version' in schema_ir

        # Should have some tables
        assert len(schema_ir['tables']) > 0 or len(schema_ir['views']) > 0

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_schema_ir_determinism(self):
        """L2: Test schema IR extraction is deterministic"""
        config = get_test_config()
        adapter = Db2Adapter(config)

        # Extract twice
        ir1 = adapter.extract_schema_ir('SYSCAT')
        ir2 = adapter.extract_schema_ir('SYSCAT')

        # Tables and views should be identical (excluding timestamp)
        assert ir1['tables'].keys() == ir2['tables'].keys()
        assert ir1['views'].keys() == ir2['views'].keys()

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_export_data_requires_order_by(self):
        """L2: Test data export requires ORDER BY"""
        config = get_test_config()
        adapter = Db2Adapter(config)

        # Should fail without ORDER BY
        with pytest.raises(Db2QueryError) as exc:
            adapter.export_data_ir('TABLES', 'SYSCAT')

        assert 'ORDER BY' in str(exc.value)

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_export_data_with_order_by(self):
        """L2: Test data export with ORDER BY"""
        config = get_test_config()
        adapter = Db2Adapter(config)

        data_ir = adapter.export_data_ir(
            'SCHEMATA', 'SYSCAT',
            order_by=['SCHEMANAME'],
            limit=10
        )

        assert 'data' in data_ir
        assert 'row_count' in data_ir
        assert 'order_by' in data_ir
        assert data_ir['order_by'] == ['SCHEMANAME']
        assert len(data_ir['data']) <= 10

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_type_mapping(self):
        """L2: Test type mapping table"""
        config = get_test_config()
        adapter = Db2Adapter(config)

        type_map = adapter.get_type_mapping()

        assert 'INTEGER' in type_map
        assert 'VARCHAR' in type_map
        assert 'TIMESTAMP' in type_map
        assert 'DECIMAL' in type_map

        # Check structure
        assert 'ir_type' in type_map['INTEGER']

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    @skip_no_test_schema
    def test_schema_round_trip(self):
        """L2: Test schema extraction and creation round trip"""
        config = get_test_config()
        adapter = Db2Adapter(config)

        test_schema = get_test_schema()
        test_table = f"RT_TEST_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        try:
            # Define table IR
            table_ir = {
                'name': test_table,
                'columns': [
                    {'name': 'ID', 'type': 'INTEGER', 'nullable': False},
                    {'name': 'NAME', 'type': 'VARCHAR', 'length': 100, 'nullable': True},
                    {'name': 'VALUE', 'type': 'DECIMAL', 'length': 10, 'scale': 2, 'nullable': True},
                ],
                'primary_key': ['ID'],
            }

            # Create table
            adapter.create_table_from_ir(table_ir, test_schema)

            # Extract and verify
            schema_ir = adapter.extract_schema_ir(test_schema)
            assert test_table in schema_ir['tables']

            created_table = schema_ir['tables'][test_table]
            assert len(created_table['columns']) == 3
            assert created_table['primary_key'] == ['ID']

        finally:
            # Cleanup
            try:
                adapter.execute_query(f'DROP TABLE "{test_schema}"."{test_table}"')
            except Exception:
                pass

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    @skip_no_test_schema
    def test_data_round_trip(self):
        """L2: Test data export and import round trip"""
        config = get_test_config()
        adapter = Db2Adapter(config)

        test_schema = get_test_schema()
        test_table = f"DRT_TEST_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        try:
            # Create table
            table_ir = {
                'name': test_table,
                'columns': [
                    {'name': 'ID', 'type': 'INTEGER', 'nullable': False},
                    {'name': 'NAME', 'type': 'VARCHAR', 'length': 50, 'nullable': True},
                ],
                'primary_key': ['ID'],
            }
            adapter.create_table_from_ir(table_ir, test_schema)

            # Load data
            test_data = [
                {'ID': 1, 'NAME': 'Alice'},
                {'ID': 2, 'NAME': 'Bob'},
                {'ID': 3, 'NAME': 'Charlie'},
            ]
            inserted = adapter.load_data_from_ir(test_table, test_data, test_schema)
            assert inserted == 3

            # Export and verify
            data_ir = adapter.export_data_ir(test_table, test_schema, order_by=['ID'])
            assert len(data_ir['data']) == 3
            assert data_ir['data'][0]['NAME'] == 'Alice'
            assert data_ir['data'][2]['NAME'] == 'Charlie'

        finally:
            try:
                adapter.execute_query(f'DROP TABLE "{test_schema}"."{test_table}"')
            except Exception:
                pass

        adapter.close()


# =============================================================================
# L3 TESTS: FIDELITY AND EDGE CASES
# =============================================================================

class TestL3FidelityEdgeCases:
    """L3: Fidelity and edge case tests"""

    @skip_no_adapter
    @skip_no_credentials
    def test_decimal_precision(self):
        """L3: Test DECIMAL precision handling"""
        config = get_test_config()
        adapter = Db2Adapter(config)

        type_map = adapter.get_type_mapping()

        assert 'DECIMAL' in type_map
        assert type_map['DECIMAL']['precision'] == 31  # Db2 max precision

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_timestamp_types(self):
        """L3: Test timestamp type handling"""
        config = get_test_config()
        adapter = Db2Adapter(config)

        type_map = adapter.get_type_mapping()

        assert 'DATE' in type_map
        assert 'TIME' in type_map
        assert 'TIMESTAMP' in type_map

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_lob_types(self):
        """L3: Test LOB type handling"""
        config = get_test_config()
        adapter = Db2Adapter(config)

        type_map = adapter.get_type_mapping()

        assert 'CLOB' in type_map
        assert 'BLOB' in type_map

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_null_handling(self):
        """L3: Test NULL handling"""
        config = get_test_config()
        adapter = Db2Adapter(config)

        result = adapter.execute_query("""
            SELECT CAST(NULL AS INTEGER) AS NULL_INT,
                   CAST(NULL AS VARCHAR(10)) AS NULL_STR
            FROM SYSIBM.SYSDUMMY1
        """)

        assert result.success
        assert result.data[0]['NULL_INT'] is None
        assert result.data[0]['NULL_STR'] is None

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_get_constraints(self):
        """L3: Test constraint retrieval"""
        config = get_test_config()
        adapter = Db2Adapter(config)

        # SYSCAT.TABLES has constraints
        constraints = adapter.get_constraints('TABLES', 'SYSCAT')

        assert isinstance(constraints, list)
        # May or may not have constraints depending on version

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    @skip_no_test_schema
    def test_view_definition_extraction(self):
        """L3: Test view definition extraction"""
        config = get_test_config()
        adapter = Db2Adapter(config)

        test_schema = get_test_schema()
        test_view = f"VW_TEST_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        try:
            # Create a view
            adapter.execute_query(f"""
                CREATE VIEW "{test_schema}"."{test_view}" AS
                SELECT SCHEMANAME FROM SYSCAT.SCHEMATA WHERE SCHEMANAME LIKE 'SYS%'
            """)

            # Extract schema and check view
            schema_ir = adapter.extract_schema_ir(test_schema)

            if test_view in schema_ir['views']:
                view = schema_ir['views'][test_view]
                assert 'definition' in view
                assert 'SELECT' in (view['definition'] or '').upper()

        finally:
            try:
                adapter.execute_query(f'DROP VIEW "{test_schema}"."{test_view}"')
            except Exception:
                pass

        adapter.close()


# =============================================================================
# L4 TESTS: AUDIT-GRADE PROOF
# =============================================================================

class TestL4AuditProof:
    """L4: Audit-grade proof tests"""

    @skip_no_adapter
    @skip_no_credentials
    def test_proof_bundle_generation(self):
        """L4: Test proof bundle generation"""
        config = get_test_config()
        adapter = Db2Adapter(config)

        with tempfile.TemporaryDirectory() as tmpdir:
            # Cost-safety: disable checksums when using arbitrary schema
            manifest = adapter.generate_proof_bundle(
                tmpdir,
                schema='SYSCAT',
                include_data_checksums=False
            )

            # Check manifest core fields
            assert 'bundle_id' in manifest
            assert 'timestamp' in manifest
            assert 'schema_hash' in manifest
            assert 'table_count' in manifest

            # Check L4 required fields
            assert 'config' in manifest
            assert 'versions' in manifest
            assert 'dataset_hash' in manifest
            assert 'hardware_summary' in manifest

            # Verify config is sanitized
            assert 'username' not in manifest['config']
            assert 'password' not in manifest['config']

            # Check files
            assert (Path(tmpdir) / 'run_manifest.json').exists()
            assert (Path(tmpdir) / 'schema_ir.json').exists()
            assert (Path(tmpdir) / 'bundle_sha256.txt').exists()

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_proof_bundle_determinism(self):
        """L4: Test proof bundle is deterministic"""
        config = get_test_config()
        adapter = Db2Adapter(config)

        with tempfile.TemporaryDirectory() as tmpdir1:
            with tempfile.TemporaryDirectory() as tmpdir2:
                # Cost-safety: disable checksums
                m1 = adapter.generate_proof_bundle(
                    tmpdir1, schema='SYSCAT', include_data_checksums=False
                )
                m2 = adapter.generate_proof_bundle(
                    tmpdir2, schema='SYSCAT', include_data_checksums=False
                )

                # Schema hash should be identical
                assert m1['schema_hash'] == m2['schema_hash']

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_no_secrets_in_stats(self):
        """L4: Test no secrets leaked in statistics"""
        config = get_test_config()
        adapter = Db2Adapter(config)

        stats = adapter.get_statistics()
        stats_str = json.dumps(stats)

        assert 'password' not in stats_str.lower()
        assert 'secret' not in stats_str.lower()

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    @skip_no_test_schema
    def test_proof_bundle_required_artifacts(self):
        """L4: Test proof bundle contains ALL required artifacts"""
        config = get_test_config()
        adapter = Db2Adapter(config)

        test_schema = get_test_schema()

        with tempfile.TemporaryDirectory() as tmpdir:
            manifest = adapter.generate_proof_bundle(
                tmpdir,
                schema=test_schema,
                include_data_checksums=True,
                max_checksum_rows=1000
            )

            required_files = [
                'run_manifest.json',
                'schema_ir.json',
                'bundle_sha256.txt',
                'schema_diff.json',
                'rowcount_diff.json',
                'checksum_diff.json',
                'limitations.md',
            ]

            for fname in required_files:
                fpath = Path(tmpdir) / fname
                assert fpath.exists(), f"Required artifact missing: {fname}"

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_proof_bundle_schema_diff_content(self):
        """L4: Test schema_diff.json structure"""
        config = get_test_config()
        adapter = Db2Adapter(config)

        with tempfile.TemporaryDirectory() as tmpdir:
            # Cost-safety: disable checksums
            adapter.generate_proof_bundle(
                tmpdir, schema='SYSCAT', include_data_checksums=False
            )

            with open(Path(tmpdir) / 'schema_diff.json') as f:
                schema_diff = json.load(f)

            assert 'status' in schema_diff
            if schema_diff['status'] == 'N/A':
                assert 'reason' in schema_diff

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_proof_bundle_rowcount_diff_content(self):
        """L4: Test rowcount_diff.json structure"""
        config = get_test_config()
        adapter = Db2Adapter(config)

        with tempfile.TemporaryDirectory() as tmpdir:
            # Cost-safety: disable checksums
            adapter.generate_proof_bundle(
                tmpdir, schema='SYSCAT', include_data_checksums=False
            )

            with open(Path(tmpdir) / 'rowcount_diff.json') as f:
                rowcount_diff = json.load(f)

            assert 'status' in rowcount_diff

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_proof_bundle_limitations_copied(self):
        """L4: Test limitations.md is included"""
        config = get_test_config()
        adapter = Db2Adapter(config)

        with tempfile.TemporaryDirectory() as tmpdir:
            # Cost-safety: disable checksums
            adapter.generate_proof_bundle(
                tmpdir, schema='SYSCAT', include_data_checksums=False
            )

            limitations_path = Path(tmpdir) / 'limitations.md'
            assert limitations_path.exists()

            content = limitations_path.read_text()
            assert len(content) > 50
            assert 'Db2' in content or 'DB2' in content

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_proof_bundle_schema_hash_stable(self):
        """L4: Test schema hash is stable"""
        config = get_test_config()
        adapter = Db2Adapter(config)

        hashes = []
        for _ in range(3):
            with tempfile.TemporaryDirectory() as tmpdir:
                # Cost-safety: disable checksums
                manifest = adapter.generate_proof_bundle(
                    tmpdir, schema='SYSCAT', include_data_checksums=False
                )
                hashes.append(manifest['schema_hash'])

        assert hashes[0] == hashes[1] == hashes[2], f"Schema hash not stable: {hashes}"

        adapter.close()


# =============================================================================
# INTEGRATION TEST RUNNER
# =============================================================================

def run_l0_tests():
    """Run L0 connectivity tests"""
    import subprocess
    result = subprocess.run([
        sys.executable, "-m", "pytest",
        __file__,
        "-v", "-k", "L0",
        "--tb=short"
    ])
    return result.returncode


def run_l1_tests():
    """Run L1 introspection tests"""
    import subprocess
    result = subprocess.run([
        sys.executable, "-m", "pytest",
        __file__,
        "-v", "-k", "L1",
        "--tb=short"
    ])
    return result.returncode


def run_all_tests():
    """Run all tests"""
    import subprocess
    result = subprocess.run([
        sys.executable, "-m", "pytest",
        __file__,
        "-v",
        "--tb=short"
    ])
    return result.returncode


if __name__ == "__main__":
    run_all_tests()
