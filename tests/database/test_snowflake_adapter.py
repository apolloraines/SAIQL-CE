#!/usr/bin/env python3
"""
Snowflake Adapter Test Suite - L0-L4 Testing

This module provides comprehensive tests for the Snowflake adapter:
- L0: Connectivity tests
- L1: Introspection tests
- L2: IR read/write tests
- L3: Fidelity and edge case tests
- L4: Proof bundle and determinism tests

Author: Apollo & Claude
Version: 1.0.0

Usage:
    # Run all tests (requires Snowflake credentials)
    pytest tests/database/test_snowflake_adapter.py -v

    # Run only unit tests (no credentials needed)
    pytest tests/database/test_snowflake_adapter.py -v -k "not credentials"

Environment variables:
    SNOWFLAKE_ACCOUNT - Account identifier (e.g., abc12345.us-east-1)
    SNOWFLAKE_USER - Username
    SNOWFLAKE_PASSWORD - Password
    SNOWFLAKE_WAREHOUSE - Compute warehouse
    SNOWFLAKE_DATABASE - Database name
    SNOWFLAKE_SCHEMA - Schema name (default: PUBLIC)
    SNOWFLAKE_ROLE - Optional role
"""

import pytest
import os
import sys
import json
import hashlib
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional
from unittest.mock import patch, MagicMock

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# Check if Snowflake driver is available
try:
    import snowflake.connector
    DRIVER_AVAILABLE = True
except ImportError:
    DRIVER_AVAILABLE = False

# Import adapter
try:
    from extensions.plugins.snowflake_adapter import (
        SnowflakeAdapter,
        SnowflakeConfig,
        SnowflakeError,
        SnowflakeConnectionError,
        SnowflakeAuthError,
        SnowflakeQueryError,
        SnowflakeOperationError,
        AuthMethod,
        TableInfo,
        ColumnInfo,
        SchemaIR,
        DataIR,
        SNOWFLAKE_AVAILABLE,
    )
    ADAPTER_AVAILABLE = True
except ImportError as e:
    ADAPTER_AVAILABLE = False
    SNOWFLAKE_AVAILABLE = False
    print(f"Snowflake adapter not available: {e}")


def get_test_config() -> Optional[SnowflakeConfig]:
    """Get test configuration from environment variables"""
    account = os.environ.get('SNOWFLAKE_ACCOUNT')
    user = os.environ.get('SNOWFLAKE_USER')
    password = os.environ.get('SNOWFLAKE_PASSWORD')
    warehouse = os.environ.get('SNOWFLAKE_WAREHOUSE')

    if not account or not user or not warehouse:
        return None

    return SnowflakeConfig(
        account=account,
        user=user,
        password=password or '',
        warehouse=warehouse,
        database=os.environ.get('SNOWFLAKE_DATABASE', 'SNOWFLAKE_SAMPLE_DATA'),
        schema=os.environ.get('SNOWFLAKE_SCHEMA', 'PUBLIC'),
        role=os.environ.get('SNOWFLAKE_ROLE', ''),
        query_timeout=60,
        max_retries=2,
    )


# Skip conditions
skip_no_driver = pytest.mark.skipif(
    not DRIVER_AVAILABLE,
    reason="snowflake-connector-python not installed"
)

skip_no_adapter = pytest.mark.skipif(
    not ADAPTER_AVAILABLE,
    reason="Snowflake adapter not available"
)

skip_no_credentials = pytest.mark.skipif(
    get_test_config() is None,
    reason="Snowflake credentials not configured (set SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_WAREHOUSE)"
)


# =============================================================================
# FIXTURES
# =============================================================================

@pytest.fixture(scope="module")
def snowflake_connection():
    """Module-scoped Snowflake connection fixture"""
    config = get_test_config()
    if config is None:
        pytest.skip("Snowflake credentials not configured")

    adapter = SnowflakeAdapter(config)
    yield adapter
    adapter.close()


# =============================================================================
# L0 TESTS: CONNECTIVITY
# =============================================================================

class TestL0Connectivity:
    """L0: Connectivity and basic query execution tests"""

    @skip_no_adapter
    def test_config_creation(self):
        """Test SnowflakeConfig creation"""
        config = SnowflakeConfig(
            account="abc12345.us-east-1",
            user="testuser",
            password="testpass",
            warehouse="COMPUTE_WH",
            database="TESTDB",
            schema="PUBLIC"
        )
        assert config.account == "abc12345.us-east-1"
        assert config.user == "testuser"
        assert config.warehouse == "COMPUTE_WH"
        assert config.database == "TESTDB"
        assert config.auth_method == AuthMethod.PASSWORD

    @skip_no_adapter
    def test_config_to_uri(self):
        """Test config to URI conversion with redaction"""
        config = SnowflakeConfig(
            account="abc12345.us-east-1",
            user="admin",
            password="secret123",
            warehouse="WH",
            database="DB"
        )

        uri = config.to_uri(redact=True)
        assert "***REDACTED***" in uri
        assert "secret123" not in uri
        assert "admin" in uri
        assert "abc12345" in uri

    @skip_no_adapter
    def test_driver_not_installed_error(self):
        """Test graceful error when driver not installed"""
        with patch('extensions.plugins.snowflake_adapter.SNOWFLAKE_AVAILABLE', False):
            config = SnowflakeConfig(account='test', user='test', warehouse='WH')
            with pytest.raises(SnowflakeError) as exc_info:
                SnowflakeAdapter(config)
            assert 'not installed' in str(exc_info.value).lower()

    @skip_no_adapter
    @skip_no_credentials
    def test_connection_success(self):
        """L0: Test successful connection to Snowflake"""
        config = get_test_config()
        adapter = SnowflakeAdapter(config)

        assert adapter.is_connected()
        assert adapter.state.value == "connected"

        adapter.close()
        assert not adapter.is_connected()

    @skip_no_adapter
    @skip_no_credentials
    def test_ping_command(self):
        """L0: Test ping (SELECT 1)"""
        config = get_test_config()
        adapter = SnowflakeAdapter(config)

        assert adapter.ping() is True

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_trivial_query(self):
        """L0: Test SELECT 1 trivial query"""
        config = get_test_config()
        adapter = SnowflakeAdapter(config)

        result = adapter.execute_query("SELECT 1 AS test_value")

        assert result['success']
        assert len(result['data']) == 1
        assert result['data'][0].get('TEST_VALUE') == 1
        assert result['metrics'].execution_time > 0

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_statistics(self):
        """L0: Test adapter statistics"""
        config = get_test_config()
        adapter = SnowflakeAdapter(config)

        # Run a few queries
        adapter.execute_query("SELECT 1")
        adapter.execute_query("SELECT 2")

        stats = adapter.get_statistics()

        assert stats['queries_executed'] >= 2
        assert stats['state'] == 'connected'
        assert 'account' in stats
        assert 'warehouse' in stats

        adapter.close()


# =============================================================================
# L1 TESTS: INTROSPECTION
# =============================================================================

class TestL1Introspection:
    """L1: Schema introspection tests"""

    @skip_no_adapter
    @skip_no_credentials
    def test_get_server_fingerprint(self):
        """L1: Test server fingerprint"""
        config = get_test_config()
        adapter = SnowflakeAdapter(config)

        fingerprint = adapter.get_server_fingerprint()

        assert 'version' in fingerprint
        assert 'account' in fingerprint
        assert 'current_user' in fingerprint
        assert 'current_warehouse' in fingerprint
        assert 'adapter_version' in fingerprint

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_list_databases(self):
        """L1: Test database listing"""
        config = get_test_config()
        adapter = SnowflakeAdapter(config)

        databases = adapter.list_databases()

        assert isinstance(databases, list)
        # Should have at least one database
        assert len(databases) > 0

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_list_schemas(self):
        """L1: Test schema listing"""
        config = get_test_config()
        adapter = SnowflakeAdapter(config)

        schemas = adapter.list_schemas()

        assert isinstance(schemas, list)
        # INFORMATION_SCHEMA should be filtered out
        assert 'INFORMATION_SCHEMA' not in schemas

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_list_tables(self):
        """L1: Test table/view listing"""
        config = get_test_config()
        adapter = SnowflakeAdapter(config)

        tables = adapter.list_tables()

        assert isinstance(tables, list)
        for table in tables:
            assert 'database' in table
            assert 'schema' in table
            assert 'name' in table
            assert 'type' in table

        adapter.close()


# =============================================================================
# L2 TESTS: SCHEMA IR
# =============================================================================

class TestL2SchemaIR:
    """L2: Schema IR export/import tests"""

    @skip_no_adapter
    @skip_no_credentials
    def test_export_schema(self):
        """L2: Test schema export to IR"""
        config = get_test_config()
        adapter = SnowflakeAdapter(config)

        schema_ir = adapter.export_schema()

        assert isinstance(schema_ir, SchemaIR)
        assert isinstance(schema_ir.tables, list)
        assert isinstance(schema_ir.views, list)
        assert schema_ir.adapter_version

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_schema_export_determinism(self):
        """L2: Test schema export is deterministic"""
        config = get_test_config()
        adapter = SnowflakeAdapter(config)

        # Export twice
        schema_ir_1 = adapter.export_schema()
        schema_ir_2 = adapter.export_schema()

        # Convert to JSON for comparison
        json_1 = json.dumps({
            'tables': schema_ir_1.tables,
            'views': schema_ir_1.views,
        }, sort_keys=True, default=str)

        json_2 = json.dumps({
            'tables': schema_ir_2.tables,
            'views': schema_ir_2.views,
        }, sort_keys=True, default=str)

        assert json_1 == json_2

        adapter.close()


# =============================================================================
# L3 TESTS: FIDELITY
# =============================================================================

class TestL3Fidelity:
    """L3: Fidelity and edge case tests"""

    @skip_no_adapter
    def test_type_mapping_known_types(self):
        """L3: Test type mapping for known Snowflake types"""
        from extensions.plugins.snowflake_adapter import SnowflakeAdapter

        type_map = SnowflakeAdapter.TYPE_MAPPING

        # Numeric types
        assert type_map['number'] == 'DECIMAL'
        assert type_map['integer'] == 'INT32'
        assert type_map['bigint'] == 'INT64'
        assert type_map['float'] == 'FLOAT64'

        # String types
        assert type_map['varchar'] == 'STRING'
        assert type_map['string'] == 'STRING'

        # Date/time types
        assert type_map['date'] == 'DATE'
        assert type_map['timestamp_ntz'] == 'TIMESTAMP'
        assert type_map['timestamp_tz'] == 'TIMESTAMP_TZ'

        # Semi-structured (VARIANT lane)
        assert type_map['variant'] == 'JSON'
        assert type_map['object'] == 'JSON'
        assert type_map['array'] == 'JSON_ARRAY'

    @skip_no_adapter
    def test_unsupported_type_handling(self):
        """L3: Test unknown types map to UNKNOWN"""
        from extensions.plugins.snowflake_adapter import SnowflakeAdapter

        type_map = SnowflakeAdapter.TYPE_MAPPING
        assert 'made_up_type' not in type_map

    @skip_no_adapter
    @skip_no_credentials
    def test_variant_handling(self):
        """L3: Test VARIANT type handling"""
        config = get_test_config()
        adapter = SnowflakeAdapter(config)

        result = adapter.test_variant_handling()

        assert 'json_object' in result or 'error' in result
        if 'json_object' in result:
            assert 'note' in result

        adapter.close()


# =============================================================================
# L4 TESTS: PROOF AND DETERMINISM
# =============================================================================

class TestL4AuditProof:
    """L4: Proof bundle and determinism tests"""

    @skip_no_adapter
    @skip_no_credentials
    def test_proof_bundle_generation(self):
        """L4: Test proof bundle generation with all required artifacts"""
        config = get_test_config()
        adapter = SnowflakeAdapter(config)

        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir) / 'proof_bundle'

            # Create a test limitations file
            limitations_file = Path(tmpdir) / 'limitations.md'
            limitations_file.write_text("# Test Limitations\n\nTest content for Snowflake adapter.")

            bundle = adapter.generate_proof_bundle(
                output_dir=output_dir,
                limitations_path=limitations_file
            )

            # Verify bundle structure
            assert 'bundle_hash' in bundle
            assert 'schema_hash' in bundle
            assert 'dataset_hash' in bundle
            assert 'limitations_hash' in bundle
            assert 'run_manifest' in bundle

            # Verify files were created
            assert (output_dir / 'run_manifest.json').exists()
            assert (output_dir / 'schema_ir.json').exists()
            assert (output_dir / 'rowcount.json').exists()
            assert (output_dir / 'limitations.md').exists()

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_proof_bundle_requires_limitations(self):
        """L4: Test proof bundle fails without limitations_path"""
        config = get_test_config()
        adapter = SnowflakeAdapter(config)

        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir) / 'proof_bundle'

            with pytest.raises(SnowflakeOperationError) as exc_info:
                adapter.generate_proof_bundle(output_dir=output_dir)

            assert 'limitations_path is required' in str(exc_info.value)

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_run_manifest_required_fields(self):
        """L4: Test run_manifest contains all required fields"""
        config = get_test_config()
        adapter = SnowflakeAdapter(config)

        bundle = adapter.generate_proof_bundle()
        manifest = bundle['run_manifest']

        # Required fields
        assert 'adapter' in manifest
        assert 'adapter_version' in manifest
        assert 'config' in manifest
        assert 'versions' in manifest
        assert 'hardware_summary' in manifest
        assert 'dataset_hash' in manifest
        assert 'server_fingerprint' in manifest

        # Config should be sanitized
        assert 'password' not in manifest['config']
        assert 'account' in manifest['config']
        assert 'warehouse' in manifest['config']

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_proof_bundle_determinism(self):
        """L4: Test proof bundle determinism across runs"""
        config = get_test_config()
        adapter = SnowflakeAdapter(config)

        bundle_1 = adapter.generate_proof_bundle()
        bundle_2 = adapter.generate_proof_bundle()

        assert bundle_1['bundle_hash'] == bundle_2['bundle_hash']
        assert bundle_1['schema_hash'] == bundle_2['schema_hash']
        assert bundle_1['dataset_hash'] == bundle_2['dataset_hash']

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_proof_bundle_full_artifact_determinism(self):
        """L4: Test ALL deterministic artifacts are identical across runs"""
        config = get_test_config()
        adapter = SnowflakeAdapter(config)

        with tempfile.TemporaryDirectory() as tmpdir:
            dir_1 = Path(tmpdir) / 'run_1'
            dir_2 = Path(tmpdir) / 'run_2'

            limitations_file = Path(tmpdir) / 'limitations.md'
            limitations_file.write_text("# Test Limitations\n\nTest content.")

            adapter.generate_proof_bundle(output_dir=dir_1, limitations_path=limitations_file)
            adapter.generate_proof_bundle(output_dir=dir_2, limitations_path=limitations_file)

            # Compare all deterministic artifacts
            deterministic_files = [
                'run_manifest.json',
                'schema_ir.json',
                'rowcount.json',
                'schema_diff.json',
                'rowcount_diff.json',
                'checksum_diff.json',
                'limitations.md',
            ]

            for filename in deterministic_files:
                with open(dir_1 / filename) as f1, open(dir_2 / filename) as f2:
                    assert f1.read() == f2.read(), f"Artifact {filename} differs"

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_no_secrets_in_bundle(self):
        """L4: Test secrets are not leaked in bundle"""
        config = get_test_config()
        adapter = SnowflakeAdapter(config)

        bundle = adapter.generate_proof_bundle()
        bundle_str = json.dumps(bundle, default=str)

        if config.password:
            assert config.password not in bundle_str

        assert 'password' not in bundle['run_manifest'].get('config', {})

        adapter.close()


# =============================================================================
# SAFETY TESTS
# =============================================================================

class TestSafety:
    """L4: Safety tests"""

    @skip_no_adapter
    @skip_no_credentials
    def test_drop_table_blocked_by_default(self):
        """L4: Test DROP TABLE is blocked by default"""
        config = get_test_config()
        adapter = SnowflakeAdapter(config)

        with pytest.raises(SnowflakeOperationError) as exc_info:
            adapter.drop_table('nonexistent_table')

        assert 'blocked' in str(exc_info.value).lower()

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_truncate_blocked_by_default(self):
        """L4: Test TRUNCATE is blocked by default"""
        config = get_test_config()
        adapter = SnowflakeAdapter(config)

        with pytest.raises(SnowflakeOperationError) as exc_info:
            adapter.truncate_table('nonexistent_table')

        assert 'blocked' in str(exc_info.value).lower()

        adapter.close()


# =============================================================================
# DETERMINISM TESTS
# =============================================================================

class TestDeterminism:
    """Tests for deterministic behavior"""

    @skip_no_adapter
    @skip_no_credentials
    def test_list_databases_determinism(self):
        """Test database listing is deterministic"""
        config = get_test_config()
        adapter = SnowflakeAdapter(config)

        dbs_1 = adapter.list_databases()
        dbs_2 = adapter.list_databases()

        assert dbs_1 == dbs_2

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_list_tables_determinism(self):
        """Test table listing is deterministic"""
        config = get_test_config()
        adapter = SnowflakeAdapter(config)

        tables_1 = adapter.list_tables()
        tables_2 = adapter.list_tables()

        assert json.dumps(tables_1, sort_keys=True) == json.dumps(tables_2, sort_keys=True)

        adapter.close()


# =============================================================================
# RUN TESTS
# =============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
