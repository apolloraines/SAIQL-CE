#!/usr/bin/env python3
"""
BigQuery Adapter Test Suite - L0-L4 Testing

This module provides comprehensive tests for the BigQuery adapter:
- L0: Connectivity tests
- L1: Introspection tests
- L2: IR read/write tests
- L3: Fidelity and edge case tests
- L4: Proof bundle and determinism tests

Author: Apollo & Claude
Version: 1.0.0

Usage:
    # Run all tests (requires BigQuery credentials)
    pytest tests/database/test_bigquery_adapter.py -v

    # Run only L0 tests
    pytest tests/database/test_bigquery_adapter.py -v -k "L0"

    # Run with specific project
    GCP_PROJECT=my-project pytest tests/database/test_bigquery_adapter.py -v
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

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# Check if BigQuery is available
try:
    from google.cloud import bigquery
    BIGQUERY_AVAILABLE = True
except ImportError:
    BIGQUERY_AVAILABLE = False

# Import adapter (will fail gracefully if BigQuery not installed)
try:
    from extensions.plugins.bigquery_adapter import (
        BigQueryAdapter,
        BigQueryConfig,
        BigQueryResult,
        AuthMethod,
        BigQueryConnectionError,
        BigQueryAuthError,
        BigQueryQueryError,
    )
    ADAPTER_AVAILABLE = True
except ImportError as e:
    ADAPTER_AVAILABLE = False
    print(f"BigQuery adapter not available: {e}")


# Test configuration from environment
def get_test_config() -> Optional[BigQueryConfig]:
    """Get test configuration from environment variables"""
    project_id = os.environ.get('GCP_PROJECT') or os.environ.get('GOOGLE_CLOUD_PROJECT')
    credentials_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')

    if not project_id:
        return None

    return BigQueryConfig(
        project_id=project_id,
        credentials_path=credentials_path,
        auth_method=AuthMethod.SERVICE_ACCOUNT if credentials_path else AuthMethod.APPLICATION_DEFAULT,
        location=os.environ.get('BQ_LOCATION', 'US'),
        query_timeout=60,
        max_retries=2,
    )


def get_test_dataset_id() -> Optional[str]:
    """
    Get dedicated test dataset ID from environment.

    Cost-safety rule: L4 tests that run checksums MUST use a dedicated small
    test dataset, not arbitrary datasets from the project. Set BQ_TEST_DATASET
    to a dataset with small tables (< 10MB total).
    """
    return os.environ.get('BQ_TEST_DATASET')


# Skip conditions
skip_no_bigquery = pytest.mark.skipif(
    not BIGQUERY_AVAILABLE,
    reason="google-cloud-bigquery not installed"
)

skip_no_adapter = pytest.mark.skipif(
    not ADAPTER_AVAILABLE,
    reason="BigQuery adapter not available"
)

skip_no_credentials = pytest.mark.skipif(
    get_test_config() is None,
    reason="BigQuery credentials not configured (set GCP_PROJECT and optionally GOOGLE_APPLICATION_CREDENTIALS)"
)

skip_no_test_dataset = pytest.mark.skipif(
    get_test_dataset_id() is None,
    reason="L4 checksum tests require dedicated test dataset (set BQ_TEST_DATASET to a small dataset)"
)


# =============================================================================
# L0 TESTS: CONNECTIVITY
# =============================================================================

class TestL0Connectivity:
    """L0: Connectivity and basic query execution tests"""

    @skip_no_adapter
    def test_config_creation(self):
        """Test BigQueryConfig creation"""
        config = BigQueryConfig(
            project_id="test-project",
            location="US"
        )
        assert config.project_id == "test-project"
        assert config.location == "US"
        assert config.auth_method == AuthMethod.APPLICATION_DEFAULT

    @skip_no_adapter
    def test_config_with_service_account(self):
        """Test config with service account path"""
        config = BigQueryConfig(
            project_id="test-project",
            auth_method=AuthMethod.SERVICE_ACCOUNT,
            credentials_path="/path/to/creds.json"
        )
        assert config.auth_method == AuthMethod.SERVICE_ACCOUNT
        assert config.credentials_path == "/path/to/creds.json"

    @skip_no_adapter
    @skip_no_credentials
    def test_connection_success(self):
        """L0: Test successful connection to BigQuery"""
        config = get_test_config()
        adapter = BigQueryAdapter(config)

        assert adapter.is_connected()
        assert adapter.config.project_id == config.project_id

        adapter.close()
        assert not adapter.is_connected()

    @skip_no_adapter
    @skip_no_credentials
    def test_trivial_query(self):
        """L0: Test SELECT 1 trivial query"""
        config = get_test_config()
        adapter = BigQueryAdapter(config)

        result = adapter.execute_query("SELECT 1 as test_value")

        assert result.success
        assert len(result.data) == 1
        assert result.data[0]['test_value'] == 1
        assert result.execution_time > 0
        assert result.job_id is not None

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_query_with_multiple_rows(self):
        """L0: Test query returning multiple rows"""
        config = get_test_config()
        adapter = BigQueryAdapter(config)

        result = adapter.execute_query("""
            SELECT num FROM UNNEST([1, 2, 3, 4, 5]) as num
            ORDER BY num
        """)

        assert result.success
        assert len(result.data) == 5
        assert [r['num'] for r in result.data] == [1, 2, 3, 4, 5]

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_error_invalid_query(self):
        """L0: Test deterministic error for invalid query"""
        config = get_test_config()
        adapter = BigQueryAdapter(config)

        result = adapter.execute_query("SELECT * FROM nonexistent_table_xyz")

        assert not result.success
        assert result.error_message is not None
        assert "not found" in result.error_message.lower() or "does not exist" in result.error_message.lower()

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_error_invalid_syntax(self):
        """L0: Test deterministic error for syntax error"""
        config = get_test_config()
        adapter = BigQueryAdapter(config)

        result = adapter.execute_query("SELEKT 1")

        assert not result.success
        assert result.error_message is not None

        adapter.close()

    @skip_no_adapter
    def test_error_missing_credentials(self):
        """L0: Test deterministic error for missing credentials"""
        config = BigQueryConfig(
            project_id="nonexistent-project-12345",
            auth_method=AuthMethod.SERVICE_ACCOUNT,
            credentials_path="/nonexistent/path/to/creds.json"
        )

        with pytest.raises((BigQueryAuthError, BigQueryConnectionError, FileNotFoundError)):
            BigQueryAdapter(config)

    @skip_no_adapter
    @skip_no_credentials
    def test_dry_run(self):
        """L0: Test dry run query validation"""
        config = get_test_config()
        adapter = BigQueryAdapter(config)

        result = adapter.execute_query("SELECT 1", dry_run=True)

        assert result.success
        assert len(result.data) == 0  # No data in dry run
        assert result.metrics.bytes_processed >= 0

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_statistics(self):
        """L0: Test adapter statistics collection"""
        config = get_test_config()
        adapter = BigQueryAdapter(config)

        # Execute a few queries
        adapter.execute_query("SELECT 1")
        adapter.execute_query("SELECT 2")
        adapter.execute_query("SELECT * FROM nonexistent_xyz")  # Will fail

        stats = adapter.get_statistics()

        assert stats['backend'] == 'bigquery'
        assert stats['queries_executed'] >= 2
        assert stats['failed_queries'] >= 1
        assert 'success_rate' in stats
        assert 'total_bytes_processed' in stats

        adapter.close()


# =============================================================================
# L1 TESTS: INTROSPECTION
# =============================================================================

class TestL1Introspection:
    """L1: Introspection and basic translation tests"""

    @skip_no_adapter
    @skip_no_credentials
    def test_list_datasets(self):
        """L1: Test listing datasets"""
        config = get_test_config()
        adapter = BigQueryAdapter(config)

        datasets = adapter.list_datasets()

        # Should return a list (may be empty if no datasets)
        assert isinstance(datasets, list)

        if datasets:
            # Check structure
            ds = datasets[0]
            assert 'dataset_id' in ds
            assert 'project' in ds
            assert 'full_id' in ds

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_list_tables(self):
        """L1: Test listing tables in a dataset"""
        config = get_test_config()
        adapter = BigQueryAdapter(config)

        datasets = adapter.list_datasets()

        if not datasets:
            pytest.skip("No datasets available for testing")

        # Try first dataset
        dataset_id = datasets[0]['dataset_id']
        tables = adapter.list_tables(dataset_id)

        assert isinstance(tables, list)

        if tables:
            t = tables[0]
            assert 'table_id' in t
            assert 'dataset_id' in t
            assert 'type' in t

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_describe_table(self):
        """L1: Test describing table schema"""
        config = get_test_config()
        adapter = BigQueryAdapter(config)

        datasets = adapter.list_datasets()
        if not datasets:
            pytest.skip("No datasets available for testing")

        dataset_id = datasets[0]['dataset_id']
        tables = adapter.list_tables(dataset_id)

        if not tables:
            pytest.skip("No tables available for testing")

        # Get first table that's not a view
        table_tables = [t for t in tables if t['type'] == 'TABLE']
        if not table_tables:
            pytest.skip("No TABLE type tables available")

        table_id = table_tables[0]['table_id']
        description = adapter.describe_table(dataset_id, table_id)

        assert 'table_id' in description
        assert 'columns' in description
        assert isinstance(description['columns'], list)

        if description['columns']:
            col = description['columns'][0]
            assert 'name' in col
            assert 'type' in col
            assert 'mode' in col

        adapter.close()


# =============================================================================
# L2 TESTS: IR READ/WRITE
# =============================================================================

class TestL2IRReadWrite:
    """L2: IR read/write for schema and data tests"""

    @skip_no_adapter
    @skip_no_credentials
    def test_extract_schema_ir(self):
        """L2: Test schema IR extraction"""
        config = get_test_config()
        adapter = BigQueryAdapter(config)

        datasets = adapter.list_datasets()
        if not datasets:
            pytest.skip("No datasets available for testing")

        dataset_id = datasets[0]['dataset_id']
        schema_ir = adapter.extract_schema_ir(dataset_id)

        assert 'dataset_id' in schema_ir
        assert 'project_id' in schema_ir
        assert 'tables' in schema_ir
        assert 'views' in schema_ir
        assert 'extraction_timestamp' in schema_ir
        assert 'adapter_version' in schema_ir

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_schema_ir_determinism(self):
        """L2: Test schema IR extraction is deterministic"""
        config = get_test_config()
        adapter = BigQueryAdapter(config)

        datasets = adapter.list_datasets()
        if not datasets:
            pytest.skip("No datasets available for testing")

        dataset_id = datasets[0]['dataset_id']

        # Extract twice
        ir1 = adapter.extract_schema_ir(dataset_id)
        ir2 = adapter.extract_schema_ir(dataset_id)

        # Remove timestamps for comparison
        del ir1['extraction_timestamp']
        del ir2['extraction_timestamp']

        # Should be identical
        assert json.dumps(ir1, sort_keys=True) == json.dumps(ir2, sort_keys=True)

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_export_data_requires_order_by(self):
        """L2: Test that data export requires ORDER BY"""
        config = get_test_config()
        adapter = BigQueryAdapter(config)

        datasets = adapter.list_datasets()
        if not datasets:
            pytest.skip("No datasets available for testing")

        dataset_id = datasets[0]['dataset_id']
        tables = adapter.list_tables(dataset_id)
        table_tables = [t for t in tables if t['type'] == 'TABLE']

        if not table_tables:
            pytest.skip("No tables available for testing")

        table_id = table_tables[0]['table_id']

        # Should fail without order_by
        with pytest.raises(BigQueryQueryError) as exc_info:
            adapter.export_data_ir(dataset_id, table_id)

        assert "ORDER BY" in str(exc_info.value)

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_type_mapping(self):
        """L2: Test type mapping table"""
        config = get_test_config()
        adapter = BigQueryAdapter(config)

        type_map = adapter.get_type_mapping()

        # Check essential types are mapped
        assert 'INT64' in type_map
        assert 'STRING' in type_map
        assert 'TIMESTAMP' in type_map
        assert 'NUMERIC' in type_map
        assert 'STRUCT' in type_map
        assert 'ARRAY' in type_map

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_schema_round_trip(self):
        """L2: Test schema create from IR and re-extract matches"""
        config = get_test_config()
        adapter = BigQueryAdapter(config)

        # Create a test dataset
        test_dataset = f"saiql_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        try:
            adapter.create_dataset(test_dataset)

            # Define a table IR - uses 'name' key per adapter's create_table_from_ir
            table_ir = {
                'name': 'round_trip_test',
                'columns': [
                    {'name': 'id', 'type': 'INT64', 'mode': 'REQUIRED'},
                    {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
                    {'name': 'value', 'type': 'FLOAT64', 'mode': 'NULLABLE'},
                ]
            }

            # Create table from IR
            adapter.create_table_from_ir(test_dataset, table_ir)

            # Extract schema IR back
            schema_ir = adapter.extract_schema_ir(test_dataset)

            # Find our table - tables is a dict keyed by table_id
            tables_dict = schema_ir.get('tables', {})
            created_table = tables_dict.get('round_trip_test')

            assert created_table is not None, "Created table not found in schema IR"
            assert len(created_table['columns']) == 3

            # Verify columns match
            col_names = [c['name'] for c in created_table['columns']]
            assert 'id' in col_names
            assert 'name' in col_names
            assert 'value' in col_names

        finally:
            # Cleanup - delete test dataset
            try:
                adapter.client.delete_dataset(
                    f"{config.project_id}.{test_dataset}",
                    delete_contents=True,
                    not_found_ok=True
                )
            except Exception:
                pass

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_data_round_trip(self):
        """L2: Test data load and export round-trip"""
        config = get_test_config()
        adapter = BigQueryAdapter(config)

        test_dataset = f"saiql_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        try:
            adapter.create_dataset(test_dataset)

            # Create table - uses 'name' key per adapter's create_table_from_ir
            table_ir = {
                'name': 'data_round_trip',
                'columns': [
                    {'name': 'id', 'type': 'INT64', 'mode': 'REQUIRED'},
                    {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
                ]
            }
            adapter.create_table_from_ir(test_dataset, table_ir)

            # Load data
            test_data = [
                {'id': 1, 'name': 'Alice'},
                {'id': 2, 'name': 'Bob'},
                {'id': 3, 'name': 'Charlie'},
            ]
            adapter.load_data_from_ir(test_dataset, 'data_round_trip', test_data)

            # Wait for streaming buffer (BigQuery streaming insert delay)
            import time
            time.sleep(5)

            # Export data with ORDER BY for determinism - returns dict with 'data' key
            exported_ir = adapter.export_data_ir(test_dataset, 'data_round_trip', order_by=['id'])
            exported = exported_ir['data']

            assert len(exported) == 3
            assert exported[0]['id'] == 1
            assert exported[0]['name'] == 'Alice'
            assert exported[2]['id'] == 3
            assert exported[2]['name'] == 'Charlie'

        finally:
            try:
                adapter.client.delete_dataset(
                    f"{config.project_id}.{test_dataset}",
                    delete_contents=True,
                    not_found_ok=True
                )
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
    def test_numeric_precision(self):
        """L3: Test NUMERIC precision handling"""
        config = get_test_config()
        adapter = BigQueryAdapter(config)

        # Test NUMERIC (38,9) precision
        result = adapter.execute_query("""
            SELECT
                CAST(12345678901234567890.123456789 AS NUMERIC) as numeric_val,
                CAST(12345678901234567890123456789012345678.12345678901234567890123456789012345678 AS BIGNUMERIC) as bignumeric_val
        """)

        assert result.success
        assert len(result.data) == 1

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_timestamp_types(self):
        """L3: Test TIMESTAMP, DATETIME, DATE, TIME handling"""
        config = get_test_config()
        adapter = BigQueryAdapter(config)

        result = adapter.execute_query("""
            SELECT
                CURRENT_TIMESTAMP() as ts,
                CURRENT_DATETIME() as dt,
                CURRENT_DATE() as d,
                CURRENT_TIME() as t
        """)

        assert result.success
        assert len(result.data) == 1
        row = result.data[0]
        assert 'ts' in row
        assert 'dt' in row
        assert 'd' in row
        assert 't' in row

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_array_type(self):
        """L3: Test ARRAY type handling"""
        config = get_test_config()
        adapter = BigQueryAdapter(config)

        result = adapter.execute_query("""
            SELECT [1, 2, 3] as arr, ['a', 'b', 'c'] as str_arr
        """)

        assert result.success
        assert len(result.data) == 1
        row = result.data[0]
        assert row['arr'] == [1, 2, 3]
        assert row['str_arr'] == ['a', 'b', 'c']

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_struct_type(self):
        """L3: Test STRUCT type handling"""
        config = get_test_config()
        adapter = BigQueryAdapter(config)

        result = adapter.execute_query("""
            SELECT STRUCT(1 as a, 'hello' as b) as my_struct
        """)

        assert result.success
        assert len(result.data) == 1
        row = result.data[0]
        assert 'my_struct' in row
        assert row['my_struct']['a'] == 1
        assert row['my_struct']['b'] == 'hello'

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_json_type(self):
        """L3: Test JSON type handling"""
        config = get_test_config()
        adapter = BigQueryAdapter(config)

        result = adapter.execute_query("""
            SELECT JSON '{"key": "value", "num": 123}' as json_val
        """)

        assert result.success
        # JSON handling may vary by BigQuery client version

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_null_handling(self):
        """L3: Test NULL value handling"""
        config = get_test_config()
        adapter = BigQueryAdapter(config)

        result = adapter.execute_query("""
            SELECT
                NULL as null_val,
                CAST(NULL AS INT64) as null_int,
                CAST(NULL AS STRING) as null_str
        """)

        assert result.success
        row = result.data[0]
        assert row['null_val'] is None
        assert row['null_int'] is None
        assert row['null_str'] is None

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_view_definition_extraction(self):
        """L3: Test view definition extraction"""
        config = get_test_config()
        adapter = BigQueryAdapter(config)

        test_dataset = f"saiql_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        try:
            adapter.create_dataset(test_dataset)

            # Create a base table - uses 'name' key per adapter's create_table_from_ir
            table_ir = {
                'name': 'base_table',
                'columns': [
                    {'name': 'id', 'type': 'INT64', 'mode': 'REQUIRED'},
                    {'name': 'value', 'type': 'STRING', 'mode': 'NULLABLE'},
                ]
            }
            adapter.create_table_from_ir(test_dataset, table_ir)

            # Create a view
            view_sql = f"""
                CREATE VIEW `{config.project_id}.{test_dataset}.test_view` AS
                SELECT id, value FROM `{config.project_id}.{test_dataset}.base_table`
                WHERE id > 0
            """
            adapter.execute_query(view_sql)

            # Extract view definition - returns dict with 'query' key
            view_def = adapter.get_view_definition(test_dataset, 'test_view')

            assert view_def is not None
            assert 'query' in view_def
            assert 'SELECT' in view_def['query'].upper()
            assert 'base_table' in view_def['query']

            # Verify view appears in schema IR - views is a dict keyed by view_id
            schema_ir = adapter.extract_schema_ir(test_dataset)
            views_dict = schema_ir.get('views', {})
            assert 'test_view' in views_dict

        finally:
            try:
                adapter.client.delete_dataset(
                    f"{config.project_id}.{test_dataset}",
                    delete_contents=True,
                    not_found_ok=True
                )
            except Exception:
                pass

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_partitioning_info_extraction(self):
        """L3: Test partitioning metadata extraction"""
        config = get_test_config()
        adapter = BigQueryAdapter(config)

        test_dataset = f"saiql_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        try:
            adapter.create_dataset(test_dataset)

            # Create partitioned table using DDL
            create_sql = f"""
                CREATE TABLE `{config.project_id}.{test_dataset}.partitioned_table` (
                    id INT64,
                    created_date DATE,
                    value STRING
                )
                PARTITION BY created_date
            """
            adapter.execute_query(create_sql)

            # Verify partitioning info via schema IR - tables is a dict
            schema_ir = adapter.extract_schema_ir(test_dataset)
            tables_dict = schema_ir.get('tables', {})
            table = tables_dict.get('partitioned_table')

            assert table is not None, "Partitioned table not found in schema IR"
            assert 'partitioning' in table, "partitioning key missing from table IR"
            partition_info = table['partitioning']
            assert partition_info is not None
            assert partition_info.get('type') is not None or partition_info.get('field') is not None

        finally:
            try:
                adapter.client.delete_dataset(
                    f"{config.project_id}.{test_dataset}",
                    delete_contents=True,
                    not_found_ok=True
                )
            except Exception:
                pass

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_clustering_info_extraction(self):
        """L3: Test clustering metadata extraction"""
        config = get_test_config()
        adapter = BigQueryAdapter(config)

        test_dataset = f"saiql_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        try:
            adapter.create_dataset(test_dataset)

            # Create clustered table using DDL
            create_sql = f"""
                CREATE TABLE `{config.project_id}.{test_dataset}.clustered_table` (
                    id INT64,
                    category STRING,
                    created_date DATE,
                    value STRING
                )
                PARTITION BY created_date
                CLUSTER BY category
            """
            adapter.execute_query(create_sql)

            # Verify in schema IR - tables is a dict keyed by table_id
            schema_ir = adapter.extract_schema_ir(test_dataset)
            tables_dict = schema_ir.get('tables', {})
            table = tables_dict.get('clustered_table')

            assert table is not None, "Clustered table not found in schema IR"
            # Clustering should be captured
            assert 'clustering' in table, "clustering key missing from table IR"
            assert table['clustering'] is not None

        finally:
            try:
                adapter.client.delete_dataset(
                    f"{config.project_id}.{test_dataset}",
                    delete_contents=True,
                    not_found_ok=True
                )
            except Exception:
                pass

        adapter.close()


# =============================================================================
# L4 TESTS: AUDIT-GRADE PROOF
# =============================================================================

class TestL4AuditProof:
    """L4: Audit-grade proof and determinism tests"""

    @skip_no_adapter
    @skip_no_credentials
    def test_proof_bundle_generation(self):
        """L4: Test proof bundle generation"""
        config = get_test_config()
        adapter = BigQueryAdapter(config)

        datasets = adapter.list_datasets()
        if not datasets:
            pytest.skip("No datasets available for testing")

        dataset_id = datasets[0]['dataset_id']

        with tempfile.TemporaryDirectory() as tmpdir:
            manifest = adapter.generate_proof_bundle(
                dataset_id,
                tmpdir,
                include_data_checksums=False  # Skip data checksums for speed
            )

            # Check manifest core fields
            assert 'bundle_id' in manifest
            assert 'timestamp' in manifest
            assert 'schema_hash' in manifest
            assert 'table_count' in manifest
            assert manifest['dataset_id'] == dataset_id

            # Check L4 required fields per spec
            assert 'config' in manifest, "L4 requires config in manifest"
            assert 'versions' in manifest, "L4 requires versions in manifest"
            assert 'dataset_hash' in manifest, "L4 requires dataset_hash in manifest"
            assert 'hardware_summary' in manifest, "L4 requires hardware_summary in manifest"

            # Verify config is sanitized (no secrets)
            config = manifest['config']
            assert 'project_id' in config
            assert 'credentials_path' not in config, "credentials_path should not be in manifest"
            assert 'credentials_json' not in config, "credentials_json should not be in manifest"

            # Verify versions
            assert 'adapter' in manifest['versions']
            assert 'python' in manifest['versions']

            # Verify hardware summary
            assert 'platform' in manifest['hardware_summary']

            # Check files
            assert (Path(tmpdir) / 'run_manifest.json').exists()
            assert (Path(tmpdir) / 'schema_ir.json').exists()
            assert (Path(tmpdir) / 'bundle_sha256.txt').exists()

            # Verify bundle hash
            with open(Path(tmpdir) / 'run_manifest.json', 'rb') as f:
                computed_hash = hashlib.sha256(f.read()).hexdigest()

            with open(Path(tmpdir) / 'bundle_sha256.txt') as f:
                stored_hash = f.read().split()[0]

            assert computed_hash == stored_hash

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_proof_bundle_determinism(self):
        """L4: Test proof bundle is deterministic (same inputs = same output)"""
        config = get_test_config()
        adapter = BigQueryAdapter(config)

        datasets = adapter.list_datasets()
        if not datasets:
            pytest.skip("No datasets available for testing")

        dataset_id = datasets[0]['dataset_id']

        with tempfile.TemporaryDirectory() as tmpdir1:
            with tempfile.TemporaryDirectory() as tmpdir2:
                # Generate two bundles
                manifest1 = adapter.generate_proof_bundle(
                    dataset_id, tmpdir1, include_data_checksums=False
                )
                manifest2 = adapter.generate_proof_bundle(
                    dataset_id, tmpdir2, include_data_checksums=False
                )

                # Schema hashes should be identical
                assert manifest1['schema_hash'] == manifest2['schema_hash']

                # Table counts should match
                assert manifest1['table_count'] == manifest2['table_count']

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_query_determinism(self):
        """L4: Test query results are deterministic with ORDER BY"""
        config = get_test_config()
        adapter = BigQueryAdapter(config)

        # Run same query twice
        query = "SELECT num FROM UNNEST([3, 1, 4, 1, 5, 9, 2, 6]) as num ORDER BY num"

        result1 = adapter.execute_query(query)
        result2 = adapter.execute_query(query)

        assert result1.success and result2.success

        # Results should be identical
        data1 = json.dumps(result1.data, sort_keys=True)
        data2 = json.dumps(result2.data, sort_keys=True)
        assert data1 == data2

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_no_secrets_in_stats(self):
        """L4: Test no secrets leaked in statistics"""
        config = get_test_config()
        adapter = BigQueryAdapter(config)

        stats = adapter.get_statistics()
        stats_str = json.dumps(stats)

        # Check no obvious credential leaks
        assert 'password' not in stats_str.lower()
        assert 'secret' not in stats_str.lower()
        assert 'private_key' not in stats_str.lower()

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    @skip_no_test_dataset
    def test_proof_bundle_required_artifacts(self):
        """L4: Test proof bundle contains ALL required artifacts per spec

        Cost-safety: This test runs checksums, so it REQUIRES a dedicated small
        test dataset (BQ_TEST_DATASET) to avoid full-scanning production tables.
        """
        config = get_test_config()
        adapter = BigQueryAdapter(config)

        # Use dedicated test dataset for cost-safety
        dataset_id = get_test_dataset_id()

        with tempfile.TemporaryDirectory() as tmpdir:
            manifest = adapter.generate_proof_bundle(
                dataset_id,
                tmpdir,
                include_data_checksums=True,
                max_checksum_table_bytes=10 * 1024 * 1024  # 10MB limit for safety
            )

            # L4 spec requires: run_manifest.json, schema_diff.json, rowcount_diff.json,
            # checksum_diff.json (or N/A), limitations.md
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

            # Verify schema_diff.json has expected structure
            with open(Path(tmpdir) / 'schema_diff.json') as f:
                schema_diff = json.load(f)
            assert 'baseline_hash' in schema_diff or 'current_hash' in schema_diff or 'status' in schema_diff

            # Verify rowcount_diff.json has expected structure
            with open(Path(tmpdir) / 'rowcount_diff.json') as f:
                rowcount_diff = json.load(f)
            assert 'tables' in rowcount_diff or 'status' in rowcount_diff

            # Verify checksum_diff.json has expected structure
            with open(Path(tmpdir) / 'checksum_diff.json') as f:
                checksum_diff = json.load(f)
            assert 'tables' in checksum_diff or 'status' in checksum_diff

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_proof_bundle_schema_diff_content(self):
        """L4: Test schema_diff.json contains comparison data"""
        config = get_test_config()
        adapter = BigQueryAdapter(config)

        datasets = adapter.list_datasets()
        if not datasets:
            pytest.skip("No datasets available for testing")

        dataset_id = datasets[0]['dataset_id']

        with tempfile.TemporaryDirectory() as tmpdir:
            # Cost-safety: disable checksums when using arbitrary dataset
            adapter.generate_proof_bundle(dataset_id, tmpdir, include_data_checksums=False)

            with open(Path(tmpdir) / 'schema_diff.json') as f:
                schema_diff = json.load(f)

            # Without baseline, should have N/A status
            assert 'status' in schema_diff
            # When status is N/A, should have reason
            if schema_diff['status'] == 'N/A':
                assert 'reason' in schema_diff
            # When status is COMPUTED, should have change tracking fields
            elif schema_diff['status'] == 'COMPUTED':
                assert 'has_changes' in schema_diff

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_proof_bundle_rowcount_diff_content(self):
        """L4: Test rowcount_diff.json contains table row counts"""
        config = get_test_config()
        adapter = BigQueryAdapter(config)

        datasets = adapter.list_datasets()
        if not datasets:
            pytest.skip("No datasets available for testing")

        dataset_id = datasets[0]['dataset_id']

        with tempfile.TemporaryDirectory() as tmpdir:
            # Cost-safety: disable checksums when using arbitrary dataset
            adapter.generate_proof_bundle(dataset_id, tmpdir, include_data_checksums=False)

            with open(Path(tmpdir) / 'rowcount_diff.json') as f:
                rowcount_diff = json.load(f)

            # Without baseline, should have N/A status
            assert 'status' in rowcount_diff
            # When status is N/A, should have reason
            if rowcount_diff['status'] == 'N/A':
                assert 'reason' in rowcount_diff
            # When status is COMPUTED, should have tables dict and has_changes flag
            elif rowcount_diff['status'] == 'COMPUTED':
                assert 'tables' in rowcount_diff
                assert isinstance(rowcount_diff['tables'], dict)
                assert 'has_changes' in rowcount_diff

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_proof_bundle_limitations_copied(self):
        """L4: Test limitations.md is included in bundle"""
        config = get_test_config()
        adapter = BigQueryAdapter(config)

        datasets = adapter.list_datasets()
        if not datasets:
            pytest.skip("No datasets available for testing")

        dataset_id = datasets[0]['dataset_id']

        with tempfile.TemporaryDirectory() as tmpdir:
            # Cost-safety: disable checksums when using arbitrary dataset
            adapter.generate_proof_bundle(dataset_id, tmpdir, include_data_checksums=False)

            limitations_path = Path(tmpdir) / 'limitations.md'
            assert limitations_path.exists(), "limitations.md not included in proof bundle"

            # Verify it has content (stub or full limitations are acceptable)
            content = limitations_path.read_text()
            assert len(content) > 50, "limitations.md appears to be empty"
            # Must contain 'Limitation' (present in both stub header and full file)
            assert 'Limitation' in content, "limitations.md missing expected content"
            # Verify it's about BigQuery
            assert 'BigQuery' in content, "limitations.md should reference BigQuery"

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_proof_bundle_schema_hash_stable(self):
        """L4: Test schema hash is stable across multiple runs (determinism)"""
        config = get_test_config()
        adapter = BigQueryAdapter(config)

        datasets = adapter.list_datasets()
        if not datasets:
            pytest.skip("No datasets available for testing")

        dataset_id = datasets[0]['dataset_id']

        hashes = []
        for _ in range(3):
            with tempfile.TemporaryDirectory() as tmpdir:
                # Cost-safety: disable checksums when using arbitrary dataset
                manifest = adapter.generate_proof_bundle(dataset_id, tmpdir, include_data_checksums=False)
                hashes.append(manifest['schema_hash'])

        # All hashes should be identical
        assert hashes[0] == hashes[1] == hashes[2], \
            f"Schema hash not stable: {hashes}"

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
    return result.returncode == 0


def run_all_tests():
    """Run all BigQuery adapter tests"""
    import subprocess
    result = subprocess.run([
        sys.executable, "-m", "pytest",
        __file__,
        "-v",
        "--tb=short"
    ])
    return result.returncode == 0


if __name__ == "__main__":
    print("BigQuery Adapter Test Suite")
    print("=" * 50)
    print(f"BigQuery SDK available: {BIGQUERY_AVAILABLE}")
    print(f"Adapter available: {ADAPTER_AVAILABLE}")

    config = get_test_config()
    if config:
        print(f"Project configured: {config.project_id}")
    else:
        print("No project configured (set GCP_PROJECT)")

    print()

    if len(sys.argv) > 1 and sys.argv[1] == "l0":
        success = run_l0_tests()
    else:
        success = run_all_tests()

    sys.exit(0 if success else 1)
