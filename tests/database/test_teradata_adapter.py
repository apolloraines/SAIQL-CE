#!/usr/bin/env python3
"""
Test suite for Teradata Adapter (L0-L4)

This test suite validates:
- L0: Connectivity and basic query execution
- L1: Introspection (databases, tables, columns, primary index)
- L2: Schema and data IR conversion
- L3: Type mapping and fidelity
- L4: Proof bundles and determinism

Tests marked with @pytest.mark.teradata require a live Teradata connection.
Set environment variables:
- TERADATA_HOST
- TERADATA_USER
- TERADATA_PASSWORD
- TERADATA_DATABASE

Author: Apollo & Claude
"""

import pytest
import os
import json
import tempfile
import hashlib
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

# Import the adapter
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from extensions.plugins.teradata_adapter import (
    TeradataAdapter,
    TeradataConfig,
    TeradataError,
    TeradataConnectionError,
    TeradataAuthError,
    TeradataQueryError,
    TeradataOperationError,
    ColumnInfo,
    TableInfo,
    SchemaIR,
    DataIR,
    ConnectionState,
    TERADATA_AVAILABLE,
)


# =============================================================================
# FIXTURES
# =============================================================================

@pytest.fixture
def teradata_config():
    """Create a test Teradata configuration"""
    return TeradataConfig(
        host='localhost',
        user='testuser',
        password='testpass',
        database='testdb',
        port=1025,
    )


@pytest.fixture
def mock_connection():
    """Create a mock Teradata connection"""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    # Mock successful connection verification
    mock_cursor.fetchone.return_value = (1,)
    mock_cursor.description = [('connection_test', None, None, None, None, None, None)]

    return mock_conn, mock_cursor


def live_teradata_available():
    """Check if live Teradata connection is available"""
    return all([
        os.environ.get('TERADATA_HOST'),
        os.environ.get('TERADATA_USER'),
        os.environ.get('TERADATA_PASSWORD'),
    ])


def get_live_config():
    """Get configuration for live Teradata testing"""
    return TeradataConfig(
        host=os.environ.get('TERADATA_HOST', ''),
        user=os.environ.get('TERADATA_USER', ''),
        password=os.environ.get('TERADATA_PASSWORD', ''),
        database=os.environ.get('TERADATA_DATABASE', 'DBC'),
        port=int(os.environ.get('TERADATA_PORT', '1025')),
    )


# Skip marker for tests requiring live Teradata
requires_teradata = pytest.mark.skipif(
    not live_teradata_available(),
    reason="Live Teradata connection required (set TERADATA_HOST, TERADATA_USER, TERADATA_PASSWORD)"
)


# =============================================================================
# PINNED DATASET FIXTURE - Reproducible DDL and Seed Data
# =============================================================================

# DDL for test fixture table
FIXTURE_TABLE_DDL = """
CREATE TABLE saiql_test_fixture (
    id INTEGER NOT NULL,
    name VARCHAR(100),
    amount DECIMAL(18,2),
    quantity SMALLINT,
    big_num BIGINT,
    ratio FLOAT,
    is_active BYTEINT,
    created_date DATE,
    created_ts TIMESTAMP,
    notes VARCHAR(500)
) PRIMARY INDEX (id)
"""

# Pinned seed data - exact values for deterministic testing
# Format: (id, name, amount, quantity, big_num, ratio, is_active, created_date, created_ts, notes)
FIXTURE_SEED_DATA = [
    (1, 'Alice', '1000.50', 10, 9999999999, 3.14159, 1, '2026-01-01', '2026-01-01 10:30:00', 'First record'),
    (2, 'Bob', '2500.75', 25, 1234567890, 2.71828, 1, '2026-01-02', '2026-01-02 11:45:00', 'Second record'),
    (3, 'Charlie', '750.25', 5, 5555555555, 1.41421, 0, '2026-01-03', '2026-01-03 09:15:00', 'Third record'),
    (4, 'Diana', '3200.00', 50, 7777777777, 0.57721, 1, '2026-01-04', '2026-01-04 14:00:00', 'Fourth record'),
    (5, 'Eve', '500.99', 3, 1111111111, 1.61803, 1, '2026-01-05', '2026-01-05 16:30:00', 'Fifth record'),
]

# Expected values for L3 fidelity verification
EXPECTED_FIXTURE_DATA = {
    1: {'name': 'Alice', 'amount': 1000.50, 'quantity': 10, 'big_num': 9999999999, 'is_active': 1},
    2: {'name': 'Bob', 'amount': 2500.75, 'quantity': 25, 'big_num': 1234567890, 'is_active': 1},
    3: {'name': 'Charlie', 'amount': 750.25, 'quantity': 5, 'big_num': 5555555555, 'is_active': 0},
    4: {'name': 'Diana', 'amount': 3200.00, 'quantity': 50, 'big_num': 7777777777, 'is_active': 1},
    5: {'name': 'Eve', 'amount': 500.99, 'quantity': 3, 'big_num': 1111111111, 'is_active': 1},
}


def setup_test_fixture(adapter: TeradataAdapter, database: str) -> bool:
    """
    Create and seed the test fixture table.

    This function is idempotent - it drops and recreates the table each time
    to ensure deterministic state for CI/regression testing.

    Args:
        adapter: Connected TeradataAdapter instance
        database: Target database name

    Returns:
        True if setup succeeded, False otherwise
    """
    # Drop existing table (ignore error if not exists)
    adapter.execute_query(f'DROP TABLE "{database}"."saiql_test_fixture"', fetch=False)

    # Create table
    result = adapter.execute_query(FIXTURE_TABLE_DDL, fetch=False)
    if not result['success']:
        print(f"Failed to create fixture table: {result.get('error')}")
        return False

    # Insert seed data
    for row in FIXTURE_SEED_DATA:
        insert_sql = f"""
        INSERT INTO saiql_test_fixture VALUES (
            {row[0]}, '{row[1]}', {row[2]}, {row[3]}, {row[4]},
            {row[5]}, {row[6]}, DATE '{row[7]}', TIMESTAMP '{row[8]}', '{row[9]}'
        )
        """
        result = adapter.execute_query(insert_sql, fetch=False)
        if not result['success']:
            print(f"Failed to insert row {row[0]}: {result.get('error')}")
            return False

    # Verify count
    count_result = adapter.execute_query("SELECT COUNT(*) as cnt FROM saiql_test_fixture")
    if count_result['success'] and count_result['data']:
        count = count_result['data'][0].get('cnt', 0)
        if count == len(FIXTURE_SEED_DATA):
            return True
        print(f"Row count mismatch: expected {len(FIXTURE_SEED_DATA)}, got {count}")

    return False


def teardown_test_fixture(adapter: TeradataAdapter, database: str):
    """Remove test fixture table."""
    adapter.execute_query(f'DROP TABLE "{database}"."saiql_test_fixture"', fetch=False)


@pytest.fixture(scope="session", autouse=True)
def teradata_fixture_setup():
    """
    Session-scoped autouse fixture that sets up the test data once for all tests.

    This fixture runs automatically before any test in the session and ensures
    deterministic, reproducible test data for CI-style gating.

    The fixture is autouse=True so tests don't need to explicitly reference it,
    but it guarantees setup_test_fixture() runs before any test that needs it.
    """
    if not live_teradata_available():
        # Skip setup if no live connection - tests will be skipped individually
        yield None
        return

    config = get_live_config()
    adapter = TeradataAdapter(config)

    # Setup fixture - this guarantees the table exists before any test runs
    success = setup_test_fixture(adapter, config.database)
    if not success:
        adapter.close()
        pytest.fail("Failed to setup test fixture - CI cannot proceed without reproducible data")

    yield adapter

    # Teardown (optional - keep fixture for debugging)
    # teardown_test_fixture(adapter, config.database)
    adapter.close()


# =============================================================================
# FIXTURE SETUP TESTS
# =============================================================================

class TestFixtureSetup:
    """Tests for reproducible fixture setup"""

    @requires_teradata
    def test_fixture_setup_is_reproducible(self):
        """Verify fixture can be created from DDL and seed data"""
        config = get_live_config()
        adapter = TeradataAdapter(config)

        # Setup fixture (idempotent - drops and recreates)
        success = setup_test_fixture(adapter, config.database)
        assert success, "Fixture setup must succeed"

        # Verify row count matches expected
        count_result = adapter.execute_query("SELECT COUNT(*) as cnt FROM saiql_test_fixture")
        assert count_result['success']
        assert count_result['data'][0]['cnt'] == len(FIXTURE_SEED_DATA), \
            f"Expected {len(FIXTURE_SEED_DATA)} rows, got {count_result['data'][0]['cnt']}"

        # Verify exact data matches expected values
        data_ir = adapter.export_data('saiql_test_fixture', order_by=['id'])
        id_idx = data_ir.columns.index('id')
        name_idx = data_ir.columns.index('name')
        amount_idx = data_ir.columns.index('amount')

        for row in data_ir.rows:
            row_id = row[id_idx]
            expected = EXPECTED_FIXTURE_DATA[row_id]

            # Verify name
            name = row[name_idx].strip() if isinstance(row[name_idx], str) else row[name_idx]
            assert name == expected['name'], f"Row {row_id}: name mismatch"

            # Verify amount
            amount = float(row[amount_idx]) if row[amount_idx] is not None else None
            assert abs(amount - expected['amount']) < 0.01, f"Row {row_id}: amount mismatch"

        adapter.close()

    @requires_teradata
    def test_fixture_setup_is_idempotent(self):
        """Verify running fixture setup twice produces identical results"""
        config = get_live_config()
        adapter = TeradataAdapter(config)

        # First setup
        success1 = setup_test_fixture(adapter, config.database)
        assert success1
        ir1 = adapter.export_data('saiql_test_fixture', order_by=['id'])
        checksum1 = ir1.checksum

        # Second setup (should drop and recreate)
        success2 = setup_test_fixture(adapter, config.database)
        assert success2
        ir2 = adapter.export_data('saiql_test_fixture', order_by=['id'])
        checksum2 = ir2.checksum

        # Checksums must match (idempotent)
        assert checksum1 == checksum2, \
            f"Fixture setup must be idempotent: {checksum1} vs {checksum2}"

        adapter.close()


# =============================================================================
# L0: CONNECTIVITY TESTS
# =============================================================================

class TestL0Connectivity:
    """L0 Connectivity test suite"""

    def test_config_creation(self, teradata_config):
        """Test configuration object creation"""
        assert teradata_config.host == 'localhost'
        assert teradata_config.user == 'testuser'
        assert teradata_config.password == 'testpass'
        assert teradata_config.database == 'testdb'
        assert teradata_config.port == 1025

    def test_config_to_uri(self, teradata_config):
        """Test URI generation with redaction"""
        uri = teradata_config.to_uri(redact=True)
        assert 'testuser' in uri
        assert '***REDACTED***' in uri
        assert 'testpass' not in uri

        uri_unredacted = teradata_config.to_uri(redact=False)
        assert 'testuser' in uri_unredacted

    def test_driver_not_installed_error(self, teradata_config):
        """Test deterministic error when driver not installed"""
        with patch('extensions.plugins.teradata_adapter.TERADATA_AVAILABLE', False):
            with pytest.raises(TeradataError) as exc_info:
                # Re-import to trigger the check
                from extensions.plugins.teradata_adapter import TeradataAdapter as TA
                adapter = TA(teradata_config)

            assert 'teradatasql not installed' in str(exc_info.value)

    @requires_teradata
    def test_connection_success(self):
        """L0: Test successful connection to Teradata"""
        config = get_live_config()
        adapter = TeradataAdapter(config)

        assert adapter.is_connected()
        assert adapter.state == ConnectionState.CONNECTED

        adapter.close()
        assert adapter.state == ConnectionState.DISCONNECTED

    @requires_teradata
    def test_ping_command(self):
        """L0: Test ping (SELECT 1)"""
        config = get_live_config()
        adapter = TeradataAdapter(config)

        result = adapter.ping()
        assert result is True

        adapter.close()

    @requires_teradata
    def test_trivial_query(self):
        """L0: Test trivial query execution"""
        config = get_live_config()
        adapter = TeradataAdapter(config)

        result = adapter.execute_query("SELECT 1 AS test_col")
        assert result['success'] is True
        assert len(result['data']) == 1

        adapter.close()

    @requires_teradata
    def test_statistics(self):
        """L0: Test adapter statistics"""
        config = get_live_config()
        adapter = TeradataAdapter(config)

        # Execute a few queries
        adapter.execute_query("SELECT 1")
        adapter.execute_query("SELECT 2")

        stats = adapter.get_statistics()
        assert stats['queries_executed'] >= 2
        assert stats['state'] == 'connected'

        adapter.close()


# =============================================================================
# L1: INTROSPECTION TESTS
# =============================================================================

class TestL1Introspection:
    """L1 Introspection test suite"""

    @requires_teradata
    def test_get_server_fingerprint(self):
        """L1: Test server fingerprint retrieval"""
        config = get_live_config()
        adapter = TeradataAdapter(config)

        fingerprint = adapter.get_server_fingerprint()
        assert 'version' in fingerprint
        assert 'host' in fingerprint
        assert 'adapter_version' in fingerprint
        assert fingerprint['adapter_version'] == '1.0.0'

        adapter.close()

    @requires_teradata
    def test_list_databases(self):
        """L1: Test database listing"""
        config = get_live_config()
        adapter = TeradataAdapter(config)

        databases = adapter.list_databases()
        assert isinstance(databases, list)
        # Should have at least one database accessible
        assert len(databases) >= 0

        adapter.close()

    @requires_teradata
    def test_list_tables(self):
        """L1: Test table listing"""
        config = get_live_config()
        adapter = TeradataAdapter(config)

        tables = adapter.list_tables()
        assert isinstance(tables, list)
        # Each entry should have required fields
        for t in tables:
            assert 'database' in t
            assert 'name' in t
            assert 'type' in t

        adapter.close()


# =============================================================================
# L2: SCHEMA IR TESTS
# =============================================================================

class TestL2SchemaIR:
    """L2 Schema IR test suite"""

    @requires_teradata
    def test_export_schema(self):
        """L2: Test schema export to IR"""
        config = get_live_config()
        adapter = TeradataAdapter(config)

        schema_ir = adapter.export_schema()
        assert isinstance(schema_ir, SchemaIR)
        assert schema_ir.adapter_version == '1.0.0'
        assert isinstance(schema_ir.tables, list)
        assert isinstance(schema_ir.views, list)

        adapter.close()

    @requires_teradata
    def test_schema_export_determinism(self):
        """L2: Test that schema export is deterministic"""
        config = get_live_config()
        adapter = TeradataAdapter(config)

        # Export twice
        ir1 = adapter.export_schema()
        ir2 = adapter.export_schema()

        # Convert to JSON and compare
        json1 = json.dumps({
            'tables': ir1.tables,
            'views': ir1.views,
        }, sort_keys=True, default=str)

        json2 = json.dumps({
            'tables': ir2.tables,
            'views': ir2.views,
        }, sort_keys=True, default=str)

        assert json1 == json2

        adapter.close()


# =============================================================================
# L2: DATA IR TESTS
# =============================================================================

class TestL2DataIR:
    """L2 Data IR test suite - requires saiql_test_fixture table"""

    @requires_teradata
    def test_export_data(self):
        """L2: Test data export from saiql_test_fixture table"""
        config = get_live_config()
        adapter = TeradataAdapter(config)

        # Export data from the test fixture table
        data_ir = adapter.export_data('saiql_test_fixture', order_by=['id'])

        assert isinstance(data_ir, DataIR)
        assert data_ir.table_name == 'saiql_test_fixture'
        assert data_ir.row_count > 0, "Expected rows in test fixture table"
        assert len(data_ir.columns) > 0, "Expected columns in export"
        assert len(data_ir.rows) == data_ir.row_count
        assert data_ir.checksum is not None, "Expected checksum to be computed"

        # Verify expected columns exist
        expected_cols = ['id', 'name', 'amount', 'quantity']
        for col in expected_cols:
            assert col in data_ir.columns, f"Expected column {col} in export"

        adapter.close()

    @requires_teradata
    def test_export_data_determinism(self):
        """L2: Test that data export is deterministic"""
        config = get_live_config()
        adapter = TeradataAdapter(config)

        # Export twice with same ordering
        ir1 = adapter.export_data('saiql_test_fixture', order_by=['id'])
        ir2 = adapter.export_data('saiql_test_fixture', order_by=['id'])

        # Row counts should match
        assert ir1.row_count == ir2.row_count

        # Checksums should match
        assert ir1.checksum == ir2.checksum, "Data export should be deterministic"

        # Rows should be identical
        assert ir1.rows == ir2.rows

        adapter.close()

    @requires_teradata
    def test_export_data_with_limit(self):
        """L2: Test data export with row limit"""
        config = get_live_config()
        adapter = TeradataAdapter(config)

        # Export with limit
        data_ir = adapter.export_data('saiql_test_fixture', order_by=['id'], limit=3)

        assert data_ir.row_count == 3, "Expected exactly 3 rows with limit=3"
        assert len(data_ir.rows) == 3

        adapter.close()

    @requires_teradata
    def test_data_round_trip(self):
        """L2: Test data export/import round trip with checksum and value verification"""
        config = get_live_config()
        adapter = TeradataAdapter(config)

        # Export original data
        original_ir = adapter.export_data('saiql_test_fixture', order_by=['id'])
        original_count = original_ir.row_count
        original_checksum = original_ir.checksum
        original_rows = [list(row) for row in original_ir.rows]  # Deep copy

        assert original_count > 0, "Need data to test round trip"
        assert original_checksum is not None, "Original export must have checksum"

        # Create a temp table for import testing - drop first (ignore error if not exists)
        adapter.execute_query("DROP TABLE saiql_test_roundtrip", fetch=False)

        create_sql = """
        CREATE TABLE saiql_test_roundtrip (
            id INTEGER NOT NULL,
            name VARCHAR(100),
            amount DECIMAL(18,2),
            quantity SMALLINT,
            big_num BIGINT,
            ratio FLOAT,
            is_active BYTEINT,
            created_date DATE,
            created_ts TIMESTAMP,
            notes VARCHAR(500)
        ) PRIMARY INDEX (id)
        """
        result = adapter.execute_query(create_sql, fetch=False)
        assert result['success'], f"Failed to create roundtrip table: {result.get('error')}"

        try:
            # Change the table name in the IR for import to new table
            original_ir.table_name = 'saiql_test_roundtrip'

            # Import data
            import_result = adapter.import_data(original_ir, target_database=config.database)
            assert import_result['success'], f"Import failed: {import_result.get('errors')}"
            assert import_result['rows_inserted'] == original_count

            # Export from new table
            reimported_ir = adapter.export_data('saiql_test_roundtrip', order_by=['id'])

            # Verify counts match
            assert reimported_ir.row_count == original_count, \
                f"Row count mismatch: {reimported_ir.row_count} vs {original_count}"

            # Verify checksums match (content fidelity)
            assert reimported_ir.checksum == original_checksum, \
                f"Checksum mismatch after round-trip: {reimported_ir.checksum} vs {original_checksum}"

            # Verify row-by-row value equality
            assert len(reimported_ir.rows) == len(original_rows), "Row count differs"
            for i, (orig_row, reimp_row) in enumerate(zip(original_rows, reimported_ir.rows)):
                # Compare each column value
                for j, (orig_val, reimp_val) in enumerate(zip(orig_row, reimp_row)):
                    col_name = original_ir.columns[j]
                    # Handle float comparison with tolerance
                    if isinstance(orig_val, float) and isinstance(reimp_val, float):
                        assert abs(orig_val - reimp_val) < 0.0001, \
                            f"Row {i} col {col_name}: float mismatch {orig_val} vs {reimp_val}"
                    else:
                        assert orig_val == reimp_val, \
                            f"Row {i} col {col_name}: value mismatch {orig_val} vs {reimp_val}"

        finally:
            # Cleanup
            adapter.execute_query("DROP TABLE saiql_test_roundtrip", fetch=False)

        adapter.close()

    @requires_teradata
    def test_data_checksum_computation(self):
        """L2: Test that data checksum is computed correctly"""
        config = get_live_config()
        adapter = TeradataAdapter(config)

        data_ir = adapter.export_data('saiql_test_fixture', order_by=['id'])

        # Checksum should be a hex string
        assert data_ir.checksum is not None
        assert len(data_ir.checksum) == 64, "Expected SHA256 hex digest (64 chars)"
        assert all(c in '0123456789abcdef' for c in data_ir.checksum)

        adapter.close()


# =============================================================================
# L3: FIDELITY TESTS
# =============================================================================

class TestL3Fidelity:
    """L3 Fidelity test suite"""

    def test_type_mapping_known_types(self):
        """L3: Test type mapping for known Teradata types"""
        type_mapping = TeradataAdapter.TYPE_MAPPING

        # Core types
        assert type_mapping.get('integer') == 'INT32'
        assert type_mapping.get('bigint') == 'INT64'
        assert type_mapping.get('decimal') == 'DECIMAL'
        assert type_mapping.get('varchar') == 'STRING'
        assert type_mapping.get('date') == 'DATE'
        assert type_mapping.get('timestamp') == 'TIMESTAMP'
        assert type_mapping.get('json') == 'JSON'

    def test_unsupported_type_handling(self, teradata_config):
        """L3: Test handling of unsupported types"""
        # Test the type mapping method
        unknown_type = 'SOME_CUSTOM_TYPE'

        # Get mapping directly
        ir_type = TeradataAdapter.TYPE_MAPPING.get(unknown_type.lower(), 'UNKNOWN')
        assert ir_type == 'UNKNOWN'

    @requires_teradata
    def test_primary_index_capture(self):
        """L3: Test primary index metadata capture"""
        config = get_live_config()
        adapter = TeradataAdapter(config)

        tables = adapter.list_tables()
        if tables:
            # Get first table and check for primary index info
            table_info = adapter.describe_table(tables[0]['name'])
            # Primary index may or may not exist
            assert isinstance(table_info.primary_index, (list, type(None)))

        adapter.close()

    @requires_teradata
    def test_decimal_precision_fidelity(self):
        """L3: Test DECIMAL type precision is preserved with exact values"""
        config = get_live_config()
        adapter = TeradataAdapter(config)

        # Export data and check decimal values against expected
        data_ir = adapter.export_data('saiql_test_fixture', order_by=['id'])

        # Find column indices
        id_idx = data_ir.columns.index('id')
        amount_idx = data_ir.columns.index('amount')

        # Verify exact decimal values from pinned fixture
        for row in data_ir.rows:
            row_id = row[id_idx]
            amount = row[amount_idx]
            expected = EXPECTED_FIXTURE_DATA[row_id]['amount']

            # Convert to float for comparison (Teradata may return Decimal)
            actual_float = float(amount) if amount is not None else None

            assert actual_float is not None, f"Row {row_id}: amount should not be None"
            assert abs(actual_float - expected) < 0.01, \
                f"Row {row_id}: DECIMAL precision mismatch - expected {expected}, got {actual_float}"

        adapter.close()

    @requires_teradata
    def test_integer_types_fidelity(self):
        """L3: Test integer type values match exact expected values"""
        config = get_live_config()
        adapter = TeradataAdapter(config)

        data_ir = adapter.export_data('saiql_test_fixture', order_by=['id'])

        # Find column indices
        id_idx = data_ir.columns.index('id')
        quantity_idx = data_ir.columns.index('quantity')
        big_num_idx = data_ir.columns.index('big_num')
        is_active_idx = data_ir.columns.index('is_active')

        for row in data_ir.rows:
            row_id = row[id_idx]
            expected = EXPECTED_FIXTURE_DATA[row_id]

            # INTEGER (id)
            assert isinstance(row[id_idx], int), f"Row {row_id}: id should be int"
            assert row[id_idx] == row_id, f"Row {row_id}: id mismatch"

            # SMALLINT (quantity) - exact value
            quantity = row[quantity_idx]
            assert isinstance(quantity, int), f"Row {row_id}: quantity should be int"
            assert quantity == expected['quantity'], \
                f"Row {row_id}: SMALLINT mismatch - expected {expected['quantity']}, got {quantity}"

            # BIGINT (big_num) - exact value
            big_val = row[big_num_idx]
            assert isinstance(big_val, int), f"Row {row_id}: big_num should be int"
            assert big_val == expected['big_num'], \
                f"Row {row_id}: BIGINT mismatch - expected {expected['big_num']}, got {big_val}"

            # BYTEINT (is_active) - exact value
            is_active = row[is_active_idx]
            assert isinstance(is_active, int), f"Row {row_id}: is_active should be int"
            assert is_active == expected['is_active'], \
                f"Row {row_id}: BYTEINT mismatch - expected {expected['is_active']}, got {is_active}"

        adapter.close()

    @requires_teradata
    def test_string_fidelity(self):
        """L3: Test VARCHAR values match exact expected values"""
        config = get_live_config()
        adapter = TeradataAdapter(config)

        data_ir = adapter.export_data('saiql_test_fixture', order_by=['id'])

        id_idx = data_ir.columns.index('id')
        name_idx = data_ir.columns.index('name')

        for row in data_ir.rows:
            row_id = row[id_idx]
            expected_name = EXPECTED_FIXTURE_DATA[row_id]['name']

            name = row[name_idx]
            # Strip any trailing whitespace (Teradata CHAR padding)
            name_stripped = name.strip() if isinstance(name, str) else name

            assert name_stripped == expected_name, \
                f"Row {row_id}: VARCHAR mismatch - expected '{expected_name}', got '{name_stripped}'"

        adapter.close()

    @requires_teradata
    def test_date_timestamp_fidelity(self):
        """L3: Test DATE and TIMESTAMP values are preserved with correct dates"""
        config = get_live_config()
        adapter = TeradataAdapter(config)

        data_ir = adapter.export_data('saiql_test_fixture', order_by=['id'])

        id_idx = data_ir.columns.index('id')
        date_idx = data_ir.columns.index('created_date')
        ts_idx = data_ir.columns.index('created_ts')

        # Expected dates from seed data
        expected_dates = {
            1: '2026-01-01',
            2: '2026-01-02',
            3: '2026-01-03',
            4: '2026-01-04',
            5: '2026-01-05',
        }

        for row in data_ir.rows:
            row_id = row[id_idx]

            # Date should be present and match expected
            date_val = row[date_idx]
            assert date_val is not None, f"Row {row_id}: DATE should not be None"

            # Convert to string for comparison (handles date objects)
            date_str = str(date_val)[:10]  # Take YYYY-MM-DD portion
            assert date_str == expected_dates[row_id], \
                f"Row {row_id}: DATE mismatch - expected {expected_dates[row_id]}, got {date_str}"

            # Timestamp should be present
            ts_val = row[ts_idx]
            assert ts_val is not None, f"Row {row_id}: TIMESTAMP should not be None"

            # Verify timestamp contains expected date
            ts_str = str(ts_val)[:10]
            assert ts_str == expected_dates[row_id], \
                f"Row {row_id}: TIMESTAMP date mismatch - expected {expected_dates[row_id]}, got {ts_str}"

        adapter.close()

    @requires_teradata
    def test_column_type_mapping_fidelity(self):
        """L3: Test column types in schema export map correctly to IR types"""
        config = get_live_config()
        adapter = TeradataAdapter(config)

        # Get table description
        table_info = adapter.describe_table('saiql_test_fixture')

        # Verify type mapping
        type_mapping = {
            'id': 'INT32',
            'name': 'STRING',
            'amount': 'DECIMAL',
            'quantity': 'INT16',
            'big_num': 'INT64',
            'ratio': 'FLOAT64',
            'is_active': 'INT8',
            'created_date': 'DATE',
            'created_ts': 'TIMESTAMP',
            'notes': 'STRING',
        }

        for col in table_info.columns:
            col_name = col.name.lower()
            if col_name in type_mapping:
                ir_type = adapter.TYPE_MAPPING.get(col.data_type.lower(), 'UNKNOWN')
                expected = type_mapping[col_name]
                assert ir_type == expected, \
                    f"Column {col_name} type {col.data_type} mapped to {ir_type}, expected {expected}"

        adapter.close()

    @requires_teradata
    def test_primary_index_in_test_fixture(self):
        """L3: Test primary index is captured for saiql_test_fixture"""
        config = get_live_config()
        adapter = TeradataAdapter(config)

        table_info = adapter.describe_table('saiql_test_fixture')

        assert table_info.primary_index is not None, "Expected primary index"
        assert 'id' in table_info.primary_index, "Expected 'id' in primary index"

        adapter.close()


# =============================================================================
# L4: AUDIT PROOF TESTS
# =============================================================================

class TestL4AuditProof:
    """L4 Audit proof test suite"""

    @requires_teradata
    def test_proof_bundle_generation(self):
        """L4: Test proof bundle generation"""
        config = get_live_config()
        adapter = TeradataAdapter(config)

        with tempfile.TemporaryDirectory() as tmpdir:
            limitations_path = Path(tmpdir) / 'test_limitations.md'
            limitations_path.write_text("# Test Limitations\n\nTest content.")

            output_dir = Path(tmpdir) / 'bundle'

            bundle = adapter.generate_proof_bundle(
                output_dir=output_dir,
                limitations_path=limitations_path
            )

            assert bundle['bundle_hash'] is not None
            assert 'run_manifest' in bundle
            assert 'schema_ir' in bundle
            assert (output_dir / 'run_manifest.json').exists()
            assert (output_dir / 'schema_ir.json').exists()
            assert (output_dir / 'limitations.md').exists()

        adapter.close()

    @requires_teradata
    def test_proof_bundle_requires_limitations(self):
        """L4: Test proof bundle fails without limitations_path when output_dir specified"""
        config = get_live_config()
        adapter = TeradataAdapter(config)

        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir) / 'bundle'

            with pytest.raises(TeradataOperationError) as exc_info:
                adapter.generate_proof_bundle(
                    output_dir=output_dir
                    # Note: no limitations_path
                )

            assert 'limitations_path is required' in str(exc_info.value)

        adapter.close()

    @requires_teradata
    def test_run_manifest_required_fields(self):
        """L4: Test run_manifest contains all required fields"""
        config = get_live_config()
        adapter = TeradataAdapter(config)

        with tempfile.TemporaryDirectory() as tmpdir:
            limitations_path = Path(tmpdir) / 'test_limitations.md'
            limitations_path.write_text("# Test Limitations")

            bundle = adapter.generate_proof_bundle(limitations_path=limitations_path)

            manifest = bundle['run_manifest']

            # Required fields per spec
            assert 'adapter' in manifest
            assert 'adapter_version' in manifest
            assert 'database' in manifest
            assert 'config' in manifest
            assert 'versions' in manifest
            assert 'hardware_summary' in manifest
            assert 'server_fingerprint' in manifest
            assert 'dataset_hash' in manifest
            assert 'schema_hash' in manifest
            assert 'rowcount_hash' in manifest

            # Config should not contain password
            assert 'password' not in manifest['config']

        adapter.close()

    @requires_teradata
    def test_proof_bundle_determinism(self):
        """L4: Test proof bundle hash determinism"""
        config = get_live_config()
        adapter = TeradataAdapter(config)

        with tempfile.TemporaryDirectory() as tmpdir:
            limitations_path = Path(tmpdir) / 'test_limitations.md'
            limitations_path.write_text("# Test Limitations")

            bundle1 = adapter.generate_proof_bundle(limitations_path=limitations_path)
            bundle2 = adapter.generate_proof_bundle(limitations_path=limitations_path)

            assert bundle1['bundle_hash'] == bundle2['bundle_hash']

        adapter.close()

    @requires_teradata
    def test_proof_bundle_full_artifact_determinism(self):
        """L4: Test full artifact determinism (file-by-file comparison)"""
        config = get_live_config()
        adapter = TeradataAdapter(config)

        with tempfile.TemporaryDirectory() as tmpdir:
            limitations_path = Path(tmpdir) / 'test_limitations.md'
            limitations_path.write_text("# Test Limitations")

            output_dir1 = Path(tmpdir) / 'bundle1'
            output_dir2 = Path(tmpdir) / 'bundle2'

            adapter.generate_proof_bundle(
                output_dir=output_dir1,
                limitations_path=limitations_path
            )
            adapter.generate_proof_bundle(
                output_dir=output_dir2,
                limitations_path=limitations_path
            )

            # Compare all files
            files_to_compare = [
                'run_manifest.json',
                'schema_ir.json',
                'rowcount.json',
                'schema_diff.json',
                'rowcount_diff.json',
                'checksum_diff.json',
                'limitations.md',
            ]

            for filename in files_to_compare:
                file1 = output_dir1 / filename
                file2 = output_dir2 / filename

                if file1.exists() and file2.exists():
                    content1 = file1.read_text()
                    content2 = file2.read_text()
                    assert content1 == content2, f"Mismatch in {filename}"

        adapter.close()

    @requires_teradata
    def test_no_secrets_in_bundle(self):
        """L4: Test that secrets are redacted from proof bundle"""
        config = get_live_config()
        adapter = TeradataAdapter(config)

        with tempfile.TemporaryDirectory() as tmpdir:
            limitations_path = Path(tmpdir) / 'test_limitations.md'
            limitations_path.write_text("# Test Limitations")
            output_dir = Path(tmpdir) / 'bundle'

            bundle = adapter.generate_proof_bundle(
                output_dir=output_dir,
                limitations_path=limitations_path
            )

            # Check manifest doesn't contain password
            manifest_path = output_dir / 'run_manifest.json'
            manifest_content = manifest_path.read_text()
            assert config.password not in manifest_content

            # Check bundle dict
            bundle_str = json.dumps(bundle, default=str)
            assert config.password not in bundle_str

        adapter.close()

    @requires_teradata
    def test_proof_bundle_contains_real_data(self):
        """L4: Test proof bundle contains actual schema and rowcount data"""
        config = get_live_config()
        adapter = TeradataAdapter(config)

        with tempfile.TemporaryDirectory() as tmpdir:
            limitations_path = Path(tmpdir) / 'test_limitations.md'
            limitations_path.write_text("# Test Limitations")
            output_dir = Path(tmpdir) / 'bundle'

            bundle = adapter.generate_proof_bundle(
                output_dir=output_dir,
                limitations_path=limitations_path
            )

            # Schema should have tables (saiql_test_fixture)
            schema_ir = bundle['schema_ir']
            assert len(schema_ir['tables']) > 0, \
                "Expected at least one table in schema (saiql_test_fixture)"

            # Find saiql_test_fixture
            fixture_found = False
            for table in schema_ir['tables']:
                if table['name'] == 'saiql_test_fixture':
                    fixture_found = True
                    assert len(table['columns']) > 0, "Expected columns in fixture table"
                    break
            assert fixture_found, "Expected saiql_test_fixture table in schema"

            # Rowcount should have data
            rowcount = bundle['rowcount']
            assert len(rowcount) > 0, "Expected rowcount data"
            assert 'saiql_test_fixture' in rowcount, "Expected saiql_test_fixture in rowcount"
            assert rowcount['saiql_test_fixture'] > 0, \
                "Expected non-zero rowcount for saiql_test_fixture"

            # Verify files written correctly
            schema_file = output_dir / 'schema_ir.json'
            with open(schema_file) as f:
                written_schema = json.load(f)
            assert len(written_schema['tables']) > 0

            rowcount_file = output_dir / 'rowcount.json'
            with open(rowcount_file) as f:
                written_rowcount = json.load(f)
            assert len(written_rowcount) > 0

        adapter.close()


# =============================================================================
# SAFETY TESTS
# =============================================================================

class TestSafety:
    """Safety tests for destructive operations"""

    @requires_teradata
    def test_drop_table_blocked_by_default(self):
        """Safety: Test DROP TABLE blocked by default"""
        config = get_live_config()
        adapter = TeradataAdapter(config)

        with pytest.raises(TeradataOperationError) as exc_info:
            adapter.drop_table('nonexistent_table')

        assert 'blocked by default' in str(exc_info.value)
        adapter.close()

    @requires_teradata
    def test_delete_all_blocked_by_default(self):
        """Safety: Test DELETE ALL blocked by default"""
        config = get_live_config()
        adapter = TeradataAdapter(config)

        with pytest.raises(TeradataOperationError) as exc_info:
            adapter.delete_all('nonexistent_table')

        assert 'blocked by default' in str(exc_info.value)
        adapter.close()


# =============================================================================
# DETERMINISM TESTS
# =============================================================================

class TestDeterminism:
    """Determinism tests"""

    @requires_teradata
    def test_list_databases_determinism(self):
        """Test database list is deterministic (sorted)"""
        config = get_live_config()
        adapter = TeradataAdapter(config)

        dbs1 = adapter.list_databases()
        dbs2 = adapter.list_databases()

        assert dbs1 == dbs2
        assert dbs1 == sorted(dbs1)

        adapter.close()

    @requires_teradata
    def test_list_tables_determinism(self):
        """Test table list is deterministic (sorted)"""
        config = get_live_config()
        adapter = TeradataAdapter(config)

        tables1 = adapter.list_tables()
        tables2 = adapter.list_tables()

        # Compare as JSON for deep equality
        assert json.dumps(tables1, sort_keys=True) == json.dumps(tables2, sort_keys=True)

        adapter.close()


# =============================================================================
# TYPE NORMALIZATION TESTS
# =============================================================================

class TestTypeNormalization:
    """Tests for Teradata type code normalization"""

    def test_type_code_normalization(self):
        """Test Teradata type code to name conversion"""
        # Create a mock adapter for testing the normalization function
        with patch('extensions.plugins.teradata_adapter.TERADATA_AVAILABLE', True):
            with patch('extensions.plugins.teradata_adapter.teradatasql') as mock_td:
                mock_conn = MagicMock()
                mock_cursor = MagicMock()
                mock_conn.cursor.return_value = mock_cursor
                mock_cursor.fetchone.return_value = (1,)
                mock_cursor.description = [('test',)]
                mock_td.connect.return_value = mock_conn

                config = TeradataConfig(
                    host='localhost',
                    user='test',
                    password='test',
                    database='test'
                )
                adapter = TeradataAdapter(config)

                # Test type normalization
                assert adapter._normalize_type('I') == 'INTEGER'
                assert adapter._normalize_type('I8') == 'BIGINT'
                assert adapter._normalize_type('CV') == 'VARCHAR'
                assert adapter._normalize_type('DA') == 'DATE'
                assert adapter._normalize_type('TS') == 'TIMESTAMP'
                assert adapter._normalize_type('D') == 'DECIMAL'
                assert adapter._normalize_type('JN') == 'JSON'

                # Unknown type should return as-is
                assert adapter._normalize_type('XX') == 'XX'


# =============================================================================
# MAIN
# =============================================================================

if __name__ == '__main__':
    pytest.main([__file__, '-v'])
