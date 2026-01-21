#!/usr/bin/env python3
"""
SAIQL Phase 11 SQLite L0 Harness Tests

Validates SQLite adapter L0 capabilities:
- L0.1: List tables
- L0.2: Schema introspection (columns, types)
- L0.3: Data extraction
- L0.4: Type mapping
- L0.5: Deterministic extraction

Evidence:
- SQLite in-memory database with Phase 11 fixture
- Fixture: /mnt/storage/DockerTests/sqlite/fixtures/01_schema.sql
"""

import pytest
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

FIXTURE_PATH = '/mnt/storage/DockerTests/sqlite/fixtures/01_schema.sql'

EXPECTED_TABLES = ['departments', 'employees', 'employee_projects', 'projects', 'type_test']
EXPECTED_ROW_COUNTS = {
    'departments': 4,
    'employees': 6,
    'projects': 4,
    'employee_projects': 7,
    'type_test': 2
}


class TestPhase11SQLiteBaseL0:
    """SQLite L0 harness tests"""

    @pytest.fixture(scope='class')
    def sqlite_adapter(self):
        """SQLite adapter fixture - fresh database for each class"""
        from extensions.plugins.sqlite_adapter import SQLiteAdapter

        # Create in-memory database
        adapter = SQLiteAdapter(database=':memory:')

        # Load the fixture
        fixture_sql = Path(FIXTURE_PATH).read_text()
        result = adapter.execute_script(fixture_sql)
        assert result['success'], f"Failed to load fixture: {result.get('error')}"

        yield adapter
        adapter.close()

    def test_l0_1_list_tables(self, sqlite_adapter):
        """L0.1: List all tables in database"""
        print("\n=== L0.1: List tables ===")

        tables = sqlite_adapter.get_tables()
        print(f"  Found tables: {tables}")

        assert len(tables) == 5, f"Expected 5 tables, got {len(tables)}"

        for expected_table in EXPECTED_TABLES:
            assert expected_table in tables, f"Missing table: {expected_table}"

        print("✓ All expected tables present")

    def test_l0_2_introspect_departments_schema(self, sqlite_adapter):
        """L0.2a: Introspect departments table schema"""
        print("\n=== L0.2a: departments schema ===")

        schema = sqlite_adapter.get_schema('departments')
        columns = schema.get('columns', [])

        print(f"  Columns: {[c['name'] for c in columns]}")

        expected_cols = ['dept_id', 'dept_name', 'dept_code', 'budget', 'created_at']
        actual_cols = [c['name'] for c in columns]

        for col in expected_cols:
            assert col in actual_cols, f"Missing column: {col}"

        print("✓ departments schema validated")

    def test_l0_3_introspect_employees_schema(self, sqlite_adapter):
        """L0.2b: Introspect employees table schema"""
        print("\n=== L0.2b: employees schema ===")

        schema = sqlite_adapter.get_schema('employees')
        columns = schema.get('columns', [])

        print(f"  Columns: {[c['name'] for c in columns]}")

        expected_cols = ['emp_id', 'email', 'first_name', 'last_name', 'dept_id', 'hire_date', 'salary', 'is_active']
        actual_cols = [c['name'] for c in columns]

        for col in expected_cols:
            assert col in actual_cols, f"Missing column: {col}"

        # Check type info exists
        for col in columns:
            assert 'type_info' in col, f"Column {col['name']} missing type_info"

        print("✓ employees schema validated")

    def test_l0_4_extract_data_row_counts(self, sqlite_adapter):
        """L0.3: Extract data and verify row counts"""
        print("\n=== L0.3: Data extraction row counts ===")

        total_rows = 0
        for table, expected_count in EXPECTED_ROW_COUNTS.items():
            result = sqlite_adapter.extract_data(table)
            actual_count = result['stats']['total_rows']
            print(f"  {table}: {actual_count} rows (expected {expected_count})")

            assert actual_count == expected_count, \
                f"Table {table}: expected {expected_count} rows, got {actual_count}"
            total_rows += actual_count

        assert total_rows == 23, f"Expected 23 total rows, got {total_rows}"
        print("✓ All row counts match")

    def test_l0_5_type_mapping_validation(self, sqlite_adapter):
        """L0.4: Validate type mapping for type_test table"""
        print("\n=== L0.4: Type mapping validation ===")

        schema = sqlite_adapter.get_schema('type_test')
        columns = schema.get('columns', [])

        print("  Column type mappings:")
        unsupported_count = 0
        for col in columns:
            type_info = col.get('type_info')
            ir_type = type_info.ir_type.name if type_info else 'NO_TYPE_INFO'
            native_type = col.get('type', 'unknown')
            is_unsupported = col.get('unsupported', False)

            status = " [UNSUPPORTED]" if is_unsupported else ""
            print(f"    {col['name']}: {native_type} -> {ir_type}{status}")

            if is_unsupported:
                unsupported_count += 1

        # Most types should be supported
        assert unsupported_count <= 3, \
            f"Too many unsupported types: {unsupported_count}"

        print("✓ Type mapping validation complete")

    def test_l0_6_deterministic_extraction(self, sqlite_adapter):
        """L0.5: Verify deterministic extraction ordering"""
        print("\n=== L0.5: Deterministic extraction ===")

        # Extract employees twice
        result1 = sqlite_adapter.extract_data('employees')
        result2 = sqlite_adapter.extract_data('employees')

        data1 = result1['data']
        data2 = result2['data']

        assert len(data1) == len(data2), "Row counts should match"

        # Compare row by row
        for i, (row1, row2) in enumerate(zip(data1, data2)):
            assert row1 == row2, f"Row {i} differs between extractions"

        print(f"  ✓ {len(data1)} rows extracted identically both times")
        print("✓ Deterministic extraction verified")


class TestPhase11SQLiteRequirements:
    """Verify all L0 requirements met"""

    @pytest.fixture(scope='class')
    def sqlite_adapter(self):
        """SQLite adapter fixture"""
        from extensions.plugins.sqlite_adapter import SQLiteAdapter

        adapter = SQLiteAdapter(database=':memory:')
        fixture_sql = Path(FIXTURE_PATH).read_text()
        adapter.execute_script(fixture_sql)

        yield adapter
        adapter.close()

    def test_all_l0_requirements_met(self, sqlite_adapter):
        """Verify adapter implements all L0 methods"""
        print("\n=== Verifying all L0 requirements ===")

        required_methods = [
            'get_tables',
            'get_schema',
            'extract_data',
            'execute_query'
        ]

        for method in required_methods:
            assert hasattr(sqlite_adapter, method), f"Missing {method}() method"
            assert callable(getattr(sqlite_adapter, method)), f"{method}() not callable"

        print("✓ All L0 methods exist")
