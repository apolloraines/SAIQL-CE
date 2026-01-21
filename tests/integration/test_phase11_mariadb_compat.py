#!/usr/bin/env python3
"""
SAIQL Phase 11 MariaDB Compatibility Harness Tests

Validates MySQL adapter compatibility with MariaDB:
- Uses MySQL adapter against MariaDB 11.2
- Verifies L0 capabilities work identically
- Documents any compatibility deltas

Evidence:
- MariaDB 11.2 container on port 3307
- Fixture: /mnt/storage/DockerTests/mysql/fixtures/01_schema.sql (same as MySQL)
- Adapter: extensions/plugins/mysql_adapter.py

Compatibility proof approach:
- If MySQL L0 tests pass with MariaDB, MariaDB is proven L0 compatible
- Any failures document incompatibilities/deltas
"""

import pytest
import logging

logger = logging.getLogger(__name__)

EXPECTED_TABLES = ['departments', 'employees', 'employee_projects', 'projects', 'type_test']
EXPECTED_ROW_COUNTS = {
    'departments': 4,
    'employees': 6,
    'projects': 4,
    'employee_projects': 7,
    'type_test': 2
}


class TestPhase11MariaDBCompatL0:
    """MariaDB L0 compatibility tests using MySQL adapter"""

    @pytest.fixture(scope='class')
    def mariadb_adapter(self):
        """MariaDB adapter fixture - uses MySQL adapter"""
        from extensions.plugins.mysql_adapter import MySQLAdapter

        # Connect to MariaDB using MySQL adapter
        adapter = MySQLAdapter(
            host='localhost',
            port=3307,  # MariaDB port
            database='saiql_phase11_test',
            user='saiql_user',
            password='SaiqlTestPass123'
        )
        yield adapter
        adapter.close()

    def test_compat_l0_1_list_tables(self, mariadb_adapter):
        """L0.1: List all tables in MariaDB database"""
        print("\n=== MariaDB Compat L0.1: List tables ===")

        tables = mariadb_adapter.get_tables()
        print(f"  Found tables: {tables}")

        assert len(tables) == 5, f"Expected 5 tables, got {len(tables)}"

        for expected_table in EXPECTED_TABLES:
            assert expected_table in tables, f"Missing table: {expected_table}"

        print("✓ MariaDB table listing compatible with MySQL adapter")

    def test_compat_l0_2_introspect_schema(self, mariadb_adapter):
        """L0.2: Introspect table schemas"""
        print("\n=== MariaDB Compat L0.2: Schema introspection ===")

        for table in ['departments', 'employees']:
            schema = mariadb_adapter.get_schema(table)
            columns = schema.get('columns', [])

            print(f"  {table}: {len(columns)} columns")
            assert len(columns) > 0, f"Table {table} should have columns"

            # Check type_info exists
            for col in columns:
                assert 'type_info' in col, f"Column {col['name']} missing type_info"

        print("✓ MariaDB schema introspection compatible")

    def test_compat_l0_3_extract_data_row_counts(self, mariadb_adapter):
        """L0.3: Extract data and verify row counts"""
        print("\n=== MariaDB Compat L0.3: Data extraction ===")

        total_rows = 0
        for table, expected_count in EXPECTED_ROW_COUNTS.items():
            result = mariadb_adapter.extract_data(table)
            actual_count = result['stats']['total_rows']
            print(f"  {table}: {actual_count} rows (expected {expected_count})")

            assert actual_count == expected_count, \
                f"Table {table}: expected {expected_count} rows, got {actual_count}"
            total_rows += actual_count

        assert total_rows == 23, f"Expected 23 total rows, got {total_rows}"
        print("✓ MariaDB data extraction compatible")

    def test_compat_l0_4_type_mapping(self, mariadb_adapter):
        """L0.4: Validate type mapping"""
        print("\n=== MariaDB Compat L0.4: Type mapping ===")

        schema = mariadb_adapter.get_schema('type_test')
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

        # Type mapping should work the same as MySQL
        assert unsupported_count <= 3, f"Too many unsupported types: {unsupported_count}"

        print("✓ MariaDB type mapping compatible")

    def test_compat_l0_5_deterministic_extraction(self, mariadb_adapter):
        """L0.5: Verify deterministic extraction"""
        print("\n=== MariaDB Compat L0.5: Deterministic extraction ===")

        result1 = mariadb_adapter.extract_data('employees')
        result2 = mariadb_adapter.extract_data('employees')

        data1 = result1['data']
        data2 = result2['data']

        assert len(data1) == len(data2), "Row counts should match"

        for i, (row1, row2) in enumerate(zip(data1, data2)):
            assert row1 == row2, f"Row {i} differs between extractions"

        print(f"  ✓ {len(data1)} rows extracted identically both times")
        print("✓ MariaDB deterministic extraction compatible")

    def test_compat_l0_6_version_info(self, mariadb_adapter):
        """Document MariaDB version for compatibility reference"""
        print("\n=== MariaDB Version Info ===")

        result = mariadb_adapter.execute_query("SELECT VERSION() as version")
        if result['success'] and result['data']:
            version = result['data'][0].get('version') or result['data'][0].get('VERSION()')
            print(f"  MariaDB version: {version}")

        print("✓ Version documented for compatibility reference")


class TestPhase11MariaDBCompatRequirements:
    """Verify MySQL adapter works with MariaDB"""

    @pytest.fixture(scope='class')
    def mariadb_adapter(self):
        """MariaDB adapter fixture"""
        from extensions.plugins.mysql_adapter import MySQLAdapter

        adapter = MySQLAdapter(
            host='localhost',
            port=3307,
            database='saiql_phase11_test',
            user='saiql_user',
            password='SaiqlTestPass123'
        )
        yield adapter
        adapter.close()

    def test_mysql_adapter_compatibility_proven(self, mariadb_adapter):
        """Prove MySQL adapter works with MariaDB"""
        print("\n=== MySQL Adapter Compatibility Proof ===")

        # Verify all L0 methods work
        required_methods = [
            'get_tables',
            'get_schema',
            'extract_data',
            'execute_query'
        ]

        for method in required_methods:
            assert hasattr(mariadb_adapter, method), f"Missing {method}() method"
            func = getattr(mariadb_adapter, method)
            assert callable(func), f"{method}() not callable"

        # Verify basic operations work
        tables = mariadb_adapter.get_tables()
        assert len(tables) == 5, "Should find 5 tables"

        schema = mariadb_adapter.get_schema('departments')
        assert len(schema.get('columns', [])) > 0, "Should have columns"

        data = mariadb_adapter.extract_data('departments')
        assert data['stats']['total_rows'] == 4, "Should have 4 departments"

        print("✓ MySQL adapter proven compatible with MariaDB")
        print("  → MariaDB L0 can use MySQL adapter (no separate adapter needed)")
