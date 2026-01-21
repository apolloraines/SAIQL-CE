#!/usr/bin/env python3
"""
SAIQL Phase 11 MySQL L0 Harness Tests

Validates MySQL adapter L0 capabilities:
- L0.1: List tables
- L0.2: Introspect schema (columns, types)
- L0.3: Extract data with row count validation
- L0.4: Type mapping via TypeRegistry
- L0.5: Deterministic extraction (repeatability)
- L0.6: Clean state verification

Evidence:
- MySQL 8.0 container on port 3308
- Fixture: /mnt/storage/DockerTests/mysql/fixtures/01_schema.sql
- 5 tables: departments, employees, projects, employee_projects, type_test
- Expected counts: 4, 6, 4, 7, 2 (total 23 rows)
"""

import pytest
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

EXPECTED_COUNTS = {
    'departments': 4,
    'employees': 6,
    'projects': 4,
    'employee_projects': 7,
    'type_test': 2
}
EXPECTED_TOTAL = 23


class TestPhase11MySQLBaseL0:
    """MySQL L0 harness tests - base capabilities"""

    @pytest.fixture(scope='class')
    def mysql_adapter(self):
        """MySQL adapter fixture"""
        from extensions.plugins.mysql_adapter import MySQLAdapter

        adapter = MySQLAdapter(
            host='localhost',
            port=3308,
            database='saiql_phase11_test',
            user='saiql_user',
            password='SaiqlTestPass123'
        )
        yield adapter
        adapter.close()

    def test_l0_1_list_tables(self, mysql_adapter):
        """L0.1: List tables from MySQL test database"""
        print("\n=== L0.1: Listing MySQL tables ===")

        tables = mysql_adapter.get_tables()
        tables_lower = [t.lower() for t in tables]

        print(f"Found {len(tables)} tables: {tables_lower}")

        expected_tables = {'departments', 'employees', 'projects', 'employee_projects', 'type_test'}
        assert expected_tables.issubset(set(tables_lower)), \
            f"Missing tables. Expected: {expected_tables}, Found: {set(tables_lower)}"

        print("✓ All expected tables found")

    def test_l0_2_introspect_departments_schema(self, mysql_adapter):
        """L0.2: Introspect 'departments' table schema"""
        print("\n=== L0.2: Introspecting 'departments' schema ===")

        schema = mysql_adapter.get_schema('departments')

        expected_columns = {'dept_id', 'dept_name', 'dept_code', 'budget', 'created_at'}
        found_columns = {col['name'].lower() for col in schema['columns']}

        print(f"Expected: {expected_columns}")
        print(f"Found: {found_columns}")

        assert expected_columns == found_columns, \
            f"Column mismatch. Missing: {expected_columns - found_columns}"

        for col in schema['columns']:
            assert 'type_info' in col, f"Column {col['name']} missing type_info"

        print("✓ Schema introspection successful")

    def test_l0_3_introspect_employees_schema(self, mysql_adapter):
        """L0.3: Introspect 'employees' table schema"""
        print("\n=== L0.3: Introspecting 'employees' schema ===")

        schema = mysql_adapter.get_schema('employees')

        expected_columns = {
            'emp_id', 'email', 'first_name', 'last_name',
            'dept_id', 'hire_date', 'salary', 'is_active'
        }
        found_columns = {col['name'].lower() for col in schema['columns']}

        print(f"Found {len(found_columns)} columns: {found_columns}")

        assert expected_columns == found_columns, \
            f"Column mismatch. Missing: {expected_columns - found_columns}"

        print("✓ Employees schema introspection successful")

    def test_l0_4_extract_data_row_counts(self, mysql_adapter):
        """L0.4: Extract data and verify row counts"""
        print("\n=== L0.4: Validating row counts ===")

        total_rows = 0
        for table, expected_count in EXPECTED_COUNTS.items():
            result = mysql_adapter.extract_data(table)

            assert 'data' in result, f"Result missing 'data' key for {table}"
            assert 'stats' in result, f"Result missing 'stats' key for {table}"

            actual_count = len(result['data'])
            total_rows += actual_count

            print(f"  {table}: expected {expected_count}, got {actual_count}")

            assert actual_count == expected_count, \
                f"Row count mismatch for {table}: expected {expected_count}, got {actual_count}"

        print(f"\nTotal rows: {total_rows} (expected {EXPECTED_TOTAL})")
        assert total_rows == EXPECTED_TOTAL

        print("✓ All row counts match")

    def test_l0_5_type_mapping_validation(self, mysql_adapter):
        """L0.5: Verify type mapping for type_test table"""
        print("\n=== L0.5: Validating type mapping ===")

        schema = mysql_adapter.get_schema('type_test')

        type_names = {col['type'].lower() for col in schema['columns']}
        print(f"Found types: {type_names}")

        for col in schema['columns']:
            assert col.get('type_info') is not None, \
                f"Column {col['name']} has null type_info"

        print("✓ Type mapping validation successful")

    def test_l0_6_deterministic_extraction(self, mysql_adapter):
        """L0.6: Verify deterministic extraction"""
        print("\n=== L0.6: Testing deterministic extraction ===")

        result1 = mysql_adapter.extract_data('departments')
        result2 = mysql_adapter.extract_data('departments')

        ids1 = [row.get('dept_id') for row in result1['data']]
        ids2 = [row.get('dept_id') for row in result2['data']]

        print(f"Run 1 IDs: {ids1}")
        print(f"Run 2 IDs: {ids2}")

        assert ids1 == ids2, "Row order should be deterministic"

        print("✓ Deterministic extraction verified")


class TestPhase11MySQLRequirements:
    """Verify all L0 requirements met"""

    @pytest.fixture(scope='class')
    def mysql_adapter(self):
        """MySQL adapter fixture"""
        from extensions.plugins.mysql_adapter import MySQLAdapter

        adapter = MySQLAdapter(
            host='localhost',
            port=3308,
            database='saiql_phase11_test',
            user='saiql_user',
            password='SaiqlTestPass123'
        )
        yield adapter
        adapter.close()

    def test_all_l0_requirements_met(self, mysql_adapter):
        """Verify adapter implements all L0 methods"""
        print("\n=== Verifying all L0 requirements ===")

        assert hasattr(mysql_adapter, 'get_tables')
        assert hasattr(mysql_adapter, 'get_schema')
        assert hasattr(mysql_adapter, 'extract_data')

        assert callable(mysql_adapter.get_tables)
        assert callable(mysql_adapter.get_schema)
        assert callable(mysql_adapter.extract_data)

        print("✓ All L0 requirements met")
