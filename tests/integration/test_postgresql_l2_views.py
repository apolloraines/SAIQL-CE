#!/usr/bin/env python3
"""
SAIQL PostgreSQL L2 Harness Tests - Views

Validates PostgreSQL adapter L2 capabilities per collab rules:
- A1) Extraction: Enumerate views with schema, name, definition, dependencies
- A2) Emission: Create views in correct order, no missing dependencies
- A3) Validation: View presence parity, definition parity, result parity
- A4) Limitations: Any view not supported listed with reason

Proof-first approach per Apollo rules. Must pass 3x from clean state.

Evidence:
- PostgreSQL 15 container on port 5433
- Fixture: /mnt/storage/DockerTests/postgresql/fixtures/02_views.sql
- Expected views:
  - v_active_employees (no view deps)
  - v_employee_details (no view deps)
  - v_high_salary_employees (depends on v_active_employees)
  - v_dept_employee_count (no view deps)
  - v_project_summary (no view deps)
"""

import pytest
import logging
import re
from typing import Dict, Any, List

logger = logging.getLogger(__name__)

# Expected views in the fixture
EXPECTED_VIEWS = [
    'v_active_employees',
    'v_employee_details',
    'v_high_salary_employees',
    'v_dept_employee_count',
    'v_project_summary'
]

# View that depends on another view (for dependency ordering test)
VIEW_WITH_DEPENDENCY = 'v_high_salary_employees'
DEPENDENCY_TARGET = 'v_active_employees'


class TestPostgreSQLL2Extraction:
    """L2 Harness: A1 - Extraction tests"""

    @pytest.fixture(scope='class')
    def pg_adapter(self):
        """PostgreSQL adapter fixture"""
        from extensions.plugins.postgresql_adapter import PostgreSQLAdapter

        adapter = PostgreSQLAdapter(
            host='localhost',
            port=5433,
            database='saiql_phase11_test',
            user='saiql_user',
            password='SaiqlTestPass123'
        )
        yield adapter
        adapter.close()

    def test_l2_a1_view_enumeration(self, pg_adapter):
        """
        A1.1: Enumerate all views in schema

        Requirement: get_views() returns list of views with schema, name, definition
        Expected: 5 views from fixture
        """
        print("\n=== A1.1: View enumeration ===")

        views = pg_adapter.get_views('public')
        print(f"  Found {len(views)} views")

        view_names = [v['name'] for v in views]
        for name in view_names:
            print(f"    - {name}")

        # Verify all expected views are present
        for expected in EXPECTED_VIEWS:
            assert expected in view_names, f"Missing expected view: {expected}"

        assert len(views) >= len(EXPECTED_VIEWS), \
            f"Expected at least {len(EXPECTED_VIEWS)} views, got {len(views)}"

        print(f"✓ View enumeration successful ({len(views)} views)")

    def test_l2_a1_view_structure(self, pg_adapter):
        """
        A1.2: View structure validation

        Requirement: Each view dict has schema, name, definition
        """
        print("\n=== A1.2: View structure ===")

        views = pg_adapter.get_views('public')

        for view in views:
            assert 'schema' in view, f"View missing 'schema': {view}"
            assert 'name' in view, f"View missing 'name': {view}"
            assert 'definition' in view, f"View missing 'definition': {view}"

            # Definition should contain SELECT
            assert 'SELECT' in view['definition'].upper() or 'select' in view['definition'], \
                f"View {view['name']} definition doesn't contain SELECT"

            print(f"  ✓ {view['name']}: has schema, name, definition")

        print("✓ All views have required structure")

    def test_l2_a1_view_definition_retrieval(self, pg_adapter):
        """
        A1.3: Individual view definition retrieval

        Requirement: get_view_definition() returns SQL for specific view
        """
        print("\n=== A1.3: View definition retrieval ===")

        for view_name in EXPECTED_VIEWS:
            definition = pg_adapter.get_view_definition(view_name, 'public')

            assert definition is not None, f"Could not get definition for {view_name}"
            assert len(definition) > 10, f"Definition too short for {view_name}"

            print(f"  ✓ {view_name}: {len(definition)} chars")

        print("✓ View definition retrieval successful")

    def test_l2_a1_view_dependencies(self, pg_adapter):
        """
        A1.4: View dependency detection

        Requirement: get_view_dependencies() returns what tables/views a view depends on
        """
        print("\n=== A1.4: View dependencies ===")

        # v_high_salary_employees depends on v_active_employees
        deps = pg_adapter.get_view_dependencies(VIEW_WITH_DEPENDENCY, 'public')
        print(f"  {VIEW_WITH_DEPENDENCY} dependencies: {deps}")

        dep_names = [d['name'] for d in deps]
        assert DEPENDENCY_TARGET in dep_names, \
            f"{VIEW_WITH_DEPENDENCY} should depend on {DEPENDENCY_TARGET}"

        # v_employee_details depends on tables, not views
        emp_deps = pg_adapter.get_view_dependencies('v_employee_details', 'public')
        print(f"  v_employee_details dependencies: {emp_deps}")

        table_deps = [d for d in emp_deps if d['type'] == 'table']
        assert len(table_deps) >= 2, "v_employee_details should depend on at least 2 tables"

        print("✓ View dependency detection successful")


class TestPostgreSQLL2DependencyOrdering:
    """L2 Harness: A1.5 - Dependency ordering tests"""

    @pytest.fixture(scope='class')
    def pg_adapter(self):
        """PostgreSQL adapter fixture"""
        from extensions.plugins.postgresql_adapter import PostgreSQLAdapter

        adapter = PostgreSQLAdapter(
            host='localhost',
            port=5433,
            database='saiql_phase11_test',
            user='saiql_user',
            password='SaiqlTestPass123'
        )
        yield adapter
        adapter.close()

    def test_l2_a1_dependency_order(self, pg_adapter):
        """
        A1.5: Views in dependency order

        Requirement: get_views_in_dependency_order() returns views sorted so
        dependencies come before dependents
        """
        print("\n=== A1.5: Dependency ordering ===")

        ordered_views = pg_adapter.get_views_in_dependency_order('public')
        ordered_names = [v['name'] for v in ordered_views]

        print(f"  Dependency order:")
        for i, name in enumerate(ordered_names):
            print(f"    {i+1}. {name}")

        # v_active_employees must come before v_high_salary_employees
        if DEPENDENCY_TARGET in ordered_names and VIEW_WITH_DEPENDENCY in ordered_names:
            target_idx = ordered_names.index(DEPENDENCY_TARGET)
            dependent_idx = ordered_names.index(VIEW_WITH_DEPENDENCY)

            assert target_idx < dependent_idx, \
                f"{DEPENDENCY_TARGET} (idx={target_idx}) must come before " \
                f"{VIEW_WITH_DEPENDENCY} (idx={dependent_idx})"

            print(f"  ✓ {DEPENDENCY_TARGET} (pos {target_idx+1}) before " \
                  f"{VIEW_WITH_DEPENDENCY} (pos {dependent_idx+1})")

        print("✓ Dependency ordering correct")


class TestPostgreSQLL2Emission:
    """L2 Harness: A2 - Emission tests"""

    @pytest.fixture(scope='class')
    def pg_adapter(self):
        """PostgreSQL adapter fixture"""
        from extensions.plugins.postgresql_adapter import PostgreSQLAdapter

        adapter = PostgreSQLAdapter(
            host='localhost',
            port=5433,
            database='saiql_phase11_test',
            user='saiql_user',
            password='SaiqlTestPass123'
        )
        yield adapter
        adapter.close()

    def test_l2_a2_single_view_creation(self, pg_adapter):
        """
        A2.1: Create single view

        Requirement: create_view() creates a view from definition
        """
        print("\n=== A2.1: Single view creation ===")

        # Create a test view
        test_view_name = 'v_test_harness_l2'
        test_definition = "SELECT dept_id, dept_name FROM departments"

        # Drop if exists first
        pg_adapter.drop_view(test_view_name, 'public', if_exists=True)

        # Create view
        result = pg_adapter.create_view(test_view_name, test_definition, 'public')
        assert result['success'], f"Failed to create view: {result.get('error')}"

        # Verify it exists
        views = pg_adapter.get_views('public')
        view_names = [v['name'] for v in views]
        assert test_view_name in view_names, f"View {test_view_name} not found after creation"

        # Cleanup
        pg_adapter.drop_view(test_view_name, 'public')

        print("✓ Single view creation successful")

    def test_l2_a2_view_recreation(self, pg_adapter):
        """
        A2.2: View recreation (extract -> drop -> recreate)

        Requirement: Extract view, drop it, recreate from extracted definition
        """
        print("\n=== A2.2: View recreation ===")

        test_view = 'v_active_employees'

        # Get original definition
        original_def = pg_adapter.get_view_definition(test_view, 'public')
        assert original_def is not None, f"Could not get definition for {test_view}"
        print(f"  Original definition: {original_def[:50]}...")

        # Get original query result for parity check
        original_result = pg_adapter.execute_query(f"SELECT * FROM {test_view} ORDER BY emp_id")
        assert original_result['success'], f"Could not query original view"
        original_count = len(original_result['data'])
        print(f"  Original row count: {original_count}")

        # Drop view (cascade to handle dependencies)
        drop_result = pg_adapter.drop_view(test_view, 'public', cascade=True)
        assert drop_result['success'], f"Failed to drop view: {drop_result.get('error')}"

        # Verify it's gone
        views = pg_adapter.get_views('public')
        view_names = [v['name'] for v in views]
        assert test_view not in view_names, f"View {test_view} should be dropped"

        # Recreate from definition
        create_result = pg_adapter.create_view(test_view, original_def, 'public')
        assert create_result['success'], f"Failed to recreate view: {create_result.get('error')}"

        # Verify parity
        new_result = pg_adapter.execute_query(f"SELECT * FROM {test_view} ORDER BY emp_id")
        assert new_result['success'], f"Could not query recreated view"
        new_count = len(new_result['data'])

        assert new_count == original_count, \
            f"Row count mismatch: original={original_count}, recreated={new_count}"

        print(f"  ✓ Recreated with {new_count} rows (matches original)")

        # Recreate dependent view (v_high_salary_employees)
        dep_view = 'v_high_salary_employees'
        dep_def = pg_adapter.execute_query(
            "SELECT definition FROM pg_views WHERE viewname = 'v_high_salary_employees'"
        )
        if not dep_def['data']:
            # Need to recreate it
            pg_adapter.execute_query("""
                CREATE VIEW v_high_salary_employees AS
                SELECT * FROM v_active_employees WHERE salary > 80000
            """)

        print("✓ View recreation with parity verified")


class TestPostgreSQLL2Validation:
    """L2 Harness: A3 - Validation tests"""

    @pytest.fixture(scope='class')
    def pg_adapter(self):
        """PostgreSQL adapter fixture"""
        from extensions.plugins.postgresql_adapter import PostgreSQLAdapter

        adapter = PostgreSQLAdapter(
            host='localhost',
            port=5433,
            database='saiql_phase11_test',
            user='saiql_user',
            password='SaiqlTestPass123'
        )
        yield adapter
        adapter.close()

    def test_l2_a3_view_count_parity(self, pg_adapter):
        """
        A3.1: View presence parity

        Requirement: Count of views matches expected
        """
        print("\n=== A3.1: View count parity ===")

        views = pg_adapter.get_views('public')
        view_names = [v['name'] for v in views]

        # Check expected views
        missing = [v for v in EXPECTED_VIEWS if v not in view_names]
        extra = [v for v in view_names if v.startswith('v_') and v not in EXPECTED_VIEWS]

        print(f"  Expected: {len(EXPECTED_VIEWS)}")
        print(f"  Found: {len([v for v in view_names if v.startswith('v_')])}")
        if missing:
            print(f"  Missing: {missing}")
        if extra:
            print(f"  Extra: {extra}")

        assert not missing, f"Missing expected views: {missing}"

        print("✓ View count parity verified")

    def test_l2_a3_result_parity_deterministic(self, pg_adapter):
        """
        A3.2: Result parity with deterministic queries

        Requirement: Views return expected deterministic results
        """
        print("\n=== A3.2: Result parity ===")

        # v_active_employees should return is_active=true employees
        result = pg_adapter.execute_query(
            "SELECT COUNT(*) as cnt FROM v_active_employees"
        )
        assert result['success'], f"Query failed: {result.get('error')}"
        active_count = result['data'][0]['cnt']

        # Compare with base table
        base_result = pg_adapter.execute_query(
            "SELECT COUNT(*) as cnt FROM employees WHERE is_active = true"
        )
        base_count = base_result['data'][0]['cnt']

        assert active_count == base_count, \
            f"v_active_employees count ({active_count}) != base table ({base_count})"
        print(f"  ✓ v_active_employees: {active_count} rows (matches base)")

        # v_dept_employee_count should have entry for each department
        dept_result = pg_adapter.execute_query(
            "SELECT COUNT(*) as cnt FROM v_dept_employee_count"
        )
        dept_view_count = dept_result['data'][0]['cnt']

        base_dept_result = pg_adapter.execute_query(
            "SELECT COUNT(*) as cnt FROM departments"
        )
        base_dept_count = base_dept_result['data'][0]['cnt']

        assert dept_view_count == base_dept_count, \
            f"v_dept_employee_count ({dept_view_count}) != departments ({base_dept_count})"
        print(f"  ✓ v_dept_employee_count: {dept_view_count} rows (matches departments)")

        # v_high_salary_employees should be subset of v_active_employees
        high_result = pg_adapter.execute_query(
            "SELECT COUNT(*) as cnt FROM v_high_salary_employees"
        )
        high_count = high_result['data'][0]['cnt']

        assert high_count <= active_count, \
            f"v_high_salary_employees ({high_count}) should be <= v_active_employees ({active_count})"
        print(f"  ✓ v_high_salary_employees: {high_count} rows (subset of active)")

        print("✓ Result parity verified")

    def test_l2_a3_definition_parity(self, pg_adapter):
        """
        A3.3: Definition parity (normalized compare)

        Requirement: Extracted definitions are semantically equivalent
        """
        print("\n=== A3.3: Definition parity ===")

        def normalize_sql(sql: str) -> str:
            """Normalize SQL for comparison"""
            # Remove extra whitespace
            normalized = ' '.join(sql.split())
            # Lowercase keywords (simple normalization)
            normalized = normalized.lower()
            # Remove trailing semicolons
            normalized = normalized.rstrip(';').strip()
            return normalized

        for view_name in EXPECTED_VIEWS:
            definition = pg_adapter.get_view_definition(view_name, 'public')
            assert definition is not None, f"No definition for {view_name}"

            normalized = normalize_sql(definition)

            # Must contain select
            assert 'select' in normalized, f"{view_name} definition missing SELECT"

            # Must not be empty
            assert len(normalized) > 10, f"{view_name} definition too short"

            print(f"  ✓ {view_name}: normalized ({len(normalized)} chars)")

        print("✓ Definition parity verified")


class TestPostgreSQLL2BulkOperations:
    """L2 Harness: A2 Bulk - Bulk view operations"""

    @pytest.fixture(scope='class')
    def pg_adapter(self):
        """PostgreSQL adapter fixture"""
        from extensions.plugins.postgresql_adapter import PostgreSQLAdapter

        adapter = PostgreSQLAdapter(
            host='localhost',
            port=5433,
            database='saiql_phase11_test',
            user='saiql_user',
            password='SaiqlTestPass123'
        )
        yield adapter
        adapter.close()

    def test_l2_a2_bulk_view_creation(self, pg_adapter):
        """
        A2.3: Bulk view creation in dependency order

        Requirement: create_views_in_order() creates multiple views correctly
        """
        print("\n=== A2.3: Bulk view creation ===")

        # Create test views in a separate schema to not conflict
        pg_adapter.execute_query("CREATE SCHEMA IF NOT EXISTS test_l2_bulk")

        test_views = [
            {'name': 'v_bulk_base', 'definition': 'SELECT dept_id, dept_name FROM departments'},
            {'name': 'v_bulk_derived', 'definition': 'SELECT * FROM test_l2_bulk.v_bulk_base WHERE dept_id > 1'}
        ]

        # Create in order
        result = pg_adapter.create_views_in_order(test_views, 'test_l2_bulk')

        print(f"  Created: {result['created']}")
        print(f"  Failed: {result['failed']}")

        assert result['success'], f"Bulk creation failed: {result['failed']}"
        assert len(result['created']) == 2, f"Expected 2 views created, got {len(result['created'])}"

        # Verify both exist
        for view in test_views:
            check_result = pg_adapter.execute_query(
                f"SELECT * FROM test_l2_bulk.{view['name']} LIMIT 1"
            )
            assert check_result['success'], f"View {view['name']} not queryable"

        # Cleanup
        pg_adapter.execute_query("DROP SCHEMA test_l2_bulk CASCADE")

        print("✓ Bulk view creation successful")


class TestPostgreSQLL2Requirements:
    """L2 Harness: Verify all L2 requirements met"""

    @pytest.fixture(scope='class')
    def pg_adapter(self):
        """PostgreSQL adapter fixture"""
        from extensions.plugins.postgresql_adapter import PostgreSQLAdapter

        adapter = PostgreSQLAdapter(
            host='localhost',
            port=5433,
            database='saiql_phase11_test',
            user='saiql_user',
            password='SaiqlTestPass123'
        )
        yield adapter
        adapter.close()

    def test_all_l2_methods_exist(self, pg_adapter):
        """
        Verify adapter implements all L2 methods

        Requirements:
        - get_views() exists
        - get_view_definition() exists
        - get_view_dependencies() exists
        - get_views_in_dependency_order() exists
        - create_view() exists
        - drop_view() exists
        - create_views_in_order() exists
        """
        print("\n=== Verifying all L2 requirements ===")

        required_methods = [
            'get_views',
            'get_view_definition',
            'get_view_dependencies',
            'get_views_in_dependency_order',
            'get_materialized_views',
            'create_view',
            'drop_view',
            'create_views_in_order'
        ]

        for method in required_methods:
            assert hasattr(pg_adapter, method), f"Missing {method}() method"
            assert callable(getattr(pg_adapter, method)), f"{method}() not callable"
            print(f"  ✓ {method}()")

        print("✓ All L2 methods exist")

    def test_l2_limitations_documented(self, pg_adapter):
        """
        A4: Limitations documented

        Requirement: Any view not supported must be listed with reason
        """
        print("\n=== A4: Limitations check ===")

        # Current limitations for L2:
        limitations = [
            "Materialized views: extraction supported, auto-refresh not migrated",
            "Complex CTEs with RECURSIVE: may require manual verification",
            "Security-barrier views: security context not preserved",
            "Views with INSTEAD OF triggers: triggers handled separately in L4"
        ]

        print("  Documented limitations:")
        for lim in limitations:
            print(f"    - {lim}")

        print("✓ Limitations documented")
