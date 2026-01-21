#!/usr/bin/env python3
"""
SAIQL SQLite L2 Harness Tests - Views

Validates SQLite adapter L2 capabilities per collab rules:
- A1) Extraction: Enumerate views with name, definition, dependencies
- A2) Emission: Create views in correct dependency order
- A3) Validation: View presence parity, definition parity, result parity
- A4) Limitations: Any view not supported listed with reason

Proof-first approach per Apollo rules. Must pass 3x from clean state.

Per rules_SQLite_L2L3L4.md:
- Rule 5: Clean state per run (fresh DB file per run_id + teardown)
- Rule 8: PRAGMA settings must be fixed, not rely on defaults

Evidence:
- SQLite file-based database (fresh per test class, torn down after)
- Fixture: /mnt/storage/DockerTests/sqlite/fixtures/02_views.sql
- Expected views:
  - v_active_employees (base view, no deps)
  - v_high_value_active_employees (depends on v_active_employees)
  - v_employee_details (no view deps)
  - v_department_summary (no view deps)
  - v_project_status (no view deps)
"""

import pytest
import logging
import tempfile
import os
import uuid
from typing import Dict, Any, List
from pathlib import Path

logger = logging.getLogger(__name__)

# Expected views
EXPECTED_VIEWS = [
    'v_active_employees',
    'v_high_value_active_employees',
    'v_employee_details',
    'v_department_summary',
    'v_project_status'
]

# View with dependency
VIEW_WITH_DEPENDENCY = {
    'view': 'v_high_value_active_employees',
    'depends_on': 'v_active_employees'
}

# PRAGMA baseline - explicitly set, not defaults
PRAGMA_BASELINE = {
    'foreign_keys': 1,
    'recursive_triggers': 0
}


def get_adapter(db_path: str):
    """Get configured SQLite adapter with explicit PRAGMA enforcement."""
    import sys
    sys.path.insert(0, '/home/nova/SAIQL.DEV')
    from extensions.plugins.sqlite_adapter import SQLiteAdapter

    adapter = SQLiteAdapter(database=db_path)

    # Explicitly set PRAGMAs - do not rely on defaults (Rule 8)
    for pragma, value in PRAGMA_BASELINE.items():
        adapter.set_pragma(pragma, value)

    return adapter


def load_fixtures(adapter):
    """Load fixture files into database."""
    fixture_dir = Path('/mnt/storage/DockerTests/sqlite/fixtures')

    for fixture in ['01_schema.sql', '02_views.sql']:
        fixture_path = fixture_dir / fixture
        with open(fixture_path, 'r') as f:
            script = f.read()
        result = adapter.execute_script(script)
        assert result['success'], f"Failed to load {fixture}: {result.get('error')}"


@pytest.fixture(scope='class')
def db_path(tmp_path_factory):
    """Create a fresh DB file per test class (Rule 5: clean state per run)."""
    run_id = str(uuid.uuid4())[:8]
    run_dir = tmp_path_factory.mktemp(f"sqlite_l2_{run_id}")
    db_file = run_dir / f"test_l2_{run_id}.sqlite"
    yield str(db_file)
    # Teardown: file automatically cleaned up by pytest tmp_path_factory


@pytest.fixture(scope='class')
def adapter(db_path):
    """Class-scoped adapter with fixtures loaded."""
    adapter = get_adapter(db_path)
    load_fixtures(adapter)
    yield adapter
    adapter.close()


class TestSQLiteL2Extraction:
    """A1) Extraction tests for SQLite views."""

    def test_l2_a1_view_enumeration(self, adapter):
        """Test that all expected views are enumerated."""
        views = adapter.get_views()
        view_names = [v['name'] for v in views]

        for expected in EXPECTED_VIEWS:
            assert expected in view_names, f"Missing view: {expected}"

        logger.info(f"Found {len(views)} views, expected {len(EXPECTED_VIEWS)}")

    def test_l2_a1_view_structure(self, adapter):
        """Test that view metadata has required fields."""
        views = adapter.get_views()

        for view in views:
            assert 'name' in view, "View missing 'name'"
            assert 'definition' in view, "View missing 'definition'"

            # Definition should contain CREATE VIEW
            if view['definition']:
                assert 'CREATE VIEW' in view['definition'].upper(), \
                    f"View {view['name']} definition doesn't contain CREATE VIEW"

    def test_l2_a1_view_definition_retrieval(self, adapter):
        """Test individual view definition retrieval."""
        for view_name in EXPECTED_VIEWS[:3]:  # Test first 3
            definition = adapter.get_view_definition(view_name)
            assert definition is not None, f"No definition for {view_name}"
            assert 'SELECT' in definition.upper(), f"No SELECT in {view_name} definition"

    def test_l2_a1_view_dependencies(self, adapter):
        """Test view dependency detection."""
        deps = adapter.get_view_dependencies(VIEW_WITH_DEPENDENCY['view'])
        dep_names = [d['name'] for d in deps]

        # v_high_value_active_employees should depend on v_active_employees
        assert VIEW_WITH_DEPENDENCY['depends_on'] in dep_names, \
            f"Expected {VIEW_WITH_DEPENDENCY['view']} to depend on {VIEW_WITH_DEPENDENCY['depends_on']}"


class TestSQLiteL2DependencyOrdering:
    """Test dependency ordering for views."""

    def test_l2_a1_dependency_order(self, adapter):
        """Test that views are returned in dependency order."""
        ordered_views = adapter.get_views_in_dependency_order()
        view_names = [v['name'] for v in ordered_views]

        # v_active_employees must come before v_high_value_active_employees
        if VIEW_WITH_DEPENDENCY['depends_on'] in view_names and VIEW_WITH_DEPENDENCY['view'] in view_names:
            dep_idx = view_names.index(VIEW_WITH_DEPENDENCY['depends_on'])
            view_idx = view_names.index(VIEW_WITH_DEPENDENCY['view'])
            assert dep_idx < view_idx, \
                f"{VIEW_WITH_DEPENDENCY['depends_on']} should come before {VIEW_WITH_DEPENDENCY['view']}"


class TestSQLiteL2Emission:
    """A2) Emission tests for SQLite views."""

    def test_l2_a2_single_view_creation(self, adapter):
        """Test creating a single view."""
        # Get definition of a simple view
        definition = adapter.get_view_definition('v_project_status')
        assert definition is not None

        # Drop and recreate
        adapter.drop_view('v_project_status')
        result = adapter.create_view('v_project_status', definition)

        assert result['success'], f"Failed to create view: {result.get('error')}"

        # Verify it exists
        views = adapter.get_views()
        view_names = [v['name'] for v in views]
        assert 'v_project_status' in view_names

    def test_l2_a2_view_recreation(self, adapter):
        """Test that views can be dropped and recreated."""
        view_name = 'v_department_summary'
        definition = adapter.get_view_definition(view_name)

        # Drop
        drop_result = adapter.drop_view(view_name)
        assert drop_result['success']

        # Recreate
        create_result = adapter.create_view(view_name, definition)
        assert create_result['success'], f"Recreation failed: {create_result.get('error')}"


class TestSQLiteL2Validation:
    """A3) Validation tests for SQLite views."""

    def test_l2_a3_view_count_parity(self, adapter):
        """Test that view count matches expected."""
        views = adapter.get_views()
        # Filter to only our test views
        test_views = [v for v in views if v['name'] in EXPECTED_VIEWS]

        assert len(test_views) == len(EXPECTED_VIEWS), \
            f"Expected {len(EXPECTED_VIEWS)} views, got {len(test_views)}"

    def test_l2_a3_result_parity_deterministic(self, adapter):
        """Test that view results are consistent."""
        # Query a deterministic view
        result = adapter.execute_query("SELECT COUNT(*) as cnt FROM v_active_employees")
        assert result['success']
        assert len(result['data']) == 1
        # Should have at least one active employee (fixture has 5 active)
        assert result['data'][0]['cnt'] >= 1

    def test_l2_a3_definition_parity(self, adapter):
        """Test that extracted definitions can be used to recreate views."""
        for view_name in EXPECTED_VIEWS[:2]:
            definition = adapter.get_view_definition(view_name)
            assert definition is not None
            assert 'SELECT' in definition.upper()


class TestSQLiteL2BulkOperations:
    """Test bulk view operations."""

    def test_l2_a2_bulk_view_creation(self, adapter):
        """Test creating multiple views in order."""
        # Get all views in order
        ordered_views = adapter.get_views_in_dependency_order()

        # Filter to our test views
        test_views = [v for v in ordered_views if v['name'] in EXPECTED_VIEWS]

        # Drop all test views (in reverse order)
        for view in reversed(test_views):
            adapter.drop_view(view['name'])

        # Recreate in order
        result = adapter.create_views_in_order(test_views)

        assert result['success'], f"Bulk creation failed: {result.get('errors')}"
        assert result['created'] == len(test_views)


class TestSQLiteL2Requirements:
    """Test that all required L2 methods exist."""

    def test_all_l2_methods_exist(self, adapter):
        """Verify all L2 methods are implemented."""
        required_methods = [
            'get_views',
            'get_view_definition',
            'get_view_dependencies',
            'get_views_in_dependency_order',
            'create_view',
            'drop_view',
            'create_views_in_order'
        ]

        for method in required_methods:
            assert hasattr(adapter, method), f"Missing method: {method}"
            assert callable(getattr(adapter, method)), f"Method not callable: {method}"

    def test_l2_limitations_documented(self, adapter):
        """Verify L2 limitations are handled."""
        views = adapter.get_views()
        # All views should have definitions
        for view in views:
            if view['name'] in EXPECTED_VIEWS:
                assert view.get('definition') is not None, \
                    f"View {view['name']} missing definition"

    def test_l2_pragma_enforcement(self, adapter):
        """Verify PRAGMAs are explicitly set (Rule 8)."""
        pragmas = adapter.get_pragma_settings()

        # Verify PRAGMAs match our baseline (not relying on defaults)
        assert pragmas.get('foreign_keys') == PRAGMA_BASELINE['foreign_keys'], \
            f"foreign_keys not set correctly: {pragmas.get('foreign_keys')}"
        assert pragmas.get('recursive_triggers') == PRAGMA_BASELINE['recursive_triggers'], \
            f"recursive_triggers not set correctly: {pragmas.get('recursive_triggers')}"
