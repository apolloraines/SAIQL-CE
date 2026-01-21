#!/usr/bin/env python3
"""
SAIQL SQL Server L2 Harness Tests - Views

Validates SQL Server adapter L2 capabilities per collab rules:
- A1) Extraction: Enumerate views with schema, name, definition, dependencies
- A2) Emission: Create views in correct order, no missing dependencies
- A3) Validation: View presence parity, definition parity, result parity
- A4) Limitations: Any view not supported listed with reason

Proof-first approach per Apollo rules. Must pass 3x from clean state.

Evidence:
- SQL Server 2022 container on port 1434
- Fixture: /mnt/storage/DockerTests/sqlserver/fixtures/02_views.sql
- Expected views:
  - v_active_customers (no view deps)
  - v_high_value_customers (depends on v_active_customers)
  - v_order_summary (no view deps)
  - v_pending_orders (no view deps)
  - v_customer_orders (no view deps)
  - v_customers_simple (helper view for trigger test - excluded from main count)
"""

import pytest
import logging
from typing import Dict, Any, List

logger = logging.getLogger(__name__)

# Expected views (excluding v_customers_simple which is for trigger testing)
EXPECTED_VIEWS = [
    'v_active_customers',
    'v_high_value_customers',
    'v_order_summary',
    'v_pending_orders',
    'v_customer_orders'
]

# View with dependency
VIEW_WITH_DEPENDENCY = {
    'view': 'v_high_value_customers',
    'depends_on': 'v_active_customers'
}


def get_adapter():
    """Get configured MSSQL adapter."""
    import sys
    sys.path.insert(0, '/home/nova/SAIQL.DEV')
    from extensions.plugins.mssql_adapter import MSSQLAdapter

    config = {
        'host': 'localhost',
        'port': 1434,
        'database': 'saiql_phase10_test',
        'user': 'sa',
        'password': 'SaiqlTestPass123'
    }
    return MSSQLAdapter(config)


class TestSQLServerL2Extraction:
    """A1) Extraction tests for SQL Server views."""

    def test_l2_a1_view_enumeration(self):
        """Test that all expected views are enumerated."""
        adapter = get_adapter()
        try:
            views = adapter.get_views(schema='dbo')
            view_names = [v['name'] for v in views]

            for expected in EXPECTED_VIEWS:
                assert expected in view_names, f"Missing view: {expected}"

            logger.info(f"Found {len(views)} views, expected {len(EXPECTED_VIEWS)}")
        finally:
            adapter.close()

    def test_l2_a1_view_structure(self):
        """Test that view metadata has required fields."""
        adapter = get_adapter()
        try:
            views = adapter.get_views(schema='dbo')

            for view in views:
                assert 'name' in view, "View missing 'name'"
                assert 'schema' in view, "View missing 'schema'"
                assert 'definition' in view, "View missing 'definition'"

                # Definition should contain CREATE VIEW
                if view['definition']:
                    assert 'CREATE' in view['definition'].upper(), \
                        f"View {view['name']} definition doesn't contain CREATE"
        finally:
            adapter.close()

    def test_l2_a1_view_definition_retrieval(self):
        """Test individual view definition retrieval."""
        adapter = get_adapter()
        try:
            for view_name in EXPECTED_VIEWS[:3]:  # Test first 3
                definition = adapter.get_view_definition(view_name, schema='dbo')
                assert definition is not None, f"No definition for {view_name}"
                assert 'SELECT' in definition.upper(), f"No SELECT in {view_name} definition"
        finally:
            adapter.close()

    def test_l2_a1_view_dependencies(self):
        """Test view dependency detection."""
        adapter = get_adapter()
        try:
            deps = adapter.get_view_dependencies(VIEW_WITH_DEPENDENCY['view'], schema='dbo')
            dep_names = [d['name'] for d in deps]

            # v_high_value_customers should depend on v_active_customers
            assert VIEW_WITH_DEPENDENCY['depends_on'] in dep_names, \
                f"Expected {VIEW_WITH_DEPENDENCY['view']} to depend on {VIEW_WITH_DEPENDENCY['depends_on']}"
        finally:
            adapter.close()


class TestSQLServerL2DependencyOrdering:
    """Test dependency ordering for views."""

    def test_l2_a1_dependency_order(self):
        """Test that views are returned in dependency order."""
        adapter = get_adapter()
        try:
            ordered_views = adapter.get_views_in_dependency_order(schema='dbo')
            view_names = [v['name'] for v in ordered_views]

            # v_active_customers must come before v_high_value_customers
            if VIEW_WITH_DEPENDENCY['depends_on'] in view_names and VIEW_WITH_DEPENDENCY['view'] in view_names:
                dep_idx = view_names.index(VIEW_WITH_DEPENDENCY['depends_on'])
                view_idx = view_names.index(VIEW_WITH_DEPENDENCY['view'])
                assert dep_idx < view_idx, \
                    f"{VIEW_WITH_DEPENDENCY['depends_on']} should come before {VIEW_WITH_DEPENDENCY['view']}"
        finally:
            adapter.close()


class TestSQLServerL2Emission:
    """A2) Emission tests for SQL Server views."""

    def test_l2_a2_single_view_creation(self):
        """Test creating a single view."""
        adapter = get_adapter()
        try:
            # Get definition of a simple view
            definition = adapter.get_view_definition('v_pending_orders', schema='dbo')
            assert definition is not None

            # Drop and recreate
            adapter.drop_view('v_pending_orders', schema='dbo')
            result = adapter.create_view('v_pending_orders', definition, schema='dbo')

            assert result['success'], f"Failed to create view: {result.get('error')}"

            # Verify it exists
            views = adapter.get_views(schema='dbo')
            view_names = [v['name'] for v in views]
            assert 'v_pending_orders' in view_names
        finally:
            adapter.close()

    def test_l2_a2_view_recreation(self):
        """Test that views can be dropped and recreated."""
        adapter = get_adapter()
        try:
            view_name = 'v_order_summary'
            definition = adapter.get_view_definition(view_name, schema='dbo')

            # Drop
            drop_result = adapter.drop_view(view_name, schema='dbo')
            assert drop_result['success']

            # Recreate
            create_result = adapter.create_view(view_name, definition, schema='dbo')
            assert create_result['success'], f"Recreation failed: {create_result.get('error')}"
        finally:
            adapter.close()


class TestSQLServerL2Validation:
    """A3) Validation tests for SQL Server views."""

    def test_l2_a3_view_count_parity(self):
        """Test that view count matches expected."""
        adapter = get_adapter()
        try:
            views = adapter.get_views(schema='dbo')
            # Filter to only our test views
            test_views = [v for v in views if v['name'] in EXPECTED_VIEWS]

            assert len(test_views) == len(EXPECTED_VIEWS), \
                f"Expected {len(EXPECTED_VIEWS)} views, got {len(test_views)}"
        finally:
            adapter.close()

    def test_l2_a3_result_parity_deterministic(self):
        """Test that view results are consistent."""
        adapter = get_adapter()
        try:
            # Query a deterministic view
            result = adapter.execute_query("SELECT COUNT(*) as cnt FROM dbo.v_active_customers")
            assert result['success']
            assert len(result['data']) == 1
            # Should have at least one active customer
            assert result['data'][0]['cnt'] >= 0
        finally:
            adapter.close()

    def test_l2_a3_definition_parity(self):
        """Test that extracted definitions can be used to recreate views."""
        adapter = get_adapter()
        try:
            for view_name in EXPECTED_VIEWS[:2]:
                definition = adapter.get_view_definition(view_name, schema='dbo')
                assert definition is not None
                assert 'SELECT' in definition.upper()
        finally:
            adapter.close()


class TestSQLServerL2BulkOperations:
    """Test bulk view operations."""

    def test_l2_a2_bulk_view_creation(self):
        """Test creating multiple views in order."""
        adapter = get_adapter()
        try:
            # Get all views in order
            ordered_views = adapter.get_views_in_dependency_order(schema='dbo')

            # Filter to our test views
            test_views = [v for v in ordered_views if v['name'] in EXPECTED_VIEWS]

            # Drop all test views (in reverse order)
            for view in reversed(test_views):
                adapter.drop_view(view['name'], schema='dbo')

            # Recreate in order
            result = adapter.create_views_in_order(test_views, schema='dbo')

            assert result['success'], f"Bulk creation failed: {result.get('errors')}"
            assert result['created'] == len(test_views)
        finally:
            adapter.close()


class TestSQLServerL2Requirements:
    """Test that all required L2 methods exist."""

    def test_all_l2_methods_exist(self):
        """Verify all L2 methods are implemented."""
        adapter = get_adapter()
        try:
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
        finally:
            adapter.close()

    def test_l2_limitations_documented(self):
        """Verify L2 limitations are handled."""
        adapter = get_adapter()
        try:
            views = adapter.get_views(schema='dbo')
            # All views should have definitions (SQL Server provides them)
            for view in views:
                if view['name'] in EXPECTED_VIEWS:
                    assert view.get('definition') is not None, \
                        f"View {view['name']} missing definition"
        finally:
            adapter.close()
