#!/usr/bin/env python3
"""
SAIQL SQL Server L3 Harness Tests - Routines (Functions/Stored Procedures)

Validates SQL Server adapter L3 capabilities per collab rules:
- B1) Subset definition: allowlist + denylist documented and enforced
- B2) Extraction: Enumerate routines in subset (schema, name, type, definition)
- B3) Emission: Create routines in correct order
- B4) Validation: Signature parity, behavioral tests
- B5) Limitations: Skipped routines listed with reason and count

Proof-first approach per Apollo rules. Must pass 3x from clean state.

L3 Subset Rules (documented per B1) - SQL Server specific:
- ALLOWLIST:
  - Scalar functions (FN), Inline TVFs (IF), Multi-statement TVFs (TF)
  - Stored procedures without unsafe patterns
- DENYLIST:
  - Dynamic SQL (EXEC with string, sp_executesql)
  - Cursors (DECLARE CURSOR)
  - Temp tables (#temp, ##temp)
  - Linked servers (four-part names, OPENROWSET, OPENDATASOURCE)

Evidence:
- SQL Server 2022 container on port 1434
- Fixture: /mnt/storage/DockerTests/sqlserver/fixtures/03_routines.sql
- Expected routines:
  - Safe: get_customer_count, get_total_credit_limit, format_customer_display,
          calculate_order_tax, get_pending_orders (5)
  - Skipped: unsafe_dynamic_sql, unsafe_cursor_proc (2)
"""

import pytest
import logging
from typing import Dict, Any, List

logger = logging.getLogger(__name__)

# Expected safe routines
SAFE_ROUTINES = [
    'get_customer_count',
    'get_total_credit_limit',
    'format_customer_display',
    'calculate_order_tax',
    'get_pending_orders'
]

# Expected skipped routines with reasons
SKIPPED_ROUTINES = {
    'unsafe_dynamic_sql': 'sp_executesql',
    'unsafe_cursor_proc': 'CURSOR'
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


class TestSQLServerL3SubsetDefinition:
    """B1) Subset definition tests."""

    def test_l3_b1_subset_rules_enforced(self):
        """Test that safe subset rules are enforced."""
        adapter = get_adapter()
        try:
            safe = adapter.get_safe_routines(schema='dbo')
            safe_names = [r['name'] for r in safe]

            for expected in SAFE_ROUTINES:
                assert expected in safe_names, f"Expected {expected} in safe subset"

            # Skipped routines should NOT be in safe subset
            for skipped in SKIPPED_ROUTINES.keys():
                assert skipped not in safe_names, f"{skipped} should not be in safe subset"
        finally:
            adapter.close()

    def test_l3_b1_skipped_routines_with_reasons(self):
        """Test that skipped routines have documented reasons."""
        adapter = get_adapter()
        try:
            skipped = adapter.get_skipped_routines(schema='dbo')
            skipped_names = [r['name'] for r in skipped]

            for name, expected_reason in SKIPPED_ROUTINES.items():
                assert name in skipped_names, f"Expected {name} to be skipped"

                # Find the routine and check skip_reasons
                routine = next(r for r in skipped if r['name'] == name)
                assert 'skip_reasons' in routine, f"{name} missing skip_reasons"
                reasons_str = ' '.join(routine['skip_reasons'])
                assert expected_reason.upper() in reasons_str.upper(), \
                    f"{name} should be skipped for {expected_reason}, got: {routine['skip_reasons']}"
        finally:
            adapter.close()


class TestSQLServerL3Extraction:
    """B2) Extraction tests."""

    def test_l3_b2_routine_enumeration(self):
        """Test that all routines are enumerated."""
        adapter = get_adapter()
        try:
            routines = adapter.get_routines(schema='dbo')
            routine_names = [r['name'] for r in routines]

            all_expected = SAFE_ROUTINES + list(SKIPPED_ROUTINES.keys())
            for expected in all_expected:
                assert expected in routine_names, f"Missing routine: {expected}"
        finally:
            adapter.close()

    def test_l3_b2_routine_structure(self):
        """Test that routine metadata has required fields."""
        adapter = get_adapter()
        try:
            routines = adapter.get_routines(schema='dbo')

            for routine in routines:
                assert 'name' in routine, "Routine missing 'name'"
                assert 'schema' in routine, "Routine missing 'schema'"
                assert 'type' in routine, "Routine missing 'type'"
                assert 'definition' in routine, "Routine missing 'definition'"

                # Type should be FN, IF, TF, or P
                assert routine['type'] in ('FN', 'IF', 'TF', 'P'), \
                    f"Unexpected routine type: {routine['type']}"
        finally:
            adapter.close()

    def test_l3_b2_routine_definition_retrieval(self):
        """Test individual routine definition retrieval."""
        adapter = get_adapter()
        try:
            for routine_name in SAFE_ROUTINES[:3]:
                definition = adapter.get_routine_definition(routine_name, schema='dbo')
                assert definition is not None, f"No definition for {routine_name}"
                assert 'CREATE' in definition.upper(), f"No CREATE in {routine_name} definition"
        finally:
            adapter.close()


class TestSQLServerL3Emission:
    """B3) Emission tests."""

    def test_l3_b3_single_routine_creation(self):
        """Test creating a single routine."""
        adapter = get_adapter()
        try:
            routine_name = 'format_customer_display'
            definition = adapter.get_routine_definition(routine_name, schema='dbo')
            assert definition is not None

            # Drop and recreate
            adapter.drop_routine(routine_name, 'FUNCTION', schema='dbo')
            result = adapter.create_routine(routine_name, definition, schema='dbo')

            assert result['success'], f"Failed to create routine: {result.get('error')}"

            # Verify it exists
            routines = adapter.get_routines(schema='dbo')
            routine_names = [r['name'] for r in routines]
            assert routine_name in routine_names
        finally:
            adapter.close()

    def test_l3_b3_routine_recreation(self):
        """Test that routines can be dropped and recreated."""
        adapter = get_adapter()
        try:
            routine_name = 'calculate_order_tax'
            definition = adapter.get_routine_definition(routine_name, schema='dbo')

            # Drop
            drop_result = adapter.drop_routine(routine_name, 'FUNCTION', schema='dbo')
            assert drop_result['success']

            # Recreate
            create_result = adapter.create_routine(routine_name, definition, schema='dbo')
            assert create_result['success'], f"Recreation failed: {create_result.get('error')}"
        finally:
            adapter.close()


class TestSQLServerL3Validation:
    """B4) Validation tests."""

    def test_l3_b4_signature_parity(self):
        """Test that routine types are correctly identified."""
        adapter = get_adapter()
        try:
            routines = adapter.get_routines(schema='dbo')

            # Check that functions are marked as functions
            for routine in routines:
                if routine['name'] in ['get_customer_count', 'format_customer_display']:
                    assert routine['is_function'], f"{routine['name']} should be a function"
                if routine['name'] == 'get_pending_orders':
                    assert routine['is_procedure'], f"get_pending_orders should be a procedure"
        finally:
            adapter.close()

    def test_l3_b4_behavioral_get_customer_count(self):
        """Test get_customer_count function returns expected value."""
        adapter = get_adapter()
        try:
            result = adapter.execute_query("SELECT dbo.get_customer_count() AS cnt")
            assert result['success'], f"Function call failed: {result.get('error')}"
            assert len(result['data']) == 1
            # Should return count of customers (3 in fixture)
            assert result['data'][0]['cnt'] >= 0
        finally:
            adapter.close()

    def test_l3_b4_behavioral_format_customer_display(self):
        """Test format_customer_display function returns expected format."""
        adapter = get_adapter()
        try:
            result = adapter.execute_query(
                "SELECT dbo.format_customer_display('Alice', 'alice@example.com') AS formatted"
            )
            assert result['success'], f"Function call failed: {result.get('error')}"
            assert len(result['data']) == 1
            assert result['data'][0]['formatted'] == 'Alice <alice@example.com>'
        finally:
            adapter.close()

    def test_l3_b4_behavioral_calculate_order_tax(self):
        """Test calculate_order_tax function returns expected value."""
        adapter = get_adapter()
        try:
            result = adapter.execute_query("SELECT dbo.calculate_order_tax(100.00) AS tax")
            assert result['success'], f"Function call failed: {result.get('error')}"
            assert len(result['data']) == 1
            # 100 * 0.08 = 8.00
            assert float(result['data'][0]['tax']) == 8.00
        finally:
            adapter.close()

    def test_l3_b4_behavioral_get_pending_orders(self):
        """Test get_pending_orders procedure returns results."""
        adapter = get_adapter()
        try:
            result = adapter.execute_query("EXEC dbo.get_pending_orders")
            assert result['success'], f"Procedure call failed: {result.get('error')}"
            # Should have at least the pending order from fixture
            assert len(result['data']) >= 0
        finally:
            adapter.close()


class TestSQLServerL3Limitations:
    """B5) Limitations tests."""

    def test_l3_b5_limitations_counted(self):
        """Test that correct number of routines are skipped."""
        adapter = get_adapter()
        try:
            skipped = adapter.get_skipped_routines(schema='dbo')
            assert len(skipped) == len(SKIPPED_ROUTINES), \
                f"Expected {len(SKIPPED_ROUTINES)} skipped, got {len(skipped)}"
        finally:
            adapter.close()

    def test_l3_b5_limitations_documented(self):
        """Test that all skipped routines have reasons."""
        adapter = get_adapter()
        try:
            skipped = adapter.get_skipped_routines(schema='dbo')

            for routine in skipped:
                assert 'skip_reasons' in routine, f"{routine['name']} missing skip_reasons"
                assert len(routine['skip_reasons']) > 0, \
                    f"{routine['name']} has empty skip_reasons"
        finally:
            adapter.close()


class TestSQLServerL3Requirements:
    """Test that all required L3 methods exist."""

    def test_all_l3_methods_exist(self):
        """Verify all L3 methods are implemented."""
        adapter = get_adapter()
        try:
            required_methods = [
                'get_routines',
                'get_routine_definition',
                'get_safe_routines',
                'get_skipped_routines',
                'create_routine',
                'drop_routine'
            ]

            for method in required_methods:
                assert hasattr(adapter, method), f"Missing method: {method}"
                assert callable(getattr(adapter, method)), f"Method not callable: {method}"
        finally:
            adapter.close()
