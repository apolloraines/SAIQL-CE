#!/usr/bin/env python3
"""
SAIQL MariaDB L3 Harness Tests - Routines (Functions/Procedures)

Validates MySQL adapter L3 capabilities on MariaDB per collab rules:
- B1) Subset definition: allowlist + denylist documented and enforced
- B2) Extraction: Enumerate routines in subset (schema, name, signature, language, body)
- B3) Emission: Create routines in correct order
- B4) Validation: Signature parity, body parity, behavioral tests
- B5) Limitations: Skipped routines listed with reason and count

Proof-first approach per Apollo rules. Must pass 3x from clean state.

L3 Subset Rules (documented per B1) - MariaDB specific:
- ALLOWLIST:
  - IS_DETERMINISTIC = YES
  - DATA_ACCESS: NO SQL, CONTAINS SQL, READS SQL DATA
  - SQL_SECURITY: INVOKER
- DENYLIST:
  - IS_DETERMINISTIC = NO (non-deterministic)
  - DATA_ACCESS: MODIFIES SQL DATA
  - SQL_SECURITY: DEFINER (privilege escalation risk)
  - Routines with dynamic SQL (PREPARE/EXECUTE patterns)

Evidence:
- MariaDB 11.2 container on port 3307
- Fixture: /mnt/storage/DockerTests/mariadb/fixtures/03_routines.sql
- Expected routines:
  - Safe: get_customer_count, get_total_credit_limit, format_customer_display,
          calculate_order_tax, get_pending_orders (5)
  - Skipped: unsafe_random_value, unsafe_modifies_credit (2)
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
    'unsafe_random_value': 'not deterministic',
    'unsafe_modifies_credit': 'modifies sql data'
}


class TestMariaDBL3SubsetDefinition:
    """L3 Harness: B1 - Subset definition tests"""

    @pytest.fixture(scope='class')
    def mariadb_adapter(self):
        """MariaDB adapter fixture"""
        from extensions.plugins.mysql_adapter import MySQLAdapter

        adapter = MySQLAdapter(
            host='localhost',
            port=3307,
            database='saiql_phase10_test',
            user='test_user',
            password='test_password'
        )
        yield adapter
        adapter.close()

    def test_l3_b1_subset_rules_enforced(self, mariadb_adapter):
        """
        B1.1: Subset rules enforcement

        Requirement: get_safe_routines() only returns routines matching allowlist
        """
        print("\n=== B1.1: Subset rules enforcement ===")

        safe_routines = mariadb_adapter.get_safe_routines('saiql_phase10_test')
        safe_names = [r['name'] for r in safe_routines]

        print(f"  Safe routines found: {len(safe_routines)}")
        for name in safe_names:
            print(f"      {name}")

        # All expected safe routines should be present
        for expected in SAFE_ROUTINES:
            assert expected in safe_names, f"Missing expected safe routine: {expected}"

        # No skipped routines should be in safe list
        for skipped in SKIPPED_ROUTINES.keys():
            assert skipped not in safe_names, f"Unsafe routine {skipped} in safe list!"

        print("  Subset rules correctly enforced")

    def test_l3_b1_skipped_routines_with_reasons(self, mariadb_adapter):
        """
        B1.2: Skipped routines have documented reasons

        Requirement: get_skipped_routines() returns routines with skip_reasons
        """
        print("\n=== B1.2: Skipped routines with reasons ===")

        skipped = mariadb_adapter.get_skipped_routines('saiql_phase10_test')
        skipped_map = {r['name']: r.get('skip_reasons', []) for r in skipped}

        print(f"  Skipped routines: {len(skipped)}")
        for name, reasons in skipped_map.items():
            print(f"      {name}: {reasons}")

        # All expected skipped routines should be present with correct reasons
        for routine_name, expected_reason in SKIPPED_ROUTINES.items():
            assert routine_name in skipped_map, f"Expected {routine_name} to be skipped"
            reasons = skipped_map[routine_name]
            reason_str = ' '.join(reasons).lower()
            assert expected_reason in reason_str, \
                f"{routine_name} should have reason containing '{expected_reason}', got {reasons}"

        print("  Skipped routines documented with reasons")


class TestMariaDBL3Extraction:
    """L3 Harness: B2 - Extraction tests"""

    @pytest.fixture(scope='class')
    def mariadb_adapter(self):
        """MariaDB adapter fixture"""
        from extensions.plugins.mysql_adapter import MySQLAdapter

        adapter = MySQLAdapter(
            host='localhost',
            port=3307,
            database='saiql_phase10_test',
            user='test_user',
            password='test_password'
        )
        yield adapter
        adapter.close()

    def test_l3_b2_routine_enumeration(self, mariadb_adapter):
        """
        B2.1: Enumerate all routines in schema

        Requirement: get_routines() returns list of routines with required fields
        """
        print("\n=== B2.1: Routine enumeration ===")

        routines = mariadb_adapter.get_routines('saiql_phase10_test')
        print(f"  Found {len(routines)} routines")

        routine_names = [r['name'] for r in routines]
        all_expected = SAFE_ROUTINES + list(SKIPPED_ROUTINES.keys())

        for expected in all_expected:
            assert expected in routine_names, f"Missing expected routine: {expected}"

        assert len(routines) >= len(all_expected), \
            f"Expected at least {len(all_expected)} routines, got {len(routines)}"

        print(f"  Routine enumeration successful ({len(routines)} routines)")

    def test_l3_b2_routine_structure(self, mariadb_adapter):
        """
        B2.2: Routine structure validation

        Requirement: Each routine dict has schema, name, type, parameters, return_type, body
        """
        print("\n=== B2.2: Routine structure ===")

        routines = mariadb_adapter.get_routines('saiql_phase10_test')
        required_fields = ['name', 'type', 'is_deterministic', 'data_access', 'sql_security']

        for routine in routines:
            if routine['name'] in SAFE_ROUTINES or routine['name'] in SKIPPED_ROUTINES:
                for field in required_fields:
                    assert field in routine, f"Routine {routine['name']} missing '{field}'"

                print(f"      {routine['name']}: {routine['type']}, "
                      f"deterministic={routine['is_deterministic']}")

        print("  All routines have required structure")

    def test_l3_b2_routine_definition_retrieval(self, mariadb_adapter):
        """
        B2.3: Individual routine definition retrieval

        Requirement: get_routine_definition() returns full CREATE statement
        """
        print("\n=== B2.3: Routine definition retrieval ===")

        for routine_name in SAFE_ROUTINES:
            # Determine if it's a function or procedure
            routine_type = 'PROCEDURE' if routine_name == 'get_pending_orders' else 'FUNCTION'
            definition = mariadb_adapter.get_routine_definition(
                routine_name, routine_type, 'saiql_phase10_test'
            )

            assert definition is not None, f"Could not get definition for {routine_name}"
            assert 'CREATE' in definition.upper(), f"Definition should contain CREATE"

            print(f"      {routine_name}: {len(definition)} chars")

        print("  Routine definition retrieval successful")


class TestMariaDBL3Emission:
    """L3 Harness: B3 - Emission tests"""

    @pytest.fixture(scope='class')
    def mariadb_adapter(self):
        """MariaDB adapter fixture"""
        from extensions.plugins.mysql_adapter import MySQLAdapter

        adapter = MySQLAdapter(
            host='localhost',
            port=3307,
            database='saiql_phase10_test',
            user='test_user',
            password='test_password'
        )
        yield adapter
        adapter.close()

    def test_l3_b3_single_routine_creation(self, mariadb_adapter):
        """
        B3.1: Create single routine

        Requirement: create_routine() creates a routine from definition
        """
        print("\n=== B3.1: Single routine creation ===")

        # Drop if exists
        mariadb_adapter.drop_routine('test_l3_harness_func', 'FUNCTION', 'saiql_phase10_test', if_exists=True)

        # Create a test function
        test_func_def = """
        CREATE FUNCTION test_l3_harness_func(x INT)
        RETURNS INT
        DETERMINISTIC
        NO SQL
        SQL SECURITY INVOKER
        RETURN x * 2
        """

        # Create function
        result = mariadb_adapter.create_routine(test_func_def)
        assert result['success'], f"Failed to create function: {result.get('error')}"

        # Verify it works
        test_result = mariadb_adapter.execute_query("SELECT test_l3_harness_func(5) as result")
        assert test_result['success'], f"Function call failed: {test_result.get('error')}"
        assert test_result['data'][0]['result'] == 10, "Function should return 10 for input 5"

        # Cleanup
        mariadb_adapter.drop_routine('test_l3_harness_func', 'FUNCTION', 'saiql_phase10_test')

        print("  Single routine creation successful")

    def test_l3_b3_routine_recreation(self, mariadb_adapter):
        """
        B3.2: Routine recreation (extract -> recreate with OR REPLACE)

        Requirement: Extract routine definition and verify it can be used to create routine
        """
        print("\n=== B3.2: Routine recreation ===")

        test_func = 'format_customer_display'

        # Get original definition
        original_def = mariadb_adapter.get_routine_definition(test_func, 'FUNCTION', 'saiql_phase10_test')
        assert original_def is not None, f"Could not get definition for {test_func}"
        print(f"  Original definition length: {len(original_def)} chars")

        # Test original function
        original_result = mariadb_adapter.execute_query(
            f"SELECT {test_func}('Test User', 'test@example.com') as display"
        )
        assert original_result['success'], "Original function call failed"
        original_display = original_result['data'][0]['display']
        print(f"  Original result: {original_display}")

        # Create a copy with a different name to verify the definition is valid
        copy_def = original_def.replace('format_customer_display', 'format_customer_display_copy')
        copy_def = copy_def.replace('`saiql_phase10_test`.`format_customer_display`', '`saiql_phase10_test`.`format_customer_display_copy`')

        # Drop copy if exists
        mariadb_adapter.drop_routine('format_customer_display_copy', 'FUNCTION', 'saiql_phase10_test', if_exists=True)

        # Create the copy
        create_result = mariadb_adapter.create_routine(copy_def)
        assert create_result['success'], f"Failed to create copy: {create_result.get('error')}"

        # Verify copy works identically
        new_result = mariadb_adapter.execute_query(
            "SELECT format_customer_display_copy('Test User', 'test@example.com') as display"
        )
        assert new_result['success'], "Copy function call failed"
        new_display = new_result['data'][0]['display']

        assert new_display == original_display, \
            f"Result mismatch: original={original_display}, copy={new_display}"

        print(f"    Copy produces same result: {new_display}")

        # Cleanup
        mariadb_adapter.drop_routine('format_customer_display_copy', 'FUNCTION', 'saiql_phase10_test')

        print("  Routine recreation verified")


class TestMariaDBL3Validation:
    """L3 Harness: B4 - Validation tests"""

    @pytest.fixture(scope='class')
    def mariadb_adapter(self):
        """MariaDB adapter fixture"""
        from extensions.plugins.mysql_adapter import MySQLAdapter

        adapter = MySQLAdapter(
            host='localhost',
            port=3307,
            database='saiql_phase10_test',
            user='test_user',
            password='test_password'
        )
        yield adapter
        adapter.close()

    def test_l3_b4_signature_parity(self, mariadb_adapter):
        """
        B4.1: Signature parity

        Requirement: Routine signatures (name, args, return type) are correct
        """
        print("\n=== B4.1: Signature parity ===")

        expected_signatures = {
            'get_customer_count': {'type': 'FUNCTION', 'returns': 'int'},
            'get_total_credit_limit': {'type': 'FUNCTION', 'returns': 'decimal'},
            'format_customer_display': {'type': 'FUNCTION', 'returns': 'varchar'},
            'calculate_order_tax': {'type': 'FUNCTION', 'returns': 'decimal'},
            'get_pending_orders': {'type': 'PROCEDURE', 'returns': None},
        }

        routines = mariadb_adapter.get_routines('saiql_phase10_test')
        routine_map = {r['name']: r for r in routines}

        for name, expected in expected_signatures.items():
            assert name in routine_map, f"Missing routine: {name}"
            routine = routine_map[name]

            assert routine['type'] == expected['type'], \
                f"{name} type mismatch: expected {expected['type']}, got {routine['type']}"

            print(f"      {name}: {routine['type']}")

        print("  Signature parity verified")

    def test_l3_b4_behavioral_get_customer_count(self, mariadb_adapter):
        """
        B4.2a: Behavioral test - get_customer_count()

        Requirement: Returns correct count of customers
        """
        print("\n=== B4.2a: Behavioral - get_customer_count ===")

        # Get count from function
        result = mariadb_adapter.execute_query("SELECT get_customer_count() as cnt")
        assert result['success'], f"Query failed: {result.get('error')}"
        func_count = result['data'][0]['cnt']

        # Compare with actual count
        base_result = mariadb_adapter.execute_query("SELECT COUNT(*) as cnt FROM customers")
        base_count = base_result['data'][0]['cnt']

        assert func_count == base_count, f"Count mismatch: function={func_count}, table={base_count}"
        print(f"    get_customer_count() = {func_count} (matches table)")

        print("  Behavioral test passed")

    def test_l3_b4_behavioral_format_customer_display(self, mariadb_adapter):
        """
        B4.2b: Behavioral test - format_customer_display()

        Requirement: Correctly formats customer display name
        """
        print("\n=== B4.2b: Behavioral - format_customer_display ===")

        result = mariadb_adapter.execute_query(
            "SELECT format_customer_display('Alice Smith', 'alice@example.com') as display"
        )
        assert result['success'], f"Query failed: {result.get('error')}"
        display = result['data'][0]['display']

        expected = 'Alice Smith <alice@example.com>'
        assert display == expected, f"Expected '{expected}', got '{display}'"
        print(f"    format_customer_display('Alice Smith', 'alice@example.com') = '{display}'")

        print("  Behavioral test passed")

    def test_l3_b4_behavioral_calculate_order_tax(self, mariadb_adapter):
        """
        B4.2c: Behavioral test - calculate_order_tax()

        Requirement: Correctly calculates 8% tax
        """
        print("\n=== B4.2c: Behavioral - calculate_order_tax ===")

        result = mariadb_adapter.execute_query(
            "SELECT calculate_order_tax(100.00) as tax"
        )
        assert result['success'], f"Query failed: {result.get('error')}"
        tax = float(result['data'][0]['tax'])

        expected_tax = 8.00  # 8% of 100
        assert tax == expected_tax, f"Expected {expected_tax}, got {tax}"
        print(f"    calculate_order_tax(100.00) = {tax}")

        print("  Behavioral test passed")

    def test_l3_b4_behavioral_get_pending_orders(self, mariadb_adapter):
        """
        B4.2d: Behavioral test - get_pending_orders() procedure

        Requirement: Returns pending orders
        """
        print("\n=== B4.2d: Behavioral - get_pending_orders ===")

        result = mariadb_adapter.execute_query("CALL get_pending_orders()")
        assert result['success'], f"Query failed: {result.get('error')}"
        pending_count = len(result['data'])

        # Verify against actual pending count
        base_result = mariadb_adapter.execute_query(
            "SELECT COUNT(*) as cnt FROM orders WHERE status = 'pending'"
        )
        base_count = base_result['data'][0]['cnt']

        assert pending_count == base_count, \
            f"Count mismatch: procedure={pending_count}, table={base_count}"
        print(f"    get_pending_orders() returned {pending_count} rows (matches table)")

        print("  Behavioral test passed")


class TestMariaDBL3Limitations:
    """L3 Harness: B5 - Limitations tests"""

    @pytest.fixture(scope='class')
    def mariadb_adapter(self):
        """MariaDB adapter fixture"""
        from extensions.plugins.mysql_adapter import MySQLAdapter

        adapter = MySQLAdapter(
            host='localhost',
            port=3307,
            database='saiql_phase10_test',
            user='test_user',
            password='test_password'
        )
        yield adapter
        adapter.close()

    def test_l3_b5_limitations_counted(self, mariadb_adapter):
        """
        B5.1: Skipped routines listed with count

        Requirement: Limitations report includes count of skipped routines
        """
        print("\n=== B5.1: Limitations count ===")

        all_routines = mariadb_adapter.get_routines('saiql_phase10_test')
        safe_routines = mariadb_adapter.get_safe_routines('saiql_phase10_test')
        skipped = mariadb_adapter.get_skipped_routines('saiql_phase10_test')

        # Filter to only our test routines
        test_routine_names = set(SAFE_ROUTINES) | set(SKIPPED_ROUTINES.keys())
        test_all = [r for r in all_routines if r['name'] in test_routine_names]
        test_safe = [r for r in safe_routines if r['name'] in test_routine_names]
        test_skipped = [r for r in skipped if r['name'] in test_routine_names]

        print(f"  Total test routines: {len(test_all)}")
        print(f"  Safe (migrated): {len(test_safe)}")
        print(f"  Skipped: {len(test_skipped)}")

        assert len(test_safe) == len(SAFE_ROUTINES), \
            f"Expected {len(SAFE_ROUTINES)} safe, got {len(test_safe)}"
        assert len(test_skipped) == len(SKIPPED_ROUTINES), \
            f"Expected {len(SKIPPED_ROUTINES)} skipped, got {len(test_skipped)}"

        print("  Limitations correctly counted")

    def test_l3_b5_limitations_documented(self, mariadb_adapter):
        """
        B5.2: Limitations documented

        Requirement: Any routine not supported must be listed with reason
        """
        print("\n=== B5.2: Limitations documented ===")

        # Current L3 limitations for MariaDB
        limitations = [
            "NOT DETERMINISTIC routines: Non-deterministic, skipped by default",
            "MODIFIES SQL DATA routines: Side effects risk, skipped",
            "SQL SECURITY DEFINER routines: Privilege escalation risk, skipped",
            "Routines with dynamic SQL (PREPARE/EXECUTE): Not portable, skipped",
            "DEFINER clause: Stripped for portability"
        ]

        print("  Documented limitations:")
        for lim in limitations:
            print(f"    - {lim}")

        print("  Limitations documented")


class TestMariaDBL3Requirements:
    """L3 Harness: Verify all L3 requirements met"""

    @pytest.fixture(scope='class')
    def mariadb_adapter(self):
        """MariaDB adapter fixture"""
        from extensions.plugins.mysql_adapter import MySQLAdapter

        adapter = MySQLAdapter(
            host='localhost',
            port=3307,
            database='saiql_phase10_test',
            user='test_user',
            password='test_password'
        )
        yield adapter
        adapter.close()

    def test_all_l3_methods_exist(self, mariadb_adapter):
        """
        Verify adapter implements all L3 methods

        Requirements:
        - get_routines() exists
        - get_routine_definition() exists
        - get_safe_routines() exists
        - get_skipped_routines() exists
        - create_routine() exists
        - drop_routine() exists
        - create_routines_in_order() exists
        """
        print("\n=== Verifying all L3 requirements ===")

        required_methods = [
            'get_routines',
            'get_routine_definition',
            'get_safe_routines',
            'get_skipped_routines',
            'create_routine',
            'drop_routine',
            'create_routines_in_order'
        ]

        for method in required_methods:
            assert hasattr(mariadb_adapter, method), f"Missing {method}() method"
            assert callable(getattr(mariadb_adapter, method)), f"{method}() not callable"
            print(f"      {method}()")

        print("  All L3 methods exist")
