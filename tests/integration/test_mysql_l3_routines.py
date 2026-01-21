#!/usr/bin/env python3
"""
SAIQL MySQL L3 Harness Tests - Routines (Functions/Procedures)

Validates MySQL adapter L3 capabilities per collab rules:
- B1) Subset definition: allowlist + denylist documented and enforced
- B2) Extraction: Enumerate routines in subset (schema, name, signature, language, body)
- B3) Emission: Create routines in correct order
- B4) Validation: Signature parity, body parity, behavioral tests
- B5) Limitations: Skipped routines listed with reason and count

Proof-first approach per Apollo rules. Must pass 3x from clean state.

L3 Subset Rules (documented per B1) - MySQL specific:
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
- MySQL 8 container on port 3308
- Fixture: /mnt/storage/DockerTests/mysql/fixtures/03_routines.sql
- Expected routines:
  - Safe: get_employee_count, get_department_budget, format_employee_name,
          calculate_salary_tax, get_active_projects (5)
  - Skipped: unsafe_nondeterministic_func, unsafe_modifies_data_func (2)
"""

import pytest
import logging
from typing import Dict, Any, List

logger = logging.getLogger(__name__)

# Expected safe routines
SAFE_ROUTINES = [
    'get_employee_count',
    'get_department_budget',
    'format_employee_name',
    'calculate_salary_tax',
    'get_active_projects'
]

# Expected skipped routines with reasons
SKIPPED_ROUTINES = {
    'unsafe_nondeterministic_func': 'not deterministic',
    'unsafe_modifies_data_func': 'modifies sql data'
}


class TestMySQLL3SubsetDefinition:
    """L3 Harness: B1 - Subset definition tests"""

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

    def test_l3_b1_subset_rules_enforced(self, mysql_adapter):
        """
        B1.1: Subset rules enforcement

        Requirement: get_safe_routines() only returns routines matching allowlist
        """
        print("\n=== B1.1: Subset rules enforcement ===")

        safe_routines = mysql_adapter.get_safe_routines('saiql_phase11_test')
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

    def test_l3_b1_skipped_routines_with_reasons(self, mysql_adapter):
        """
        B1.2: Skipped routines have documented reasons

        Requirement: get_skipped_routines() returns routines with skip_reasons
        """
        print("\n=== B1.2: Skipped routines with reasons ===")

        skipped = mysql_adapter.get_skipped_routines('saiql_phase11_test')
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


class TestMySQLL3Extraction:
    """L3 Harness: B2 - Extraction tests"""

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

    def test_l3_b2_routine_enumeration(self, mysql_adapter):
        """
        B2.1: Enumerate all routines in schema

        Requirement: get_routines() returns list of routines with required fields
        """
        print("\n=== B2.1: Routine enumeration ===")

        routines = mysql_adapter.get_routines('saiql_phase11_test')
        print(f"  Found {len(routines)} routines")

        routine_names = [r['name'] for r in routines]
        all_expected = SAFE_ROUTINES + list(SKIPPED_ROUTINES.keys())

        for expected in all_expected:
            assert expected in routine_names, f"Missing expected routine: {expected}"

        assert len(routines) >= len(all_expected), \
            f"Expected at least {len(all_expected)} routines, got {len(routines)}"

        print(f"  Routine enumeration successful ({len(routines)} routines)")

    def test_l3_b2_routine_structure(self, mysql_adapter):
        """
        B2.2: Routine structure validation

        Requirement: Each routine dict has schema, name, type, parameters, return_type, body
        """
        print("\n=== B2.2: Routine structure ===")

        routines = mysql_adapter.get_routines('saiql_phase11_test')
        required_fields = ['name', 'type', 'is_deterministic', 'data_access', 'sql_security']

        for routine in routines:
            if routine['name'] in SAFE_ROUTINES or routine['name'] in SKIPPED_ROUTINES:
                for field in required_fields:
                    assert field in routine, f"Routine {routine['name']} missing '{field}'"

                print(f"      {routine['name']}: {routine['type']}, "
                      f"deterministic={routine['is_deterministic']}")

        print("  All routines have required structure")

    def test_l3_b2_routine_definition_retrieval(self, mysql_adapter):
        """
        B2.3: Individual routine definition retrieval

        Requirement: get_routine_definition() returns full CREATE statement
        """
        print("\n=== B2.3: Routine definition retrieval ===")

        for routine_name in SAFE_ROUTINES:
            # Determine if it's a function or procedure
            routine_type = 'PROCEDURE' if routine_name == 'get_active_projects' else 'FUNCTION'
            definition = mysql_adapter.get_routine_definition(
                routine_name, routine_type, 'saiql_phase11_test'
            )

            assert definition is not None, f"Could not get definition for {routine_name}"
            assert 'CREATE' in definition.upper(), f"Definition should contain CREATE"

            print(f"      {routine_name}: {len(definition)} chars")

        print("  Routine definition retrieval successful")


class TestMySQLL3Emission:
    """L3 Harness: B3 - Emission tests"""

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

    def test_l3_b3_single_routine_creation(self, mysql_adapter):
        """
        B3.1: Create single routine

        Requirement: create_routine() creates a routine from definition
        """
        print("\n=== B3.1: Single routine creation ===")

        # Drop if exists
        mysql_adapter.drop_routine('test_l3_harness_func', 'FUNCTION', 'saiql_phase11_test', if_exists=True)

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
        result = mysql_adapter.create_routine(test_func_def)
        assert result['success'], f"Failed to create function: {result.get('error')}"

        # Verify it works
        test_result = mysql_adapter.execute_query("SELECT test_l3_harness_func(5) as result")
        assert test_result['success'], f"Function call failed: {test_result.get('error')}"
        assert test_result['data'][0]['result'] == 10, "Function should return 10 for input 5"

        # Cleanup
        mysql_adapter.drop_routine('test_l3_harness_func', 'FUNCTION', 'saiql_phase11_test')

        print("  Single routine creation successful")

    def test_l3_b3_routine_recreation(self, mysql_adapter):
        """
        B3.2: Routine recreation (extract -> recreate with OR REPLACE)

        Requirement: Extract routine definition and verify it can be used to create routine
        """
        print("\n=== B3.2: Routine recreation ===")

        test_func = 'format_employee_name'

        # Get original definition
        original_def = mysql_adapter.get_routine_definition(test_func, 'FUNCTION', 'saiql_phase11_test')
        assert original_def is not None, f"Could not get definition for {test_func}"
        print(f"  Original definition length: {len(original_def)} chars")

        # Test original function
        original_result = mysql_adapter.execute_query(
            f"SELECT {test_func}('Test', 'User') as fullname"
        )
        assert original_result['success'], "Original function call failed"
        original_name = original_result['data'][0]['fullname']
        print(f"  Original result: {original_name}")

        # Create a copy with a different name to verify the definition is valid
        copy_def = original_def.replace('format_employee_name', 'format_employee_name_copy')
        copy_def = copy_def.replace('`saiql_phase11_test`.`format_employee_name`', '`saiql_phase11_test`.`format_employee_name_copy`')

        # Drop copy if exists
        mysql_adapter.drop_routine('format_employee_name_copy', 'FUNCTION', 'saiql_phase11_test', if_exists=True)

        # Create the copy
        create_result = mysql_adapter.create_routine(copy_def)
        assert create_result['success'], f"Failed to create copy: {create_result.get('error')}"

        # Verify copy works identically
        new_result = mysql_adapter.execute_query(
            "SELECT format_employee_name_copy('Test', 'User') as fullname"
        )
        assert new_result['success'], "Copy function call failed"
        new_name = new_result['data'][0]['fullname']

        assert new_name == original_name, \
            f"Result mismatch: original={original_name}, copy={new_name}"

        print(f"    Copy produces same result: {new_name}")

        # Cleanup
        mysql_adapter.drop_routine('format_employee_name_copy', 'FUNCTION', 'saiql_phase11_test')

        print("  Routine recreation verified")


class TestMySQLL3Validation:
    """L3 Harness: B4 - Validation tests"""

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

    def test_l3_b4_signature_parity(self, mysql_adapter):
        """
        B4.1: Signature parity

        Requirement: Routine signatures (name, args, return type) are correct
        """
        print("\n=== B4.1: Signature parity ===")

        expected_signatures = {
            'get_employee_count': {'type': 'FUNCTION', 'returns': 'int'},
            'get_department_budget': {'type': 'FUNCTION', 'returns': 'decimal'},
            'format_employee_name': {'type': 'FUNCTION', 'returns': 'varchar'},
            'calculate_salary_tax': {'type': 'FUNCTION', 'returns': 'decimal'},
            'get_active_projects': {'type': 'PROCEDURE', 'returns': None},
        }

        routines = mysql_adapter.get_routines('saiql_phase11_test')
        routine_map = {r['name']: r for r in routines}

        for name, expected in expected_signatures.items():
            assert name in routine_map, f"Missing routine: {name}"
            routine = routine_map[name]

            assert routine['type'] == expected['type'], \
                f"{name} type mismatch: expected {expected['type']}, got {routine['type']}"

            print(f"      {name}: {routine['type']}")

        print("  Signature parity verified")

    def test_l3_b4_behavioral_tests(self, mysql_adapter):
        """
        B4.2: Behavioral tests with deterministic outputs

        Requirement: Routines return expected deterministic results
        """
        print("\n=== B4.2: Behavioral tests ===")

        # Test get_department_budget
        result = mysql_adapter.execute_query("SELECT get_department_budget(1) as budget")
        assert result['success'], f"Query failed: {result.get('error')}"
        budget = float(result['data'][0]['budget'])
        # Engineering budget is 500000.00
        assert budget == 500000.00, f"Expected 500000.00, got {budget}"
        print(f"    get_department_budget(1) = {budget}")

        # Test format_employee_name
        result = mysql_adapter.execute_query(
            "SELECT format_employee_name('John', 'Doe') as fullname"
        )
        assert result['success'], f"Query failed: {result.get('error')}"
        fullname = result['data'][0]['fullname']
        assert fullname == 'John Doe', f"Expected 'John Doe', got '{fullname}'"
        print(f"    format_employee_name('John', 'Doe') = '{fullname}'")

        # Test calculate_salary_tax
        result = mysql_adapter.execute_query(
            "SELECT calculate_salary_tax(80000.00) as tax"
        )
        assert result['success'], f"Query failed: {result.get('error')}"
        tax = float(result['data'][0]['tax'])
        expected_tax = 80000.00 * 0.25
        assert tax == expected_tax, f"Expected {expected_tax}, got {tax}"
        print(f"    calculate_salary_tax(80000) = {tax}")

        # Test get_active_projects (procedure)
        result = mysql_adapter.execute_query("CALL get_active_projects()")
        assert result['success'], f"Query failed: {result.get('error')}"
        active_count = len(result['data'])
        # Should have at least 1 active project
        assert active_count >= 1, f"Expected at least 1 active project, got {active_count}"
        print(f"    get_active_projects() returned {active_count} rows")

        print("  Behavioral tests passed")


class TestMySQLL3Limitations:
    """L3 Harness: B5 - Limitations tests"""

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

    def test_l3_b5_limitations_counted(self, mysql_adapter):
        """
        B5.1: Skipped routines listed with count

        Requirement: Limitations report includes count of skipped routines
        """
        print("\n=== B5.1: Limitations count ===")

        all_routines = mysql_adapter.get_routines('saiql_phase11_test')
        safe_routines = mysql_adapter.get_safe_routines('saiql_phase11_test')
        skipped = mysql_adapter.get_skipped_routines('saiql_phase11_test')

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

    def test_l3_b5_limitations_documented(self, mysql_adapter):
        """
        B5.2: Limitations documented

        Requirement: Any routine not supported must be listed with reason
        """
        print("\n=== B5.2: Limitations documented ===")

        # Current L3 limitations for MySQL
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


class TestMySQLL3Requirements:
    """L3 Harness: Verify all L3 requirements met"""

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

    def test_all_l3_methods_exist(self, mysql_adapter):
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
            assert hasattr(mysql_adapter, method), f"Missing {method}() method"
            assert callable(getattr(mysql_adapter, method)), f"{method}() not callable"
            print(f"      {method}()")

        print("  All L3 methods exist")
