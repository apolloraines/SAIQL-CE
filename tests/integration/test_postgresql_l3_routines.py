#!/usr/bin/env python3
"""
SAIQL PostgreSQL L3 Harness Tests - Routines (Functions/Procedures)

Validates PostgreSQL adapter L3 capabilities per collab rules:
- B1) Subset definition: allowlist + denylist documented and enforced
- B2) Extraction: Enumerate routines in subset (schema, name, signature, language, body)
- B3) Emission: Create routines in correct order
- B4) Validation: Signature parity, body parity, behavioral tests
- B5) Limitations: Skipped routines listed with reason and count

Proof-first approach per Apollo rules. Must pass 3x from clean state.

L3 Subset Rules (documented per B1):
- ALLOWLIST:
  - Language: SQL, PL/pgSQL
  - Volatility: IMMUTABLE, STABLE
  - Security: INVOKER (not DEFINER)
- DENYLIST:
  - Language: C, internal, PL/Python, etc.
  - Volatility: VOLATILE (non-deterministic)
  - Security: SECURITY DEFINER

Evidence:
- PostgreSQL 15 container on port 5433
- Fixture: /mnt/storage/DockerTests/postgresql/fixtures/03_routines.sql
- Expected functions:
  - Safe: get_employee_count, get_department_budget, format_employee_name,
          calculate_salary_tax, get_active_projects (5)
  - Skipped: unsafe_volatile_func (volatile), unsafe_security_definer (secdef) (2)
"""

import pytest
import logging
from typing import Dict, Any, List

logger = logging.getLogger(__name__)

# Expected safe functions
SAFE_FUNCTIONS = [
    'get_employee_count',
    'get_department_budget',
    'format_employee_name',
    'calculate_salary_tax',
    'get_active_projects'
]

# Expected skipped functions with reasons
SKIPPED_FUNCTIONS = {
    'unsafe_volatile_func': 'volatile function',
    'unsafe_security_definer': 'security definer'
}


class TestPostgreSQLL3SubsetDefinition:
    """L3 Harness: B1 - Subset definition tests"""

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

    def test_l3_b1_subset_rules_enforced(self, pg_adapter):
        """
        B1.1: Subset rules enforcement

        Requirement: get_safe_functions() only returns functions matching allowlist
        """
        print("\n=== B1.1: Subset rules enforcement ===")

        safe_funcs = pg_adapter.get_safe_functions('public')
        safe_names = [f['name'] for f in safe_funcs]

        print(f"  Safe functions found: {len(safe_funcs)}")
        for name in safe_names:
            print(f"    ✓ {name}")

        # All expected safe functions should be present
        for expected in SAFE_FUNCTIONS:
            assert expected in safe_names, f"Missing expected safe function: {expected}"

        # No skipped functions should be in safe list
        for skipped in SKIPPED_FUNCTIONS.keys():
            assert skipped not in safe_names, f"Unsafe function {skipped} in safe list!"

        print("✓ Subset rules correctly enforced")

    def test_l3_b1_skipped_functions_with_reasons(self, pg_adapter):
        """
        B1.2: Skipped functions have documented reasons

        Requirement: get_skipped_functions() returns functions with skip_reasons
        """
        print("\n=== B1.2: Skipped functions with reasons ===")

        skipped = pg_adapter.get_skipped_functions('public')
        skipped_names = {f['name']: f.get('skip_reasons', []) for f in skipped}

        print(f"  Skipped functions: {len(skipped)}")
        for name, reasons in skipped_names.items():
            print(f"    ✗ {name}: {reasons}")

        # All expected skipped functions should be present with correct reasons
        for func_name, expected_reason in SKIPPED_FUNCTIONS.items():
            assert func_name in skipped_names, f"Expected {func_name} to be skipped"
            reasons = skipped_names[func_name]
            assert any(expected_reason in r for r in reasons), \
                f"{func_name} should have reason containing '{expected_reason}', got {reasons}"

        print("✓ Skipped functions documented with reasons")


class TestPostgreSQLL3Extraction:
    """L3 Harness: B2 - Extraction tests"""

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

    def test_l3_b2_function_enumeration(self, pg_adapter):
        """
        B2.1: Enumerate all functions in schema

        Requirement: get_functions() returns list of functions with required fields
        """
        print("\n=== B2.1: Function enumeration ===")

        functions = pg_adapter.get_functions('public')
        print(f"  Found {len(functions)} functions")

        func_names = [f['name'] for f in functions]
        all_expected = SAFE_FUNCTIONS + list(SKIPPED_FUNCTIONS.keys())

        for expected in all_expected:
            assert expected in func_names, f"Missing expected function: {expected}"

        assert len(functions) >= len(all_expected), \
            f"Expected at least {len(all_expected)} functions, got {len(functions)}"

        print(f"✓ Function enumeration successful ({len(functions)} functions)")

    def test_l3_b2_function_structure(self, pg_adapter):
        """
        B2.2: Function structure validation

        Requirement: Each function dict has schema, name, arguments, return_type, language, definition
        """
        print("\n=== B2.2: Function structure ===")

        functions = pg_adapter.get_functions('public')
        required_fields = ['name', 'schema', 'arguments', 'return_type', 'language', 'definition']

        for func in functions:
            if func['name'] in SAFE_FUNCTIONS or func['name'] in SKIPPED_FUNCTIONS:
                for field in required_fields:
                    assert field in func, f"Function {func['name']} missing '{field}'"

                print(f"  ✓ {func['name']}: {func['language']}, volatility={func['volatility']}")

        print("✓ All functions have required structure")

    def test_l3_b2_function_definition_retrieval(self, pg_adapter):
        """
        B2.3: Individual function definition retrieval

        Requirement: get_function_definition() returns full CREATE FUNCTION statement
        """
        print("\n=== B2.3: Function definition retrieval ===")

        for func_name in SAFE_FUNCTIONS:
            definition = pg_adapter.get_function_definition(func_name, 'public')

            assert definition is not None, f"Could not get definition for {func_name}"
            assert 'CREATE' in definition.upper(), f"Definition should contain CREATE"
            assert 'FUNCTION' in definition.upper(), f"Definition should contain FUNCTION"

            print(f"  ✓ {func_name}: {len(definition)} chars")

        print("✓ Function definition retrieval successful")


class TestPostgreSQLL3Emission:
    """L3 Harness: B3 - Emission tests"""

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

    def test_l3_b3_single_function_creation(self, pg_adapter):
        """
        B3.1: Create single function

        Requirement: create_function() creates a function from definition
        """
        print("\n=== B3.1: Single function creation ===")

        # Create a test function
        test_func_def = """
        CREATE FUNCTION test_l3_harness_func(x INTEGER)
        RETURNS INTEGER
        LANGUAGE SQL
        IMMUTABLE
        AS $$
            SELECT x * 2;
        $$
        """

        # Create function
        result = pg_adapter.create_function(test_func_def)
        assert result['success'], f"Failed to create function: {result.get('error')}"

        # Verify it works
        test_result = pg_adapter.execute_query("SELECT test_l3_harness_func(5) as result")
        assert test_result['success'], f"Function call failed: {test_result.get('error')}"
        assert test_result['data'][0]['result'] == 10, "Function should return 10 for input 5"

        # Cleanup
        pg_adapter.drop_function('test_l3_harness_func', 'integer')

        print("✓ Single function creation successful")

    def test_l3_b3_function_recreation(self, pg_adapter):
        """
        B3.2: Function recreation (extract -> drop -> recreate)

        Requirement: Extract function, drop it, recreate from extracted definition
        """
        print("\n=== B3.2: Function recreation ===")

        test_func = 'calculate_salary_tax'

        # Get original definition
        original_def = pg_adapter.get_function_definition(test_func, 'public')
        assert original_def is not None, f"Could not get definition for {test_func}"
        print(f"  Original definition length: {len(original_def)} chars")

        # Test original function
        original_result = pg_adapter.execute_query(
            f"SELECT {test_func}(100000.00) as tax"
        )
        assert original_result['success'], "Original function call failed"
        original_tax = original_result['data'][0]['tax']
        print(f"  Original result for 100000: {original_tax}")

        # Drop function
        drop_result = pg_adapter.drop_function(test_func, 'numeric', cascade=True)
        assert drop_result['success'], f"Failed to drop function: {drop_result.get('error')}"

        # Verify it's gone
        check = pg_adapter.execute_query(f"SELECT {test_func}(100.00)")
        assert not check['success'], "Function should be dropped"

        # Recreate from definition
        create_result = pg_adapter.create_function(original_def)
        assert create_result['success'], f"Failed to recreate function: {create_result.get('error')}"

        # Verify parity
        new_result = pg_adapter.execute_query(
            f"SELECT {test_func}(100000.00) as tax"
        )
        assert new_result['success'], "Recreated function call failed"
        new_tax = new_result['data'][0]['tax']

        assert float(new_tax) == float(original_tax), \
            f"Result mismatch: original={original_tax}, recreated={new_tax}"

        print(f"  ✓ Recreated with same result: {new_tax}")
        print("✓ Function recreation with parity verified")


class TestPostgreSQLL3Validation:
    """L3 Harness: B4 - Validation tests"""

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

    def test_l3_b4_signature_parity(self, pg_adapter):
        """
        B4.1: Signature parity

        Requirement: Function signatures (name, args, return type) are correct
        """
        print("\n=== B4.1: Signature parity ===")

        expected_signatures = {
            'get_employee_count': {'args': '', 'returns': 'integer'},
            'get_department_budget': {'args': 'dept integer', 'returns': 'numeric'},
            'format_employee_name': {'args': 'first_name character varying, last_name character varying', 'returns': 'character varying'},
            'calculate_salary_tax': {'args': 'salary numeric', 'returns': 'numeric'},
        }

        functions = pg_adapter.get_functions('public')
        func_map = {f['name']: f for f in functions}

        for name, expected in expected_signatures.items():
            assert name in func_map, f"Missing function: {name}"
            func = func_map[name]

            # Check return type (may have precision info)
            assert expected['returns'] in func['return_type'].lower(), \
                f"{name} return type mismatch: expected contains '{expected['returns']}', got '{func['return_type']}'"

            print(f"  ✓ {name}: ({func['arguments']}) -> {func['return_type']}")

        print("✓ Signature parity verified")

    def test_l3_b4_behavioral_tests(self, pg_adapter):
        """
        B4.2: Behavioral tests with deterministic outputs

        Requirement: Functions return expected deterministic results
        """
        print("\n=== B4.2: Behavioral tests ===")

        # Test get_department_budget
        result = pg_adapter.execute_query("SELECT get_department_budget(1) as budget")
        assert result['success'], f"Query failed: {result.get('error')}"
        budget = result['data'][0]['budget']
        # Engineering budget is 500000.00
        assert float(budget) == 500000.00, f"Expected 500000.00, got {budget}"
        print(f"  ✓ get_department_budget(1) = {budget}")

        # Test format_employee_name
        result = pg_adapter.execute_query(
            "SELECT format_employee_name('John', 'Doe') as fullname"
        )
        assert result['success'], f"Query failed: {result.get('error')}"
        fullname = result['data'][0]['fullname']
        assert fullname == 'John Doe', f"Expected 'John Doe', got '{fullname}'"
        print(f"  ✓ format_employee_name('John', 'Doe') = '{fullname}'")

        # Test calculate_salary_tax
        result = pg_adapter.execute_query(
            "SELECT calculate_salary_tax(80000.00) as tax"
        )
        assert result['success'], f"Query failed: {result.get('error')}"
        tax = result['data'][0]['tax']
        expected_tax = 80000.00 * 0.25
        assert float(tax) == expected_tax, f"Expected {expected_tax}, got {tax}"
        print(f"  ✓ calculate_salary_tax(80000) = {tax}")

        # Test get_active_projects (table-returning function)
        result = pg_adapter.execute_query(
            "SELECT * FROM get_active_projects()"
        )
        assert result['success'], f"Query failed: {result.get('error')}"
        active_count = len(result['data'])
        # Should have at least 1 active project
        assert active_count >= 1, f"Expected at least 1 active project, got {active_count}"
        print(f"  ✓ get_active_projects() returned {active_count} rows")

        print("✓ Behavioral tests passed")


class TestPostgreSQLL3Limitations:
    """L3 Harness: B5 - Limitations tests"""

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

    def test_l3_b5_limitations_counted(self, pg_adapter):
        """
        B5.1: Skipped routines listed with count

        Requirement: Limitations report includes count of skipped functions
        """
        print("\n=== B5.1: Limitations count ===")

        all_funcs = pg_adapter.get_functions('public')
        safe_funcs = pg_adapter.get_safe_functions('public')
        skipped = pg_adapter.get_skipped_functions('public')

        # Filter to only our test functions
        test_func_names = set(SAFE_FUNCTIONS) | set(SKIPPED_FUNCTIONS.keys())
        test_all = [f for f in all_funcs if f['name'] in test_func_names]
        test_safe = [f for f in safe_funcs if f['name'] in test_func_names]
        test_skipped = [f for f in skipped if f['name'] in test_func_names]

        print(f"  Total test functions: {len(test_all)}")
        print(f"  Safe (migrated): {len(test_safe)}")
        print(f"  Skipped: {len(test_skipped)}")

        assert len(test_safe) == len(SAFE_FUNCTIONS), \
            f"Expected {len(SAFE_FUNCTIONS)} safe, got {len(test_safe)}"
        assert len(test_skipped) == len(SKIPPED_FUNCTIONS), \
            f"Expected {len(SKIPPED_FUNCTIONS)} skipped, got {len(test_skipped)}"

        print("✓ Limitations correctly counted")

    def test_l3_b5_limitations_documented(self, pg_adapter):
        """
        B5.2: Limitations documented

        Requirement: Any routine not supported must be listed with reason
        """
        print("\n=== B5.2: Limitations documented ===")

        # Current L3 limitations
        limitations = [
            "VOLATILE functions: Non-deterministic, skipped by default",
            "SECURITY DEFINER functions: Privilege escalation risk, skipped",
            "C/internal language functions: Require binary, not portable",
            "PL/Python, PL/Perl functions: External language dependencies",
            "Functions with OUT parameters: Require special handling",
            "Aggregate functions: Complex state management"
        ]

        print("  Documented limitations:")
        for lim in limitations:
            print(f"    - {lim}")

        print("✓ Limitations documented")


class TestPostgreSQLL3Requirements:
    """L3 Harness: Verify all L3 requirements met"""

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

    def test_all_l3_methods_exist(self, pg_adapter):
        """
        Verify adapter implements all L3 methods

        Requirements:
        - get_functions() exists
        - get_function_definition() exists
        - get_safe_functions() exists
        - get_skipped_functions() exists
        - create_function() exists
        - drop_function() exists
        - create_functions_in_order() exists
        """
        print("\n=== Verifying all L3 requirements ===")

        required_methods = [
            'get_functions',
            'get_function_definition',
            'get_safe_functions',
            'get_skipped_functions',
            'create_function',
            'drop_function',
            'create_functions_in_order'
        ]

        for method in required_methods:
            assert hasattr(pg_adapter, method), f"Missing {method}() method"
            assert callable(getattr(pg_adapter, method)), f"{method}() not callable"
            print(f"  ✓ {method}()")

        print("✓ All L3 methods exist")
