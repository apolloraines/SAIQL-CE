#!/usr/bin/env python3
"""
SAIQL Phase 11 PostgreSQL L1 Harness Tests

Validates PostgreSQL adapter L1 capabilities:
- L1.1: Primary keys (created and enforced)
- L1.2: Unique constraints (enforced)
- L1.3: Foreign keys (enforced, expected failure on invalid inserts)
- L1.4: Indexes (presence validated via catalog)
- L1.5: Identity/auto-increment (SERIAL verified by inserts)

Proof-first approach per Phase 11 rules.

Evidence:
- PostgreSQL 15 container on port 5433
- Fixture: /mnt/storage/DockerTests/postgresql/fixtures/01_schema.sql
- Expected L1 objects:
  - 5 PKs (departments, employees, projects, employee_projects composite, type_test)
  - 4 FKs (employees.dept_id, projects.dept_id, employee_projects.emp_id, employee_projects.project_id)
  - 3 Unique constraints (departments.dept_code, employees.email, projects(dept_id,project_name))
  - 2 User indexes (idx_employees_dept, idx_employees_name)
  - 3 Identity columns (employees.emp_id, projects.project_id, type_test.id)
"""

import pytest
import logging
from typing import Dict, Any, List

logger = logging.getLogger(__name__)


class TestPhase11PostgreSQLConstraintsL1:
    """PostgreSQL L1 harness tests - constraints"""

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

    def test_l1_1_primary_key_introspection(self, pg_adapter):
        """
        L1.1a: Introspect primary keys

        Requirement: get_primary_keys() returns PK columns for tables
        Expected: All 5 tables have PKs defined
        """
        print("\n=== L1.1a: Primary key introspection ===")

        tables_with_pk = ['departments', 'employees', 'projects', 'employee_projects', 'type_test']

        for table in tables_with_pk:
            pk_cols = pg_adapter.get_primary_keys(table)
            print(f"  {table}: PK columns = {pk_cols}")
            assert len(pk_cols) > 0, f"Table {table} should have a primary key"

        # Check composite PK on employee_projects
        ep_pk = pg_adapter.get_primary_keys('employee_projects')
        assert len(ep_pk) == 2, f"employee_projects should have composite PK (2 cols), got {len(ep_pk)}"
        assert set(ep_pk) == {'emp_id', 'project_id'}, f"Wrong PK columns: {ep_pk}"

        print("✓ Primary key introspection successful")

    def test_l1_1_primary_key_enforcement(self, pg_adapter):
        """
        L1.1b: Verify PK enforcement (duplicate insert should fail)

        Requirement: PK constraint prevents duplicate key values
        """
        print("\n=== L1.1b: Primary key enforcement ===")

        # Try to insert duplicate dept_id (should fail)
        result = pg_adapter.execute_query(
            "INSERT INTO departments (dept_id, dept_name, dept_code) VALUES (1, 'Duplicate', 'DUPE')"
        )

        assert not result['success'], "Duplicate PK insert should fail"
        assert 'duplicate key' in result.get('error', '').lower() or 'unique constraint' in result.get('error', '').lower(), \
            f"Expected duplicate key error, got: {result.get('error')}"

        print("✓ Primary key enforcement verified (duplicate rejected)")

    def test_l1_2_unique_constraint_introspection(self, pg_adapter):
        """
        L1.2a: Introspect unique constraints

        Requirement: get_unique_constraints() returns unique constraints
        Expected: departments.dept_code, employees.email, projects(dept_id,project_name)
        """
        print("\n=== L1.2a: Unique constraint introspection ===")

        # Check departments unique on dept_code
        dept_unique = pg_adapter.get_unique_constraints('departments')
        print(f"  departments unique constraints: {dept_unique}")
        assert any('dept_code' in str(uc) for uc in dept_unique), "departments should have unique on dept_code"

        # Check employees unique on email
        emp_unique = pg_adapter.get_unique_constraints('employees')
        print(f"  employees unique constraints: {emp_unique}")
        assert any('email' in str(uc) for uc in emp_unique), "employees should have unique on email"

        print("✓ Unique constraint introspection successful")

    def test_l1_2_unique_constraint_enforcement(self, pg_adapter):
        """
        L1.2b: Verify unique constraint enforcement

        Requirement: Unique constraint prevents duplicate values
        """
        print("\n=== L1.2b: Unique constraint enforcement ===")

        # Try to insert duplicate email (should fail)
        result = pg_adapter.execute_query(
            "INSERT INTO employees (email, first_name, last_name, dept_id, hire_date) "
            "VALUES ('alice@example.com', 'Alice2', 'Smith2', 1, '2024-01-01')"
        )

        assert not result['success'], "Duplicate unique value insert should fail"
        assert 'duplicate key' in result.get('error', '').lower() or 'unique constraint' in result.get('error', '').lower(), \
            f"Expected unique constraint error, got: {result.get('error')}"

        print("✓ Unique constraint enforcement verified")

    def test_l1_3_foreign_key_introspection(self, pg_adapter):
        """
        L1.3a: Introspect foreign keys

        Requirement: get_foreign_keys() returns FK definitions
        Expected: employees.dept_id -> departments, projects.dept_id -> departments, etc.
        """
        print("\n=== L1.3a: Foreign key introspection ===")

        # Check employees FK
        emp_fks = pg_adapter.get_foreign_keys('employees')
        print(f"  employees FKs: {emp_fks}")
        assert len(emp_fks) >= 1, "employees should have at least 1 FK"

        # Verify FK to departments
        dept_fk = [fk for fk in emp_fks if fk.get('ref_table') == 'departments']
        assert len(dept_fk) == 1, "employees should have FK to departments"

        # Check employee_projects FKs (should have 2)
        ep_fks = pg_adapter.get_foreign_keys('employee_projects')
        print(f"  employee_projects FKs: {ep_fks}")
        assert len(ep_fks) == 2, f"employee_projects should have 2 FKs, got {len(ep_fks)}"

        print("✓ Foreign key introspection successful")

    def test_l1_3_foreign_key_enforcement(self, pg_adapter):
        """
        L1.3b: Verify FK enforcement (invalid reference should fail)

        Requirement: FK constraint prevents invalid references
        """
        print("\n=== L1.3b: Foreign key enforcement ===")

        # Try to insert employee with invalid dept_id (should fail)
        result = pg_adapter.execute_query(
            "INSERT INTO employees (email, first_name, last_name, dept_id, hire_date) "
            "VALUES ('invalid@example.com', 'Invalid', 'User', 9999, '2024-01-01')"
        )

        assert not result['success'], "FK violation insert should fail"
        assert 'foreign key' in result.get('error', '').lower() or 'violates' in result.get('error', '').lower(), \
            f"Expected FK violation error, got: {result.get('error')}"

        print("✓ Foreign key enforcement verified (invalid reference rejected)")


class TestPhase11PostgreSQLIndexesL1:
    """PostgreSQL L1 harness tests - indexes"""

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

    def test_l1_4_index_introspection(self, pg_adapter):
        """
        L1.4: Introspect indexes

        Requirement: get_indexes() returns user-defined indexes
        Expected: idx_employees_dept, idx_employees_name
        """
        print("\n=== L1.4: Index introspection ===")

        indexes = pg_adapter.get_indexes('employees')
        print(f"  employees indexes: {indexes}")

        index_names = [idx.get('name', '').lower() for idx in indexes]

        # Should have our custom indexes (may also have PK/unique indexes)
        assert 'idx_employees_dept' in index_names, "Should have idx_employees_dept"
        assert 'idx_employees_name' in index_names, "Should have idx_employees_name"

        print("✓ Index introspection successful")


class TestPhase11PostgreSQLIdentityL1:
    """PostgreSQL L1 harness tests - identity/serial"""

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

    def test_l1_5_identity_insert_verification(self, pg_adapter):
        """
        L1.5: Verify identity/serial auto-increment

        Requirement: SERIAL columns auto-generate values on insert
        """
        print("\n=== L1.5: Identity/serial verification ===")

        # Get current max emp_id
        result = pg_adapter.execute_query("SELECT MAX(emp_id) as max_id FROM employees")
        current_max = result['data'][0]['max_id'] if result['data'] else 0
        print(f"  Current max emp_id: {current_max}")

        # Insert without specifying emp_id (should auto-generate)
        insert_result = pg_adapter.execute_query(
            "INSERT INTO employees (email, first_name, last_name, dept_id, hire_date) "
            "VALUES ('serial_test@example.com', 'Serial', 'Test', 1, '2024-06-01') "
            "RETURNING emp_id"
        )

        assert insert_result['success'], f"Insert should succeed: {insert_result.get('error')}"
        new_id = insert_result['data'][0]['emp_id']
        print(f"  New auto-generated emp_id: {new_id}")

        assert new_id > current_max, f"New ID ({new_id}) should be greater than previous max ({current_max})"

        # Cleanup
        pg_adapter.execute_query(f"DELETE FROM employees WHERE emp_id = {new_id}")

        print("✓ Identity/serial auto-increment verified")


class TestPhase11PostgreSQLL1Requirements:
    """Verify all L1 requirements met"""

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

    def test_all_l1_methods_exist(self, pg_adapter):
        """
        Verify adapter implements all L1 methods

        Requirements:
        - get_primary_keys() exists
        - get_foreign_keys() exists
        - get_unique_constraints() exists
        - get_indexes() exists
        """
        print("\n=== Verifying all L1 requirements ===")

        required_methods = [
            'get_primary_keys',
            'get_foreign_keys',
            'get_unique_constraints',
            'get_indexes'
        ]

        for method in required_methods:
            assert hasattr(pg_adapter, method), f"Missing {method}() method"
            assert callable(getattr(pg_adapter, method)), f"{method}() not callable"

        print("✓ All L1 methods exist")
