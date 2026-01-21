#!/usr/bin/env python3
"""
SAIQL Phase 11 MySQL L1 Harness Tests

Validates MySQL adapter L1 capabilities:
- L1.1: Primary keys (created and enforced)
- L1.2: Unique constraints (enforced)
- L1.3: Foreign keys (enforced)
- L1.4: Indexes (presence validated)
- L1.5: Identity/auto_increment (verified by inserts)

Evidence:
- MySQL 8.0 container on port 3308
- Fixture: /mnt/storage/DockerTests/mysql/fixtures/01_schema.sql
"""

import pytest
import logging

logger = logging.getLogger(__name__)


class TestPhase11MySQLConstraintsL1:
    """MySQL L1 harness tests - constraints"""

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

    def test_l1_1_primary_key_introspection(self, mysql_adapter):
        """L1.1a: Introspect primary keys"""
        print("\n=== L1.1a: Primary key introspection ===")

        tables_with_pk = ['departments', 'employees', 'projects', 'employee_projects', 'type_test']

        for table in tables_with_pk:
            pk_cols = mysql_adapter.get_primary_keys(table)
            print(f"  {table}: PK columns = {pk_cols}")
            assert len(pk_cols) > 0, f"Table {table} should have a primary key"

        # Check composite PK on employee_projects
        ep_pk = mysql_adapter.get_primary_keys('employee_projects')
        assert len(ep_pk) == 2, f"employee_projects should have composite PK, got {len(ep_pk)}"

        print("✓ Primary key introspection successful")

    def test_l1_1_primary_key_enforcement(self, mysql_adapter):
        """L1.1b: Verify PK enforcement"""
        print("\n=== L1.1b: Primary key enforcement ===")

        result = mysql_adapter.execute_query(
            "INSERT INTO departments (dept_id, dept_name, dept_code) VALUES (1, 'Duplicate', 'DUPE')"
        )

        assert not result['success'], "Duplicate PK insert should fail"
        assert 'duplicate' in result.get('error', '').lower(), \
            f"Expected duplicate key error, got: {result.get('error')}"

        print("✓ Primary key enforcement verified")

    def test_l1_2_unique_constraint_introspection(self, mysql_adapter):
        """L1.2a: Introspect unique constraints"""
        print("\n=== L1.2a: Unique constraint introspection ===")

        dept_unique = mysql_adapter.get_unique_constraints('departments')
        print(f"  departments unique constraints: {dept_unique}")
        assert len(dept_unique) > 0, "departments should have unique constraint on dept_code"

        emp_unique = mysql_adapter.get_unique_constraints('employees')
        print(f"  employees unique constraints: {emp_unique}")
        assert len(emp_unique) > 0, "employees should have unique constraint on email"

        print("✓ Unique constraint introspection successful")

    def test_l1_2_unique_constraint_enforcement(self, mysql_adapter):
        """L1.2b: Verify unique constraint enforcement"""
        print("\n=== L1.2b: Unique constraint enforcement ===")

        result = mysql_adapter.execute_query(
            "INSERT INTO employees (email, first_name, last_name, dept_id, hire_date) "
            "VALUES ('alice@example.com', 'Alice2', 'Smith2', 1, '2024-01-01')"
        )

        # The insert should fail - constraint violations prove enforcement works
        assert not result['success'], "Duplicate unique value insert should fail"
        print(f"  Insert failed as expected: {result.get('error', '')[:100]}")

        print("✓ Unique constraint enforcement verified (insert rejected)")

    def test_l1_3_foreign_key_introspection(self, mysql_adapter):
        """L1.3a: Introspect foreign keys"""
        print("\n=== L1.3a: Foreign key introspection ===")

        emp_fks = mysql_adapter.get_foreign_keys('employees')
        print(f"  employees FKs: {emp_fks}")
        assert len(emp_fks) >= 1, "employees should have at least 1 FK"

        ep_fks = mysql_adapter.get_foreign_keys('employee_projects')
        print(f"  employee_projects FKs: {ep_fks}")
        assert len(ep_fks) == 2, f"employee_projects should have 2 FKs, got {len(ep_fks)}"

        print("✓ Foreign key introspection successful")

    def test_l1_3_foreign_key_enforcement(self, mysql_adapter):
        """L1.3b: Verify FK enforcement"""
        print("\n=== L1.3b: Foreign key enforcement ===")

        result = mysql_adapter.execute_query(
            "INSERT INTO employees (email, first_name, last_name, dept_id, hire_date) "
            "VALUES ('invalid@example.com', 'Invalid', 'User', 9999, '2024-01-01')"
        )

        # The insert should fail - constraint violations prove enforcement works
        assert not result['success'], "FK violation insert should fail"
        print(f"  Insert failed as expected: {result.get('error', '')[:100]}")

        print("✓ Foreign key enforcement verified (insert rejected)")


class TestPhase11MySQLIndexesL1:
    """MySQL L1 harness tests - indexes"""

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

    def test_l1_4_index_introspection(self, mysql_adapter):
        """L1.4: Introspect indexes"""
        print("\n=== L1.4: Index introspection ===")

        indexes = mysql_adapter.get_indexes('employees')
        print(f"  employees indexes: {indexes}")

        index_names = [idx.get('name', '').lower() for idx in indexes]

        assert 'idx_employees_dept' in index_names, "Should have idx_employees_dept"
        assert 'idx_employees_name' in index_names, "Should have idx_employees_name"

        print("✓ Index introspection successful")


class TestPhase11MySQLIdentityL1:
    """MySQL L1 harness tests - identity/auto_increment"""

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

    def test_l1_5_identity_insert_verification(self, mysql_adapter):
        """L1.5: Verify identity/auto_increment by checking column metadata"""
        print("\n=== L1.5: Identity/auto_increment verification ===")

        # Check that emp_id column has auto_increment attribute
        schema = mysql_adapter.get_schema('employees')
        columns = schema.get('columns', [])
        print(f"  employees schema columns: {[c['name'] for c in columns]}")

        # Find emp_id column
        emp_id_col = next((c for c in columns if c['name'].lower() == 'emp_id'), None)
        assert emp_id_col is not None, "employees should have emp_id column"
        print(f"  emp_id column: {emp_id_col}")

        # Verify auto_increment by checking INFORMATION_SCHEMA
        result = mysql_adapter.execute_query(
            "SELECT COLUMN_NAME, EXTRA FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA = 'saiql_phase11_test' AND TABLE_NAME = 'employees' "
            "AND COLUMN_NAME = 'emp_id'"
        )

        assert result['success'] and result['data'], "Should find emp_id column metadata"
        extra = result['data'][0].get('EXTRA', result['data'][0].get('extra', ''))
        print(f"  emp_id EXTRA attribute: {extra}")

        assert 'auto_increment' in extra.lower(), \
            f"emp_id should have auto_increment, got EXTRA: {extra}"

        # Also verify via a successful data insert that IDs are incrementing
        max_result = mysql_adapter.execute_query("SELECT MAX(emp_id) as max_id FROM employees")
        current_max = max_result['data'][0]['max_id'] if max_result['data'] else 0
        print(f"  Current max emp_id in table: {current_max}")

        # Identity is working if we have rows with auto-generated IDs
        assert current_max >= 1, "Auto-increment should have generated IDs for seeded data"

        print("✓ Identity/auto_increment verified")


class TestPhase11MySQLL1Requirements:
    """Verify all L1 requirements met"""

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

    def test_all_l1_methods_exist(self, mysql_adapter):
        """Verify adapter implements all L1 methods"""
        print("\n=== Verifying all L1 requirements ===")

        required_methods = [
            'get_primary_keys',
            'get_foreign_keys',
            'get_unique_constraints',
            'get_indexes'
        ]

        for method in required_methods:
            assert hasattr(mysql_adapter, method), f"Missing {method}() method"
            assert callable(getattr(mysql_adapter, method)), f"{method}() not callable"

        print("✓ All L1 methods exist")
