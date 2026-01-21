#!/usr/bin/env python3
"""
SAIQL SQLite L4 Harness Tests - Triggers

Validates SQLite adapter L4 capabilities per collab rules:
- C1) Subset definition: allowlist + denylist documented and enforced
- C2) Extraction: Enumerate triggers in subset (table, timing, event, definition)
- C3) Emission: Create triggers on target tables
- C4) Validation (Behavioral): Verify trigger side-effects work correctly
- C5) Limitations: Skipped triggers listed with reason and count

Proof-first approach per Apollo rules. Must pass 3x from clean state.

Per rules_SQLite_L2L3L4.md:
- Rule 5: Clean state per run (fresh DB file per run_id + teardown)
- Rule 8: PRAGMA settings must be fixed, not rely on defaults

Evidence:
- SQLite file-based database (fresh per test class, torn down after)
- Fixture: /mnt/storage/DockerTests/sqlite/fixtures/04_triggers.sql
- Expected triggers:
  - Safe: trg_employee_insert_audit, trg_employee_update_audit,
          trg_employee_delete_audit, trg_employee_salary_validate,
          trg_employee_salary_validate_update, trg_department_budget_check (6)
  - Skipped: None (all are BEFORE/AFTER on tables)
"""

import pytest
import logging
import uuid
from typing import Dict, Any, List
from pathlib import Path

logger = logging.getLogger(__name__)

# Expected safe triggers
SAFE_TRIGGERS = [
    'trg_employee_insert_audit',
    'trg_employee_update_audit',
    'trg_employee_delete_audit',
    'trg_employee_salary_validate',
    'trg_employee_salary_validate_update',
    'trg_department_budget_check'
]

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

    for fixture in ['01_schema.sql', '04_triggers.sql']:
        fixture_path = fixture_dir / fixture
        with open(fixture_path, 'r') as f:
            script = f.read()
        result = adapter.execute_script(script)
        assert result['success'], f"Failed to load {fixture}: {result.get('error')}"


@pytest.fixture(scope='class')
def db_path(tmp_path_factory):
    """Create a fresh DB file per test class (Rule 5: clean state per run)."""
    run_id = str(uuid.uuid4())[:8]
    run_dir = tmp_path_factory.mktemp(f"sqlite_l4_{run_id}")
    db_file = run_dir / f"test_l4_{run_id}.sqlite"
    yield str(db_file)
    # Teardown: file automatically cleaned up by pytest tmp_path_factory


@pytest.fixture(scope='class')
def adapter(db_path):
    """Class-scoped adapter with fixtures loaded."""
    adapter = get_adapter(db_path)
    load_fixtures(adapter)
    yield adapter
    adapter.close()


class TestSQLiteL4SubsetDefinition:
    """C1) Subset definition tests."""

    def test_l4_c1_subset_rules_enforced(self, adapter):
        """Test that safe subset rules are enforced."""
        safe = adapter.get_safe_triggers()
        safe_names = [t['name'] for t in safe]

        for expected in SAFE_TRIGGERS:
            assert expected in safe_names, f"Expected {expected} in safe subset"

    def test_l4_c1_no_skipped_triggers(self, adapter):
        """Test that there are no skipped triggers (all are safe in fixture)."""
        skipped = adapter.get_skipped_triggers()

        # Our fixture has no INSTEAD OF triggers, so none should be skipped
        assert len(skipped) == 0, f"Expected 0 skipped, got {len(skipped)}: {[t['name'] for t in skipped]}"


class TestSQLiteL4Extraction:
    """C2) Extraction tests."""

    def test_l4_c2_trigger_enumeration(self, adapter):
        """Test that all triggers are enumerated."""
        triggers = adapter.get_triggers()
        trigger_names = [t['name'] for t in triggers]

        for expected in SAFE_TRIGGERS:
            assert expected in trigger_names, f"Missing trigger: {expected}"

    def test_l4_c2_trigger_structure(self, adapter):
        """Test that trigger metadata has required fields."""
        triggers = adapter.get_triggers()

        for trigger in triggers:
            assert 'name' in trigger, "Trigger missing 'name'"
            assert 'table_name' in trigger, f"Trigger {trigger['name']} missing 'table_name'"
            assert 'timing' in trigger, f"Trigger {trigger['name']} missing 'timing'"
            assert 'event' in trigger, f"Trigger {trigger['name']} missing 'event'"
            assert 'definition' in trigger, f"Trigger {trigger['name']} missing 'definition'"

    def test_l4_c2_trigger_timing_event_parsing(self, adapter):
        """Test that trigger timing and event are correctly parsed."""
        triggers = adapter.get_triggers()
        trigger_dict = {t['name']: t for t in triggers}

        # Check specific triggers
        insert_audit = trigger_dict.get('trg_employee_insert_audit')
        assert insert_audit is not None
        assert insert_audit['timing'] == 'AFTER'
        assert insert_audit['event'] == 'INSERT'

        salary_validate = trigger_dict.get('trg_employee_salary_validate')
        assert salary_validate is not None
        assert salary_validate['timing'] == 'BEFORE'
        assert salary_validate['event'] == 'INSERT'

    def test_l4_c2_trigger_definition_retrieval(self, adapter):
        """Test individual trigger definition retrieval."""
        for trigger_name in SAFE_TRIGGERS[:3]:
            definition = adapter.get_trigger_definition(trigger_name)
            assert definition is not None, f"No definition for {trigger_name}"
            assert 'CREATE TRIGGER' in definition.upper(), \
                f"No CREATE TRIGGER in {trigger_name} definition"


class TestSQLiteL4Emission:
    """C3) Emission tests."""

    def test_l4_c3_single_trigger_creation(self, adapter):
        """Test creating a single trigger."""
        trigger_name = 'trg_employee_delete_audit'
        definition = adapter.get_trigger_definition(trigger_name)
        assert definition is not None

        # Drop and recreate
        adapter.drop_trigger(trigger_name)
        result = adapter.create_trigger(trigger_name, definition)

        assert result['success'], f"Failed to create trigger: {result.get('error')}"

        # Verify it exists
        triggers = adapter.get_triggers()
        trigger_names = [t['name'] for t in triggers]
        assert trigger_name in trigger_names

    def test_l4_c3_trigger_recreation(self, adapter):
        """Test that triggers can be dropped and recreated."""
        trigger_name = 'trg_employee_update_audit'
        definition = adapter.get_trigger_definition(trigger_name)

        # Drop
        drop_result = adapter.drop_trigger(trigger_name)
        assert drop_result['success']

        # Recreate
        create_result = adapter.create_trigger(trigger_name, definition)
        assert create_result['success'], f"Recreation failed: {create_result.get('error')}"


class TestSQLiteL4Validation:
    """C4) Behavioral validation tests."""

    def test_l4_c4_behavioral_audit_trigger_insert(self, adapter):
        """Test that INSERT audit trigger logs to audit_log."""
        # Clear audit log
        adapter.execute_query("DELETE FROM audit_log")

        # Insert a new employee
        insert_result = adapter.execute_query(
            "INSERT INTO employees (email, first_name, last_name, dept_id, hire_date, salary, is_active) "
            "VALUES ('test_audit@example.com', 'Test', 'Audit', 1, '2024-01-01', 50000, 1)"
        )
        assert insert_result['success'], f"Insert failed: {insert_result.get('error')}"

        # Check audit log
        audit_result = adapter.execute_query(
            "SELECT * FROM audit_log WHERE table_name = 'employees' AND action = 'INSERT'"
        )
        assert audit_result['success']
        assert len(audit_result['data']) >= 1, "Audit log should have INSERT entry"

        # Cleanup
        adapter.execute_query("DELETE FROM employees WHERE email = 'test_audit@example.com'")

    def test_l4_c4_behavioral_audit_trigger_update(self, adapter):
        """Test that UPDATE audit trigger logs to audit_log."""
        # Clear audit log
        adapter.execute_query("DELETE FROM audit_log")

        # Update an existing employee
        update_result = adapter.execute_query(
            "UPDATE employees SET salary = salary + 1000 WHERE emp_id = 1"
        )
        assert update_result['success'], f"Update failed: {update_result.get('error')}"

        # Check audit log
        audit_result = adapter.execute_query(
            "SELECT * FROM audit_log WHERE table_name = 'employees' AND action = 'UPDATE'"
        )
        assert audit_result['success']
        assert len(audit_result['data']) >= 1, "Audit log should have UPDATE entry"

    def test_l4_c4_behavioral_audit_trigger_delete(self, adapter):
        """Test that DELETE audit trigger logs to audit_log."""
        # Insert a test record to delete
        adapter.execute_query(
            "INSERT INTO employees (email, first_name, last_name, dept_id, hire_date, salary, is_active) "
            "VALUES ('delete_test@example.com', 'Delete', 'Test', 1, '2024-01-01', 50000, 1)"
        )

        # Clear audit log
        adapter.execute_query("DELETE FROM audit_log")

        # Delete the employee
        delete_result = adapter.execute_query(
            "DELETE FROM employees WHERE email = 'delete_test@example.com'"
        )
        assert delete_result['success'], f"Delete failed: {delete_result.get('error')}"

        # Check audit log
        audit_result = adapter.execute_query(
            "SELECT * FROM audit_log WHERE table_name = 'employees' AND action = 'DELETE'"
        )
        assert audit_result['success']
        assert len(audit_result['data']) >= 1, "Audit log should have DELETE entry"

    def test_l4_c4_behavioral_salary_validation_trigger(self, adapter):
        """Test that salary validation trigger rejects negative salary."""
        # Try to insert employee with negative salary
        insert_result = adapter.execute_query(
            "INSERT INTO employees (email, first_name, last_name, dept_id, hire_date, salary, is_active) "
            "VALUES ('negative@example.com', 'Neg', 'Salary', 1, '2024-01-01', -1000, 1)"
        )

        # Should fail due to trigger
        assert not insert_result['success'], "Insert with negative salary should fail"
        assert 'Salary cannot be negative' in str(insert_result.get('error', '')), \
            f"Error should mention salary validation: {insert_result.get('error')}"

    def test_l4_c4_behavioral_positive_salary_allowed(self, adapter):
        """Test that positive salary values are allowed."""
        expected_salary = 75000

        insert_result = adapter.execute_query(
            f"INSERT INTO employees (email, first_name, last_name, dept_id, hire_date, salary, is_active) "
            f"VALUES ('positive@example.com', 'Pos', 'Salary', 1, '2024-01-01', {expected_salary}, 1)"
        )
        assert insert_result['success'], f"Insert failed: {insert_result.get('error')}"

        # Verify salary was stored correctly
        check_result = adapter.execute_query(
            "SELECT salary FROM employees WHERE email = 'positive@example.com'"
        )
        assert check_result['success']
        assert len(check_result['data']) == 1
        assert check_result['data'][0]['salary'] == expected_salary

        # Cleanup
        adapter.execute_query("DELETE FROM employees WHERE email = 'positive@example.com'")

    def test_l4_c4_behavioral_budget_validation_trigger(self, adapter):
        """Test that budget validation trigger rejects negative budget."""
        # Try to update department with negative budget
        update_result = adapter.execute_query(
            "UPDATE departments SET budget = -1000 WHERE dept_id = 1"
        )

        # Should fail due to trigger
        assert not update_result['success'], "Update with negative budget should fail"
        assert 'Budget cannot be negative' in str(update_result.get('error', '')), \
            f"Error should mention budget validation: {update_result.get('error')}"


class TestSQLiteL4PragmaSettings:
    """Test PRAGMA settings for determinism."""

    def test_l4_pragma_settings_retrieval(self, adapter):
        """Test that PRAGMA settings can be retrieved."""
        pragmas = adapter.get_pragma_settings()

        assert 'foreign_keys' in pragmas
        assert 'recursive_triggers' in pragmas
        assert 'encoding' in pragmas

    def test_l4_pragma_enforcement(self, adapter):
        """Test that PRAGMAs are explicitly set (Rule 8)."""
        pragmas = adapter.get_pragma_settings()

        # Verify PRAGMAs match our baseline (not relying on defaults)
        assert pragmas.get('foreign_keys') == PRAGMA_BASELINE['foreign_keys'], \
            f"foreign_keys not set correctly: {pragmas.get('foreign_keys')}"
        assert pragmas.get('recursive_triggers') == PRAGMA_BASELINE['recursive_triggers'], \
            f"recursive_triggers not set correctly: {pragmas.get('recursive_triggers')}"


class TestSQLiteL4Requirements:
    """Test that all required L4 methods exist."""

    def test_all_l4_methods_exist(self, adapter):
        """Verify all L4 methods are implemented."""
        required_methods = [
            'get_triggers',
            'get_trigger_definition',
            'get_safe_triggers',
            'get_skipped_triggers',
            'create_trigger',
            'drop_trigger',
            'get_pragma_settings',
            'set_pragma'
        ]

        for method in required_methods:
            assert hasattr(adapter, method), f"Missing method: {method}"
            assert callable(getattr(adapter, method)), f"Method not callable: {method}"
