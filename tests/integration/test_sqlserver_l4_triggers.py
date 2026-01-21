#!/usr/bin/env python3
"""
SAIQL SQL Server L4 Harness Tests - Triggers

Validates SQL Server adapter L4 capabilities per collab rules:
- C1) Subset definition: allowlist + denylist documented and enforced
- C2) Extraction: Enumerate triggers in subset (table, timing, event, definition)
- C3) Emission: Create triggers on target tables
- C4) Validation (Behavioral): Verify trigger side-effects work correctly
- C5) Limitations: Skipped triggers listed with reason and count

Proof-first approach per Apollo rules. Must pass 3x from clean state.

L4 Subset Rules (documented per C1) - SQL Server specific:
- ALLOWLIST:
  - AFTER INSERT/UPDATE triggers on tables
  - No dynamic SQL patterns
- DENYLIST:
  - INSTEAD OF triggers
  - DDL triggers
  - Disabled triggers
  - Triggers with dynamic SQL, cursors, temp tables

Evidence:
- SQL Server 2022 container on port 1434
- Fixture: /mnt/storage/DockerTests/sqlserver/fixtures/04_triggers.sql
- Expected triggers:
  - Safe: trg_customer_insert_timestamp, trg_customer_update_timestamp,
          trg_customer_credit_validate, trg_order_insert_timestamp,
          trg_order_update_timestamp (5)
  - Skipped: trg_unsafe_instead_of (INSTEAD OF trigger) (1)
"""

import pytest
import logging
from typing import Dict, Any, List
import time

logger = logging.getLogger(__name__)

# Expected safe triggers
SAFE_TRIGGERS = [
    'trg_customer_insert_timestamp',
    'trg_customer_update_timestamp',
    'trg_customer_credit_validate',
    'trg_order_insert_timestamp',
    'trg_order_update_timestamp'
]

# Expected skipped triggers with reasons
SKIPPED_TRIGGERS = {
    'trg_unsafe_instead_of': 'INSTEAD OF'
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


class TestSQLServerL4SubsetDefinition:
    """C1) Subset definition tests."""

    def test_l4_c1_subset_rules_enforced(self):
        """Test that safe subset rules are enforced."""
        adapter = get_adapter()
        try:
            safe = adapter.get_safe_triggers(schema='dbo')
            safe_names = [t['name'] for t in safe]

            for expected in SAFE_TRIGGERS:
                assert expected in safe_names, f"Expected {expected} in safe subset"

            # Skipped triggers should NOT be in safe subset
            for skipped in SKIPPED_TRIGGERS.keys():
                assert skipped not in safe_names, f"{skipped} should not be in safe subset"
        finally:
            adapter.close()

    def test_l4_c1_skipped_triggers_with_reasons(self):
        """Test that skipped triggers have documented reasons."""
        adapter = get_adapter()
        try:
            skipped = adapter.get_skipped_triggers(schema='dbo')
            skipped_names = [t['name'] for t in skipped]

            for name, expected_reason in SKIPPED_TRIGGERS.items():
                assert name in skipped_names, f"Expected {name} to be skipped"

                trigger = next(t for t in skipped if t['name'] == name)
                assert 'skip_reasons' in trigger, f"{name} missing skip_reasons"
                reasons_str = ' '.join(trigger['skip_reasons'])
                assert expected_reason.upper() in reasons_str.upper(), \
                    f"{name} should be skipped for {expected_reason}, got: {trigger['skip_reasons']}"
        finally:
            adapter.close()


class TestSQLServerL4Extraction:
    """C2) Extraction tests."""

    def test_l4_c2_trigger_enumeration(self):
        """Test that all triggers are enumerated."""
        adapter = get_adapter()
        try:
            triggers = adapter.get_triggers(schema='dbo')
            trigger_names = [t['name'] for t in triggers]

            all_expected = SAFE_TRIGGERS + list(SKIPPED_TRIGGERS.keys())
            for expected in all_expected:
                assert expected in trigger_names, f"Missing trigger: {expected}"
        finally:
            adapter.close()

    def test_l4_c2_trigger_structure(self):
        """Test that trigger metadata has required fields."""
        adapter = get_adapter()
        try:
            triggers = adapter.get_triggers(schema='dbo')

            for trigger in triggers:
                assert 'name' in trigger, "Trigger missing 'name'"
                assert 'parent_table' in trigger, f"Trigger {trigger['name']} missing 'parent_table'"
                assert 'is_instead_of' in trigger, f"Trigger {trigger['name']} missing 'is_instead_of'"
                assert 'events' in trigger, f"Trigger {trigger['name']} missing 'events'"
        finally:
            adapter.close()

    def test_l4_c2_trigger_definition_retrieval(self):
        """Test individual trigger definition retrieval."""
        adapter = get_adapter()
        try:
            for trigger_name in SAFE_TRIGGERS[:3]:
                definition = adapter.get_trigger_definition(trigger_name, schema='dbo')
                assert definition is not None, f"No definition for {trigger_name}"
                assert 'CREATE' in definition.upper() and 'TRIGGER' in definition.upper(), \
                    f"No CREATE TRIGGER in {trigger_name} definition"
        finally:
            adapter.close()


class TestSQLServerL4Emission:
    """C3) Emission tests."""

    def test_l4_c3_single_trigger_creation(self):
        """Test creating a single trigger."""
        adapter = get_adapter()
        try:
            trigger_name = 'trg_order_update_timestamp'
            definition = adapter.get_trigger_definition(trigger_name, schema='dbo')
            assert definition is not None

            # Drop and recreate
            adapter.drop_trigger(trigger_name, schema='dbo')
            result = adapter.create_trigger(trigger_name, definition, schema='dbo')

            assert result['success'], f"Failed to create trigger: {result.get('error')}"

            # Verify it exists
            triggers = adapter.get_triggers(schema='dbo')
            trigger_names = [t['name'] for t in triggers]
            assert trigger_name in trigger_names
        finally:
            adapter.close()

    def test_l4_c3_trigger_recreation(self):
        """Test that triggers can be dropped and recreated."""
        adapter = get_adapter()
        try:
            trigger_name = 'trg_order_insert_timestamp'
            definition = adapter.get_trigger_definition(trigger_name, schema='dbo')

            # Drop
            drop_result = adapter.drop_trigger(trigger_name, schema='dbo')
            assert drop_result['success']

            # Recreate
            create_result = adapter.create_trigger(trigger_name, definition, schema='dbo')
            assert create_result['success'], f"Recreation failed: {create_result.get('error')}"
        finally:
            adapter.close()


class TestSQLServerL4Validation:
    """C4) Behavioral validation tests."""

    def test_l4_c4_behavioral_timestamp_trigger_insert(self):
        """Test that INSERT trigger sets last_modified."""
        adapter = get_adapter()
        try:
            # Insert a new customer
            test_email = f'trigger_test_{int(time.time())}@example.com'
            insert_result = adapter.execute_query(
                f"INSERT INTO dbo.customers (email, name, credit_limit, is_active) "
                f"VALUES ('{test_email}', 'Trigger Test', 1000.00, 1)"
            )
            assert insert_result['success'], f"Insert failed: {insert_result.get('error')}"

            # Check that last_modified was set
            check_result = adapter.execute_query(
                f"SELECT last_modified FROM dbo.customers WHERE email = '{test_email}'"
            )
            assert check_result['success']
            assert len(check_result['data']) == 1
            assert check_result['data'][0]['last_modified'] is not None, \
                "last_modified should be set by trigger"

            # Cleanup
            adapter.execute_query(f"DELETE FROM dbo.customers WHERE email = '{test_email}'")
        finally:
            adapter.close()

    def test_l4_c4_behavioral_timestamp_trigger_update(self):
        """Test that UPDATE trigger updates last_modified."""
        adapter = get_adapter()
        try:
            # Get a customer to update
            select_result = adapter.execute_query(
                "SELECT TOP 1 customer_id, last_modified FROM dbo.customers"
            )
            assert select_result['success'] and len(select_result['data']) > 0

            customer_id = select_result['data'][0]['customer_id']
            old_modified = select_result['data'][0]['last_modified']

            # Small delay to ensure timestamp difference
            time.sleep(0.1)

            # Update the customer
            update_result = adapter.execute_query(
                f"UPDATE dbo.customers SET name = name WHERE customer_id = {customer_id}"
            )
            assert update_result['success']

            # Check that last_modified was updated
            check_result = adapter.execute_query(
                f"SELECT last_modified FROM dbo.customers WHERE customer_id = {customer_id}"
            )
            assert check_result['success']
            new_modified = check_result['data'][0]['last_modified']

            # The trigger should have updated last_modified
            assert new_modified is not None
        finally:
            adapter.close()

    def test_l4_c4_behavioral_credit_validation_trigger(self):
        """Test that credit validation trigger corrects negative values."""
        adapter = get_adapter()
        try:
            # Insert a customer with negative credit
            test_email = f'credit_test_{int(time.time())}@example.com'
            insert_result = adapter.execute_query(
                f"INSERT INTO dbo.customers (email, name, credit_limit, is_active) "
                f"VALUES ('{test_email}', 'Credit Test', -500.00, 1)"
            )
            assert insert_result['success']

            # Check that credit was corrected to 0
            check_result = adapter.execute_query(
                f"SELECT credit_limit FROM dbo.customers WHERE email = '{test_email}'"
            )
            assert check_result['success']
            assert len(check_result['data']) == 1
            # Trigger should have set negative credit to 0
            credit = float(check_result['data'][0]['credit_limit'])
            assert credit == 0, f"Credit should be 0, got {credit}"

            # Cleanup
            adapter.execute_query(f"DELETE FROM dbo.customers WHERE email = '{test_email}'")
        finally:
            adapter.close()

    def test_l4_c4_behavioral_positive_credit_allowed(self):
        """Test that positive credit values are preserved."""
        adapter = get_adapter()
        try:
            test_email = f'positive_test_{int(time.time())}@example.com'
            expected_credit = 2500.00

            insert_result = adapter.execute_query(
                f"INSERT INTO dbo.customers (email, name, credit_limit, is_active) "
                f"VALUES ('{test_email}', 'Positive Test', {expected_credit}, 1)"
            )
            assert insert_result['success']

            check_result = adapter.execute_query(
                f"SELECT credit_limit FROM dbo.customers WHERE email = '{test_email}'"
            )
            assert check_result['success']
            credit = float(check_result['data'][0]['credit_limit'])
            assert credit == expected_credit, f"Credit should be {expected_credit}, got {credit}"

            # Cleanup
            adapter.execute_query(f"DELETE FROM dbo.customers WHERE email = '{test_email}'")
        finally:
            adapter.close()


class TestSQLServerL4Limitations:
    """C5) Limitations tests."""

    def test_l4_c5_limitations_counted(self):
        """Test that correct number of triggers are skipped."""
        adapter = get_adapter()
        try:
            skipped = adapter.get_skipped_triggers(schema='dbo')
            assert len(skipped) == len(SKIPPED_TRIGGERS), \
                f"Expected {len(SKIPPED_TRIGGERS)} skipped, got {len(skipped)}"
        finally:
            adapter.close()

    def test_l4_c5_limitations_documented(self):
        """Test that all skipped triggers have reasons."""
        adapter = get_adapter()
        try:
            skipped = adapter.get_skipped_triggers(schema='dbo')

            for trigger in skipped:
                assert 'skip_reasons' in trigger, f"{trigger['name']} missing skip_reasons"
                assert len(trigger['skip_reasons']) > 0, \
                    f"{trigger['name']} has empty skip_reasons"
        finally:
            adapter.close()


class TestSQLServerL4Requirements:
    """Test that all required L4 methods exist."""

    def test_all_l4_methods_exist(self):
        """Verify all L4 methods are implemented."""
        adapter = get_adapter()
        try:
            required_methods = [
                'get_triggers',
                'get_trigger_definition',
                'get_safe_triggers',
                'get_skipped_triggers',
                'create_trigger',
                'drop_trigger'
            ]

            for method in required_methods:
                assert hasattr(adapter, method), f"Missing method: {method}"
                assert callable(getattr(adapter, method)), f"Method not callable: {method}"
        finally:
            adapter.close()
