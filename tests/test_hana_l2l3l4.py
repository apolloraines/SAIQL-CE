#!/usr/bin/env python3
"""
SAIQL HANA Adapter L2/L3/L4 Unit Tests

Tests L2 (Views), L3 (Routines), L4 (Triggers) functionality with mocked connections.

Unit tests use mocking and should run without hdbcli installed.

Author: Claude (Phase HANA L2L3L4)
Status: Development
"""

import pytest
import logging
from unittest.mock import Mock, MagicMock, patch
from typing import Dict, Any
import sys

logger = logging.getLogger(__name__)

# Mock hdbcli if not available so adapter can be imported
if 'hdbcli' not in sys.modules:
    sys.modules['hdbcli'] = MagicMock()
    sys.modules['hdbcli.dbapi'] = MagicMock()

from extensions.plugins.hana_adapter import HANAAdapter


# ===== L2 Tests (Views) =====

class TestHANAAdapterL2Views:
    """Test L2 view extraction methods."""

    @patch('extensions.plugins.hana_adapter.dbapi')
    def test_get_views_returns_list(self, mock_dbapi):
        """Test get_views() returns list of view dicts."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_dbapi.connect.return_value = mock_conn

        # Mock views query
        def mock_execute(query, params=None):
            if 'SYS.VIEWS' in query and 'VIEW_NAME =' not in query:
                mock_cursor.description = [('VIEW_NAME',), ('SCHEMA_NAME',), ('DEFINITION',)]
                mock_cursor.fetchall.return_value = [
                    ('USER_SUMMARY', 'TEST_SCHEMA', 'SELECT id, name FROM users'),
                    ('ORDER_DETAILS', 'TEST_SCHEMA', 'SELECT o.id, u.name FROM orders o JOIN users u ON o.user_id = u.id'),
                ]
            elif 'OBJECT_DEPENDENCIES' in query:
                mock_cursor.description = [('DEPENDENT_OBJECT_NAME',)]
                mock_cursor.fetchall.return_value = []

        mock_cursor.execute.side_effect = mock_execute

        config = {
            'host': 'hana-test.example.com',
            'user': 'test_user',
            'password': 'test_password'
        }

        adapter = HANAAdapter(config)
        views = adapter.get_views()

        assert len(views) == 2
        assert views[0]['name'] == 'user_summary'
        assert views[0]['type'] == 'SQL_VIEW'
        assert 'definition' in views[0]
        assert 'dependencies' in views[0]

    @patch('extensions.plugins.hana_adapter.dbapi')
    def test_get_view_by_name_returns_single_view(self, mock_dbapi):
        """Test get_view_by_name() returns a single view dict."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_dbapi.connect.return_value = mock_conn

        def mock_execute(query, params=None):
            if 'SYS.VIEWS' in query and 'VIEW_NAME = ?' in query:
                mock_cursor.description = [('VIEW_NAME',), ('SCHEMA_NAME',), ('DEFINITION',)]
                mock_cursor.fetchall.return_value = [
                    ('USER_SUMMARY', 'TEST_SCHEMA', 'SELECT id, name FROM users'),
                ]
            elif 'OBJECT_DEPENDENCIES' in query:
                mock_cursor.description = [('DEPENDENT_OBJECT_NAME',)]
                mock_cursor.fetchall.return_value = [('USERS',)]

        mock_cursor.execute.side_effect = mock_execute

        config = {
            'host': 'hana-test.example.com',
            'user': 'test_user',
            'password': 'test_password'
        }

        adapter = HANAAdapter(config)
        view = adapter.get_view_by_name('user_summary')

        assert view is not None
        assert view['name'] == 'user_summary'
        assert view['type'] == 'SQL_VIEW'
        assert view['dependencies'] == ['users']

    @patch('extensions.plugins.hana_adapter.dbapi')
    def test_get_view_by_name_returns_none_if_not_found(self, mock_dbapi):
        """Test get_view_by_name() returns None if view not found."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_dbapi.connect.return_value = mock_conn

        mock_cursor.fetchall.return_value = []

        config = {
            'host': 'hana-test.example.com',
            'user': 'test_user',
            'password': 'test_password'
        }

        adapter = HANAAdapter(config)
        view = adapter.get_view_by_name('nonexistent_view')

        assert view is None


# ===== L3 Tests (Routines) =====

class TestHANAAdapterL3Routines:
    """Test L3 routine extraction methods."""

    @patch('extensions.plugins.hana_adapter.dbapi')
    def test_get_routines_returns_procedures_and_functions(self, mock_dbapi):
        """Test get_routines() returns both procedures and functions."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_dbapi.connect.return_value = mock_conn

        def mock_execute(query, params=None):
            if 'SYS.PROCEDURES' in query and 'PROCEDURE_NAME = ?' not in query:
                mock_cursor.description = [
                    ('PROCEDURE_NAME',), ('SCHEMA_NAME',), ('DEFINITION',),
                    ('PROCEDURE_TYPE',), ('IS_VALID',), ('DEFAULT_SCHEMA_NAME',)
                ]
                mock_cursor.fetchall.return_value = [
                    ('UPDATE_USER', 'TEST_SCHEMA', 'BEGIN UPDATE users SET name = :name WHERE id = :id; END;',
                     'SQL', 'TRUE', None),
                ]
            elif 'SYS.FUNCTIONS' in query and 'FUNCTION_NAME = ?' not in query:
                mock_cursor.description = [
                    ('FUNCTION_NAME',), ('SCHEMA_NAME',), ('DEFINITION',),
                    ('FUNCTION_TYPE',), ('IS_VALID',), ('RETURN_TYPE',)
                ]
                mock_cursor.fetchall.return_value = [
                    ('GET_USER_NAME', 'TEST_SCHEMA', 'BEGIN RETURN SELECT name FROM users WHERE id = :id; END;',
                     'SQL', 'TRUE', 'NVARCHAR'),
                ]
            elif 'PROCEDURE_PARAMETERS' in query:
                mock_cursor.description = [
                    ('PARAMETER_NAME',), ('DATA_TYPE_NAME',), ('PARAMETER_TYPE',),
                    ('HAS_DEFAULT_VALUE',), ('POSITION',)
                ]
                mock_cursor.fetchall.return_value = [
                    ('ID', 'INTEGER', 'IN', 'FALSE', 1),
                    ('NAME', 'NVARCHAR', 'IN', 'FALSE', 2),
                ]
            elif 'FUNCTION_PARAMETERS' in query:
                mock_cursor.description = [
                    ('PARAMETER_NAME',), ('DATA_TYPE_NAME',), ('PARAMETER_TYPE',),
                    ('HAS_DEFAULT_VALUE',), ('POSITION',)
                ]
                mock_cursor.fetchall.return_value = [
                    ('ID', 'INTEGER', 'IN', 'FALSE', 1),
                ]
            elif 'OBJECT_DEPENDENCIES' in query:
                mock_cursor.description = [('DEPENDENT_OBJECT_NAME',), ('DEPENDENT_OBJECT_TYPE',)]
                mock_cursor.fetchall.return_value = [('USERS', 'TABLE')]

        mock_cursor.execute.side_effect = mock_execute

        config = {
            'host': 'hana-test.example.com',
            'user': 'test_user',
            'password': 'test_password'
        }

        adapter = HANAAdapter(config)
        routines = adapter.get_routines()

        assert len(routines) == 2

        procedures = [r for r in routines if r['type'] == 'PROCEDURE']
        functions = [r for r in routines if r['type'] == 'FUNCTION']

        assert len(procedures) == 1
        assert len(functions) == 1

        assert procedures[0]['name'] == 'update_user'
        assert procedures[0]['language'] == 'SQLSCRIPT'
        assert len(procedures[0]['arguments']) == 2

        assert functions[0]['name'] == 'get_user_name'
        assert functions[0]['return_type'] == 'NVARCHAR'
        assert len(functions[0]['arguments']) == 1

    @patch('extensions.plugins.hana_adapter.dbapi')
    def test_get_routine_by_name_returns_procedure(self, mock_dbapi):
        """Test get_routine_by_name() returns a procedure."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_dbapi.connect.return_value = mock_conn

        def mock_execute(query, params=None):
            if 'SYS.PROCEDURES' in query and 'PROCEDURE_NAME = ?' in query:
                mock_cursor.description = [
                    ('PROCEDURE_NAME',), ('SCHEMA_NAME',), ('DEFINITION',),
                    ('PROCEDURE_TYPE',), ('IS_VALID',)
                ]
                mock_cursor.fetchall.return_value = [
                    ('UPDATE_USER', 'TEST_SCHEMA', 'BEGIN UPDATE users SET name = :name WHERE id = :id; END;',
                     'SQL', 'TRUE'),
                ]
            elif 'PROCEDURE_PARAMETERS' in query:
                mock_cursor.description = [
                    ('PARAMETER_NAME',), ('DATA_TYPE_NAME',), ('PARAMETER_TYPE',),
                    ('HAS_DEFAULT_VALUE',), ('POSITION',)
                ]
                mock_cursor.fetchall.return_value = []
            elif 'OBJECT_DEPENDENCIES' in query:
                mock_cursor.description = [('DEPENDENT_OBJECT_NAME',), ('DEPENDENT_OBJECT_TYPE',)]
                mock_cursor.fetchall.return_value = []

        mock_cursor.execute.side_effect = mock_execute

        config = {
            'host': 'hana-test.example.com',
            'user': 'test_user',
            'password': 'test_password'
        }

        adapter = HANAAdapter(config)
        routine = adapter.get_routine_by_name('update_user')

        assert routine is not None
        assert routine['name'] == 'update_user'
        assert routine['type'] == 'PROCEDURE'

    @patch('extensions.plugins.hana_adapter.dbapi')
    def test_get_routine_by_name_returns_function_if_not_procedure(self, mock_dbapi):
        """Test get_routine_by_name() falls back to functions if not a procedure."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_dbapi.connect.return_value = mock_conn

        call_count = [0]

        def mock_execute(query, params=None):
            if 'SYS.PROCEDURES' in query and 'PROCEDURE_NAME = ?' in query:
                mock_cursor.description = [
                    ('PROCEDURE_NAME',), ('SCHEMA_NAME',), ('DEFINITION',),
                    ('PROCEDURE_TYPE',), ('IS_VALID',)
                ]
                mock_cursor.fetchall.return_value = []  # Not found as procedure
            elif 'SYS.FUNCTIONS' in query and 'FUNCTION_NAME = ?' in query:
                mock_cursor.description = [
                    ('FUNCTION_NAME',), ('SCHEMA_NAME',), ('DEFINITION',),
                    ('FUNCTION_TYPE',), ('IS_VALID',), ('RETURN_TYPE',)
                ]
                mock_cursor.fetchall.return_value = [
                    ('GET_USER_NAME', 'TEST_SCHEMA', 'BEGIN RETURN SELECT name FROM users WHERE id = :id; END;',
                     'SQL', 'TRUE', 'NVARCHAR'),
                ]
            elif 'FUNCTION_PARAMETERS' in query:
                mock_cursor.description = [
                    ('PARAMETER_NAME',), ('DATA_TYPE_NAME',), ('PARAMETER_TYPE',),
                    ('HAS_DEFAULT_VALUE',), ('POSITION',)
                ]
                mock_cursor.fetchall.return_value = []
            elif 'OBJECT_DEPENDENCIES' in query:
                mock_cursor.description = [('DEPENDENT_OBJECT_NAME',), ('DEPENDENT_OBJECT_TYPE',)]
                mock_cursor.fetchall.return_value = []

        mock_cursor.execute.side_effect = mock_execute

        config = {
            'host': 'hana-test.example.com',
            'user': 'test_user',
            'password': 'test_password'
        }

        adapter = HANAAdapter(config)
        routine = adapter.get_routine_by_name('get_user_name')

        assert routine is not None
        assert routine['name'] == 'get_user_name'
        assert routine['type'] == 'FUNCTION'


# ===== L4 Tests (Triggers) =====

class TestHANAAdapterL4Triggers:
    """Test L4 trigger extraction methods."""

    @patch('extensions.plugins.hana_adapter.dbapi')
    def test_get_triggers_returns_list(self, mock_dbapi):
        """Test get_triggers() returns list of trigger dicts."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_dbapi.connect.return_value = mock_conn

        def mock_execute(query, params=None):
            if 'SYS.TRIGGERS' in query and 'TRIGGER_NAME = ?' not in query:
                mock_cursor.description = [
                    ('TRIGGER_NAME',), ('SCHEMA_NAME',), ('SUBJECT_TABLE_NAME',),
                    ('TRIGGER_ACTION_TIME',), ('TRIGGER_EVENT',), ('TRIGGERED_ACTION_LEVEL',),
                    ('DEFINITION',), ('IS_ENABLED',), ('IS_VALID',)
                ]
                mock_cursor.fetchall.return_value = [
                    ('TRG_UPPER_NAME', 'TEST_SCHEMA', 'USERS', 'BEFORE', 'INSERT', 'ROW',
                     'BEGIN :NEW.name := UPPER(:NEW.name); END;', 'TRUE', 'TRUE'),
                    ('TRG_AUDIT', 'TEST_SCHEMA', 'ORDERS', 'AFTER', 'INSERT', 'ROW',
                     'BEGIN INSERT INTO audit_log VALUES (:NEW.id, CURRENT_TIMESTAMP); END;', 'TRUE', 'TRUE'),
                ]

        mock_cursor.execute.side_effect = mock_execute

        config = {
            'host': 'hana-test.example.com',
            'user': 'test_user',
            'password': 'test_password'
        }

        adapter = HANAAdapter(config)
        triggers = adapter.get_triggers()

        assert len(triggers) == 2
        assert triggers[0]['name'] == 'trg_upper_name'
        assert triggers[0]['table_name'] == 'users'
        assert triggers[0]['trigger_type'] == 'BEFORE'
        assert triggers[0]['trigger_event'] == 'INSERT'

    @patch('extensions.plugins.hana_adapter.dbapi')
    def test_trigger_subset_classification_supported(self, mock_dbapi):
        """Test trigger subset classification - supported pattern."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_dbapi.connect.return_value = mock_conn

        def mock_execute(query, params=None):
            if 'SYS.TRIGGERS' in query:
                mock_cursor.description = [
                    ('TRIGGER_NAME',), ('SCHEMA_NAME',), ('SUBJECT_TABLE_NAME',),
                    ('TRIGGER_ACTION_TIME',), ('TRIGGER_EVENT',), ('TRIGGERED_ACTION_LEVEL',),
                    ('DEFINITION',), ('IS_ENABLED',), ('IS_VALID',)
                ]
                # Simple normalization trigger - should be supported
                mock_cursor.fetchall.return_value = [
                    ('TRG_UPPER_NAME', 'TEST_SCHEMA', 'USERS', 'BEFORE', 'INSERT', 'ROW',
                     'BEGIN :NEW.name := UPPER(:NEW.name); END;', 'TRUE', 'TRUE'),
                ]

        mock_cursor.execute.side_effect = mock_execute

        config = {
            'host': 'hana-test.example.com',
            'user': 'test_user',
            'password': 'test_password'
        }

        adapter = HANAAdapter(config)
        triggers = adapter.get_triggers()

        assert len(triggers) == 1
        assert triggers[0]['supported_subset'] == True
        assert triggers[0]['unsupported_reason'] is None

    @patch('extensions.plugins.hana_adapter.dbapi')
    def test_trigger_subset_classification_unsupported_after(self, mock_dbapi):
        """Test trigger subset classification - AFTER triggers unsupported."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_dbapi.connect.return_value = mock_conn

        def mock_execute(query, params=None):
            if 'SYS.TRIGGERS' in query:
                mock_cursor.description = [
                    ('TRIGGER_NAME',), ('SCHEMA_NAME',), ('SUBJECT_TABLE_NAME',),
                    ('TRIGGER_ACTION_TIME',), ('TRIGGER_EVENT',), ('TRIGGERED_ACTION_LEVEL',),
                    ('DEFINITION',), ('IS_ENABLED',), ('IS_VALID',)
                ]
                # AFTER trigger - should be unsupported
                mock_cursor.fetchall.return_value = [
                    ('TRG_AUDIT', 'TEST_SCHEMA', 'ORDERS', 'AFTER', 'INSERT', 'ROW',
                     'BEGIN INSERT INTO audit_log VALUES (:NEW.id, CURRENT_TIMESTAMP); END;', 'TRUE', 'TRUE'),
                ]

        mock_cursor.execute.side_effect = mock_execute

        config = {
            'host': 'hana-test.example.com',
            'user': 'test_user',
            'password': 'test_password'
        }

        adapter = HANAAdapter(config)
        triggers = adapter.get_triggers()

        assert len(triggers) == 1
        assert triggers[0]['supported_subset'] == False
        assert 'AFTER' in triggers[0]['unsupported_reason']

    @patch('extensions.plugins.hana_adapter.dbapi')
    def test_trigger_subset_classification_unsupported_complex(self, mock_dbapi):
        """Test trigger subset classification - complex logic unsupported."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_dbapi.connect.return_value = mock_conn

        def mock_execute(query, params=None):
            if 'SYS.TRIGGERS' in query:
                mock_cursor.description = [
                    ('TRIGGER_NAME',), ('SCHEMA_NAME',), ('SUBJECT_TABLE_NAME',),
                    ('TRIGGER_ACTION_TIME',), ('TRIGGER_EVENT',), ('TRIGGERED_ACTION_LEVEL',),
                    ('DEFINITION',), ('IS_ENABLED',), ('IS_VALID',)
                ]
                # Complex trigger with loop - should be unsupported
                mock_cursor.fetchall.return_value = [
                    ('TRG_COMPLEX', 'TEST_SCHEMA', 'ORDERS', 'BEFORE', 'INSERT', 'ROW',
                     'BEGIN FOR i IN 1..10 LOOP :NEW.counter := :NEW.counter + 1; END LOOP; END;', 'TRUE', 'TRUE'),
                ]

        mock_cursor.execute.side_effect = mock_execute

        config = {
            'host': 'hana-test.example.com',
            'user': 'test_user',
            'password': 'test_password'
        }

        adapter = HANAAdapter(config)
        triggers = adapter.get_triggers()

        assert len(triggers) == 1
        assert triggers[0]['supported_subset'] == False
        assert 'loop' in triggers[0]['unsupported_reason'].lower()


# ===== Dependency Ordering Tests =====

class TestHANAAdapterDependencyOrdering:
    """Test dependency ordering functionality."""

    def test_dependency_order_simple(self):
        """Test simple dependency ordering."""
        config = {
            'host': 'hana-test.example.com',
            'user': 'test_user',
            'password': 'test_password'
        }

        # Need to mock hdbcli for adapter init
        with patch('extensions.plugins.hana_adapter.dbapi'):
            adapter = HANAAdapter(config)

        objects = [
            {'name': 'view_c', 'dependencies': ['view_b']},
            {'name': 'view_a', 'dependencies': []},
            {'name': 'view_b', 'dependencies': ['view_a']},
        ]

        ordered = adapter.get_dependency_order(objects)

        # Should be: view_a, view_b, view_c (dependencies first)
        names = [o['name'] for o in ordered]
        assert names.index('view_a') < names.index('view_b')
        assert names.index('view_b') < names.index('view_c')

    def test_dependency_order_handles_circular(self, caplog):
        """Test that circular dependencies are handled gracefully."""
        config = {
            'host': 'hana-test.example.com',
            'user': 'test_user',
            'password': 'test_password'
        }

        with patch('extensions.plugins.hana_adapter.dbapi'):
            adapter = HANAAdapter(config)

        # Circular dependency: a -> b -> c -> a
        objects = [
            {'name': 'view_a', 'dependencies': ['view_c']},
            {'name': 'view_b', 'dependencies': ['view_a']},
            {'name': 'view_c', 'dependencies': ['view_b']},
        ]

        with caplog.at_level(logging.WARNING):
            ordered = adapter.get_dependency_order(objects)

        # Should still return all objects
        assert len(ordered) == 3
        # Should have logged a warning
        assert any('Circular dependency' in record.message for record in caplog.records)


# ===== L2/L3/L4 Summary Tests =====

class TestHANAAdapterL2L3L4Summary:
    """Test L2/L3/L4 summary method."""

    @patch('extensions.plugins.hana_adapter.dbapi')
    def test_get_l2l3l4_summary_structure(self, mock_dbapi):
        """Test get_l2l3l4_summary() returns correct structure."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_dbapi.connect.return_value = mock_conn

        # Mock empty results for simplicity
        mock_cursor.fetchall.return_value = []

        config = {
            'host': 'hana-test.example.com',
            'user': 'test_user',
            'password': 'test_password'
        }

        adapter = HANAAdapter(config)
        summary = adapter.get_l2l3l4_summary()

        # Check structure
        assert 'l2_views' in summary
        assert 'l3_procedures' in summary
        assert 'l3_functions' in summary
        assert 'l4_triggers' in summary
        assert 'limitations' in summary

        # Check sub-structure
        assert 'count' in summary['l2_views']
        assert 'names' in summary['l2_views']

        assert 'total_count' in summary['l4_triggers']
        assert 'supported_count' in summary['l4_triggers']
        assert 'unsupported_count' in summary['l4_triggers']

        # Check limitations are documented
        assert 'calculation_views' in summary['limitations']
        assert 'amdp' in summary['limitations']
        assert 'trigger_subset' in summary['limitations']


# ===== Trigger Classification Unit Tests =====

class TestTriggerClassification:
    """Unit tests for trigger classification logic."""

    def test_classify_before_insert_simple_normalization(self):
        """Test classification of simple BEFORE INSERT normalization trigger."""
        config = {
            'host': 'hana-test.example.com',
            'user': 'test_user',
            'password': 'test_password'
        }

        with patch('extensions.plugins.hana_adapter.dbapi'):
            adapter = HANAAdapter(config)

        # Simple UPPER normalization
        is_supported, reason = adapter._classify_trigger_subset(
            'BEFORE', 'INSERT', 'ROW',
            'BEGIN :NEW.name := UPPER(:NEW.name); END;'
        )

        assert is_supported == True
        assert reason is None

    def test_classify_before_update_simple_normalization(self):
        """Test classification of simple BEFORE UPDATE normalization trigger."""
        config = {
            'host': 'hana-test.example.com',
            'user': 'test_user',
            'password': 'test_password'
        }

        with patch('extensions.plugins.hana_adapter.dbapi'):
            adapter = HANAAdapter(config)

        # Simple TRIM normalization
        is_supported, reason = adapter._classify_trigger_subset(
            'BEFORE', 'UPDATE', 'ROW',
            'BEGIN :NEW.email := TRIM(:NEW.email); END;'
        )

        assert is_supported == True
        assert reason is None

    def test_classify_after_trigger_unsupported(self):
        """Test that AFTER triggers are classified as unsupported."""
        config = {
            'host': 'hana-test.example.com',
            'user': 'test_user',
            'password': 'test_password'
        }

        with patch('extensions.plugins.hana_adapter.dbapi'):
            adapter = HANAAdapter(config)

        is_supported, reason = adapter._classify_trigger_subset(
            'AFTER', 'INSERT', 'ROW',
            'BEGIN INSERT INTO audit_log VALUES (:NEW.id); END;'
        )

        assert is_supported == False
        assert 'AFTER' in reason

    def test_classify_delete_trigger_unsupported(self):
        """Test that DELETE triggers are classified as unsupported."""
        config = {
            'host': 'hana-test.example.com',
            'user': 'test_user',
            'password': 'test_password'
        }

        with patch('extensions.plugins.hana_adapter.dbapi'):
            adapter = HANAAdapter(config)

        is_supported, reason = adapter._classify_trigger_subset(
            'BEFORE', 'DELETE', 'ROW',
            'BEGIN :OLD.deleted_at := CURRENT_TIMESTAMP; END;'
        )

        assert is_supported == False
        assert 'DELETE' in reason

    def test_classify_statement_level_unsupported(self):
        """Test that statement-level triggers are classified as unsupported."""
        config = {
            'host': 'hana-test.example.com',
            'user': 'test_user',
            'password': 'test_password'
        }

        with patch('extensions.plugins.hana_adapter.dbapi'):
            adapter = HANAAdapter(config)

        is_supported, reason = adapter._classify_trigger_subset(
            'BEFORE', 'INSERT', 'STATEMENT',
            'BEGIN :NEW.name := UPPER(:NEW.name); END;'
        )

        assert is_supported == False
        assert 'statement' in reason.lower()

    def test_classify_trigger_with_loop_unsupported(self):
        """Test that triggers with loops are classified as unsupported."""
        config = {
            'host': 'hana-test.example.com',
            'user': 'test_user',
            'password': 'test_password'
        }

        with patch('extensions.plugins.hana_adapter.dbapi'):
            adapter = HANAAdapter(config)

        is_supported, reason = adapter._classify_trigger_subset(
            'BEFORE', 'INSERT', 'ROW',
            'BEGIN FOR i IN 1..10 LOOP :NEW.val := :NEW.val + 1; END LOOP; END;'
        )

        assert is_supported == False
        assert 'loop' in reason.lower()

    def test_classify_trigger_with_dml_unsupported(self):
        """Test that triggers with DML operations are classified as unsupported."""
        config = {
            'host': 'hana-test.example.com',
            'user': 'test_user',
            'password': 'test_password'
        }

        with patch('extensions.plugins.hana_adapter.dbapi'):
            adapter = HANAAdapter(config)

        is_supported, reason = adapter._classify_trigger_subset(
            'BEFORE', 'INSERT', 'ROW',
            'BEGIN INSERT INTO audit_log VALUES (:NEW.id); END;'
        )

        assert is_supported == False
        assert 'DML' in reason

    def test_classify_trigger_with_conditional_unsupported(self):
        """Test that triggers with conditional logic are classified as unsupported."""
        config = {
            'host': 'hana-test.example.com',
            'user': 'test_user',
            'password': 'test_password'
        }

        with patch('extensions.plugins.hana_adapter.dbapi'):
            adapter = HANAAdapter(config)

        is_supported, reason = adapter._classify_trigger_subset(
            'BEFORE', 'INSERT', 'ROW',
            'BEGIN IF :NEW.status IS NULL THEN :NEW.status := 0; END IF; END;'
        )

        assert is_supported == False
        assert 'conditional' in reason.lower()


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
