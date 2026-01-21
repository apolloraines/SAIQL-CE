#!/usr/bin/env python3
"""
Phase 06 Tests: Whitelist Enforcement (Codex Issue #2)

Tests that whitelist criteria are properly enforced:
- No computed columns
- No GROUP BY, HAVING, ORDER BY
- No DISTINCT
- No aggregates
"""

import pytest
from core.view_translator import ViewTranslator, ViewPattern


class TestWhitelistEnforcement:
    """Test that unsupported patterns are correctly rejected."""

    def test_group_by_rejected(self):
        """GROUP BY is rejected."""
        view_translator = ViewTranslator()

        view_def = "SELECT dept_id, COUNT(*) FROM users GROUP BY dept_id"
        pattern = view_translator._identify_pattern(view_def)

        assert pattern == ViewPattern.UNSUPPORTED

    def test_having_rejected(self):
        """HAVING is rejected."""
        view_translator = ViewTranslator()

        view_def = "SELECT dept_id, COUNT(*) FROM users GROUP BY dept_id HAVING COUNT(*) > 10"
        pattern = view_translator._identify_pattern(view_def)

        assert pattern == ViewPattern.UNSUPPORTED

    def test_order_by_rejected(self):
        """ORDER BY is rejected."""
        view_translator = ViewTranslator()

        view_def = "SELECT user_id, username FROM users ORDER BY username"
        pattern = view_translator._identify_pattern(view_def)

        assert pattern == ViewPattern.UNSUPPORTED

    def test_distinct_rejected(self):
        """DISTINCT is rejected."""
        view_translator = ViewTranslator()

        view_def = "SELECT DISTINCT dept_id FROM users"
        pattern = view_translator._identify_pattern(view_def)

        assert pattern == ViewPattern.UNSUPPORTED

    def test_aggregate_count_rejected(self):
        """Aggregate functions (COUNT) are rejected."""
        view_translator = ViewTranslator()

        view_def = "SELECT COUNT(*) FROM users"
        pattern = view_translator._identify_pattern(view_def)

        assert pattern == ViewPattern.UNSUPPORTED

    def test_aggregate_sum_rejected(self):
        """Aggregate functions (SUM) are rejected."""
        view_translator = ViewTranslator()

        view_def = "SELECT SUM(amount) FROM orders"
        pattern = view_translator._identify_pattern(view_def)

        assert pattern == ViewPattern.UNSUPPORTED

    def test_computed_column_addition_rejected(self):
        """Computed columns with addition are rejected."""
        view_translator = ViewTranslator()

        view_def = "SELECT user_id, salary + bonus AS total FROM employees"
        pattern = view_translator._identify_pattern(view_def)

        assert pattern == ViewPattern.UNSUPPORTED

    def test_computed_column_concat_rejected(self):
        """Computed columns with concatenation are rejected."""
        view_translator = ViewTranslator()

        view_def = "SELECT user_id, first_name || ' ' || last_name AS full_name FROM users"
        pattern = view_translator._identify_pattern(view_def)

        assert pattern == ViewPattern.UNSUPPORTED

    def test_computed_column_case_rejected(self):
        """Computed columns with CASE are rejected."""
        view_translator = ViewTranslator()

        view_def = "SELECT user_id, CASE WHEN active = 1 THEN 'Active' ELSE 'Inactive' END AS status FROM users"
        pattern = view_translator._identify_pattern(view_def)

        assert pattern == ViewPattern.UNSUPPORTED

    def test_computed_column_cast_rejected(self):
        """Computed columns with CAST are rejected."""
        view_translator = ViewTranslator()

        view_def = "SELECT user_id, CAST(created_date AS VARCHAR) AS created_str FROM users"
        pattern = view_translator._identify_pattern(view_def)

        assert pattern == ViewPattern.UNSUPPORTED

    def test_computed_column_upper_rejected(self):
        """Computed columns with UPPER() are rejected."""
        view_translator = ViewTranslator()

        view_def = "SELECT user_id, UPPER(username) AS username_upper FROM users"
        pattern = view_translator._identify_pattern(view_def)

        assert pattern == ViewPattern.UNSUPPORTED


class TestWhitelistAcceptance:
    """Test that supported patterns are still accepted."""

    def test_simple_select_still_accepted(self):
        """Simple SELECT with column list is still accepted."""
        view_translator = ViewTranslator()

        view_def = "SELECT user_id, username, email FROM users"
        pattern = view_translator._identify_pattern(view_def)

        assert pattern == ViewPattern.SIMPLE_SELECT

    def test_select_where_still_accepted(self):
        """SELECT + WHERE with simple predicate is still accepted."""
        view_translator = ViewTranslator()

        view_def = "SELECT user_id, username FROM users WHERE active = 1"
        pattern = view_translator._identify_pattern(view_def)

        assert pattern == ViewPattern.SELECT_WHERE

    def test_basic_join_still_accepted(self):
        """Basic INNER JOIN is still accepted."""
        view_translator = ViewTranslator()

        view_def = """
        SELECT u.user_id, o.order_id
        FROM users u
        INNER JOIN orders o ON u.user_id = o.user_id
        """
        pattern = view_translator._identify_pattern(view_def)

        assert pattern == ViewPattern.BASIC_JOIN


class TestStubGeneration:
    """Test that rejected patterns generate stubs."""

    def test_group_by_generates_stub(self):
        """GROUP BY generates stub in subset_translate mode."""
        from core.translator import Translator, TranslateMode, ObjectType

        translator = Translator(mode=TranslateMode.SUBSET_TRANSLATE)

        result = translator.translate_object(
            obj_type=ObjectType.VIEW,
            obj_name="dept_counts",
            obj_def="SELECT dept_id, COUNT(*) FROM users GROUP BY dept_id"
        )

        # Should fall back to stub
        assert result.sql_output is not None
        assert "STUB" in result.sql_output
        assert "RAISE EXCEPTION" in result.sql_output

    def test_computed_column_generates_stub(self):
        """Computed columns generate stub in subset_translate mode."""
        from core.translator import Translator, TranslateMode, ObjectType

        translator = Translator(mode=TranslateMode.SUBSET_TRANSLATE)

        result = translator.translate_object(
            obj_type=ObjectType.VIEW,
            obj_name="full_names",
            obj_def="SELECT user_id, first_name || ' ' || last_name AS full_name FROM users"
        )

        # Should fall back to stub
        assert result.sql_output is not None
        assert "STUB" in result.sql_output
        assert "RAISE EXCEPTION" in result.sql_output
