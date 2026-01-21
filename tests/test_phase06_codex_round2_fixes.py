#!/usr/bin/env python3
"""
Phase 06 Tests: Codex Round 2 Fixes

Tests for issues caught in Codex's second hostile QA pass:
1. JOIN ON clause equality-only validation
2. Dialect-aware stubs
3. Computed column detection (no-space expressions)
"""

import pytest
from core.translator import Translator, TranslateMode, ObjectType
from core.view_translator import ViewTranslator, ViewPattern


class TestJoinOnClauseValidation:
    """Test that JOIN ON clauses are validated for equality-only."""

    def test_join_with_greater_than_rejected(self):
        """JOIN with > in ON clause is rejected."""
        view_translator = ViewTranslator()

        view_def = """
        SELECT u.user_id, o.order_id
        FROM users u
        INNER JOIN orders o ON u.user_id > o.user_id
        """
        pattern = view_translator._identify_pattern(view_def)

        assert pattern == ViewPattern.UNSUPPORTED

    def test_join_with_less_than_rejected(self):
        """JOIN with < in ON clause is rejected."""
        view_translator = ViewTranslator()

        view_def = """
        SELECT u.user_id, o.order_id
        FROM users u
        INNER JOIN orders o ON u.created_date < o.created_date
        """
        pattern = view_translator._identify_pattern(view_def)

        assert pattern == ViewPattern.UNSUPPORTED

    def test_join_with_between_rejected(self):
        """JOIN with BETWEEN in ON clause is rejected."""
        view_translator = ViewTranslator()

        view_def = """
        SELECT u.user_id, o.order_id
        FROM users u
        INNER JOIN orders o ON o.amount BETWEEN u.min_amount AND u.max_amount
        """
        pattern = view_translator._identify_pattern(view_def)

        assert pattern == ViewPattern.UNSUPPORTED

    def test_join_with_like_rejected(self):
        """JOIN with LIKE in ON clause is rejected."""
        view_translator = ViewTranslator()

        view_def = """
        SELECT u.user_id, o.order_id
        FROM users u
        INNER JOIN orders o ON o.user_name LIKE u.name_pattern
        """
        pattern = view_translator._identify_pattern(view_def)

        assert pattern == ViewPattern.UNSUPPORTED

    def test_join_with_equality_accepted(self):
        """JOIN with = in ON clause is accepted."""
        view_translator = ViewTranslator()

        view_def = """
        SELECT u.user_id, o.order_id
        FROM users u
        INNER JOIN orders o ON u.user_id = o.user_id
        """
        pattern = view_translator._identify_pattern(view_def)

        assert pattern == ViewPattern.BASIC_JOIN


class TestDialectAwareStubs:
    """Test that stubs are generated for target dialect."""

    def test_postgres_stub_uses_plpgsql(self):
        """Postgres stubs use plpgsql."""
        translator = Translator(
            mode=TranslateMode.STUB,
            source_dialect="oracle",
            target_dialect="postgres"
        )

        result = translator.translate_object(
            obj_type=ObjectType.VIEW,
            obj_name="test_view",
            obj_def="..."
        )

        # Should use plpgsql
        assert "plpgsql" in result.sql_output.lower()
        assert "RAISE EXCEPTION" in result.sql_output

    def test_mysql_stub_has_limitation_warning(self):
        """MySQL stubs include limitation warning (may not fail loudly)."""
        translator = Translator(
            mode=TranslateMode.STUB,
            source_dialect="oracle",
            target_dialect="mysql"
        )

        result = translator.translate_object(
            obj_type=ObjectType.VIEW,
            obj_name="test_view",
            obj_def="..."
        )

        # Should use division by zero
        assert "1/0" in result.sql_output
        assert "force_error" in result.sql_output
        # CRITICAL: Must include limitation warning
        assert "LIMITATION" in result.sql_output
        assert "may return NULL" in result.sql_output or "sql_mode" in result.sql_output

    def test_oracle_stub_uses_division_by_zero(self):
        """Oracle stubs use division by zero."""
        translator = Translator(
            mode=TranslateMode.STUB,
            source_dialect="postgres",
            target_dialect="oracle"
        )

        result = translator.translate_object(
            obj_type=ObjectType.VIEW,
            obj_name="test_view",
            obj_def="..."
        )

        # Should use division by zero
        assert "1/0" in result.sql_output

    def test_sqlite_stub_has_limitation_warning(self):
        """SQLite stubs include limitation warning (does not fail loudly)."""
        translator = Translator(
            mode=TranslateMode.STUB,
            source_dialect="postgres",
            target_dialect="sqlite"
        )

        result = translator.translate_object(
            obj_type=ObjectType.VIEW,
            obj_name="test_view",
            obj_def="..."
        )

        # Should use division by zero
        assert "1/0" in result.sql_output
        # CRITICAL: Must include limitation warning
        assert "LIMITATION" in result.sql_output
        assert "does not fail loudly" in result.sql_output or "returns NULL" in result.sql_output


class TestComputedColumnDetection:
    """Test that computed columns are detected even without spaces."""

    def test_addition_without_spaces_rejected(self):
        """Computed column a+b (no spaces) is rejected."""
        view_translator = ViewTranslator()

        view_def = "SELECT user_id, salary+bonus AS total FROM employees"
        pattern = view_translator._identify_pattern(view_def)

        assert pattern == ViewPattern.UNSUPPORTED

    def test_subtraction_without_spaces_rejected(self):
        """Computed column a-b (no spaces) is rejected."""
        view_translator = ViewTranslator()

        view_def = "SELECT user_id, budget-spent AS remaining FROM projects"
        pattern = view_translator._identify_pattern(view_def)

        assert pattern == ViewPattern.UNSUPPORTED

    def test_multiplication_without_spaces_rejected(self):
        """Computed column a*b (no spaces) is rejected."""
        view_translator = ViewTranslator()

        view_def = "SELECT user_id, quantity*price AS total FROM orders"
        pattern = view_translator._identify_pattern(view_def)

        assert pattern == ViewPattern.UNSUPPORTED

    def test_division_without_spaces_rejected(self):
        """Computed column a/b (no spaces) is rejected."""
        view_translator = ViewTranslator()

        view_def = "SELECT user_id, total/count AS average FROM stats"
        pattern = view_translator._identify_pattern(view_def)

        assert pattern == ViewPattern.UNSUPPORTED

    def test_nvl_function_rejected(self):
        """NVL() function (Oracle) is rejected."""
        view_translator = ViewTranslator()

        view_def = "SELECT user_id, NVL(email, 'none') AS email_safe FROM users"
        pattern = view_translator._identify_pattern(view_def)

        assert pattern == ViewPattern.UNSUPPORTED

    def test_ifnull_function_rejected(self):
        """IFNULL() function (MySQL) is rejected."""
        view_translator = ViewTranslator()

        view_def = "SELECT user_id, IFNULL(phone, '000') AS phone_safe FROM users"
        pattern = view_translator._identify_pattern(view_def)

        assert pattern == ViewPattern.UNSUPPORTED

    def test_substring_function_rejected(self):
        """SUBSTRING() function is rejected."""
        view_translator = ViewTranslator()

        view_def = "SELECT user_id, SUBSTRING(name, 1, 10) AS name_short FROM users"
        pattern = view_translator._identify_pattern(view_def)

        assert pattern == ViewPattern.UNSUPPORTED

    def test_round_function_rejected(self):
        """ROUND() function is rejected."""
        view_translator = ViewTranslator()

        view_def = "SELECT user_id, ROUND(salary, 2) AS salary_rounded FROM employees"
        pattern = view_translator._identify_pattern(view_def)

        assert pattern == ViewPattern.UNSUPPORTED


class TestIntegration:
    """Test integration of Codex Round 2 fixes."""

    def test_non_equality_join_generates_stub(self):
        """Non-equality JOIN generates stub in subset_translate mode."""
        translator = Translator(mode=TranslateMode.SUBSET_TRANSLATE)

        result = translator.translate_object(
            obj_type=ObjectType.VIEW,
            obj_name="range_join",
            obj_def="SELECT u.id FROM users u INNER JOIN orders o ON u.id > o.id"
        )

        # Should fall back to stub
        assert result.sql_output is not None
        assert "STUB" in result.sql_output

    def test_no_space_expression_generates_stub(self):
        """Expression without spaces generates stub in subset_translate mode."""
        translator = Translator(mode=TranslateMode.SUBSET_TRANSLATE)

        result = translator.translate_object(
            obj_type=ObjectType.VIEW,
            obj_name="computed_view",
            obj_def="SELECT user_id, salary+bonus AS total FROM employees"
        )

        # Should fall back to stub
        assert result.sql_output is not None
        assert "STUB" in result.sql_output
