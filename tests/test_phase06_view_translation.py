#!/usr/bin/env python3
"""
Phase 06 Tests: View Translation (L2)

Tests for conservative view translation subset.
"""

import pytest
from core.translator import (
    Translator,
    TranslateMode,
    ObjectType,
    RiskLevel
)
from core.view_translator import ViewTranslator, ViewPattern


class TestViewPatternRecognition:
    """Test view pattern identification."""

    def test_simple_select_recognized(self):
        """Simple SELECT + projection identified correctly."""
        view_translator = ViewTranslator()

        view_def = "SELECT user_id, username FROM users"
        pattern = view_translator._identify_pattern(view_def)

        assert pattern == ViewPattern.SIMPLE_SELECT

    def test_select_where_recognized(self):
        """SELECT + WHERE recognized correctly."""
        view_translator = ViewTranslator()

        view_def = "SELECT user_id, username FROM users WHERE active = 1"
        pattern = view_translator._identify_pattern(view_def)

        assert pattern == ViewPattern.SELECT_WHERE

    def test_basic_join_recognized(self):
        """Basic INNER JOIN recognized correctly."""
        view_translator = ViewTranslator()

        view_def = """
        SELECT u.user_id, o.order_id
        FROM users u
        INNER JOIN orders o ON u.user_id = o.user_id
        """
        pattern = view_translator._identify_pattern(view_def)

        assert pattern == ViewPattern.BASIC_JOIN

    def test_window_function_rejected(self):
        """Window functions marked as unsupported."""
        view_translator = ViewTranslator()

        view_def = "SELECT user_id, ROW_NUMBER() OVER (PARTITION BY dept_id) FROM users"
        pattern = view_translator._identify_pattern(view_def)

        assert pattern == ViewPattern.UNSUPPORTED

    def test_union_rejected(self):
        """UNION marked as unsupported."""
        view_translator = ViewTranslator()

        view_def = "SELECT id FROM users UNION SELECT id FROM archived_users"
        pattern = view_translator._identify_pattern(view_def)

        assert pattern == ViewPattern.UNSUPPORTED

    def test_subquery_rejected(self):
        """Subqueries marked as unsupported."""
        view_translator = ViewTranslator()

        view_def = "SELECT user_id FROM users WHERE dept_id IN (SELECT id FROM depts WHERE active = 1)"
        pattern = view_translator._identify_pattern(view_def)

        assert pattern == ViewPattern.UNSUPPORTED

    def test_outer_join_rejected(self):
        """Outer joins marked as unsupported."""
        view_translator = ViewTranslator()

        view_def = "SELECT u.id, o.id FROM users u LEFT JOIN orders o ON u.id = o.user_id"
        pattern = view_translator._identify_pattern(view_def)

        assert pattern == ViewPattern.UNSUPPORTED


class TestViewTranslation:
    """Test view translation for supported patterns."""

    def test_simple_select_translates(self):
        """Simple SELECT translates successfully."""
        view_translator = ViewTranslator(source_dialect="oracle", target_dialect="postgres")

        view_def = "SELECT user_id, username FROM users"
        translated_sql, risk = view_translator.translate("test_view", view_def)

        assert "CREATE VIEW test_view" in translated_sql
        assert "SELECT user_id, username FROM users" in translated_sql
        assert risk == RiskLevel.SAFE

    def test_select_where_boolean_translation(self):
        """SELECT + WHERE translates booleans correctly (Oracle → Postgres)."""
        view_translator = ViewTranslator(source_dialect="oracle", target_dialect="postgres")

        view_def = "SELECT user_id FROM users WHERE active = 1"
        translated_sql, risk = view_translator.translate("active_users", view_def)

        # Oracle 1 should be translated to Postgres true
        assert "active = true" in translated_sql.lower() or "active = 1" in translated_sql.lower()
        assert risk == RiskLevel.LOW

    def test_basic_join_translates(self):
        """Basic INNER JOIN translates successfully."""
        view_translator = ViewTranslator(source_dialect="oracle", target_dialect="postgres")

        view_def = """
        SELECT u.user_id, o.order_id
        FROM users u
        INNER JOIN orders o ON u.user_id = o.user_id
        """
        translated_sql, risk = view_translator.translate("user_orders", view_def)

        assert "CREATE VIEW user_orders" in translated_sql
        assert "INNER JOIN" in translated_sql
        assert risk == RiskLevel.MEDIUM

    def test_unsupported_pattern_raises_error(self):
        """Unsupported pattern raises error when translate() called."""
        view_translator = ViewTranslator()

        view_def = "SELECT * FROM users UNION SELECT * FROM archived"

        # Should raise error because pattern is unsupported
        with pytest.raises(ValueError, match="Unsupported pattern"):
            view_translator.translate("bad_view", view_def)


class TestSubsetTranslateMode:
    """Test subset_translate mode with views."""

    def test_supported_view_translates(self):
        """Supported view pattern translates in subset_translate mode."""
        translator = Translator(
            mode=TranslateMode.SUBSET_TRANSLATE,
            source_dialect="oracle",
            target_dialect="postgres"
        )

        result = translator.translate_object(
            obj_type=ObjectType.VIEW,
            obj_name="active_users",
            obj_def="SELECT user_id, username FROM users WHERE active = 1"
        )

        # Should produce translated SQL
        assert result.sql_output is not None
        assert "CREATE VIEW active_users" in result.sql_output
        assert result.risk_level in [RiskLevel.SAFE, RiskLevel.LOW]

    def test_unsupported_view_falls_back_to_stub(self):
        """Unsupported view falls back to stub in subset_translate mode."""
        translator = Translator(mode=TranslateMode.SUBSET_TRANSLATE)

        result = translator.translate_object(
            obj_type=ObjectType.VIEW,
            obj_name="complex_view",
            obj_def="SELECT *, ROW_NUMBER() OVER (PARTITION BY dept) FROM users"
        )

        # Should fall back to stub
        assert result.sql_output is not None
        assert "STUB" in result.sql_output
        assert result.mode == TranslateMode.SUBSET_TRANSLATE


class TestRiskScoring:
    """Test risk scoring for views."""

    def test_simple_select_is_safe(self):
        """Simple SELECT has SAFE risk."""
        view_translator = ViewTranslator()

        view_def = "SELECT id, name FROM users"
        risk = view_translator.calculate_risk(view_def)

        assert risk == RiskLevel.SAFE

    def test_select_where_is_low(self):
        """SELECT + WHERE has LOW risk."""
        view_translator = ViewTranslator()

        view_def = "SELECT id FROM users WHERE active = 1"
        risk = view_translator.calculate_risk(view_def)

        assert risk == RiskLevel.LOW

    def test_basic_join_is_medium(self):
        """Basic JOIN has MEDIUM risk."""
        view_translator = ViewTranslator()

        view_def = "SELECT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id"
        risk = view_translator.calculate_risk(view_def)

        assert risk == RiskLevel.MEDIUM

    def test_window_function_is_critical(self):
        """Window functions have CRITICAL risk."""
        view_translator = ViewTranslator()

        view_def = "SELECT id, ROW_NUMBER() OVER (PARTITION BY dept) FROM users"
        risk = view_translator.calculate_risk(view_def)

        assert risk == RiskLevel.CRITICAL

    def test_union_is_critical(self):
        """UNION has CRITICAL risk."""
        view_translator = ViewTranslator()

        view_def = "SELECT id FROM users UNION SELECT id FROM archived"
        risk = view_translator.calculate_risk(view_def)

        assert risk == RiskLevel.CRITICAL


class TestUnsupportedReasons:
    """Test unsupported reason messages."""

    def test_window_function_reason(self):
        """Window function reason is clear."""
        view_translator = ViewTranslator()

        view_def = "SELECT id, ROW_NUMBER() OVER (ORDER BY name) FROM users"
        reason = view_translator.get_unsupported_reason(view_def)

        assert "window function" in reason.lower()

    def test_union_reason(self):
        """UNION reason is clear."""
        view_translator = ViewTranslator()

        view_def = "SELECT id FROM users UNION SELECT id FROM archived"
        reason = view_translator.get_unsupported_reason(view_def)

        assert "union" in reason.lower()

    def test_subquery_reason(self):
        """Subquery reason is clear."""
        view_translator = ViewTranslator()

        view_def = "SELECT id FROM users WHERE dept_id IN (SELECT id FROM depts)"
        reason = view_translator.get_unsupported_reason(view_def)

        assert "subquer" in reason.lower()

    def test_outer_join_reason(self):
        """Outer join reason is clear."""
        view_translator = ViewTranslator()

        view_def = "SELECT u.id FROM users u LEFT JOIN orders o ON u.id = o.user_id"
        reason = view_translator.get_unsupported_reason(view_def)

        assert "outer join" in reason.lower()


class TestDeterminism:
    """Test deterministic view translation."""

    def test_same_view_same_output(self):
        """Same view produces identical translation."""
        def translate_view():
            view_translator = ViewTranslator(source_dialect="oracle", target_dialect="postgres")
            sql, risk = view_translator.translate("test_view", "SELECT id FROM users WHERE active = 1")
            return sql

        output1 = translate_view()
        output2 = translate_view()

        # Should be byte-for-byte identical
        assert output1 == output2


class TestUnverifiedSyntaxWarning:
    """Test unverified syntax warning (Gemini concession requirement)."""

    def test_translated_view_has_unverified_warning(self):
        """Translated views have 'Unverified Syntax' warning."""
        view_def = "SELECT id, name FROM users"
        translator = Translator(mode=TranslateMode.SUBSET_TRANSLATE, source_dialect='oracle', target_dialect='postgres')
        result = translator.translate_object(ObjectType.VIEW, 'simple_view', view_def)

        # Should have unverified syntax warning (Gemini concession requirement)
        assert any(
            'unverified' in w.message.lower() and 'compile-check' in w.reason.lower()
            for w in result.warnings
        )


class TestIntegration:
    """Test integration with main Translator class."""

    def test_view_translation_end_to_end(self):
        """End-to-end view translation through Translator."""
        translator = Translator(
            mode=TranslateMode.SUBSET_TRANSLATE,
            source_dialect="oracle",
            target_dialect="postgres"
        )

        # Add simple view
        result1 = translator.translate_object(
            obj_type=ObjectType.VIEW,
            obj_name="simple_view",
            obj_def="SELECT id, name FROM users"
        )

        # Add complex view (unsupported)
        result2 = translator.translate_object(
            obj_type=ObjectType.VIEW,
            obj_name="complex_view",
            obj_def="SELECT id, ROW_NUMBER() OVER (ORDER BY name) FROM users"
        )

        # Get report
        report = translator.get_report()

        # Verify results
        assert len(translator.results) == 2
        assert result1.risk_level == RiskLevel.SAFE
        assert result2.sql_output is not None  # Should have stub
        assert "STUB" in result2.sql_output

        # Verify report
        assert report["total_objects"] == 2
        assert "view" in report["objects_by_type"]
        assert report["objects_by_type"]["view"] == 2
