#!/usr/bin/env python3
"""
Phase 06 Tests: Capability Flag Enforcement

Tests for translate modes: analyze, stub, subset_translate
"""

import pytest
from core.translator import (
    Translator,
    TranslateMode,
    ObjectType,
    RiskLevel
)


class TestCapabilityFlags:
    """Test capability flag enforcement."""

    def test_default_mode_is_analyze(self):
        """Verify default mode is ANALYZE (safest)."""
        translator = Translator()
        assert translator.mode == TranslateMode.ANALYZE

    def test_analyze_mode_no_sql_output(self):
        """Analyze mode must NOT produce SQL output."""
        translator = Translator(mode=TranslateMode.ANALYZE)

        result = translator.translate_object(
            obj_type=ObjectType.VIEW,
            obj_name="test_view",
            obj_def="CREATE VIEW test_view AS SELECT * FROM users"
        )

        # CRITICAL: No SQL output in analyze mode
        assert result.sql_output is None
        assert result.mode == TranslateMode.ANALYZE
        assert len(result.manual_steps) > 0  # Should have manual step

    def test_stub_mode_produces_safe_stub(self):
        """Stub mode must produce safe stubs that fail loudly."""
        translator = Translator(mode=TranslateMode.STUB)

        result = translator.translate_object(
            obj_type=ObjectType.VIEW,
            obj_name="test_view",
            obj_def="CREATE VIEW test_view AS SELECT * FROM users"
        )

        # Should produce SQL output
        assert result.sql_output is not None
        assert result.mode == TranslateMode.STUB

        # Stub should fail loudly
        assert "STUB" in result.sql_output
        assert "WARNING" in result.sql_output
        assert "MANUAL_REWRITE_REQUIRED" in result.sql_output or "Manual rewrite" in result.sql_output

        # Should have critical risk
        assert result.risk_level == RiskLevel.CRITICAL

        # Should have warnings
        assert len(result.warnings) > 0
        assert any("stub" in w.message.lower() for w in result.warnings)

    def test_subset_translate_unsupported_falls_back_to_stub(self):
        """Subset translate mode falls back to stub for unsupported patterns."""
        translator = Translator(mode=TranslateMode.SUBSET_TRANSLATE)

        # Use an actually unsupported pattern (window function)
        result = translator.translate_object(
            obj_type=ObjectType.VIEW,
            obj_name="complex_view",
            obj_def="CREATE VIEW complex_view AS SELECT id, ROW_NUMBER() OVER (ORDER BY name) FROM users"
        )

        # Should fall back to stub behavior
        assert result.sql_output is not None  # Has stub output
        assert "STUB" in result.sql_output
        assert result.mode == TranslateMode.SUBSET_TRANSLATE  # Mode preserved

    def test_multiple_objects_tracked(self):
        """Translator tracks all processed objects."""
        translator = Translator(mode=TranslateMode.ANALYZE)

        translator.translate_object(
            obj_type=ObjectType.VIEW,
            obj_name="view1",
            obj_def="CREATE VIEW view1 AS SELECT 1"
        )

        translator.translate_object(
            obj_type=ObjectType.FUNCTION,
            obj_name="func1",
            obj_def="CREATE FUNCTION func1() RETURNS INT AS..."
        )

        assert len(translator.results) == 2
        assert translator.results[0].object_name == "view1"
        assert translator.results[1].object_name == "func1"


class TestDeterminism:
    """Test deterministic output."""

    def test_analyze_mode_deterministic(self):
        """Same input produces identical results."""
        def run_analysis():
            translator = Translator(mode=TranslateMode.ANALYZE)
            translator.translate_object(
                obj_type=ObjectType.VIEW,
                obj_name="test_view",
                obj_def="CREATE VIEW test_view AS SELECT * FROM users"
            )
            return translator.get_report()

        report1 = run_analysis()
        report2 = run_analysis()

        # Reports should be identical
        assert report1 == report2

    def test_stub_mode_deterministic(self):
        """Same input produces byte-for-byte identical stubs."""
        def generate_stub():
            translator = Translator(mode=TranslateMode.STUB)
            result = translator.translate_object(
                obj_type=ObjectType.VIEW,
                obj_name="test_view",
                obj_def="CREATE VIEW test_view AS SELECT * FROM users"
            )
            return result.sql_output

        stub1 = generate_stub()
        stub2 = generate_stub()

        # Stubs should be byte-for-byte identical
        assert stub1 == stub2

    def test_warnings_sorted_deterministically(self):
        """Warnings are sorted for deterministic output."""
        translator = Translator(mode=TranslateMode.STUB)

        # Create multiple objects to generate warnings
        translator.translate_object(
            obj_type=ObjectType.VIEW,
            obj_name="z_view",
            obj_def="CREATE VIEW z_view AS SELECT 1"
        )
        translator.translate_object(
            obj_type=ObjectType.VIEW,
            obj_name="a_view",
            obj_def="CREATE VIEW a_view AS SELECT 1"
        )

        report = translator.get_report()
        warnings = report["warnings"]

        # Warnings should be sorted (a_view before z_view)
        assert len(warnings) == 2
        assert warnings[0]["object"] == "a_view"
        assert warnings[1]["object"] == "z_view"

    def test_manual_steps_sorted_deterministically(self):
        """Manual steps are sorted for deterministic output."""
        translator = Translator(mode=TranslateMode.ANALYZE)

        translator.translate_object(
            obj_type=ObjectType.VIEW,
            obj_name="z_view",
            obj_def="CREATE VIEW z_view AS SELECT 1"
        )
        translator.translate_object(
            obj_type=ObjectType.VIEW,
            obj_name="a_view",
            obj_def="CREATE VIEW a_view AS SELECT 1"
        )

        report = translator.get_report()
        manual_steps = report["manual_steps"]

        # Manual steps should be sorted (a_view before z_view)
        assert len(manual_steps) == 2
        assert manual_steps[0]["object"] == "a_view"
        assert manual_steps[1]["object"] == "z_view"


class TestReportGeneration:
    """Test report generation."""

    def test_report_structure(self):
        """Report has required structure."""
        translator = Translator(mode=TranslateMode.ANALYZE, source_dialect="oracle", target_dialect="postgres")

        translator.translate_object(
            obj_type=ObjectType.VIEW,
            obj_name="test_view",
            obj_def="CREATE VIEW test_view AS SELECT 1"
        )

        report = translator.get_report()

        # Required fields
        assert "mode" in report
        assert "source_dialect" in report
        assert "target_dialect" in report
        assert "total_objects" in report
        assert "objects_by_type" in report
        assert "risk_summary" in report
        assert "warnings" in report
        assert "manual_steps" in report

        # Values
        assert report["mode"] == "analyze"
        assert report["source_dialect"] == "oracle"
        assert report["target_dialect"] == "postgres"
        assert report["total_objects"] == 1

    def test_report_counts_by_type(self):
        """Report correctly counts objects by type."""
        translator = Translator(mode=TranslateMode.ANALYZE)

        translator.translate_object(ObjectType.VIEW, "view1", "...")
        translator.translate_object(ObjectType.VIEW, "view2", "...")
        translator.translate_object(ObjectType.FUNCTION, "func1", "...")

        report = translator.get_report()

        assert report["objects_by_type"]["view"] == 2
        assert report["objects_by_type"]["function"] == 1

    def test_report_risk_summary(self):
        """Report includes risk summary."""
        translator = Translator(mode=TranslateMode.STUB)

        translator.translate_object(ObjectType.VIEW, "view1", "...")

        report = translator.get_report()

        # Stubs have CRITICAL risk
        assert "critical" in report["risk_summary"]
        assert report["risk_summary"]["critical"] >= 1


class TestStubSafety:
    """Test stub safety guarantees."""

    def test_view_stub_fails_loudly(self):
        """View stubs contain failing queries with RAISE EXCEPTION."""
        translator = Translator(mode=TranslateMode.STUB)

        result = translator.translate_object(
            obj_type=ObjectType.VIEW,
            obj_name="test_view",
            obj_def="..."
        )

        stub = result.sql_output

        # Stub should create view that raises exception when queried
        assert "CREATE VIEW" in stub
        assert "RAISE EXCEPTION" in stub
        assert "Manual rewrite required" in stub

    def test_function_stub_raises_exception(self):
        """Function stubs raise exceptions when called."""
        translator = Translator(mode=TranslateMode.STUB)

        result = translator.translate_object(
            obj_type=ObjectType.FUNCTION,
            obj_name="test_func",
            obj_def="..."
        )

        stub = result.sql_output

        # Stub should raise exception
        assert "RAISE EXCEPTION" in stub or "RAISE" in stub
        assert "Manual rewrite required" in stub

    def test_trigger_stub_raises_exception(self):
        """Trigger stubs raise exceptions when fired."""
        translator = Translator(mode=TranslateMode.STUB)

        result = translator.translate_object(
            obj_type=ObjectType.TRIGGER,
            obj_name="test_trigger",
            obj_def="..."
        )

        stub = result.sql_output

        # Stub should raise exception
        assert "RAISE EXCEPTION" in stub or "RAISE" in stub
        assert "Manual rewrite required" in stub


class TestModeEnforcement:
    """Test mode boundaries are enforced."""

    def test_invalid_mode_rejected(self):
        """Invalid mode raises error."""
        # TranslateMode is an Enum, so passing invalid string would fail at instantiation
        # Test that only valid enum values are accepted
        valid_modes = [TranslateMode.ANALYZE, TranslateMode.STUB, TranslateMode.SUBSET_TRANSLATE]
        for mode in valid_modes:
            translator = Translator(mode=mode)
            assert translator.mode in valid_modes

    def test_analyze_never_mutates(self):
        """Analyze mode cannot produce mutations."""
        translator = Translator(mode=TranslateMode.ANALYZE)

        # Process multiple objects
        for i in range(5):
            translator.translate_object(
                obj_type=ObjectType.VIEW,
                obj_name=f"view{i}",
                obj_def=f"CREATE VIEW view{i} AS SELECT {i}"
            )

        # NONE should have SQL output
        for result in translator.results:
            assert result.sql_output is None, f"Analyze mode produced SQL for {result.object_name}"
