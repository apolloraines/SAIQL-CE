"""
Tests for Phase 06 Workstream 06.4: Report Generator.

Test coverage:
- Object counts (detected/translated/stubbed/skipped)
- Risk score summary
- Manual steps checklist
- Warning collection and sorting
- Determinism
"""

import pytest
from core.translator import Translator, TranslateMode, ObjectType


class TestReportCounts:
    """Test report count generation."""

    def test_analyze_mode_counts(self):
        """Analyze mode report shows detected and analyzed_only counts."""
        translator = Translator(mode=TranslateMode.ANALYZE)

        translator.translate_object(ObjectType.VIEW, 'view1', 'CREATE VIEW view1 AS SELECT * FROM table1')
        translator.translate_object(ObjectType.VIEW, 'view2', 'CREATE VIEW view2 AS SELECT * FROM table2')
        translator.translate_object(ObjectType.TRIGGER, 'trg1', 'CREATE TRIGGER trg1 ...')

        report = translator.get_report()

        assert report['counts']['detected'] == 3
        assert report['counts']['analyzed_only'] == 3
        assert report['counts']['translated'] == 0
        assert report['counts']['stubbed'] == 0

    def test_stub_mode_counts(self):
        """Stub mode report shows stubbed counts."""
        translator = Translator(mode=TranslateMode.STUB)

        translator.translate_object(ObjectType.VIEW, 'view1', 'CREATE VIEW view1 AS SELECT * FROM table1')
        translator.translate_object(ObjectType.PACKAGE, 'pkg1', 'CREATE PACKAGE pkg1 AS ...')

        report = translator.get_report()

        assert report['counts']['detected'] == 2
        assert report['counts']['stubbed'] == 2

    def test_subset_translate_mode_counts(self):
        """Subset translate mode shows translated and stubbed counts."""
        translator = Translator(mode=TranslateMode.SUBSET_TRANSLATE, source_dialect='oracle', target_dialect='postgres')

        # Supported pattern (will translate)
        translator.translate_object(ObjectType.VIEW, 'simple_view', 'CREATE VIEW simple_view AS SELECT id, name FROM users')

        # Unsupported pattern (will stub)
        translator.translate_object(ObjectType.VIEW, 'complex_view', 'CREATE VIEW complex_view AS SELECT * FROM table1 UNION SELECT * FROM table2')

        report = translator.get_report()

        assert report['counts']['detected'] == 2
        assert report['counts']['translated'] == 1
        assert report['counts']['stubbed'] == 1

    def test_counts_by_type(self):
        """Report includes breakdown by object type."""
        translator = Translator(mode=TranslateMode.ANALYZE)

        translator.translate_object(ObjectType.VIEW, 'view1', 'CREATE VIEW ...')
        translator.translate_object(ObjectType.VIEW, 'view2', 'CREATE VIEW ...')
        translator.translate_object(ObjectType.TRIGGER, 'trg1', 'CREATE TRIGGER ...')
        translator.translate_object(ObjectType.PACKAGE, 'pkg1', 'CREATE PACKAGE ...')

        report = translator.get_report()

        assert report['counts']['by_type']['view'] == 2
        assert report['counts']['by_type']['trigger'] == 1
        assert report['counts']['by_type']['package'] == 1


class TestReportRiskSummary:
    """Test risk summary generation."""

    def test_risk_summary_counts(self):
        """Risk summary shows count by risk level."""
        translator = Translator(mode=TranslateMode.SUBSET_TRANSLATE, source_dialect='oracle', target_dialect='postgres')

        # Safe (simple SELECT)
        translator.translate_object(ObjectType.VIEW, 'v1', 'CREATE VIEW v1 AS SELECT id FROM users')

        # Low (SELECT WHERE)
        translator.translate_object(ObjectType.VIEW, 'v2', 'CREATE VIEW v2 AS SELECT id FROM users WHERE active = 1')

        # Medium (JOIN) - Note: may be stubbed if not in whitelist
        translator.translate_object(ObjectType.VIEW, 'v3', 'CREATE VIEW v3 AS SELECT u.id FROM users u JOIN orders o ON u.id = o.user_id')

        # Critical (unsupported, stubbed)
        translator.translate_object(ObjectType.VIEW, 'v4', 'CREATE VIEW v4 AS SELECT * FROM table1 UNION SELECT * FROM table2')

        report = translator.get_report()

        # Check that various risk levels are present (at least 2 different levels)
        risk_levels_present = sum(1 for count in report['risk_summary'].values() if count > 0)
        assert risk_levels_present >= 2

        # Should have at least some safe/low and some critical
        assert report['risk_summary']['safe'] + report['risk_summary']['low'] >= 1
        assert report['risk_summary']['critical'] >= 1


class TestReportManualSteps:
    """Test manual steps checklist generation."""

    def test_manual_steps_collected(self):
        """Manual steps are collected from all objects."""
        translator = Translator(mode=TranslateMode.ANALYZE)

        translator.translate_object(ObjectType.PACKAGE, 'pkg1', 'CREATE PACKAGE pkg1 AS PROCEDURE proc1; END;')

        report = translator.get_report()

        assert len(report['manual_steps']) > 0
        # Package should have manual steps
        steps_text = ' '.join([s['action'] for s in report['manual_steps']])
        assert 'review' in steps_text.lower() or 'rewrite' in steps_text.lower()

    def test_manual_steps_sorted_deterministically(self):
        """Manual steps are sorted deterministically."""
        translator1 = Translator(mode=TranslateMode.ANALYZE)
        translator2 = Translator(mode=TranslateMode.ANALYZE)

        # Add same objects in different order
        translator1.translate_object(ObjectType.PACKAGE, 'pkg_a', 'CREATE PACKAGE ...')
        translator1.translate_object(ObjectType.PACKAGE, 'pkg_b', 'CREATE PACKAGE ...')

        translator2.translate_object(ObjectType.PACKAGE, 'pkg_b', 'CREATE PACKAGE ...')
        translator2.translate_object(ObjectType.PACKAGE, 'pkg_a', 'CREATE PACKAGE ...')

        report1 = translator1.get_report()
        report2 = translator2.get_report()

        # Manual steps should be in same order (sorted)
        assert report1['manual_steps'] == report2['manual_steps']


class TestReportWarnings:
    """Test warning collection and sorting."""

    def test_warnings_collected(self):
        """Warnings are collected from all objects."""
        translator = Translator(mode=TranslateMode.ANALYZE)

        # Package with Oracle-specific features (generates warnings)
        translator.translate_object(
            ObjectType.PACKAGE,
            'complex_pkg',
            'CREATE PACKAGE BODY pkg AS CURSOR c1 IS SELECT * FROM table1; PRAGMA AUTONOMOUS_TRANSACTION; END;'
        )

        report = translator.get_report()

        assert len(report['warnings']) > 0

    def test_warnings_sorted_by_severity(self):
        """Warnings are sorted by severity."""
        translator = Translator(mode=TranslateMode.ANALYZE)

        # Add objects that generate various warnings
        translator.translate_object(
            ObjectType.PACKAGE,
            'pkg1',
            'CREATE PACKAGE BODY pkg AS BEGIN INSERT INTO table1 VALUES (1); END;'
        )

        report = translator.get_report()

        # Warnings should be present and sorted
        if len(report['warnings']) > 1:
            severities = [w['severity'] for w in report['warnings']]
            severity_order = {'critical': 0, 'high': 1, 'medium': 2, 'low': 3}
            severity_values = [severity_order.get(s, 99) for s in severities]
            assert severity_values == sorted(severity_values)


class TestReportDeterminism:
    """Test report determinism."""

    def test_same_objects_produce_same_report(self):
        """Same objects in same order produce identical report."""
        translator1 = Translator(mode=TranslateMode.ANALYZE)
        translator2 = Translator(mode=TranslateMode.ANALYZE)

        objects = [
            (ObjectType.VIEW, 'view1', 'CREATE VIEW view1 AS SELECT * FROM table1'),
            (ObjectType.VIEW, 'view2', 'CREATE VIEW view2 AS SELECT * FROM table2'),
            (ObjectType.TRIGGER, 'trg1', 'CREATE TRIGGER trg1 ...')
        ]

        for obj_type, name, definition in objects:
            translator1.translate_object(obj_type, name, definition)
            translator2.translate_object(obj_type, name, definition)

        report1 = translator1.get_report()
        report2 = translator2.get_report()

        assert report1 == report2

    def test_report_text_is_deterministic(self):
        """Text report is deterministic."""
        translator1 = Translator(mode=TranslateMode.ANALYZE)
        translator2 = Translator(mode=TranslateMode.ANALYZE)

        translator1.translate_object(ObjectType.VIEW, 'view1', 'CREATE VIEW view1 AS SELECT * FROM table1')
        translator2.translate_object(ObjectType.VIEW, 'view1', 'CREATE VIEW view1 AS SELECT * FROM table1')

        text1 = translator1.get_report_text()
        text2 = translator2.get_report_text()

        assert text1 == text2


class TestReportFormats:
    """Test report output formats."""

    def test_report_dict_structure(self):
        """Report dict has required structure."""
        translator = Translator(mode=TranslateMode.ANALYZE)
        translator.translate_object(ObjectType.VIEW, 'view1', 'CREATE VIEW ...')

        report = translator.get_report()

        # Check required keys
        assert 'mode' in report
        assert 'counts' in report
        assert 'risk_summary' in report
        assert 'manual_steps' in report
        assert 'warnings' in report

        # Check counts structure
        assert 'detected' in report['counts']
        assert 'by_type' in report['counts']

    def test_report_text_format(self):
        """Text report has human-readable format."""
        translator = Translator(mode=TranslateMode.ANALYZE)
        translator.translate_object(ObjectType.VIEW, 'view1', 'CREATE VIEW view1 AS SELECT * FROM table1')

        text = translator.get_report_text()

        # Check for key sections
        assert 'SAIQL Translation Report' in text
        assert 'Object Counts' in text
        assert 'Risk Summary' in text
        assert 'Mode:' in text
        assert '=' in text  # Header separators

    def test_text_report_includes_all_sections(self):
        """Text report includes all relevant sections."""
        translator = Translator(mode=TranslateMode.SUBSET_TRANSLATE)

        # Add objects that will generate warnings and manual steps
        translator.translate_object(ObjectType.VIEW, 'v1', 'CREATE VIEW v1 AS SELECT * FROM table1 UNION SELECT * FROM table2')

        text = translator.get_report_text()

        assert 'Object Counts' in text
        assert 'Risk Summary' in text
        assert 'Manual Steps Checklist' in text or 'Warnings' in text  # At least one should be present


class TestReportIntegration:
    """Test report generator integration."""

    def test_empty_results_handled(self):
        """Report handles empty results gracefully."""
        translator = Translator(mode=TranslateMode.ANALYZE)

        # No objects translated
        report = translator.get_report()

        assert report['counts']['detected'] == 0
        assert report['manual_steps'] == []
        assert report['warnings'] == []

    def test_mixed_mode_objects(self):
        """Report correctly categorizes objects in different modes."""
        translator = Translator(mode=TranslateMode.SUBSET_TRANSLATE)

        # Supported (translates)
        translator.translate_object(ObjectType.VIEW, 'simple', 'CREATE VIEW simple AS SELECT id FROM users')

        # Unsupported (stubs)
        translator.translate_object(ObjectType.PACKAGE, 'pkg1', 'CREATE PACKAGE pkg1 AS ...')

        report = translator.get_report()

        assert report['counts']['detected'] == 2
        assert report['counts']['translated'] >= 1
        assert report['counts']['stubbed'] >= 1
