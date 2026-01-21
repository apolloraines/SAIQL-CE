"""
Tests for Phase 06 Workstream 06.3: Package Strategy.

Test coverage:
- Package analysis (structure, dependencies, complexity)
- Package stub generation
- "Not semantically equivalent" disclaimers
"""

import pytest
from core.translator import Translator, TranslateMode, ObjectType
from core.package_analyzer import PackageAnalyzer


class TestPackageAnalysis:
    """Test package analysis capabilities."""

    def test_simple_package_spec_analyzed(self):
        """Simple package spec is analyzed successfully."""
        package_def = """
        CREATE OR REPLACE PACKAGE test_pkg AS
            PROCEDURE proc1(p_id IN NUMBER);
            FUNCTION func1(p_name IN VARCHAR2) RETURN VARCHAR2;
        END test_pkg;
        """

        analyzer = PackageAnalyzer(source_dialect='oracle', target_dialect='postgres')
        analysis = analyzer.analyze(package_def, 'test_pkg')

        assert analysis.package_name == 'test_pkg'
        assert analysis.has_spec
        assert len(analysis.procedures) == 1
        assert len(analysis.functions) == 1
        assert analysis.procedures[0].name == 'proc1'
        assert analysis.functions[0].name == 'func1'

    def test_package_body_detected(self):
        """Package body is detected correctly."""
        package_def = """
        CREATE OR REPLACE PACKAGE BODY test_pkg AS
            PROCEDURE proc1(p_id IN NUMBER) IS
            BEGIN
                NULL;
            END proc1;
        END test_pkg;
        """

        analyzer = PackageAnalyzer(source_dialect='oracle', target_dialect='postgres')
        analysis = analyzer.analyze(package_def, 'test_pkg')

        assert analysis.has_body
        assert len(analysis.procedures) == 1

    def test_package_dependencies_extracted(self):
        """Package dependencies (tables/views) are extracted."""
        package_def = """
        CREATE OR REPLACE PACKAGE BODY test_pkg AS
            PROCEDURE load_data IS
            BEGIN
                INSERT INTO target_table
                SELECT * FROM source_table
                JOIN dim_table ON source_table.id = dim_table.id;
            END load_data;
        END test_pkg;
        """

        analyzer = PackageAnalyzer(source_dialect='oracle', target_dialect='postgres')
        analysis = analyzer.analyze(package_def, 'test_pkg')

        assert 'target_table' in analysis.dependencies
        assert 'source_table' in analysis.dependencies
        assert 'dim_table' in analysis.dependencies

    def test_package_complexity_score(self):
        """Package complexity score is calculated."""
        simple_package = """
        CREATE OR REPLACE PACKAGE simple_pkg AS
            PROCEDURE proc1;
        END simple_pkg;
        """

        complex_package = """
        CREATE OR REPLACE PACKAGE complex_pkg AS
            PROCEDURE proc1; PROCEDURE proc2; PROCEDURE proc3;
            FUNCTION func1 RETURN VARCHAR2; FUNCTION func2 RETURN NUMBER;
        END complex_pkg;
        /
        CREATE OR REPLACE PACKAGE BODY complex_pkg AS
            CURSOR c1 IS SELECT * FROM table1;
            PROCEDURE proc1 IS
            BEGIN
                IF condition THEN
                    FOR rec IN c1 LOOP
                        UPDATE table2 SET col1 = rec.val;
                    END LOOP;
                END IF;
            END proc1;
        END complex_pkg;
        """

        analyzer = PackageAnalyzer()
        simple_analysis = analyzer.analyze(simple_package, 'simple_pkg')
        complex_analysis = analyzer.analyze(complex_package, 'complex_pkg')

        assert simple_analysis.complexity_score < complex_analysis.complexity_score
        assert complex_analysis.complexity_score >= 15  # Should have significant complexity

    def test_package_warnings_generated(self):
        """Package analysis generates warnings for complex features."""
        package_def = """
        CREATE OR REPLACE PACKAGE BODY test_pkg AS
            CURSOR c1 IS SELECT * FROM table1;
            PROCEDURE proc1 IS
                PRAGMA AUTONOMOUS_TRANSACTION;
            BEGIN
                SELECT * FROM table1 WHERE ROWNUM = 1;
                INSERT INTO table2 VALUES (1, 2, 3);
            END proc1;
        END test_pkg;
        """

        analyzer = PackageAnalyzer()
        analysis = analyzer.analyze(package_def, 'test_pkg')

        # Should have multiple warnings
        assert len(analysis.warnings) > 0
        warning_messages = ' '.join(analysis.warnings)
        assert 'cursor' in warning_messages.lower()
        assert 'autonomous' in warning_messages.lower() or 'pragma' in warning_messages.lower()
        assert 'rownum' in warning_messages.lower()
        assert 'dml' in warning_messages.lower() or 'insert' in warning_messages.lower()

    def test_package_manual_steps_generated(self):
        """Package analysis generates manual steps checklist."""
        package_def = """
        CREATE OR REPLACE PACKAGE test_pkg AS
            PROCEDURE proc1;
            FUNCTION func1 RETURN VARCHAR2;
        END test_pkg;
        """

        analyzer = PackageAnalyzer(target_dialect='postgres')
        analysis = analyzer.analyze(package_def, 'test_pkg')

        assert len(analysis.manual_steps) > 0
        steps_text = ' '.join(analysis.manual_steps)
        assert 'review' in steps_text.lower()
        assert 'rewrite' in steps_text.lower()
        assert 'postgres' in steps_text.lower()


class TestPackageIntegration:
    """Test package integration with Translator."""

    def test_analyze_mode_produces_package_analysis(self):
        """Analyze mode produces package analysis in metadata."""
        translator = Translator(mode=TranslateMode.ANALYZE, source_dialect='oracle', target_dialect='postgres')

        package_def = """
        CREATE OR REPLACE PACKAGE test_pkg AS
            PROCEDURE proc1(p_id IN NUMBER);
            FUNCTION func1(p_name IN VARCHAR2) RETURN VARCHAR2;
        END test_pkg;
        """

        result = translator.translate_object(ObjectType.PACKAGE, 'test_pkg', package_def)

        assert result.mode == TranslateMode.ANALYZE
        assert result.sql_output is None  # No SQL in analyze mode
        assert 'package_analysis' in result.metadata
        assert result.metadata['package_analysis']['procedure_count'] == 1
        assert result.metadata['package_analysis']['function_count'] == 1
        assert len(result.manual_steps) > 0

    def test_stub_mode_produces_package_stub(self):
        """Stub mode produces comment-only package stub."""
        translator = Translator(mode=TranslateMode.STUB, source_dialect='oracle', target_dialect='postgres')

        package_def = """
        CREATE OR REPLACE PACKAGE test_pkg AS
            PROCEDURE proc1;
        END test_pkg;
        """

        result = translator.translate_object(ObjectType.PACKAGE, 'test_pkg', package_def)

        assert result.mode == TranslateMode.STUB
        assert result.sql_output is not None
        assert 'STUB: test_pkg' in result.sql_output
        assert 'Manual rewrite required' in result.sql_output
        assert 'PACKAGE MIGRATION NOTES' in result.sql_output
        assert 'No automatic package translation' in result.sql_output

    def test_subset_translate_mode_generates_stub_for_packages(self):
        """Subset translate mode generates stub for packages (no translation)."""
        translator = Translator(mode=TranslateMode.SUBSET_TRANSLATE, source_dialect='oracle', target_dialect='postgres')

        package_def = """
        CREATE OR REPLACE PACKAGE test_pkg AS
            PROCEDURE simple_proc;
        END test_pkg;
        """

        result = translator.translate_object(ObjectType.PACKAGE, 'test_pkg', package_def)

        # Packages are never in "supported subset", so should get stub
        assert result.sql_output is not None
        assert 'STUB' in result.sql_output
        assert 'Manual rewrite required' in result.sql_output


class TestPackageStubSafety:
    """Test package stub safety and disclaimers."""

    def test_package_stub_has_not_semantically_equivalent_disclaimer(self):
        """Package stub includes 'not semantically equivalent' disclaimer."""
        translator = Translator(mode=TranslateMode.STUB, target_dialect='postgres')

        package_def = """
        CREATE OR REPLACE PACKAGE test_pkg AS
            PROCEDURE proc1;
        END test_pkg;
        """

        result = translator.translate_object(ObjectType.PACKAGE, 'test_pkg', package_def)

        assert 'Manual rewrite required for semantic equivalence' in result.sql_output
        assert 'cannot be automatically translated' in result.sql_output

    def test_package_stub_includes_target_dialect(self):
        """Package stub mentions target dialect."""
        translator = Translator(mode=TranslateMode.STUB, target_dialect='postgres')

        package_def = """
        CREATE OR REPLACE PACKAGE test_pkg AS
            PROCEDURE proc1;
        END test_pkg;
        """

        result = translator.translate_object(ObjectType.PACKAGE, 'test_pkg', package_def)

        assert 'postgres' in result.sql_output.lower()

    def test_package_stub_includes_migration_guidance(self):
        """Package stub includes migration guidance."""
        translator = Translator(mode=TranslateMode.STUB, target_dialect='postgres')

        package_def = """
        CREATE OR REPLACE PACKAGE test_pkg AS
            PROCEDURE proc1;
        END test_pkg;
        """

        result = translator.translate_object(ObjectType.PACKAGE, 'test_pkg', package_def)

        assert 'PACKAGE MIGRATION NOTES' in result.sql_output
        assert 'Recommended approach' in result.sql_output
        assert 'Extract procedures/functions' in result.sql_output


class TestPackageDeterminism:
    """Test package analysis determinism."""

    def test_same_package_produces_same_analysis(self):
        """Same package definition produces identical analysis."""
        package_def = """
        CREATE OR REPLACE PACKAGE test_pkg AS
            PROCEDURE proc1(p_id IN NUMBER);
            FUNCTION func1 RETURN VARCHAR2;
        END test_pkg;
        """

        translator = Translator(mode=TranslateMode.ANALYZE)
        result1 = translator.translate_object(ObjectType.PACKAGE, 'test_pkg', package_def)
        result2 = translator.translate_object(ObjectType.PACKAGE, 'test_pkg', package_def)

        assert result1.metadata['package_analysis'] == result2.metadata['package_analysis']
        assert len(result1.warnings) == len(result2.warnings)
        assert len(result1.manual_steps) == len(result2.manual_steps)

    def test_package_stub_is_deterministic(self):
        """Same package produces identical stub."""
        package_def = """
        CREATE OR REPLACE PACKAGE test_pkg AS
            PROCEDURE proc1;
        END test_pkg;
        """

        translator = Translator(mode=TranslateMode.STUB, target_dialect='postgres')
        result1 = translator.translate_object(ObjectType.PACKAGE, 'test_pkg', package_def)
        result2 = translator.translate_object(ObjectType.PACKAGE, 'test_pkg', package_def)

        assert result1.sql_output == result2.sql_output
