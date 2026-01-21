#!/usr/bin/env python3
"""
SAIQL SQLite L3 Harness Tests - Dependency Analysis

Validates SQLite adapter L3 capabilities per collab rules:
- B1) Dependency extraction: Extract function calls from SQL definitions
- B2) Classification: Classify functions as builtin/extension/unknown
- B3) Allowlist enforcement: Distinguish safe vs needs-extension vs needs-app-layer
- B4) Deterministic reporting: Generate consistent dependency reports
- B5) Limitations: Document unsupported patterns

SQLite L3 is unique - no stored procedures/UDFs in DB file.
L3 tests dependency DETECTION and CLASSIFICATION, not execution.

Proof-first approach per Apollo rules. Must pass 3x from clean state.

Per rules_SQLite_L2L3L4.md:
- Rule 5: Clean state per run (fresh DB file per run_id + teardown)
- Rule 8: PRAGMA settings must be fixed, not rely on defaults

Evidence:
- SQLite file-based database (fresh per test class, torn down after)
- Fixtures: 01_schema.sql, 02_views.sql, 03_dependencies.sql, 04_triggers.sql
- Expected classifications:
  - Builtin: UPPER, LOWER, COALESCE, COUNT, SUM, AVG, etc.
  - JSON extension: JSON_OBJECT, JSON_ARRAY
  - Unknown: my_custom_udf (if present)
"""

import pytest
import logging
import uuid
from typing import Dict, Any, List
from pathlib import Path

logger = logging.getLogger(__name__)

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

    for fixture in ['01_schema.sql', '02_views.sql', '03_dependencies.sql', '04_triggers.sql']:
        fixture_path = fixture_dir / fixture
        with open(fixture_path, 'r') as f:
            script = f.read()
        result = adapter.execute_script(script)
        assert result['success'], f"Failed to load {fixture}: {result.get('error')}"


@pytest.fixture(scope='class')
def db_path(tmp_path_factory):
    """Create a fresh DB file per test class (Rule 5: clean state per run)."""
    run_id = str(uuid.uuid4())[:8]
    run_dir = tmp_path_factory.mktemp(f"sqlite_l3_{run_id}")
    db_file = run_dir / f"test_l3_{run_id}.sqlite"
    yield str(db_file)
    # Teardown: file automatically cleaned up by pytest tmp_path_factory


@pytest.fixture(scope='class')
def adapter(db_path):
    """Class-scoped adapter with fixtures loaded."""
    adapter = get_adapter(db_path)
    load_fixtures(adapter)
    yield adapter
    adapter.close()


@pytest.fixture(scope='class')
def adapter_no_fixtures(db_path):
    """Class-scoped adapter without fixtures (for pure function tests)."""
    # Use a separate path to avoid conflicts
    import tempfile
    import os
    with tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False) as f:
        temp_path = f.name

    adapter = get_adapter(temp_path)
    yield adapter
    adapter.close()
    os.unlink(temp_path)


class TestSQLiteL3FunctionExtraction:
    """B1) Function extraction tests."""

    def test_l3_b1_extract_function_calls_basic(self, adapter):
        """Test that function calls are extracted from SQL."""
        sql = "SELECT UPPER(name), LOWER(email), COUNT(*) FROM users"
        functions = adapter.extract_function_calls(sql)

        assert 'upper' in functions, "Should detect UPPER"
        assert 'lower' in functions, "Should detect LOWER"
        assert 'count' in functions, "Should detect COUNT"

    def test_l3_b1_extract_function_calls_nested(self, adapter):
        """Test extraction of nested function calls."""
        sql = "SELECT COALESCE(UPPER(name), 'unknown') FROM users"
        functions = adapter.extract_function_calls(sql)

        assert 'coalesce' in functions, "Should detect COALESCE"
        assert 'upper' in functions, "Should detect UPPER"

    def test_l3_b1_extract_function_calls_aggregate(self, adapter):
        """Test extraction of aggregate functions."""
        sql = "SELECT dept_id, COUNT(*), SUM(salary), AVG(salary), MAX(salary), MIN(salary) FROM employees GROUP BY dept_id"
        functions = adapter.extract_function_calls(sql)

        assert 'count' in functions, "Should detect COUNT"
        assert 'sum' in functions, "Should detect SUM"
        assert 'avg' in functions, "Should detect AVG"
        assert 'max' in functions, "Should detect MAX"
        assert 'min' in functions, "Should detect MIN"

    def test_l3_b1_extract_excludes_keywords(self, adapter):
        """Test that SQL keywords are not extracted as functions."""
        sql = "SELECT name FROM users WHERE id IN (1, 2, 3)"
        functions = adapter.extract_function_calls(sql)

        # These should NOT be in the list
        assert 'select' not in functions, "Should not extract SELECT"
        assert 'from' not in functions, "Should not extract FROM"
        assert 'where' not in functions, "Should not extract WHERE"


class TestSQLiteL3Classification:
    """B2) Classification tests."""

    def test_l3_b2_classify_builtin_functions(self, adapter):
        """Test classification of SQLite builtin functions."""
        builtins = ['upper', 'lower', 'length', 'substr', 'coalesce', 'abs', 'round']

        for func in builtins:
            classification = adapter.classify_function(func)
            assert classification == 'builtin', f"{func} should be classified as builtin"

    def test_l3_b2_classify_aggregate_functions(self, adapter):
        """Test classification of SQLite aggregate functions."""
        aggregates = ['count', 'sum', 'avg', 'min', 'max', 'group_concat']

        for func in aggregates:
            classification = adapter.classify_function(func)
            assert classification == 'builtin', f"{func} should be classified as builtin"

    def test_l3_b2_classify_date_functions(self, adapter):
        """Test classification of SQLite date/time functions."""
        date_funcs = ['date', 'time', 'datetime', 'julianday', 'strftime']

        for func in date_funcs:
            classification = adapter.classify_function(func)
            assert classification == 'builtin', f"{func} should be classified as builtin"

    def test_l3_b2_classify_json_extension(self, adapter):
        """Test classification of JSON1 extension functions."""
        json_funcs = ['json_object', 'json_array', 'json_extract', 'json_type']

        for func in json_funcs:
            classification = adapter.classify_function(func)
            assert classification == 'json_extension', f"{func} should be classified as json_extension"

    def test_l3_b2_classify_unknown_functions(self, adapter):
        """Test classification of unknown/UDF functions."""
        unknown_funcs = ['my_custom_udf', 'calculate_bonus', 'app_specific_func']

        for func in unknown_funcs:
            classification = adapter.classify_function(func)
            assert classification == 'unknown', f"{func} should be classified as unknown"


class TestSQLiteL3DependencyAnalysis:
    """B3) Dependency analysis tests."""

    def test_l3_b3_analyze_builtin_only_sql(self, adapter):
        """Test analysis of SQL with only builtin functions."""
        sql = "SELECT UPPER(name), LENGTH(email), COALESCE(salary, 0) FROM employees"
        deps = adapter.analyze_dependencies(sql)

        assert deps['is_safe'], "Should be safe (all builtins)"
        assert not deps['needs_extension'], "Should not need extensions"
        assert len(deps['unknown']) == 0, "Should have no unknown functions"
        assert len(deps['builtin']) >= 3, "Should have at least 3 builtin functions"

    def test_l3_b3_analyze_json_extension_sql(self, adapter):
        """Test analysis of SQL with JSON extension functions."""
        sql = "SELECT JSON_OBJECT('name', name) FROM employees"
        deps = adapter.analyze_dependencies(sql)

        assert deps['is_safe'], "Should be safe (no unknown)"
        assert deps['needs_extension'], "Should need JSON extension"
        assert 'json_object' in deps['json_extension'], "Should detect JSON_OBJECT"

    def test_l3_b3_analyze_unknown_function_sql(self, adapter):
        """Test analysis of SQL with unknown functions."""
        sql = "SELECT my_custom_udf(salary) FROM employees"
        deps = adapter.analyze_dependencies(sql)

        assert not deps['is_safe'], "Should NOT be safe (unknown function)"
        assert 'my_custom_udf' in deps['unknown'], "Should detect unknown function"


class TestSQLiteL3AllDependencies:
    """B4) Full dependency analysis tests."""

    def test_l3_b4_get_all_dependencies(self, adapter):
        """Test getting all dependencies from database."""
        all_deps = adapter.get_all_dependencies()

        # Should have views analyzed
        assert 'views' in all_deps
        assert len(all_deps['views']) > 0, "Should have analyzed views"

        # Should have triggers analyzed
        assert 'triggers' in all_deps
        assert len(all_deps['triggers']) > 0, "Should have analyzed triggers"

        # Should have summary
        assert 'summary' in all_deps
        assert 'total_objects' in all_deps['summary']
        assert all_deps['summary']['total_objects'] > 0

    def test_l3_b4_get_safe_objects(self, adapter):
        """Test getting objects with only builtin functions."""
        safe = adapter.get_safe_objects()

        assert 'views' in safe
        assert 'triggers' in safe

        # Most objects should be safe (use only builtins)
        total_safe = len(safe['views']) + len(safe['triggers'])
        assert total_safe > 0, "Should have some safe objects"

    def test_l3_b4_get_objects_needing_extension(self, adapter):
        """Test getting objects that need extensions."""
        needs_ext = adapter.get_objects_needing_extension()

        assert 'views' in needs_ext
        assert 'triggers' in needs_ext

        # v_json_extension should need JSON extension
        ext_view_names = [v['name'] for v in needs_ext['views']]
        assert 'v_json_extension' in ext_view_names, \
            "v_json_extension should need JSON extension"

    def test_l3_b4_deterministic_analysis(self, adapter):
        """Test that dependency analysis is deterministic."""
        # Run analysis twice
        deps1 = adapter.get_all_dependencies()
        deps2 = adapter.get_all_dependencies()

        # Results should be identical
        assert deps1['summary']['total_objects'] == deps2['summary']['total_objects']
        assert deps1['summary']['safe_objects'] == deps2['summary']['safe_objects']
        assert set(deps1['views'].keys()) == set(deps2['views'].keys())


class TestSQLiteL3Limitations:
    """B5) Limitations tests."""

    def test_l3_b5_app_layer_report(self, adapter):
        """Test that app-layer dependencies are properly reported."""
        # Test with hypothetical unknown function
        sql = "SELECT unknown_app_func(value) FROM data"
        deps = adapter.analyze_dependencies(sql)

        assert not deps['is_safe'], "Should not be safe"
        assert 'unknown_app_func' in deps['unknown'], "Should report unknown function"

    def test_l3_b5_extension_report(self, adapter):
        """Test that extension dependencies are properly reported."""
        sql = "SELECT JSON_OBJECT('key', value), JSON_ARRAY(a, b, c) FROM data"
        deps = adapter.analyze_dependencies(sql)

        assert deps['needs_extension'], "Should need extension"
        assert 'json_object' in deps['json_extension'], "Should report JSON_OBJECT"
        assert 'json_array' in deps['json_extension'], "Should report JSON_ARRAY"


class TestSQLiteL3Requirements:
    """Test that all required L3 methods exist."""

    def test_all_l3_methods_exist(self, adapter):
        """Verify all L3 methods are implemented."""
        required_methods = [
            'extract_function_calls',
            'classify_function',
            'analyze_dependencies',
            'get_all_dependencies',
            'get_safe_objects',
            'get_objects_needing_extension',
            'get_objects_needing_app_layer'
        ]

        for method in required_methods:
            assert hasattr(adapter, method), f"Missing method: {method}"
            assert callable(getattr(adapter, method)), f"Method not callable: {method}"

    def test_l3_pragma_enforcement(self, adapter):
        """Verify PRAGMAs are explicitly set (Rule 8)."""
        pragmas = adapter.get_pragma_settings()

        # Verify PRAGMAs match our baseline (not relying on defaults)
        assert pragmas.get('foreign_keys') == PRAGMA_BASELINE['foreign_keys'], \
            f"foreign_keys not set correctly: {pragmas.get('foreign_keys')}"
        assert pragmas.get('recursive_triggers') == PRAGMA_BASELINE['recursive_triggers'], \
            f"recursive_triggers not set correctly: {pragmas.get('recursive_triggers')}"
