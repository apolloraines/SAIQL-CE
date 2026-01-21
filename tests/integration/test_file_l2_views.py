"""
FILE L2 (Views) Test Harness

Tests view extraction, emission, and validation for CSV/Excel file sources.
Follows Apollo Standard: clean state per run, deterministic fixtures, file-based isolation.
"""

import pytest
import shutil
import uuid
import json
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from extensions.plugins.file_adapter import FileAdapter, ViewDefinition

# =============================================================================
# Fixtures
# =============================================================================

FIXTURES_DIR = Path("/mnt/storage/DockerTests/file/fixtures")


@pytest.fixture(scope='class')
def run_dir(tmp_path_factory):
    """Create a fresh directory per test class (Rule 5: clean state per run)."""
    run_id = str(uuid.uuid4())[:8]
    run_path = tmp_path_factory.mktemp(f"file_l2_{run_id}")

    # Copy fixtures to run directory for isolation
    fixtures_dst = run_path / "fixtures"
    shutil.copytree(FIXTURES_DIR, fixtures_dst)

    yield run_path

    # Teardown: clean up (already handled by pytest tmp_path_factory)


@pytest.fixture(scope='class')
def adapter(run_dir):
    """Create FileAdapter with fresh fixtures."""
    fixtures_path = run_dir / "fixtures"
    adapter = FileAdapter(
        path=str(fixtures_path),
        schema_file=str(fixtures_path / "schema.json")
    )
    adapter.connect()

    # Load view definitions
    adapter.load_views(str(fixtures_path / "views.json"))

    yield adapter
    adapter.close()


@pytest.fixture(scope='class')
def output_dir(run_dir):
    """Output directory for test artifacts."""
    out_dir = run_dir / "output"
    out_dir.mkdir(exist_ok=True)
    return out_dir


# =============================================================================
# L2 Extraction Tests
# =============================================================================

class TestFileL2Extraction:
    """Test view extraction capabilities."""

    def test_enumerate_tables(self, adapter):
        """Test that base tables are enumerated correctly."""
        tables = adapter.get_tables()
        assert len(tables) >= 4, f"Expected at least 4 tables, got {len(tables)}"

        expected_tables = {'employees', 'departments', 'projects', 'audit_log'}
        actual_tables = set(tables)
        assert expected_tables.issubset(actual_tables), f"Missing tables: {expected_tables - actual_tables}"

    def test_enumerate_views(self, adapter):
        """Test that views are enumerated correctly."""
        views = adapter.get_views()
        assert len(views) == 5, f"Expected 5 views, got {len(views)}"

        expected_views = {
            'v_active_employees',
            'v_high_value_employees',
            'v_employee_details',
            'v_department_summary',
            'v_project_status'
        }
        actual_views = set(views)
        assert expected_views == actual_views, f"View mismatch: expected {expected_views}, got {actual_views}"

    def test_get_view_definition(self, adapter):
        """Test view definition retrieval."""
        view = adapter.get_view_definition('v_active_employees')
        assert view is not None, "View v_active_employees should exist"
        assert view.name == 'v_active_employees'
        assert 'employees' in view.source_tables
        assert 'SELECT' in view.definition.upper()
        assert len(view.columns) > 0

    def test_get_view_dependencies(self, adapter):
        """Test view dependency detection."""
        # v_high_value_employees depends on v_active_employees
        deps = adapter.get_view_dependencies('v_high_value_employees')
        assert 'v_active_employees' in deps, f"Expected dependency on v_active_employees, got {deps}"

        # v_active_employees has no view dependencies
        deps = adapter.get_view_dependencies('v_active_employees')
        assert len(deps) == 0, f"v_active_employees should have no view dependencies, got {deps}"


class TestFileL2DependencyOrdering:
    """Test view dependency ordering."""

    def test_topological_order(self, adapter):
        """Test that views are ordered with dependencies first."""
        ordered = adapter.get_views_in_dependency_order()

        assert len(ordered) == 5, f"Expected 5 views in order, got {len(ordered)}"

        # v_active_employees must come before v_high_value_employees
        active_idx = ordered.index('v_active_employees')
        high_value_idx = ordered.index('v_high_value_employees')

        assert active_idx < high_value_idx, \
            f"v_active_employees (idx={active_idx}) must come before v_high_value_employees (idx={high_value_idx})"


# =============================================================================
# L2 Emission Tests
# =============================================================================

class TestFileL2Emission:
    """Test view emission/creation capabilities."""

    def test_emit_view_definition(self, adapter):
        """Test view definition emission as SQL-like string."""
        sql = adapter.emit_view_definition('v_active_employees')

        assert 'CREATE VIEW' in sql.upper()
        assert 'v_active_employees' in sql
        assert 'SELECT' in sql.upper()

    def test_create_and_drop_view(self, adapter):
        """Test view creation and deletion."""
        test_view = ViewDefinition(
            name='v_test_view',
            description='Test view',
            source_tables=['employees'],
            definition='SELECT employee_id, first_name FROM employees WHERE is_active = 1',
            columns=[
                {'name': 'employee_id', 'type': 'INTEGER'},
                {'name': 'first_name', 'type': 'TEXT'}
            ],
            expected_row_count=8
        )

        # Create view
        adapter.create_view('v_test_view', test_view)
        assert 'v_test_view' in adapter.get_views()

        # Drop view
        result = adapter.drop_view('v_test_view')
        assert result is True
        assert 'v_test_view' not in adapter.get_views()


# =============================================================================
# L2 Validation Tests
# =============================================================================

class TestFileL2Validation:
    """Test view validation capabilities."""

    def test_validate_view_presence(self, adapter):
        """Test view presence validation."""
        for view_name in adapter.get_views():
            result = adapter.validate_view_parity(view_name)
            assert result['exists'] is True, f"View {view_name} should exist"
            assert result['definition_valid'] is True, f"View {view_name} definition should be valid"

    def test_validate_view_parity_active_employees(self, adapter):
        """Test result parity for v_active_employees."""
        result = adapter.validate_view_parity('v_active_employees')

        assert result['exists'] is True
        assert result['query_success'] is True
        assert result['row_count'] == 8, f"Expected 8 active employees, got {result['row_count']}"
        assert result['parity'] is True

    def test_validate_view_parity_high_value(self, adapter):
        """Test result parity for v_high_value_employees."""
        result = adapter.validate_view_parity('v_high_value_employees')

        assert result['exists'] is True
        assert result['query_success'] is True
        assert result['row_count'] == 5, f"Expected 5 high-value employees, got {result['row_count']}"
        assert result['parity'] is True


class TestFileL2QueryExecution:
    """Test view query execution."""

    def test_query_simple_view(self, adapter):
        """Test querying a simple view."""
        df = adapter.query_view('v_active_employees')

        assert len(df) == 8, f"Expected 8 rows, got {len(df)}"
        assert 'employee_id' in df.columns
        assert 'first_name' in df.columns
        assert 'salary' in df.columns

    def test_query_dependent_view(self, adapter):
        """Test querying a view that depends on another view."""
        df = adapter.query_view('v_high_value_employees')

        assert len(df) == 5, f"Expected 5 rows, got {len(df)}"
        # All salaries should be >= 70000
        assert all(df['salary'] >= 70000), "All salaries should be >= 70000"

    def test_query_determinism(self, adapter):
        """Test that view queries return deterministic results."""
        df1 = adapter.query_view('v_active_employees')
        df2 = adapter.query_view('v_active_employees')
        df3 = adapter.query_view('v_active_employees')

        assert len(df1) == len(df2) == len(df3), "Row counts should be identical"
        assert list(df1.columns) == list(df2.columns) == list(df3.columns), "Columns should be identical"


# =============================================================================
# L2 Requirements Tests
# =============================================================================

class TestFileL2Requirements:
    """Test that L2 requirements are met."""

    def test_a1_extraction_requirement(self, adapter):
        """A1) Enumerate views with name, definition, dependencies."""
        views = adapter.get_views()
        assert len(views) == 5

        for view_name in views:
            view = adapter.get_view_definition(view_name)
            assert view is not None
            assert view.name == view_name
            assert view.definition is not None
            assert isinstance(view.dependencies, list)

    def test_a2_emission_requirement(self, adapter):
        """A2) Create views in correct dependency order."""
        ordered = adapter.get_views_in_dependency_order()

        # Verify base views come before dependent views
        seen = set()
        for view_name in ordered:
            deps = adapter.get_view_dependencies(view_name)
            for dep in deps:
                if dep in adapter.get_views():
                    assert dep in seen, f"Dependency {dep} should come before {view_name}"
            seen.add(view_name)

    def test_a3_validation_requirement(self, adapter):
        """A3) View presence parity, definition parity, result parity."""
        for view_name in adapter.get_views():
            result = adapter.validate_view_parity(view_name)

            # Presence parity
            assert result['exists'] is True

            # Definition parity
            assert result['definition_valid'] is True

            # Result parity (query succeeds and row count matches)
            assert result['query_success'] is True
            assert result['parity'] is True


class TestFileL2CleanState:
    """Test clean state isolation (Rule 5)."""

    def test_isolation_fresh_fixtures(self, run_dir):
        """Verify fixtures are copied fresh for each run."""
        fixtures_path = run_dir / "fixtures"
        assert fixtures_path.exists(), "Fixtures should be copied to run directory"
        assert (fixtures_path / "employees.csv").exists()
        assert (fixtures_path / "views.json").exists()

    def test_isolation_run_id_unique(self, run_dir):
        """Verify run directory has unique ID."""
        # run_dir path contains UUID
        assert 'file_l2_' in str(run_dir), "Run directory should contain file_l2_ prefix"
