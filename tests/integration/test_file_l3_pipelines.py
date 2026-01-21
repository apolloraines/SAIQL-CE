"""
FILE L3 (Transform Pipelines) Test Harness

Tests deterministic transform pipeline execution for CSV/Excel file sources.
Follows Apollo Standard: clean state per run, deterministic fixtures, file-based isolation.
"""

import pytest
import shutil
import uuid
import json
import pandas as pd
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from extensions.plugins.file_adapter import FileAdapter, PipelineDefinition

# =============================================================================
# Fixtures
# =============================================================================

FIXTURES_DIR = Path("/mnt/storage/DockerTests/file/fixtures")


@pytest.fixture(scope='class')
def run_dir(tmp_path_factory):
    """Create a fresh directory per test class (Rule 5: clean state per run)."""
    run_id = str(uuid.uuid4())[:8]
    run_path = tmp_path_factory.mktemp(f"file_l3_{run_id}")

    # Copy fixtures to run directory for isolation
    fixtures_dst = run_path / "fixtures"
    shutil.copytree(FIXTURES_DIR, fixtures_dst)

    yield run_path


@pytest.fixture(scope='class')
def adapter(run_dir):
    """Create FileAdapter with fresh fixtures."""
    fixtures_path = run_dir / "fixtures"
    adapter = FileAdapter(
        path=str(fixtures_path),
        schema_file=str(fixtures_path / "schema.json")
    )
    adapter.connect()

    # Load pipeline definitions
    adapter.load_pipelines(str(fixtures_path / "pipelines.json"))

    yield adapter
    adapter.close()


@pytest.fixture(scope='class')
def output_dir(run_dir):
    """Output directory for pipeline results."""
    out_dir = run_dir / "output"
    out_dir.mkdir(exist_ok=True)
    return out_dir


# =============================================================================
# L3 Pipeline Definition Tests
# =============================================================================

class TestFileL3PipelineDefinitions:
    """Test pipeline definition loading and validation."""

    def test_enumerate_pipelines(self, adapter):
        """Test that pipelines are enumerated correctly."""
        pipelines = adapter.get_pipelines()
        assert len(pipelines) == 5, f"Expected 5 pipelines, got {len(pipelines)}"

        expected = {
            'p_monthly_salary_report',
            'p_active_employee_export',
            'p_project_budget_analysis',
            'p_high_salary_employees',
            'p_department_employee_list'
        }
        actual = set(pipelines)
        assert expected == actual, f"Pipeline mismatch: expected {expected}, got {actual}"

    def test_get_pipeline_definition(self, adapter):
        """Test pipeline definition retrieval."""
        pipeline = adapter.get_pipeline_definition('p_monthly_salary_report')
        assert pipeline is not None
        assert pipeline.name == 'p_monthly_salary_report'
        assert len(pipeline.steps) > 0
        assert pipeline.output_file == 'monthly_salary_report.csv'

    def test_pipeline_has_expected_output(self, adapter):
        """Test that pipelines have expected output definitions."""
        for pipeline_name in adapter.get_pipelines():
            pipeline = adapter.get_pipeline_definition(pipeline_name)
            assert 'columns' in pipeline.expected_output, f"Pipeline {pipeline_name} missing expected columns"
            assert 'row_count' in pipeline.expected_output, f"Pipeline {pipeline_name} missing expected row_count"


class TestFileL3DeterminismValidation:
    """Test that pipelines use only deterministic operations."""

    def test_validate_deterministic_operations(self, adapter):
        """Test that all pipelines use only allowed operations."""
        for pipeline_name in adapter.get_pipelines():
            result = adapter.validate_pipeline_definition(pipeline_name)
            assert result['valid'] is True, f"Pipeline {pipeline_name} has invalid operations: {result.get('issues')}"

    def test_no_nondeterministic_functions(self, adapter):
        """Test that no pipeline uses NOW(), RANDOM(), etc."""
        for pipeline_name in adapter.get_pipelines():
            result = adapter.validate_pipeline_definition(pipeline_name)
            for issue in result.get('issues', []):
                assert 'non-deterministic' not in issue.lower(), f"Pipeline {pipeline_name} uses non-deterministic function"

    def test_operations_are_in_allowlist(self, adapter):
        """Test that all operations are in the allowed set."""
        allowed = {'JOIN', 'FILTER', 'AGGREGATE', 'PROJECT', 'SORT', 'UNION'}

        for pipeline_name in adapter.get_pipelines():
            result = adapter.validate_pipeline_definition(pipeline_name)
            for op in result.get('operations', []):
                assert op.upper() in allowed, f"Pipeline {pipeline_name} uses non-allowed operation: {op}"


# =============================================================================
# L3 Pipeline Execution Tests
# =============================================================================

class TestFileL3PipelineExecution:
    """Test pipeline execution capabilities."""

    def test_execute_salary_report_pipeline(self, adapter, output_dir):
        """Test monthly salary report pipeline execution."""
        result = adapter.execute_pipeline('p_monthly_salary_report', str(output_dir))

        assert result['success'] is True
        assert result['row_count'] == 4, f"Expected 4 departments, got {result['row_count']}"
        assert Path(result['output_file']).exists()

    def test_execute_employee_export_pipeline(self, adapter, output_dir):
        """Test active employee export pipeline execution."""
        result = adapter.execute_pipeline('p_active_employee_export', str(output_dir))

        assert result['success'] is True
        assert result['row_count'] == 8, f"Expected 8 active employees, got {result['row_count']}"

        # Verify output columns
        expected_cols = ['employee_id', 'first_name', 'last_name', 'email', 'department_name', 'salary']
        assert set(result['columns']) == set(expected_cols), f"Column mismatch: {result['columns']}"

    def test_execute_project_budget_pipeline(self, adapter, output_dir):
        """Test project budget analysis pipeline execution."""
        result = adapter.execute_pipeline('p_project_budget_analysis', str(output_dir))

        assert result['success'] is True
        assert result['row_count'] == 2, f"Expected 2 status types, got {result['row_count']}"

    def test_execute_high_salary_pipeline(self, adapter, output_dir):
        """Test high salary employees pipeline with parameter."""
        result = adapter.execute_pipeline('p_high_salary_employees', str(output_dir))

        assert result['success'] is True
        assert result['row_count'] == 5, f"Expected 5 high-salary employees, got {result['row_count']}"

    def test_execute_department_list_pipeline(self, adapter, output_dir):
        """Test department employee list pipeline."""
        result = adapter.execute_pipeline('p_department_employee_list', str(output_dir))

        assert result['success'] is True
        assert result['row_count'] == 10, f"Expected 10 employees, got {result['row_count']}"


class TestFileL3PipelineOutputValidation:
    """Test pipeline output validation."""

    def test_validate_salary_report_output(self, adapter, output_dir):
        """Test validation of salary report output."""
        # Execute first
        adapter.execute_pipeline('p_monthly_salary_report', str(output_dir))

        # Validate
        result = adapter.validate_pipeline_output('p_monthly_salary_report', str(output_dir))

        assert result['valid'] is True
        assert result['row_count_match'] is True
        assert result['columns_match'] is True

    def test_validate_employee_export_output(self, adapter, output_dir):
        """Test validation of employee export output."""
        adapter.execute_pipeline('p_active_employee_export', str(output_dir))
        result = adapter.validate_pipeline_output('p_active_employee_export', str(output_dir))

        assert result['valid'] is True
        assert result['row_count'] == 8
        assert result['row_count_match'] is True

    def test_validate_all_pipeline_outputs(self, adapter, output_dir):
        """Test validation of all pipeline outputs."""
        # Execute all pipelines
        for pipeline_name in adapter.get_pipelines():
            adapter.execute_pipeline(pipeline_name, str(output_dir))

        # Validate all
        for pipeline_name in adapter.get_pipelines():
            result = adapter.validate_pipeline_output(pipeline_name, str(output_dir))
            assert result['valid'] is True, f"Pipeline {pipeline_name} output validation failed: {result}"


# =============================================================================
# L3 Determinism Tests
# =============================================================================

class TestFileL3Determinism:
    """Test that pipelines produce deterministic results."""

    def test_execution_determinism(self, adapter, output_dir):
        """Test that multiple executions produce identical results."""
        results = []

        for i in range(3):
            result = adapter.execute_pipeline('p_monthly_salary_report', str(output_dir))
            results.append(result)

        # All executions should have same row count
        row_counts = [r['row_count'] for r in results]
        assert len(set(row_counts)) == 1, f"Non-deterministic row counts: {row_counts}"

        # All executions should have same columns
        col_sets = [tuple(sorted(r['columns'])) for r in results]
        assert len(set(col_sets)) == 1, "Non-deterministic columns"

    def test_output_content_determinism(self, adapter, output_dir):
        """Test that output file content is deterministic."""
        # Execute twice
        adapter.execute_pipeline('p_high_salary_employees', str(output_dir))
        df1 = pd.read_csv(output_dir / 'high_salary_employees.csv')

        adapter.execute_pipeline('p_high_salary_employees', str(output_dir))
        df2 = pd.read_csv(output_dir / 'high_salary_employees.csv')

        # Compare content
        assert len(df1) == len(df2)
        assert list(df1.columns) == list(df2.columns)
        assert df1.equals(df2), "Output content should be identical"


# =============================================================================
# L3 Requirements Tests
# =============================================================================

class TestFileL3Requirements:
    """Test that L3 requirements are met."""

    def test_b1_presence_parity(self, adapter):
        """B1) Pipeline definitions present and retrievable."""
        pipelines = adapter.get_pipelines()
        assert len(pipelines) == 5

        for name in pipelines:
            pipeline = adapter.get_pipeline_definition(name)
            assert pipeline is not None
            assert pipeline.name == name

    def test_b2_definition_parity(self, adapter):
        """B2) Pipeline definitions are valid and normalized."""
        for name in adapter.get_pipelines():
            result = adapter.validate_pipeline_definition(name)
            assert result['valid'] is True

    def test_b3_behavioral_parity(self, adapter, output_dir):
        """B3) Pipelines produce expected outputs."""
        for name in adapter.get_pipelines():
            exec_result = adapter.execute_pipeline(name, str(output_dir))
            assert exec_result['success'] is True

            val_result = adapter.validate_pipeline_output(name, str(output_dir))
            assert val_result['valid'] is True


class TestFileL3CleanState:
    """Test clean state isolation (Rule 5)."""

    def test_fresh_output_directory(self, output_dir):
        """Verify output directory is fresh per run."""
        assert output_dir.exists()
        # Directory should be within a unique run path
        assert 'file_l3_' in str(output_dir.parent)

    def test_isolated_fixtures(self, run_dir):
        """Verify fixtures are isolated per run."""
        fixtures_path = run_dir / "fixtures"
        assert fixtures_path.exists()
        assert (fixtures_path / "employees.csv").exists()
        assert (fixtures_path / "pipelines.json").exists()
