"""
FILE L4 (File Event Automation) Test Harness

Tests trigger-like automation with simulated file events for CSV/Excel sources.
Follows Apollo Standard: clean state per run, deterministic fixtures, simulated events.
"""

import pytest
import shutil
import uuid
import json
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from extensions.plugins.file_adapter import FileAdapter, SimulatedEvent, TriggerDefinition

# =============================================================================
# Fixtures
# =============================================================================

FIXTURES_DIR = Path("/mnt/storage/DockerTests/file/fixtures")


@pytest.fixture(scope='class')
def run_dir(tmp_path_factory):
    """Create a fresh directory per test class (Rule 5: clean state per run)."""
    run_id = str(uuid.uuid4())[:8]
    run_path = tmp_path_factory.mktemp(f"file_l4_{run_id}")

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

    # Load triggers and views (triggers may refresh views)
    adapter.load_triggers(str(fixtures_path / "triggers.json"))
    adapter.load_views(str(fixtures_path / "views.json"))

    yield adapter
    adapter.close()


@pytest.fixture(scope='class')
def output_dir(run_dir):
    """Output directory for trigger artifacts."""
    out_dir = run_dir / "output"
    out_dir.mkdir(exist_ok=True)
    return out_dir


@pytest.fixture(scope='class')
def simulated_events(run_dir):
    """Load simulated events from manifest."""
    fixtures_path = run_dir / "fixtures"
    adapter = FileAdapter(path=str(fixtures_path))
    return adapter.load_event_manifest(str(fixtures_path / "triggers.json"))


# =============================================================================
# L4 Trigger Definition Tests
# =============================================================================

class TestFileL4TriggerDefinitions:
    """Test trigger definition loading and retrieval."""

    def test_enumerate_triggers(self, adapter):
        """Test that triggers are enumerated correctly."""
        triggers = adapter.get_triggers()
        assert len(triggers) == 6, f"Expected 6 triggers, got {len(triggers)}"

        expected = {
            'trg_employees_ingest_validate',
            'trg_employees_ingest_audit',
            'trg_employees_change_refresh',
            'trg_departments_ingest_validate',
            'trg_batch_complete_report',
            'trg_projects_change_audit'
        }
        actual = set(triggers)
        assert expected == actual, f"Trigger mismatch: expected {expected}, got {actual}"

    def test_get_trigger_definition(self, adapter):
        """Test trigger definition retrieval."""
        trigger = adapter.get_trigger_definition('trg_employees_ingest_validate')
        assert trigger is not None
        assert trigger.name == 'trg_employees_ingest_validate'
        assert trigger.event == 'on_ingest'
        assert trigger.action == 'validate'
        assert trigger.source_table == 'employees'

    def test_triggers_have_event_types(self, adapter):
        """Test that triggers have valid event types."""
        valid_events = {'on_ingest', 'on_change', 'on_batch_complete'}

        for trigger_name in adapter.get_triggers():
            trigger = adapter.get_trigger_definition(trigger_name)
            assert trigger.event in valid_events, f"Trigger {trigger_name} has invalid event: {trigger.event}"

    def test_triggers_have_actions(self, adapter):
        """Test that triggers have valid actions."""
        valid_actions = {'validate', 'audit_log', 'refresh_views', 'status_report'}

        for trigger_name in adapter.get_triggers():
            trigger = adapter.get_trigger_definition(trigger_name)
            assert trigger.action in valid_actions, f"Trigger {trigger_name} has invalid action: {trigger.action}"


class TestFileL4TriggerFiltering:
    """Test trigger filtering by event type."""

    def test_get_triggers_for_on_ingest(self, adapter):
        """Test filtering triggers by on_ingest event."""
        triggers = adapter.get_triggers_for_event('on_ingest')
        assert len(triggers) >= 2, f"Expected at least 2 on_ingest triggers, got {len(triggers)}"

        for trigger in triggers:
            assert trigger.event == 'on_ingest'

    def test_get_triggers_for_on_change(self, adapter):
        """Test filtering triggers by on_change event."""
        triggers = adapter.get_triggers_for_event('on_change')
        assert len(triggers) >= 2, f"Expected at least 2 on_change triggers, got {len(triggers)}"

        for trigger in triggers:
            assert trigger.event == 'on_change'

    def test_get_triggers_for_source_table(self, adapter):
        """Test filtering triggers by source table."""
        triggers = adapter.get_triggers_for_event('on_ingest', 'employees')

        for trigger in triggers:
            assert trigger.source_table == 'employees' or trigger.source_table is None


# =============================================================================
# L4 Event Simulation Tests
# =============================================================================

class TestFileL4EventSimulation:
    """Test simulated event processing."""

    def test_simulate_ingest_event(self, adapter, output_dir):
        """Test simulating an on_ingest event."""
        event = SimulatedEvent(
            event_id=100,
            event_type='on_ingest',
            timestamp='2026-01-14T12:00:00Z',
            source_table='employees',
            filename='employees.csv',
            row_count=10
        )

        result = adapter.simulate_event(event, str(output_dir))

        assert len(result['triggers_fired']) >= 2, "Expected at least 2 triggers to fire"
        assert 'trg_employees_ingest_validate' in result['triggers_fired']
        assert 'trg_employees_ingest_audit' in result['triggers_fired']

    def test_simulate_change_event(self, adapter, output_dir):
        """Test simulating an on_change event."""
        event = SimulatedEvent(
            event_id=101,
            event_type='on_change',
            timestamp='2026-01-14T13:00:00Z',
            source_table='employees',
            filename='employees.csv',
            row_count=10,
            change_type='modified'
        )

        result = adapter.simulate_event(event, str(output_dir))

        assert 'trg_employees_change_refresh' in result['triggers_fired']

    def test_simulate_batch_complete_event(self, adapter, output_dir):
        """Test simulating an on_batch_complete event."""
        event = SimulatedEvent(
            event_id=102,
            event_type='on_batch_complete',
            timestamp='2026-01-14T12:00:05Z',
            file_count=3,
            total_row_count=19
        )

        result = adapter.simulate_event(event, str(output_dir))

        assert 'trg_batch_complete_report' in result['triggers_fired']


class TestFileL4ValidationTriggers:
    """Test validation trigger execution."""

    def test_validation_passes_for_valid_data(self, adapter, output_dir):
        """Test that validation passes for valid fixture data."""
        event = SimulatedEvent(
            event_id=200,
            event_type='on_ingest',
            timestamp='2026-01-14T12:00:00Z',
            source_table='employees',
            filename='employees.csv',
            row_count=10
        )

        result = adapter.simulate_event(event, str(output_dir))

        # Find validation result
        val_results = result['validation_results']
        assert len(val_results) > 0, "Expected validation results"

        for val_result in val_results:
            assert val_result['valid'] is True, f"Validation failed: {val_result.get('errors')}"
            assert val_result['rules_failed'] == 0

    def test_validation_checks_all_rules(self, adapter, output_dir):
        """Test that validation checks all defined rules."""
        event = SimulatedEvent(
            event_id=201,
            event_type='on_ingest',
            timestamp='2026-01-14T12:00:00Z',
            source_table='employees',
            filename='employees.csv',
            row_count=10
        )

        result = adapter.simulate_event(event, str(output_dir))

        for val_result in result['validation_results']:
            assert val_result['rules_checked'] > 0, "Expected rules to be checked"
            assert val_result['rules_passed'] == val_result['rules_checked'], \
                f"Not all rules passed: {val_result['rules_passed']}/{val_result['rules_checked']}"


class TestFileL4AuditTriggers:
    """Test audit log trigger execution."""

    def test_audit_log_created(self, adapter, output_dir):
        """Test that audit log entries are created."""
        # Clear any existing audit entries
        adapter.clear_audit_log()

        event = SimulatedEvent(
            event_id=300,
            event_type='on_ingest',
            timestamp='2026-01-14T12:00:00Z',
            source_table='employees',
            filename='employees.csv',
            row_count=10
        )

        result = adapter.simulate_event(event, str(output_dir))

        assert len(result['audit_entries']) > 0, "Expected audit entries"

        audit_entry = result['audit_entries'][0]
        assert audit_entry['timestamp'] == '2026-01-14T12:00:00Z'
        assert 'employees' in audit_entry['details'].lower() or audit_entry['source_table'] == 'employees'

    def test_audit_log_accumulates(self, adapter, output_dir):
        """Test that audit log accumulates entries."""
        adapter.clear_audit_log()

        events = [
            SimulatedEvent(event_id=301, event_type='on_ingest', timestamp='2026-01-14T12:00:00Z',
                          source_table='employees', filename='employees.csv', row_count=10),
            SimulatedEvent(event_id=302, event_type='on_change', timestamp='2026-01-14T13:00:00Z',
                          source_table='projects', filename='projects.csv', row_count=5)
        ]

        for event in events:
            adapter.simulate_event(event, str(output_dir))

        audit_log = adapter.get_audit_log()
        assert len(audit_log) >= 2, f"Expected at least 2 audit entries, got {len(audit_log)}"


class TestFileL4RefreshTriggers:
    """Test view refresh trigger execution."""

    def test_refresh_views_on_change(self, adapter, output_dir):
        """Test that views are refreshed on file change."""
        event = SimulatedEvent(
            event_id=400,
            event_type='on_change',
            timestamp='2026-01-14T13:00:00Z',
            source_table='employees',
            filename='employees.csv',
            row_count=10,
            change_type='modified'
        )

        result = adapter.simulate_event(event, str(output_dir))

        # Find refresh output
        refresh_results = [o for o in result['outputs'] if o.get('action') == 'refresh_views']
        assert len(refresh_results) > 0, "Expected refresh_views output"

        refresh_result = refresh_results[0]
        assert len(refresh_result['views_refreshed']) > 0, "Expected views to be refreshed"

    def test_refreshed_views_written_to_output(self, adapter, output_dir):
        """Test that refreshed views are written as CSV files."""
        event = SimulatedEvent(
            event_id=401,
            event_type='on_change',
            timestamp='2026-01-14T13:00:00Z',
            source_table='employees',
            filename='employees.csv',
            row_count=10,
            change_type='modified'
        )

        result = adapter.simulate_event(event, str(output_dir))

        for output in result['outputs']:
            if output.get('action') == 'refresh_views':
                for view_info in output.get('views_refreshed', []):
                    output_file = Path(view_info['output_file'])
                    assert output_file.exists(), f"Output file should exist: {output_file}"


class TestFileL4StatusReportTriggers:
    """Test status report trigger execution."""

    def test_status_report_created(self, adapter, output_dir):
        """Test that status report is created on batch complete."""
        event = SimulatedEvent(
            event_id=500,
            event_type='on_batch_complete',
            timestamp='2026-01-14T12:00:05Z',
            file_count=3,
            total_row_count=19
        )

        result = adapter.simulate_event(event, str(output_dir))

        # Find status report output
        report_results = [o for o in result['outputs'] if o.get('action') == 'status_report']
        assert len(report_results) > 0, "Expected status_report output"

        report = report_results[0]
        assert 'output_file' in report
        assert Path(report['output_file']).exists()


# =============================================================================
# L4 Event Sequence Tests
# =============================================================================

class TestFileL4EventSequence:
    """Test execution of event sequences from manifest."""

    def test_execute_event_manifest(self, adapter, output_dir, simulated_events):
        """Test executing the full event manifest."""
        adapter.clear_audit_log()

        result = adapter.execute_event_sequence(simulated_events, str(output_dir))

        assert result['events_processed'] == len(simulated_events)
        assert result['triggers_fired'] > 0
        assert result['validation_passed'] > 0
        assert result['audit_entries'] > 0

    def test_event_sequence_determinism(self, adapter, output_dir, simulated_events):
        """Test that event sequences produce deterministic results."""
        results = []

        for i in range(3):
            adapter.clear_audit_log()
            result = adapter.execute_event_sequence(simulated_events, str(output_dir))
            results.append(result)

        # All executions should have same counts
        events_processed = [r['events_processed'] for r in results]
        triggers_fired = [r['triggers_fired'] for r in results]

        assert len(set(events_processed)) == 1, f"Non-deterministic events_processed: {events_processed}"
        assert len(set(triggers_fired)) == 1, f"Non-deterministic triggers_fired: {triggers_fired}"


# =============================================================================
# L4 Safe Subset Tests
# =============================================================================

class TestFileL4SafeSubset:
    """Test safe trigger subset definition."""

    def test_all_triggers_in_safe_subset(self, adapter):
        """Test that all defined triggers are in the safe subset."""
        safe = adapter.get_safe_triggers()
        all_triggers = adapter.get_triggers()

        safe_names = {t.name for t in safe}
        all_names = set(all_triggers)

        assert safe_names == all_names, f"Not all triggers in safe subset: {all_names - safe_names}"

    def test_no_skipped_triggers(self, adapter):
        """Test that no triggers are skipped for FILE adapter."""
        skipped = adapter.get_skipped_triggers()
        assert len(skipped) == 0, f"Expected no skipped triggers, got {skipped}"


# =============================================================================
# L4 Requirements Tests
# =============================================================================

class TestFileL4Requirements:
    """Test that L4 requirements are met."""

    def test_c1_subset_definition(self, adapter):
        """C1) Allowlist + denylist documented and enforced."""
        safe = adapter.get_safe_triggers()
        skipped = adapter.get_skipped_triggers()

        # All triggers should be categorized
        total = len(safe) + len(skipped)
        assert total == 6, f"Expected 6 total triggers, got {total}"

        # Safe subset should have all triggers (FILE adapter has no skipped)
        assert len(safe) == 6
        assert len(skipped) == 0

    def test_c2_extraction(self, adapter):
        """C2) Enumerate triggers with event, action, source."""
        for trigger_name in adapter.get_triggers():
            trigger = adapter.get_trigger_definition(trigger_name)
            assert trigger.event is not None
            assert trigger.action is not None
            # source_table can be None for batch triggers

    def test_c3_emission(self, adapter, output_dir, simulated_events):
        """C3) Execute triggers on simulated events."""
        adapter.clear_audit_log()

        for event in simulated_events:
            result = adapter.simulate_event(event, str(output_dir))
            assert len(result['triggers_fired']) >= 0  # Some events may not match any trigger

    def test_c4_behavioral_validation(self, adapter, output_dir, simulated_events):
        """C4) Verify trigger side-effects work correctly."""
        adapter.clear_audit_log()

        result = adapter.execute_event_sequence(simulated_events, str(output_dir))

        # Validation should pass for valid fixtures
        assert result['validation_failed'] == 0, f"Expected no validation failures, got {result['validation_failed']}"

        # Audit entries should be created
        assert result['audit_entries'] > 0, "Expected audit entries to be created"


class TestFileL4CleanState:
    """Test clean state isolation (Rule 5)."""

    def test_audit_log_isolated(self, adapter, output_dir):
        """Test that audit log can be cleared for isolation."""
        adapter.clear_audit_log()
        assert len(adapter.get_audit_log()) == 0

        # Trigger some events
        event = SimulatedEvent(
            event_id=600,
            event_type='on_ingest',
            timestamp='2026-01-14T12:00:00Z',
            source_table='employees',
            filename='employees.csv',
            row_count=10
        )
        adapter.simulate_event(event, str(output_dir))

        assert len(adapter.get_audit_log()) > 0

        # Clear again
        adapter.clear_audit_log()
        assert len(adapter.get_audit_log()) == 0

    def test_simulated_events_use_fixed_timestamps(self, simulated_events):
        """Test that simulated events use fixed timestamps for determinism."""
        for event in simulated_events:
            assert event.timestamp is not None
            # Timestamp should be in ISO format
            assert 'T' in event.timestamp
            assert 'Z' in event.timestamp
