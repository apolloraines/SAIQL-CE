#!/usr/bin/env python3
"""
SQLite L2/L3/L4 Test Fixtures - Session-Scoped DB File

Per rules_SQLite_L2L3L4.md:
- Rule 5: Clean state per run is required (fresh DB file per run_id + teardown)
- Rule 8: PRAGMA settings must be fixed, not rely on machine defaults

This conftest provides:
1. A session-scoped temporary DB file (one per test run/pass)
2. Explicit PRAGMA enforcement on connection
3. Automatic teardown after session completes
"""

import pytest
import tempfile
import os
import uuid
from pathlib import Path

# Generate unique run_id per session
RUN_ID = str(uuid.uuid4())[:8]

# PRAGMA baseline - explicitly set, not defaults
PRAGMA_BASELINE = {
    'foreign_keys': 1,
    'recursive_triggers': 0,
    'encoding': 'UTF-8'
}


@pytest.fixture(scope='session')
def sqlite_run_id():
    """Return unique run_id for this test session."""
    return RUN_ID


@pytest.fixture(scope='session')
def sqlite_db_path(tmp_path_factory):
    """
    Create a fresh SQLite DB file for this test session.

    Per Rule 5: fresh DB file per run_id + teardown.
    The file is created in a temp directory and deleted after the session.
    """
    # Create temp directory for this run
    run_dir = tmp_path_factory.mktemp(f"sqlite_run_{RUN_ID}")
    db_path = run_dir / f"test_db_{RUN_ID}.sqlite"

    yield str(db_path)

    # Teardown: remove DB file after session
    if db_path.exists():
        os.remove(db_path)


@pytest.fixture(scope='session')
def sqlite_adapter_with_fixtures(sqlite_db_path):
    """
    Session-scoped adapter with fixtures loaded and PRAGMAs enforced.

    Per Rule 8: PRAGMA settings must be fixed for fixtures.
    """
    import sys
    sys.path.insert(0, '/home/nova/SAIQL.DEV')
    from extensions.plugins.sqlite_adapter import SQLiteAdapter

    adapter = SQLiteAdapter(database=sqlite_db_path)

    # Explicitly set PRAGMAs - do not rely on defaults
    for pragma, value in PRAGMA_BASELINE.items():
        if pragma != 'encoding':  # encoding is read-only after DB creation
            adapter.set_pragma(pragma, value)

    # Verify PRAGMAs were set
    pragmas = adapter.get_pragma_settings()
    assert pragmas.get('foreign_keys') == PRAGMA_BASELINE['foreign_keys'], \
        f"foreign_keys PRAGMA not set correctly: {pragmas.get('foreign_keys')}"
    assert pragmas.get('recursive_triggers') == PRAGMA_BASELINE['recursive_triggers'], \
        f"recursive_triggers PRAGMA not set correctly: {pragmas.get('recursive_triggers')}"

    # Load fixtures
    fixture_dir = Path('/mnt/storage/DockerTests/sqlite/fixtures')
    for fixture in ['01_schema.sql', '02_views.sql', '03_dependencies.sql', '04_triggers.sql']:
        fixture_path = fixture_dir / fixture
        with open(fixture_path, 'r') as f:
            script = f.read()
        result = adapter.execute_script(script)
        assert result['success'], f"Failed to load {fixture}: {result.get('error')}"

    yield adapter

    # Teardown: close connection
    adapter.close()


def get_pragma_baseline():
    """Return the PRAGMA baseline for manifest recording."""
    return PRAGMA_BASELINE.copy()


def get_run_id():
    """Return the current run_id."""
    return RUN_ID
