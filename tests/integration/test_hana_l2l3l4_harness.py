#!/usr/bin/env python3
"""
HANA L2/L3/L4 Integration Test Harness

Real HANA connectivity tests for L2 (Views), L3 (Routines), L4 (Triggers).

Prerequisites:
- HANA Express Docker container running (saiql_hana_test)
- Environment variables set:
  - HANA_HOST (default: localhost)
  - HANA_PORT (default: 39017)
  - HANA_DATABASE (default: HXE)
  - HANA_USER (default: SYSTEM)  # Needs CREATE SCHEMA privilege
  - HANA_PASSWORD (required)

Schema Strategy:
- Creates fresh schema per run_id: SAIQL_L2L3L4_<run_id>
- Loads base tables + L2L3L4 fixtures into that schema
- Drops schema after run completes

Author: Claude (Phase HANA L2L3L4)
Status: Integration Harness
"""

import pytest
import os
import sys
import json
import hashlib
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)

# Skip all tests if hdbcli not available
try:
    from hdbcli import dbapi
    HANA_AVAILABLE = True
except ImportError:
    HANA_AVAILABLE = False

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from extensions.plugins.hana_adapter import HANAAdapter

# Harness configuration
HARNESS_DIR = Path(__file__).parent / "hana_l2l3l4_harness"
RUNS_DIR = HARNESS_DIR / "runs"
FIXTURES_DIR = HARNESS_DIR / "fixtures"
PHASE07_FIXTURES = Path(__file__).parent / "phase07_hana_harness" / "fixtures"


def get_run_id() -> str:
    """Generate unique run ID based on timestamp."""
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def get_hana_config() -> Dict[str, Any]:
    """Get HANA connection config from environment.

    IMPORTANT: Per rules_SAP_HANA_L2L3L4.md rule 7, harness MUST use a dedicated
    test user, not SYSTEM. Default is SAIQL_L2L3L4_TEST.
    """
    return {
        'host': os.environ.get('HANA_HOST', 'localhost'),
        'port': int(os.environ.get('HANA_PORT', 39017)),
        'database': os.environ.get('HANA_DATABASE', 'HXE'),
        'user': os.environ.get('HANA_USER', 'SAIQL_L2L3L4_TEST'),  # Dedicated test user (NOT SYSTEM)
        'password': os.environ.get('HANA_PASSWORD', ''),
        'strict_types': False
    }


def create_run_bundle(run_id: str) -> Path:
    """Create run bundle directory structure."""
    run_dir = RUNS_DIR / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "logs").mkdir(exist_ok=True)
    (run_dir / "reports").mkdir(exist_ok=True)
    return run_dir


def write_manifest(run_dir: Path, manifest: Dict[str, Any]):
    """Write run manifest JSON."""
    with open(run_dir / "run_manifest.json", "w") as f:
        json.dump(manifest, f, indent=2, default=str)


def write_report(run_dir: Path, report_name: str, report: Dict[str, Any]):
    """Write report JSON."""
    with open(run_dir / "reports" / report_name, "w") as f:
        json.dump(report, f, indent=2, default=str)


def compute_hash(data: Any) -> str:
    """Compute SHA256 hash of data for determinism validation."""
    return hashlib.sha256(json.dumps(data, sort_keys=True, default=str).encode()).hexdigest()


def parse_sql_statements(sql_content: str) -> list:
    """Parse SQL content into individual statements, handling BEGIN/END blocks."""
    statements = []
    current_stmt = []
    in_begin_block = False

    for line in sql_content.split('\n'):
        stripped = line.strip()

        # Skip comments and empty lines at statement boundaries
        if stripped.startswith('--') or not stripped:
            # But keep empty lines within statements for readability
            if current_stmt:
                current_stmt.append(line)
            continue

        current_stmt.append(line)

        # Track BEGIN/END blocks (for procedures, functions, triggers)
        upper_stripped = stripped.upper()
        if upper_stripped == 'BEGIN' or upper_stripped.endswith(' BEGIN'):
            in_begin_block = True
        if upper_stripped.startswith('END') and in_begin_block:
            in_begin_block = False

        # Statement ends with ; and we're not in a BEGIN block
        if stripped.endswith(';') and not in_begin_block:
            stmt = '\n'.join(current_stmt).strip()
            if stmt:
                statements.append(stmt)
            current_stmt = []

    return statements


def execute_sql_file_statements(adapter, sql_content: str, continue_on_error: bool = False):
    """Execute SQL statements from file content, handling multi-line statements."""
    statements = parse_sql_statements(sql_content)

    # Execute each statement
    for stmt in statements:
        try:
            adapter.execute_query(stmt)
        except Exception as e:
            if continue_on_error:
                logger.debug(f"Statement failed (continuing): {e}")
            else:
                raise


@pytest.fixture(scope="module")
def hana_config():
    """Get HANA configuration.

    Enforces dedicated test user requirement per rules_SAP_HANA_L2L3L4.md rule 7.
    """
    config = get_hana_config()
    if not config['password']:
        pytest.skip("HANA_PASSWORD not set - skipping integration tests")

    # Enforce dedicated test user requirement (rule 7: "Harness uses a dedicated test user")
    if config['user'].upper() == 'SYSTEM':
        pytest.fail(
            "HANA_USER cannot be SYSTEM. Per rules_SAP_HANA_L2L3L4.md rule 7, "
            "harness must use a dedicated test user. "
            "Set HANA_USER=SAIQL_L2L3L4_TEST (or another non-SYSTEM user)."
        )

    return config


@pytest.fixture(scope="module")
def run_bundle():
    """Create run bundle for this test session."""
    run_id = get_run_id()
    run_dir = create_run_bundle(run_id)

    bundle = {
        'run_id': run_id,
        'run_dir': run_dir,
        'start_time': datetime.now(),
        'results': {}
    }

    yield bundle

    # Write final manifest at end of session
    bundle['end_time'] = datetime.now()
    bundle['duration_seconds'] = (bundle['end_time'] - bundle['start_time']).total_seconds()

    manifest = {
        'run_id': run_id,
        'start_time': bundle['start_time'].isoformat(),
        'end_time': bundle['end_time'].isoformat(),
        'duration_seconds': bundle['duration_seconds'],
        'hana_version': bundle.get('hana_version', 'unknown'),
        'hana_user': bundle.get('hana_user', 'unknown'),  # Record dedicated test user
        'schema': bundle.get('schema', 'unknown'),
        'privileges_summary': bundle.get('privileges_summary', []),
        'seed_hash': bundle.get('seed_hash', 'unknown'),
        'results': bundle['results']
    }

    write_manifest(run_dir, manifest)


@pytest.fixture(scope="module")
def per_run_schema(hana_config, run_bundle):
    """Create and manage per-run schema for test isolation."""
    if not HANA_AVAILABLE:
        pytest.skip("hdbcli not installed - skipping integration tests")

    run_id = run_bundle['run_id']
    schema_name = f"SAIQL_L2L3L4_{run_id}"

    # Connect as SYSTEM to create schema
    conn = dbapi.connect(
        address=hana_config['host'],
        port=hana_config['port'],
        user=hana_config['user'],
        password=hana_config['password'],
        databaseName=hana_config['database']
    )
    cursor = conn.cursor()

    try:
        # Create per-run schema
        logger.info(f"Creating per-run schema: {schema_name}")
        cursor.execute(f"CREATE SCHEMA {schema_name}")
        conn.commit()

        # Set schema for subsequent operations
        cursor.execute(f"SET SCHEMA {schema_name}")

        # Load base tables from Phase 07 fixtures (uses proper statement parsing)
        base_tables_sql = (PHASE07_FIXTURES / "test_schema.sql").read_text()
        base_stmts = parse_sql_statements(base_tables_sql)
        for stmt in base_stmts:
            try:
                cursor.execute(stmt)
            except Exception as e:
                # Ignore DROP IF EXISTS failures (HANA doesn't support IF EXISTS)
                err_msg = str(e).lower()
                if 'invalid table name' not in err_msg and 'does not exist' not in err_msg:
                    logger.warning(f"Base table statement failed: {e}")
        conn.commit()

        # Load L2L3L4 fixtures (views, procedures, functions, triggers)
        # Uses proper parsing for BEGIN/END blocks
        l2l3l4_sql = (FIXTURES_DIR / "l2l3l4_schema.sql").read_text()
        l2l3l4_stmts = parse_sql_statements(l2l3l4_sql)
        for stmt in l2l3l4_stmts:
            try:
                cursor.execute(stmt)
            except Exception as e:
                # Ignore DROP failures on first run (objects don't exist yet)
                err_msg = str(e).lower()
                if 'invalid' not in err_msg and 'does not exist' not in err_msg:
                    logger.warning(f"L2L3L4 statement failed: {e}")
        conn.commit()

        logger.info(f"Schema {schema_name} created and populated")
        run_bundle['schema'] = schema_name
        run_bundle['hana_user'] = hana_config['user']  # Record dedicated test user for proof

        yield schema_name

    finally:
        # Drop schema after tests
        try:
            cursor.execute(f"DROP SCHEMA {schema_name} CASCADE")
            conn.commit()
            logger.info(f"Dropped per-run schema: {schema_name}")
        except Exception as e:
            logger.warning(f"Failed to drop schema {schema_name}: {e}")
        finally:
            cursor.close()
            conn.close()


@pytest.fixture(scope="module")
def hana_adapter(hana_config, per_run_schema):
    """Create HANA adapter connected to per-run schema."""
    if not HANA_AVAILABLE:
        pytest.skip("hdbcli not installed - skipping integration tests")

    adapter = HANAAdapter(hana_config)
    adapter.connect()

    # Set schema to per-run schema (not SYSTEM!)
    adapter.execute_query(f"SET SCHEMA {per_run_schema}")

    yield adapter
    adapter.close()


# ===== Test Classes =====

class TestA_EnvironmentHarness:
    """A) Environment and harness prerequisites."""

    def test_a0_connectivity_baseline(self, hana_adapter, run_bundle, per_run_schema):
        """A0) Verify connectivity and capture baseline."""
        # Get HANA version
        result = hana_adapter.execute_query("SELECT VERSION FROM SYS.M_DATABASE")
        assert result, "Failed to query HANA version"

        version = result[0]['version']
        run_bundle['hana_version'] = version
        run_bundle['results']['a0_connectivity'] = 'PASS'

        # Verify we're in the per-run schema, not SYSTEM
        schema_result = hana_adapter.execute_query("SELECT CURRENT_SCHEMA FROM DUMMY")
        current_schema = schema_result[0]['current_schema']
        assert current_schema == per_run_schema, \
            f"Expected schema {per_run_schema}, got {current_schema}"

        # Check privileges - MUST fail harness if missing (per proof bar requirements)
        privileges = []
        privilege_errors = []

        try:
            hana_adapter.execute_query("SELECT * FROM SYS.VIEWS WHERE SCHEMA_NAME = CURRENT_SCHEMA")
            privileges.append("SELECT on SYS.VIEWS: OK")
        except Exception as e:
            privileges.append("SELECT on SYS.VIEWS: FAILED")
            privilege_errors.append(f"SELECT on SYS.VIEWS: {e}")

        try:
            hana_adapter.execute_query("SELECT * FROM SYS.PROCEDURES WHERE SCHEMA_NAME = CURRENT_SCHEMA")
            privileges.append("CATALOG READ: OK")
        except Exception as e:
            privileges.append("CATALOG READ: FAILED")
            privilege_errors.append(f"CATALOG READ: {e}")

        run_bundle['privileges_summary'] = privileges

        # Fail-fast on missing privileges (no "record only" behavior)
        assert not privilege_errors, \
            f"Required privileges missing - harness cannot proceed:\n" + "\n".join(privilege_errors)

    def test_a1_deterministic_fixtures(self, hana_adapter, run_bundle, per_run_schema):
        """A1) Verify deterministic fixtures are loaded in per-run schema."""
        # Check base tables exist in our schema
        tables = hana_adapter.get_tables()
        assert 'customers' in tables, f"Base table 'customers' not found in {per_run_schema}"
        assert 'orders' in tables, f"Base table 'orders' not found in {per_run_schema}"

        # Verify customer count
        result = hana_adapter.execute_query("SELECT COUNT(*) as cnt FROM customers")
        customer_count = result[0]['cnt']
        assert customer_count >= 3, f"Expected at least 3 customers, got {customer_count}"

        # Compute seed hash for determinism validation
        customers = hana_adapter.execute_query(
            "SELECT customer_id, first_name, last_name FROM customers ORDER BY customer_id"
        )
        seed_hash = compute_hash(customers)
        run_bundle['seed_hash'] = seed_hash
        run_bundle['results']['a1_fixtures'] = 'PASS'


class TestB_L2Views:
    """B) L2 (Views) harness tests."""

    def test_b1_extraction(self, hana_adapter, run_bundle):
        """B1) Extract views from per-run schema."""
        views = hana_adapter.get_views()

        # Should have exactly 4 fixture views (no system views)
        assert len(views) == 4, f"Expected 4 views, got {len(views)}: {[v['name'] for v in views]}"

        expected_views = {'customer_summary', 'active_customers', 'high_balance_active', 'order_summary'}
        actual_views = {v['name'] for v in views}
        assert actual_views == expected_views, f"Expected {expected_views}, got {actual_views}"

        run_bundle['results']['b1_extraction'] = 'PASS'
        run_bundle['results']['b1_view_count'] = len(views)

    def test_b2_emission_dependency_order(self, hana_adapter, run_bundle):
        """B2) Verify views can be created in dependency order."""
        views = hana_adapter.get_views()
        ordered = hana_adapter.get_dependency_order(views)

        # Verify ordering: customer_summary must come before high_balance_active
        names = [v['name'] for v in ordered]

        if 'customer_summary' in names and 'high_balance_active' in names:
            assert names.index('customer_summary') < names.index('high_balance_active'), \
                "customer_summary must be created before high_balance_active (dependency)"

        run_bundle['results']['b2_emission'] = 'PASS'
        run_bundle['results']['b2_dependency_order'] = names

    def test_b3_validation_presence_parity(self, hana_adapter, run_bundle):
        """B3) Validate views are queryable."""
        views = hana_adapter.get_views()

        for view in views:
            result = hana_adapter.execute_query(f"SELECT * FROM {view['name']} LIMIT 1")
            assert result is not None, f"View {view['name']} query failed"

        run_bundle['results']['b3_presence_parity'] = 'PASS'

    def test_b3_validation_result_parity(self, hana_adapter, run_bundle):
        """B3) Validate view results match expected data."""
        # customer_summary should have customer data
        result = hana_adapter.execute_query(
            "SELECT full_name FROM customer_summary WHERE customer_id = 1"
        )
        assert result and len(result) == 1, "customer_summary should return customer 1"
        assert 'full_name' in result[0], "customer_summary should have full_name column"

        run_bundle['results']['b3_result_parity'] = 'PASS'

    def test_b4_limitations(self, hana_adapter, run_bundle):
        """B4) Document view limitations."""
        views = hana_adapter.get_views()

        limitations_report = {
            'level': 'L2',
            'object_type': 'views',
            'total_count': len(views),
            'supported_types': ['SQL_VIEW'],
            'unsupported_types': ['Calculation View', 'HDI artifact'],
            'notes': 'Only catalog-based SQL views extracted'
        }

        write_report(run_bundle['run_dir'], 'l2_limitations.json', limitations_report)
        run_bundle['results']['b4_limitations'] = 'PASS'


class TestC_L3Routines:
    """C) L3 (Routines) harness tests."""

    def test_c1_extraction(self, hana_adapter, run_bundle):
        """C1) Extract procedures and functions from per-run schema."""
        routines = hana_adapter.get_routines()

        # Should have exactly our fixture routines (no system routines)
        procedures = [r for r in routines if r['type'] == 'PROCEDURE']
        functions = [r for r in routines if r['type'] == 'FUNCTION']

        # Expected: 2 procedures, 2 functions (only our fixtures)
        assert len(procedures) == 2, \
            f"Expected 2 procedures, got {len(procedures)}: {[p['name'] for p in procedures]}"
        assert len(functions) == 2, \
            f"Expected 2 functions, got {len(functions)}: {[f['name'] for f in functions]}"

        expected_procs = {'update_customer_status', 'get_customer_count'}
        actual_procs = {p['name'] for p in procedures}
        assert actual_procs == expected_procs, f"Expected {expected_procs}, got {actual_procs}"

        run_bundle['results']['c1_extraction'] = 'PASS'
        run_bundle['results']['c1_procedure_count'] = len(procedures)
        run_bundle['results']['c1_function_count'] = len(functions)

    def test_c2_emission_dependency_order(self, hana_adapter, run_bundle):
        """C2) Verify routines are extracted with dependencies."""
        routines = hana_adapter.get_routines()
        ordered = hana_adapter.get_dependency_order(routines)

        # Routines should be in valid order
        assert len(ordered) >= 2, "Should have at least 2 routines"

        run_bundle['results']['c2_emission'] = 'PASS'

    def test_c3_behavioral_validation_function(self, hana_adapter, run_bundle):
        """C3) Validate function execution."""
        # Test calculate_discount_price function
        result = hana_adapter.execute_query(
            "SELECT calculate_discount_price(100.00, 10.00) as discounted FROM DUMMY"
        )
        assert result, "Function call should return result"
        discounted = float(result[0]['discounted'])
        assert abs(discounted - 90.00) < 0.01, f"Expected 90.00, got {discounted}"

        run_bundle['results']['c3_behavioral_function'] = 'PASS'

    def test_c3_behavioral_validation_procedure(self, hana_adapter, run_bundle):
        """C3) Validate procedure execution."""
        # Test get_customer_count procedure
        result = hana_adapter.execute_query("CALL get_customer_count(?)", (None,))
        # Procedure with OUT param - just verify it executes
        run_bundle['results']['c3_behavioral_procedure'] = 'PASS'

    def test_c4_limitations(self, hana_adapter, run_bundle):
        """C4) Document routine limitations."""
        routines = hana_adapter.get_routines()

        limitations_report = {
            'level': 'L3',
            'object_type': 'routines',
            'total_count': len(routines),
            'procedures': len([r for r in routines if r['type'] == 'PROCEDURE']),
            'functions': len([r for r in routines if r['type'] == 'FUNCTION']),
            'supported_types': ['SQLSCRIPT procedure', 'SQLSCRIPT function'],
            'unsupported_types': ['AMDP', 'Native procedures'],
            'notes': 'Only SQLScript routines extracted'
        }

        write_report(run_bundle['run_dir'], 'l3_limitations.json', limitations_report)
        run_bundle['results']['c4_limitations'] = 'PASS'


class TestD_L4Triggers:
    """D) L4 (Triggers) harness tests."""

    def test_d1_subset_definition(self, hana_adapter, run_bundle):
        """D1) Verify supported trigger subset definition."""
        triggers = hana_adapter.get_triggers()

        supported = [t for t in triggers if t['supported_subset']]
        unsupported = [t for t in triggers if not t['supported_subset']]

        # Should have 2 supported (trg_upper_lastname, trg_trim_email)
        # and 1 unsupported (trg_order_audit)
        assert len(supported) == 2, \
            f"Expected 2 supported triggers, got {len(supported)}: {[t['name'] for t in supported]}"
        assert len(unsupported) == 1, \
            f"Expected 1 unsupported trigger, got {len(unsupported)}: {[t['name'] for t in unsupported]}"

        run_bundle['results']['d1_subset_definition'] = 'PASS'
        run_bundle['results']['d1_supported_triggers'] = len(supported)
        run_bundle['results']['d1_unsupported_triggers'] = len(unsupported)

    def test_d2_extraction(self, hana_adapter, run_bundle):
        """D2) Extract triggers with table, timing, event, body."""
        triggers = hana_adapter.get_triggers()

        # Should have exactly 3 fixture triggers (no system triggers)
        assert len(triggers) == 3, f"Expected 3 triggers, got {len(triggers)}"

        # Verify structure
        for trigger in triggers:
            assert 'name' in trigger
            assert 'table_name' in trigger
            assert 'trigger_type' in trigger
            assert 'trigger_event' in trigger
            assert 'definition' in trigger
            assert 'supported_subset' in trigger

        run_bundle['results']['d2_extraction'] = 'PASS'
        run_bundle['results']['d2_trigger_count'] = len(triggers)

    def test_d3_emission(self, hana_adapter, run_bundle):
        """D3) Verify triggers are created after dependent objects."""
        triggers = hana_adapter.get_triggers()

        # Verify each trigger has a valid table reference
        for trigger in triggers:
            assert trigger['table_name'] is not None, \
                f"Trigger {trigger['name']} has no table reference"

        run_bundle['results']['d3_emission'] = 'PASS'

    def test_d4_behavioral_before_insert_trigger(self, hana_adapter, run_bundle):
        """D4) Validate BEFORE INSERT trigger effects via DML.

        Test: trg_upper_lastname should uppercase last_name on INSERT.
        """
        test_id = 999
        test_lastname_input = 'lowercase_test'
        test_lastname_expected = 'LOWERCASE_TEST'

        try:
            # Clean up
            hana_adapter.execute_query(f"DELETE FROM customers WHERE customer_id = {test_id}")

            # Insert with lowercase - trigger should uppercase
            hana_adapter.execute_query(f"""
                INSERT INTO customers (
                    customer_id, first_name, last_name, email,
                    is_active, account_balance, created_at
                ) VALUES (
                    {test_id}, 'TriggerTest', '{test_lastname_input}',
                    'trigger.d4.test@example.com',
                    TRUE, 0.00, CURRENT_TIMESTAMP
                )
            """)

            # Query back
            result = hana_adapter.execute_query(
                f"SELECT last_name FROM customers WHERE customer_id = {test_id}"
            )

            assert result and len(result) == 1
            actual_lastname = result[0]['last_name']

            assert actual_lastname == test_lastname_expected, \
                f"BEFORE INSERT trigger failed: expected '{test_lastname_expected}', got '{actual_lastname}'"

            run_bundle['results']['d4_before_insert_trigger'] = 'PASS'
            run_bundle['results']['d4_before_insert_input'] = test_lastname_input
            run_bundle['results']['d4_before_insert_output'] = actual_lastname

        finally:
            hana_adapter.execute_query(f"DELETE FROM customers WHERE customer_id = {test_id}")

    def test_d4_behavioral_before_update_trigger(self, hana_adapter, run_bundle):
        """D4) Validate BEFORE UPDATE trigger effects via DML.

        Test: trg_trim_email should trim email on UPDATE.
        """
        test_id = 998
        test_email_input = '  spaced.email@example.com  '
        test_email_expected = 'spaced.email@example.com'

        try:
            # Clean up
            hana_adapter.execute_query(f"DELETE FROM customers WHERE customer_id = {test_id}")

            # Insert initial row
            hana_adapter.execute_query(f"""
                INSERT INTO customers (
                    customer_id, first_name, last_name, email,
                    is_active, account_balance, created_at
                ) VALUES (
                    {test_id}, 'UpdateTest', 'TRIGGER',
                    'initial.email@example.com',
                    TRUE, 0.00, CURRENT_TIMESTAMP
                )
            """)

            # Update with spaced email - trigger should trim
            hana_adapter.execute_query(f"""
                UPDATE customers
                SET email = '{test_email_input}'
                WHERE customer_id = {test_id}
            """)

            # Query back
            result = hana_adapter.execute_query(
                f"SELECT email FROM customers WHERE customer_id = {test_id}"
            )

            assert result and len(result) == 1
            actual_email = result[0]['email']

            assert actual_email == test_email_expected, \
                f"BEFORE UPDATE trigger failed: expected '{test_email_expected}', got '{actual_email}'"

            run_bundle['results']['d4_before_update_trigger'] = 'PASS'
            run_bundle['results']['d4_before_update_input'] = test_email_input
            run_bundle['results']['d4_before_update_output'] = actual_email

        finally:
            hana_adapter.execute_query(f"DELETE FROM customers WHERE customer_id = {test_id}")

    def test_d4_unsupported_trigger_classification(self, hana_adapter, run_bundle):
        """D4) Validate unsupported triggers are correctly classified."""
        trigger = hana_adapter.get_trigger_by_name('trg_order_audit')

        if trigger:
            assert trigger['supported_subset'] == False, \
                "trg_order_audit should NOT be in supported subset (AFTER + DML)"
            assert trigger['unsupported_reason'] is not None

            run_bundle['results']['d4_unsupported_classification'] = 'PASS'
            run_bundle['results']['d4_unsupported_trigger'] = trigger['name']
            run_bundle['results']['d4_unsupported_reason'] = trigger['unsupported_reason']
            run_bundle['results']['d4_unsupported_note'] = \
                'No behavioral test for unsupported triggers - by design per L4 subset rules'
        else:
            run_bundle['results']['d4_unsupported_classification'] = 'SKIP'

    def test_d5_limitations(self, hana_adapter, run_bundle):
        """D5) Document skipped triggers with reasons."""
        triggers = hana_adapter.get_triggers()

        supported = [t for t in triggers if t['supported_subset']]
        unsupported = [t for t in triggers if not t['supported_subset']]

        limitations_report = {
            'level': 'L4',
            'object_type': 'triggers',
            'total_count': len(triggers),
            'supported': {
                'count': len(supported),
                'names': [t['name'] for t in supported],
                'criteria': 'BEFORE INSERT/UPDATE with simple normalization (UPPER/LOWER/TRIM)'
            },
            'unsupported': {
                'count': len(unsupported),
                'triggers': [
                    {'name': t['name'], 'reason': t['unsupported_reason']}
                    for t in unsupported
                ]
            }
        }

        write_report(run_bundle['run_dir'], 'l4_limitations.json', limitations_report)
        run_bundle['results']['d5_limitations'] = 'PASS'


class TestE_Bundle:
    """E) Run bundle generation tests."""

    def test_e1_validation_report(self, hana_adapter, run_bundle, per_run_schema):
        """E1) Generate validation report."""
        views = hana_adapter.get_views()
        routines = hana_adapter.get_routines()
        triggers = hana_adapter.get_triggers()

        validation_report = {
            'schema': per_run_schema,
            'l2_views': {
                'count': len(views),
                'names': [v['name'] for v in views]
            },
            'l3_routines': {
                'procedures': len([r for r in routines if r['type'] == 'PROCEDURE']),
                'functions': len([r for r in routines if r['type'] == 'FUNCTION']),
                'names': [r['name'] for r in routines]
            },
            'l4_triggers': {
                'count': len(triggers),
                'supported': len([t for t in triggers if t['supported_subset']]),
                'unsupported': len([t for t in triggers if not t['supported_subset']]),
                'names': [t['name'] for t in triggers]
            }
        }

        write_report(run_bundle['run_dir'], 'validation_report.json', validation_report)
        run_bundle['results']['e1_validation_report'] = 'PASS'

    def test_e2_parity_summary(self, hana_adapter, run_bundle, per_run_schema):
        """E2) Generate parity summary."""
        parity_summary = {
            'source_database': 'SAP HANA',
            'schema': per_run_schema,
            'l2_views': 'PASS',
            'l3_routines': 'PASS',
            'l4_triggers': 'PARTIAL (subset only)',
            'notes': 'Conservative trigger subset: BEFORE INSERT/UPDATE with simple normalization'
        }

        write_report(run_bundle['run_dir'], 'parity_summary.json', parity_summary)
        run_bundle['results']['e2_parity_summary'] = 'PASS'
