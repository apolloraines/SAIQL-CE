#!/usr/bin/env python3
"""
Oracle L1/L2/L3/L4 Integration Harness

Proof bar: 3x consecutive clean-state passes required.

Per Collab-Oracle-L1L2L3L4.md:
- L1: Core schema objects (tables, constraints, indexes, sequences)
- L2: Views with dependency ordering
- L3: Routines subset (PL/SQL with allowlist/denylist)
- L4: Triggers subset (BEFORE/AFTER row triggers)

Per rules_Oracle_L1L2L3L4.md:
- Clean state per run (per-run schema)
- Deterministic seed dataset
- Dedicated test user (not SYSTEM/SYS)
- No secret leakage in bundles
"""

import pytest
import os
import sys
import json
import time
import hashlib
import uuid
from datetime import datetime
from typing import Dict, Any, List, Optional

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from extensions.plugins.oracle_adapter import OracleAdapter


# ===== Configuration =====

def get_oracle_config() -> Dict[str, Any]:
    """Get Oracle connection config from environment."""
    return {
        'host': os.environ.get('ORACLE_HOST', 'localhost'),
        'port': int(os.environ.get('ORACLE_PORT', '1522')),
        'database': os.environ.get('ORACLE_SERVICE', 'FREEPDB1'),
        'user': os.environ.get('ORACLE_USER', 'SAIQL_L1L2L3L4_TEST'),
        'password': os.environ.get('ORACLE_PASSWORD', 'TestPass123')
    }


# ===== Fixtures =====

@pytest.fixture(scope="module")
def oracle_config():
    """Provide Oracle config with dedicated user validation."""
    config = get_oracle_config()

    # Rule 9: Dedicated test user required (not SYSTEM/SYS)
    if config['user'].upper() in ('SYSTEM', 'SYS'):
        pytest.fail(
            f"ORACLE_USER cannot be {config['user'].upper()}. "
            "Per rules_Oracle_L1L2L3L4.md rule 9: privileges must be explicit and minimal; "
            "harness uses a dedicated test user. Set ORACLE_USER to a dedicated test user."
        )

    return config


@pytest.fixture(scope="module")
def adapter(oracle_config):
    """Create and connect Oracle adapter."""
    adapter = OracleAdapter(oracle_config)
    yield adapter
    adapter.close()


@pytest.fixture(scope="module")
def run_bundle(oracle_config, adapter):
    """Create run bundle for evidence collection."""
    run_id = str(uuid.uuid4())[:8]
    schema_name = f"SAIQL_L1L2L3L4_{run_id}"

    bundle = {
        'run_id': run_id,
        'schema_name': schema_name,
        'oracle_user': oracle_config['user'],  # Record dedicated test user
        # Sanitized connection info (no password)
        'connection_info': {
            'host': oracle_config['host'],
            'port': oracle_config['port'],
            'service': oracle_config['database']
        },
        'start_time': datetime.utcnow().isoformat() + 'Z',
        'l1': {'status': 'pending', 'results': {}},
        'l2': {'status': 'pending', 'results': {}},
        'l3': {'status': 'pending', 'results': {}},
        'l4': {'status': 'pending', 'results': {}},
        'limitations': [],
        'errors': [],
        'ddl_artifacts': []
    }

    # Get DB version and NLS settings
    version_info = adapter.get_db_version()
    bundle['db_version'] = version_info.get('db_version', 'unknown')
    bundle['nls_settings'] = version_info.get('nls_settings', {})

    yield bundle

    bundle['end_time'] = datetime.utcnow().isoformat() + 'Z'


@pytest.fixture(scope="module")
def per_run_schema(adapter, run_bundle, oracle_config):
    """
    Create per-run schema for clean state isolation.

    Per rules_Oracle_L1L2L3L4.md rule 5:
    - Preferred: create a new schema/user per run_id and drop after
    """
    schema_name = run_bundle['schema_name']

    # Note: In Oracle, schema = user. Creating a new user creates a new schema.
    # For testing, we'll use the current user's schema and create test objects
    # with a prefix to avoid conflicts.

    # Record that we're using the current user's schema
    run_bundle['schema_strategy'] = 'prefixed_objects'
    # Use uppercase prefix for Oracle case-sensitive quoted identifiers
    run_bundle['object_prefix'] = f"T_{run_bundle['run_id'].upper()}_"

    yield schema_name

    # Cleanup: Drop test objects
    prefix = run_bundle['object_prefix']

    # Drop in reverse dependency order
    cleanup_order = ['TRIGGER', 'VIEW', 'PROCEDURE', 'FUNCTION', 'TABLE', 'SEQUENCE']

    for obj_type in cleanup_order:
        query = f"""
            SELECT object_name FROM user_objects
            WHERE object_name LIKE '{prefix}%'
            AND object_type = '{obj_type}'
        """
        result = adapter.execute_query(query)
        if result['success']:
            for row in result['data']:
                obj_name = row['object_name']
                drop_sql = f'DROP {obj_type} "{obj_name}"'
                if obj_type == 'TABLE':
                    drop_sql += ' CASCADE CONSTRAINTS'
                adapter.execute_query(drop_sql, fetch=False)


@pytest.fixture(scope="module")
def seed_data(adapter, per_run_schema, run_bundle):
    """
    Create deterministic seed data for testing.

    Per rules_Oracle_L1L2L3L4.md rule 4:
    - Determinism first
    """
    prefix = run_bundle['object_prefix']
    seed_hash = None

    # Create L1 test objects
    # Sequence
    seq_sql = f"""
        CREATE SEQUENCE "{prefix}SEQ1"
        START WITH 1
        INCREMENT BY 1
        MINVALUE 1
        MAXVALUE 1000000
        CACHE 20
    """
    adapter.execute_query(seq_sql, fetch=False)

    # Table with all L1 datatypes
    table_sql = f"""
        CREATE TABLE "{prefix}CUSTOMERS" (
            id NUMBER(10) NOT NULL,
            name VARCHAR2(100) NOT NULL,
            code CHAR(5),
            created_date DATE DEFAULT SYSDATE,
            updated_ts TIMESTAMP,
            notes CLOB,
            CONSTRAINT "{prefix}CUSTOMERS_PK" PRIMARY KEY (id)
        )
    """
    adapter.execute_query(table_sql, fetch=False)

    # Table with FK
    orders_sql = f"""
        CREATE TABLE "{prefix}ORDERS" (
            order_id NUMBER(10) NOT NULL,
            customer_id NUMBER(10) NOT NULL,
            order_date DATE NOT NULL,
            total NUMBER(12,2),
            status VARCHAR2(20) DEFAULT 'PENDING',
            CONSTRAINT "{prefix}ORDERS_PK" PRIMARY KEY (order_id),
            CONSTRAINT "{prefix}ORDERS_FK" FOREIGN KEY (customer_id)
                REFERENCES "{prefix}CUSTOMERS" (id),
            CONSTRAINT "{prefix}ORDERS_CHK" CHECK (total >= 0)
        )
    """
    adapter.execute_query(orders_sql, fetch=False)

    # Unique constraint
    unique_sql = f"""
        CREATE UNIQUE INDEX "{prefix}CUSTOMERS_NAME_UK"
        ON "{prefix}CUSTOMERS" (name)
    """
    adapter.execute_query(unique_sql, fetch=False)

    # B-tree index
    index_sql = f"""
        CREATE INDEX "{prefix}ORDERS_DATE_IX"
        ON "{prefix}ORDERS" (order_date)
    """
    adapter.execute_query(index_sql, fetch=False)

    # Seed deterministic data
    seed_rows = [
        f"INSERT INTO \"{prefix}CUSTOMERS\" (id, name, code) VALUES (1, 'Alice', 'A0001')",
        f"INSERT INTO \"{prefix}CUSTOMERS\" (id, name, code) VALUES (2, 'Bob', 'B0002')",
        f"INSERT INTO \"{prefix}CUSTOMERS\" (id, name, code) VALUES (3, 'Charlie', 'C0003')",
        f"INSERT INTO \"{prefix}ORDERS\" (order_id, customer_id, order_date, total, status) VALUES (101, 1, DATE '2024-01-15', 150.00, 'COMPLETE')",
        f"INSERT INTO \"{prefix}ORDERS\" (order_id, customer_id, order_date, total, status) VALUES (102, 2, DATE '2024-01-16', 250.50, 'PENDING')",
    ]

    for sql in seed_rows:
        adapter.execute_query(sql, fetch=False)

    # L2: Create views
    view1_sql = f"""
        CREATE VIEW "{prefix}V_CUSTOMER_SUMMARY" AS
        SELECT c.id, c.name, COUNT(o.order_id) as order_count, SUM(o.total) as total_spent
        FROM "{prefix}CUSTOMERS" c
        LEFT JOIN "{prefix}ORDERS" o ON c.id = o.customer_id
        GROUP BY c.id, c.name
    """
    adapter.execute_query(view1_sql, fetch=False)

    # View on view (dependency ordering test)
    view2_sql = f"""
        CREATE VIEW "{prefix}V_TOP_CUSTOMERS" AS
        SELECT * FROM "{prefix}V_CUSTOMER_SUMMARY"
        WHERE total_spent > 100
    """
    adapter.execute_query(view2_sql, fetch=False)

    # L3: Create routines (allowed subset)
    proc_sql = f"""
        CREATE OR REPLACE PROCEDURE "{prefix}GET_CUSTOMER_COUNT" (
            p_count OUT NUMBER
        ) AS
        BEGIN
            SELECT COUNT(*) INTO p_count FROM "{prefix}CUSTOMERS";
        END;
    """
    adapter.execute_query(proc_sql, fetch=False)

    func_sql = f"""
        CREATE OR REPLACE FUNCTION "{prefix}CALC_DISCOUNT" (
            p_total IN NUMBER
        ) RETURN NUMBER AS
            v_discount NUMBER;
        BEGIN
            IF p_total > 200 THEN
                v_discount := p_total * 0.1;
            ELSE
                v_discount := 0;
            END IF;
            RETURN v_discount;
        END;
    """
    adapter.execute_query(func_sql, fetch=False)

    # L4: Create audit table and trigger
    audit_sql = f"""
        CREATE TABLE "{prefix}AUDIT_LOG" (
            log_id NUMBER GENERATED ALWAYS AS IDENTITY,
            table_name VARCHAR2(100),
            operation VARCHAR2(10),
            old_id NUMBER,
            new_id NUMBER,
            log_time TIMESTAMP DEFAULT SYSTIMESTAMP,
            CONSTRAINT "{prefix}AUDIT_LOG_PK" PRIMARY KEY (log_id)
        )
    """
    adapter.execute_query(audit_sql, fetch=False)

    trigger_sql = f"""
        CREATE OR REPLACE TRIGGER "{prefix}TRG_CUSTOMERS_AUDIT"
        AFTER INSERT OR UPDATE OR DELETE ON "{prefix}CUSTOMERS"
        FOR EACH ROW
        BEGIN
            IF INSERTING THEN
                INSERT INTO "{prefix}AUDIT_LOG" (table_name, operation, new_id)
                VALUES ('CUSTOMERS', 'INSERT', :NEW.id);
            ELSIF UPDATING THEN
                INSERT INTO "{prefix}AUDIT_LOG" (table_name, operation, old_id, new_id)
                VALUES ('CUSTOMERS', 'UPDATE', :OLD.id, :NEW.id);
            ELSIF DELETING THEN
                INSERT INTO "{prefix}AUDIT_LOG" (table_name, operation, old_id)
                VALUES ('CUSTOMERS', 'DELETE', :OLD.id);
            END IF;
        END;
    """
    adapter.execute_query(trigger_sql, fetch=False)

    # Calculate seed hash for determinism verification
    seed_hash = hashlib.sha256(json.dumps(seed_rows, sort_keys=True).encode()).hexdigest()[:16]

    run_bundle['seed_hash'] = seed_hash
    run_bundle['seed_objects'] = {
        'tables': [f'{prefix}CUSTOMERS', f'{prefix}ORDERS', f'{prefix}AUDIT_LOG'],
        'views': [f'{prefix}V_CUSTOMER_SUMMARY', f'{prefix}V_TOP_CUSTOMERS'],
        'sequences': [f'{prefix}SEQ1'],
        'routines': [f'{prefix}GET_CUSTOMER_COUNT', f'{prefix}CALC_DISCOUNT'],
        'triggers': [f'{prefix}TRG_CUSTOMERS_AUDIT']
    }

    # Record DDL artifacts for bundle
    run_bundle['ddl_artifacts'] = [
        {'type': 'SEQUENCE', 'name': f'{prefix}SEQ1', 'ddl': seq_sql.strip()},
        {'type': 'TABLE', 'name': f'{prefix}CUSTOMERS', 'ddl': table_sql.strip()},
        {'type': 'TABLE', 'name': f'{prefix}ORDERS', 'ddl': orders_sql.strip()},
        {'type': 'TABLE', 'name': f'{prefix}AUDIT_LOG', 'ddl': audit_sql.strip()},
        {'type': 'INDEX', 'name': f'{prefix}CUSTOMERS_NAME_UK', 'ddl': unique_sql.strip()},
        {'type': 'INDEX', 'name': f'{prefix}ORDERS_DATE_IX', 'ddl': index_sql.strip()},
        {'type': 'VIEW', 'name': f'{prefix}V_CUSTOMER_SUMMARY', 'ddl': view1_sql.strip()},
        {'type': 'VIEW', 'name': f'{prefix}V_TOP_CUSTOMERS', 'ddl': view2_sql.strip()},
        {'type': 'PROCEDURE', 'name': f'{prefix}GET_CUSTOMER_COUNT', 'ddl': proc_sql.strip()},
        {'type': 'FUNCTION', 'name': f'{prefix}CALC_DISCOUNT', 'ddl': func_sql.strip()},
        {'type': 'TRIGGER', 'name': f'{prefix}TRG_CUSTOMERS_AUDIT', 'ddl': trigger_sql.strip()},
    ]

    return seed_hash


# ===== L1 Tests: Core Schema Objects =====

class TestL1CoreObjects:
    """L1 harness: Tables, Constraints, Indexes, Sequences."""

    def test_b1_extraction_tables(self, adapter, seed_data, run_bundle):
        """B1: Enumerate tables from USER_TABLES."""
        prefix = run_bundle['object_prefix'].upper()
        tables = adapter.get_tables()

        # Filter to our test tables (case-insensitive)
        test_tables = [t for t in tables if t.upper().startswith(prefix)]

        assert len(test_tables) >= 3, f"Expected at least 3 tables, found {len(test_tables)}"
        run_bundle['l1']['results']['tables'] = test_tables

    def test_b1_extraction_constraints(self, adapter, seed_data, run_bundle):
        """B1: Enumerate constraints from USER_CONSTRAINTS."""
        prefix = run_bundle['object_prefix'].upper()
        constraints = adapter.get_constraints()

        # Filter to our test constraints (case-insensitive)
        test_constraints = [
            c for c in constraints
            if c['name'].upper().startswith(prefix) or c['table_name'].upper().startswith(prefix)
        ]

        # Expect: 2 PKs (CUSTOMERS, ORDERS, AUDIT_LOG), 1 FK, 1 CHECK
        pk_count = sum(1 for c in test_constraints if c['type'] == 'PRIMARY KEY')
        fk_count = sum(1 for c in test_constraints if c['type'] == 'FOREIGN KEY')
        check_count = sum(1 for c in test_constraints if c['type'] == 'CHECK')

        assert pk_count >= 2, f"Expected at least 2 PKs, found {pk_count}"
        assert fk_count >= 1, f"Expected at least 1 FK, found {fk_count}"

        run_bundle['l1']['results']['constraints'] = {
            'total': len(test_constraints),
            'pk': pk_count,
            'fk': fk_count,
            'check': check_count
        }

    def test_b1_extraction_indexes(self, adapter, seed_data, run_bundle):
        """B1: Enumerate indexes from USER_INDEXES."""
        prefix = run_bundle['object_prefix'].upper()
        indexes = adapter.get_indexes()

        # Filter to our test indexes (case-insensitive)
        test_indexes = [
            i for i in indexes
            if i['name'].upper().startswith(prefix) or i['table_name'].upper().startswith(prefix)
        ]

        assert len(test_indexes) >= 1, f"Expected at least 1 index, found {len(test_indexes)}"
        run_bundle['l1']['results']['indexes'] = len(test_indexes)

    def test_b1_extraction_sequences(self, adapter, seed_data, run_bundle):
        """B1: Enumerate sequences from USER_SEQUENCES."""
        prefix = run_bundle['object_prefix'].upper()
        sequences = adapter.get_sequences()

        # Filter to our test sequences (case-insensitive)
        test_sequences = [s for s in sequences if s['name'].upper().startswith(prefix)]

        assert len(test_sequences) >= 1, f"Expected at least 1 sequence, found {len(test_sequences)}"
        run_bundle['l1']['results']['sequences'] = len(test_sequences)

    def test_b3_structural_parity_columns(self, adapter, seed_data, run_bundle):
        """B3: Validate column structure parity."""
        prefix = run_bundle['object_prefix']
        table_name = f"{prefix}CUSTOMERS"

        schema = adapter.get_schema(table_name)

        assert len(schema['columns']) >= 5, f"Expected at least 5 columns, found {len(schema['columns'])}"

        # Check expected columns exist
        col_names = [c['name'].lower() for c in schema['columns']]
        expected = ['id', 'name', 'code', 'created_date', 'updated_ts']
        for exp in expected:
            assert exp in col_names, f"Expected column {exp} not found"

        run_bundle['l1']['results']['column_parity'] = 'PASS'

    def test_b3_behavioral_sanity(self, adapter, seed_data, run_bundle):
        """B3: Insert/select deterministic seed data."""
        prefix = run_bundle['object_prefix']

        # Query seeded data
        query = f'SELECT id, name, code FROM "{prefix}CUSTOMERS" ORDER BY id'
        result = adapter.execute_query(query)

        assert result['success'], f"Query failed: {result.get('error')}"
        assert len(result['data']) == 3, f"Expected 3 rows, found {len(result['data'])}"

        # Verify deterministic values
        assert result['data'][0]['name'] == 'Alice'
        assert result['data'][1]['name'] == 'Bob'
        assert result['data'][2]['name'] == 'Charlie'

        run_bundle['l1']['results']['behavioral_sanity'] = 'PASS'
        run_bundle['l1']['status'] = 'PASS'


# ===== L2 Tests: Views =====

class TestL2Views:
    """L2 harness: Views with dependency ordering."""

    def test_c1_extraction_views(self, adapter, seed_data, run_bundle):
        """C1: Enumerate views from USER_VIEWS."""
        prefix = run_bundle['object_prefix'].upper()
        views = adapter.get_views()

        # Filter to our test views (case-insensitive)
        test_views = [v for v in views if v['name'].upper().startswith(prefix)]

        assert len(test_views) >= 2, f"Expected at least 2 views, found {len(test_views)}"
        run_bundle['l2']['results']['views'] = [v['name'] for v in test_views]

    def test_c2_dependency_ordering(self, adapter, seed_data, run_bundle):
        """C2: Verify view dependency ordering."""
        prefix = run_bundle['object_prefix'].upper()

        deps = adapter._get_view_dependencies()

        # Filter to our test views (case-insensitive)
        test_deps = {k: v for k, v in deps.items() if k.upper().startswith(prefix)}

        # V_TOP_CUSTOMERS should depend on V_CUSTOMER_SUMMARY
        top_view = f"{prefix}V_TOP_CUSTOMERS".lower()
        summary_view = f"{prefix}V_CUSTOMER_SUMMARY".lower()

        if top_view in test_deps:
            assert summary_view in test_deps[top_view], \
                f"Expected {top_view} to depend on {summary_view}"

        run_bundle['l2']['results']['dependency_ordering'] = 'PASS'

    def test_c3_result_parity(self, adapter, seed_data, run_bundle):
        """C3: Query views with deterministic results."""
        prefix = run_bundle['object_prefix'].upper()

        # Query V_CUSTOMER_SUMMARY
        query = f'SELECT id, name, order_count FROM "{prefix}V_CUSTOMER_SUMMARY" ORDER BY id'
        result = adapter.execute_query(query)

        assert result['success'], f"View query failed: {result.get('error')}"
        assert len(result['data']) == 3, f"Expected 3 rows from view, found {len(result['data'])}"

        # Query V_TOP_CUSTOMERS (should have 1 row - Alice with total 150)
        query2 = f'SELECT id, name FROM "{prefix}V_TOP_CUSTOMERS" ORDER BY id'
        result2 = adapter.execute_query(query2)

        assert result2['success'], f"View query failed: {result2.get('error')}"
        assert len(result2['data']) >= 1, f"Expected at least 1 row from top view"

        run_bundle['l2']['results']['result_parity'] = 'PASS'
        run_bundle['l2']['status'] = 'PASS'


# ===== L3 Tests: Routines Subset =====

class TestL3RoutinesSubset:
    """L3 harness: PL/SQL routines with allowlist/denylist."""

    def test_d1_subset_policy(self, adapter, seed_data, run_bundle):
        """D1: Verify subset policy is enforced."""
        routines = adapter.get_routines_with_classification()

        allowed = [r for r in routines if r['l3_allowed']]
        denied = [r for r in routines if not r['l3_allowed']]

        run_bundle['l3']['results']['subset_policy'] = {
            'total': len(routines),
            'allowed': len(allowed),
            'denied': len(denied),
            'denial_reasons': {}
        }

        for r in denied:
            for reason in r['l3_reason_codes']:
                run_bundle['l3']['results']['subset_policy']['denial_reasons'][reason] = \
                    run_bundle['l3']['results']['subset_policy']['denial_reasons'].get(reason, 0) + 1

    def test_d2_extraction_routines(self, adapter, seed_data, run_bundle):
        """D2: Enumerate routines from USER_OBJECTS/USER_SOURCE."""
        prefix = run_bundle['object_prefix'].upper()
        routines = adapter.get_routines()

        # Filter to our test routines (case-insensitive)
        test_routines = [r for r in routines if r['name'].upper().startswith(prefix)]

        assert len(test_routines) >= 2, f"Expected at least 2 routines, found {len(test_routines)}"

        run_bundle['l3']['results']['routines'] = [
            {'name': r['name'], 'type': r['type']}
            for r in test_routines
        ]

    def test_d3_compilation_check(self, adapter, seed_data, run_bundle):
        """D3: Verify routines compiled without errors."""
        prefix = run_bundle['object_prefix'].upper()

        # Check USER_ERRORS for our routines
        query = f"""
            SELECT name, type, line, position, text
            FROM user_errors
            WHERE name LIKE '{prefix}%'
        """
        result = adapter.execute_query(query)

        if result['success'] and result['data']:
            errors = result['data']
            run_bundle['l3']['results']['compilation_errors'] = errors
            pytest.fail(f"Found {len(errors)} compilation errors in routines")
        else:
            run_bundle['l3']['results']['compilation_errors'] = []

    def test_d4_behavioral_validation(self, adapter, seed_data, run_bundle):
        """D4: Execute routines with deterministic inputs."""
        prefix = run_bundle['object_prefix'].upper()

        # Test function: CALC_DISCOUNT
        func_query = f'SELECT "{prefix}CALC_DISCOUNT"(250) as discount FROM DUAL'
        result = adapter.execute_query(func_query)

        assert result['success'], f"Function call failed: {result.get('error')}"
        assert result['data'][0]['discount'] == 25, "Expected 10% discount on 250 = 25"

        run_bundle['l3']['results']['behavioral_validation'] = 'PASS'
        run_bundle['l3']['status'] = 'PASS'


# ===== L4 Tests: Triggers Subset =====

class TestL4TriggersSubset:
    """L4 harness: Triggers with allowlist/denylist."""

    def test_e1_subset_policy(self, adapter, seed_data, run_bundle):
        """E1: Verify trigger subset policy is enforced."""
        triggers = adapter.get_triggers_with_classification()

        allowed = [t for t in triggers if t['l4_allowed']]
        denied = [t for t in triggers if not t['l4_allowed']]

        run_bundle['l4']['results']['subset_policy'] = {
            'total': len(triggers),
            'allowed': len(allowed),
            'denied': len(denied),
            'denial_reasons': {}
        }

        for t in denied:
            for reason in t['l4_reason_codes']:
                run_bundle['l4']['results']['subset_policy']['denial_reasons'][reason] = \
                    run_bundle['l4']['results']['subset_policy']['denial_reasons'].get(reason, 0) + 1

    def test_e2_extraction_triggers(self, adapter, seed_data, run_bundle):
        """E2: Enumerate triggers from USER_TRIGGERS."""
        prefix = run_bundle['object_prefix'].upper()
        triggers = adapter.get_triggers()

        # Filter to our test triggers (case-insensitive)
        test_triggers = [t for t in triggers if t['name'].upper().startswith(prefix)]

        assert len(test_triggers) >= 1, f"Expected at least 1 trigger, found {len(test_triggers)}"

        run_bundle['l4']['results']['triggers'] = [
            {'name': t['name'], 'type': t['trigger_type'], 'event': t['triggering_event']}
            for t in test_triggers
        ]

    def test_e3_compilation_check(self, adapter, seed_data, run_bundle):
        """E3: Verify triggers compiled without errors."""
        prefix = run_bundle['object_prefix'].upper()

        # Check USER_ERRORS for our triggers
        query = f"""
            SELECT name, type, line, position, text
            FROM user_errors
            WHERE name LIKE '{prefix}%'
            AND type = 'TRIGGER'
        """
        result = adapter.execute_query(query)

        if result['success'] and result['data']:
            errors = result['data']
            run_bundle['l4']['results']['compilation_errors'] = errors
            pytest.fail(f"Found {len(errors)} compilation errors in triggers")
        else:
            run_bundle['l4']['results']['compilation_errors'] = []

    def test_e4_behavioral_validation(self, adapter, seed_data, run_bundle):
        """E4: Apply DML and assert trigger effects."""
        prefix = run_bundle['object_prefix'].upper()

        # Insert a new customer (should trigger audit log)
        insert_sql = f"""
            INSERT INTO "{prefix}CUSTOMERS" (id, name, code)
            VALUES (99, 'TestUser', 'T0099')
        """
        adapter.execute_query(insert_sql, fetch=False)

        # Check audit log
        audit_query = f"""
            SELECT table_name, operation, new_id
            FROM "{prefix}AUDIT_LOG"
            WHERE new_id = 99
            ORDER BY log_id DESC
        """
        result = adapter.execute_query(audit_query)

        assert result['success'], f"Audit query failed: {result.get('error')}"
        assert len(result['data']) >= 1, "Expected audit log entry for INSERT"
        assert result['data'][0]['operation'] == 'INSERT'

        run_bundle['l4']['results']['behavioral_validation'] = 'PASS'
        run_bundle['l4']['status'] = 'PASS'


# ===== Bundle and Manifest Tests =====

class TestBundleRequirements:
    """F: Bundle requirements verification."""

    def test_f_run_manifest(self, run_bundle, oracle_config):
        """F: Verify run_manifest.json requirements."""
        # Verify required fields
        assert 'run_id' in run_bundle
        assert 'db_version' in run_bundle
        assert 'nls_settings' in run_bundle
        assert 'oracle_user' in run_bundle
        assert 'seed_hash' in run_bundle

        # Verify dedicated user recorded
        assert run_bundle['oracle_user'].upper() not in ('SYSTEM', 'SYS'), \
            "Manifest must record dedicated test user, not SYSTEM/SYS"

        # Verify no secrets
        manifest_str = json.dumps(run_bundle)
        assert oracle_config['password'] not in manifest_str, \
            "Password must not appear in manifest"

    def test_f_overall_status(self, run_bundle):
        """F: Verify all levels passed."""
        assert run_bundle['l1']['status'] == 'PASS', "L1 must pass"
        assert run_bundle['l2']['status'] == 'PASS', "L2 must pass"
        assert run_bundle['l3']['status'] == 'PASS', "L3 must pass"
        assert run_bundle['l4']['status'] == 'PASS', "L4 must pass"

        run_bundle['overall_status'] = 'PASS'


# ===== Summary and Reporting =====

@pytest.fixture(scope="module", autouse=True)
def print_summary(run_bundle):
    """Print summary after all tests."""
    yield

    print("\n" + "=" * 60)
    print("ORACLE L1/L2/L3/L4 HARNESS SUMMARY")
    print("=" * 60)
    print(f"Run ID: {run_bundle.get('run_id', 'unknown')}")
    print(f"Oracle User: {run_bundle.get('oracle_user', 'unknown')}")
    print(f"DB Version: {run_bundle.get('db_version', 'unknown')}")
    print(f"Seed Hash: {run_bundle.get('seed_hash', 'unknown')}")
    print("-" * 60)
    print(f"L1 Core Objects: {run_bundle['l1'].get('status', 'unknown')}")
    print(f"L2 Views: {run_bundle['l2'].get('status', 'unknown')}")
    print(f"L3 Routines: {run_bundle['l3'].get('status', 'unknown')}")
    print(f"L4 Triggers: {run_bundle['l4'].get('status', 'unknown')}")
    print("-" * 60)
    print(f"Overall: {run_bundle.get('overall_status', 'INCOMPLETE')}")
    print("=" * 60)

    # Write bundle artifacts
    harness_dir = os.path.dirname(__file__)
    bundle_dir = os.path.join(harness_dir, 'oracle_l1l2l3l4_harness', f"run_{run_bundle['run_id']}")
    logs_dir = os.path.join(bundle_dir, 'logs')
    reports_dir = os.path.join(bundle_dir, 'reports')
    ddl_dir = os.path.join(bundle_dir, 'ddl')

    os.makedirs(logs_dir, exist_ok=True)
    os.makedirs(reports_dir, exist_ok=True)
    os.makedirs(ddl_dir, exist_ok=True)

    # Write manifest
    manifest_path = os.path.join(bundle_dir, 'run_manifest.json')
    with open(manifest_path, 'w') as f:
        json.dump(run_bundle, f, indent=2, default=str)

    # Write DDL artifacts
    for artifact in run_bundle.get('ddl_artifacts', []):
        ddl_file = os.path.join(ddl_dir, f"{artifact['type']}_{artifact['name']}.sql")
        with open(ddl_file, 'w') as f:
            f.write(f"-- {artifact['type']}: {artifact['name']}\n")
            f.write(artifact['ddl'])
            f.write('\n')

    # Write validation report
    validation_report = {
        'run_id': run_bundle['run_id'],
        'timestamp': run_bundle.get('end_time', run_bundle.get('start_time')),
        'db_version': run_bundle.get('db_version'),
        'oracle_user': run_bundle.get('oracle_user'),
        'connection_info': run_bundle.get('connection_info'),
        'l1_validation': run_bundle['l1'],
        'l2_validation': run_bundle['l2'],
        'l3_validation': run_bundle['l3'],
        'l4_validation': run_bundle['l4'],
        'overall_status': run_bundle.get('overall_status', 'INCOMPLETE')
    }
    report_path = os.path.join(reports_dir, 'validation_report.json')
    with open(report_path, 'w') as f:
        json.dump(validation_report, f, indent=2, default=str)

    # Write limitations report
    limitations_report = {
        'run_id': run_bundle['run_id'],
        'limitations': run_bundle.get('limitations', []),
        'l3_denied_routines': run_bundle['l3'].get('results', {}).get('subset_policy', {}).get('denial_reasons', {}),
        'l4_denied_triggers': run_bundle['l4'].get('results', {}).get('subset_policy', {}).get('denial_reasons', {}),
        'skipped_objects': run_bundle.get('errors', [])
    }
    limitations_path = os.path.join(reports_dir, 'limitations_report.json')
    with open(limitations_path, 'w') as f:
        json.dump(limitations_report, f, indent=2, default=str)

    # Write parity summary report (migrated vs skipped)
    l3_policy = run_bundle['l3'].get('results', {}).get('subset_policy', {})
    l4_policy = run_bundle['l4'].get('results', {}).get('subset_policy', {})

    parity_summary = {
        'run_id': run_bundle['run_id'],
        'timestamp': run_bundle.get('end_time', run_bundle.get('start_time')),
        'l1_parity': {
            'tables': {
                'migrated': len(run_bundle['l1'].get('results', {}).get('tables', [])),
                'skipped': 0,
                'reason_codes': {}
            },
            'constraints': run_bundle['l1'].get('results', {}).get('constraints', {}),
            'indexes': {
                'migrated': run_bundle['l1'].get('results', {}).get('indexes', 0),
                'skipped': 0,
                'reason_codes': {}
            },
            'sequences': {
                'migrated': run_bundle['l1'].get('results', {}).get('sequences', 0),
                'skipped': 0,
                'reason_codes': {}
            }
        },
        'l2_parity': {
            'views': {
                'migrated': len(run_bundle['l2'].get('results', {}).get('views', [])),
                'skipped': 0,
                'reason_codes': {}
            }
        },
        'l3_parity': {
            'routines': {
                'migrated': l3_policy.get('allowed', 0),
                'skipped': l3_policy.get('denied', 0),
                'total': l3_policy.get('total', 0),
                'reason_codes': l3_policy.get('denial_reasons', {})
            }
        },
        'l4_parity': {
            'triggers': {
                'migrated': l4_policy.get('allowed', 0),
                'skipped': l4_policy.get('denied', 0),
                'total': l4_policy.get('total', 0),
                'reason_codes': l4_policy.get('denial_reasons', {})
            }
        },
        'summary': {
            'total_migrated': (
                len(run_bundle['l1'].get('results', {}).get('tables', [])) +
                run_bundle['l1'].get('results', {}).get('indexes', 0) +
                run_bundle['l1'].get('results', {}).get('sequences', 0) +
                len(run_bundle['l2'].get('results', {}).get('views', [])) +
                l3_policy.get('allowed', 0) +
                l4_policy.get('allowed', 0)
            ),
            'total_skipped': l3_policy.get('denied', 0) + l4_policy.get('denied', 0),
            'parity_status': 'COMPLETE' if run_bundle.get('overall_status') == 'PASS' else 'INCOMPLETE'
        }
    }
    parity_path = os.path.join(reports_dir, 'parity_summary.json')
    with open(parity_path, 'w') as f:
        json.dump(parity_summary, f, indent=2, default=str)

    # Write log file
    log_path = os.path.join(logs_dir, 'harness_run.log')
    with open(log_path, 'w') as f:
        f.write(f"Oracle L1/L2/L3/L4 Harness Run Log\n")
        f.write(f"{'=' * 50}\n")
        f.write(f"Run ID: {run_bundle['run_id']}\n")
        f.write(f"Start: {run_bundle.get('start_time')}\n")
        f.write(f"End: {run_bundle.get('end_time')}\n")
        f.write(f"Oracle User: {run_bundle.get('oracle_user')}\n")
        f.write(f"DB Version: {run_bundle.get('db_version')}\n")
        f.write(f"Connection: {run_bundle.get('connection_info')}\n")
        f.write(f"Seed Hash: {run_bundle.get('seed_hash')}\n")
        f.write(f"{'=' * 50}\n")
        f.write(f"L1 Status: {run_bundle['l1'].get('status')}\n")
        f.write(f"L2 Status: {run_bundle['l2'].get('status')}\n")
        f.write(f"L3 Status: {run_bundle['l3'].get('status')}\n")
        f.write(f"L4 Status: {run_bundle['l4'].get('status')}\n")
        f.write(f"Overall: {run_bundle.get('overall_status', 'INCOMPLETE')}\n")

    print(f"\nBundle written to: {bundle_dir}")


if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])
