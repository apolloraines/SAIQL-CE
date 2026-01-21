# HANA L2/L3/L4 Integration Test Harness

Complete end-to-end integration test harness for HANA L2 (Views), L3 (Routines), L4 (Triggers).

## Requirements

### Software Requirements
- Docker (for SAP HANA Express Edition)
- Python 3.8+
- hdbcli (SAP HANA Python driver): `pip install hdbcli`
- pytest: `pip install pytest`

### System Requirements
- **RAM**: Minimum 8GB (HANA Express requires 4GB+)
- **Disk**: 15GB free (HANA image is ~7GB)
- **Docker**: Configured with sufficient resources

## Quick Start

### 1. Setup Phase 07 HANA Container (Prerequisite)

```bash
cd tests/integration/phase07_hana_harness/scripts
./setup_hana_docker.sh
```

Wait 3-5 minutes for HANA to initialize.

### 2. Setup L2/L3/L4 Fixtures

```bash
cd tests/integration/hana_l2l3l4_harness/scripts
./setup_hana_l2l3l4.sh
```

This creates:
- L2/L3/L4 test user with required privileges
- SQL views (4)
- Stored procedures (2)
- Functions (2)
- Triggers (3)

### 3. Set Environment Variables

```bash
export HANA_HOST=localhost
export HANA_PORT=39017
export HANA_DATABASE=HXE
export HANA_USER=SAIQL_L2L3L4_TEST
export HANA_PASSWORD=L2L3L4Test123!
```

### 4. Run Integration Tests

```bash
# From project root
pytest tests/integration/test_hana_l2l3l4_harness.py -v -s
```

### 5. View Run Bundle

```bash
ls tests/integration/hana_l2l3l4_harness/runs/<run_id>/
# run_manifest.json
# logs/
# reports/
#   validation_report.json
#   l2_limitations.json
#   l3_limitations.json
#   l4_limitations.json
#   parity_summary.json
```

---

## Privilege Requirements

### Test User: SAIQL_L2L3L4_TEST

The test user requires the following **minimal privileges**:

#### System Privileges
| Privilege | Purpose |
|-----------|---------|
| CREATE SCHEMA | Create fresh schema per run_id |

#### Catalog Privileges
| Object | Privilege | Purpose |
|--------|-----------|---------|
| SYS.VIEWS | SELECT | View introspection |
| SYS.PROCEDURES | SELECT | Procedure introspection |
| SYS.PROCEDURE_PARAMETERS | SELECT | Procedure argument introspection |
| SYS.FUNCTIONS | SELECT | Function introspection |
| SYS.FUNCTION_PARAMETERS | SELECT | Function argument introspection |
| SYS.TRIGGERS | SELECT | Trigger introspection |
| SYS.OBJECT_DEPENDENCIES | SELECT | Dependency resolution |
| SYS.TABLES | SELECT | Table introspection |
| SYS.TABLE_COLUMNS | SELECT | Column introspection |
| SYS.CONSTRAINTS | SELECT | Constraint introspection |
| SYS.REFERENTIAL_CONSTRAINTS | SELECT | FK introspection |
| SYS.INDEXES | SELECT | Index introspection |

#### Object Privileges
| Object | Privilege | Purpose |
|--------|-----------|---------|
| Test tables | SELECT, INSERT, UPDATE, DELETE | Data operations |
| Test views | SELECT | View queries |
| Test routines | EXECUTE | Routine execution |

#### Grant Script
```sql
-- Create user
CREATE USER SAIQL_L2L3L4_TEST PASSWORD 'L2L3L4Test123!' NO FORCE_FIRST_PASSWORD_CHANGE;

-- System privileges
GRANT CREATE SCHEMA TO SAIQL_L2L3L4_TEST;

-- Catalog read
GRANT CATALOG READ TO SAIQL_L2L3L4_TEST;
GRANT SELECT ON SCHEMA SYS TO SAIQL_L2L3L4_TEST;
```

---

## Schema Strategy

### Mode: Fresh Schema Per Run

Each harness run creates a new schema with the run_id as suffix:
```
SAIQL_L2L3L4_TEST_20260114_143022
```

### Teardown
After run completes, schema is dropped:
```sql
DROP SCHEMA SAIQL_L2L3L4_TEST_<run_id> CASCADE;
```

### Benefits
- Clean state guaranteed per run
- No leftover objects
- Deterministic results
- Safe for parallel runs

---

## Test Fixtures

### L2 Views (4)
| Name | Type | Description |
|------|------|-------------|
| customer_summary | Simple SELECT | Basic view |
| active_customers | SELECT + WHERE | Filtered view |
| high_balance_active | View-on-view | Tests dependency |
| order_summary | SELECT + JOIN | Equality join |

### L3 Procedures (2)
| Name | Description |
|------|-------------|
| update_customer_status | Simple UPDATE with params |
| get_customer_count | SELECT INTO with OUT param |

### L3 Functions (2)
| Name | Returns | Description |
|------|---------|-------------|
| get_customer_balance | DECIMAL | Scalar lookup |
| calculate_discount_price | DECIMAL | Calculation |

### L4 Triggers (3)
| Name | Timing | Event | Subset |
|------|--------|-------|--------|
| trg_upper_lastname | BEFORE | INSERT | SUPPORTED |
| trg_trim_email | BEFORE | UPDATE | SUPPORTED |
| trg_order_audit | AFTER | INSERT | UNSUPPORTED |

---

## Run Bundle Structure

Each run produces a bundle in `runs/<run_id>/`:

```
runs/
└── 20260114_143022/
    ├── run_manifest.json      # Run metadata
    ├── logs/                  # Execution logs
    └── reports/
        ├── validation_report.json   # L2/L3/L4 results
        ├── l2_limitations.json      # View limitations
        ├── l3_limitations.json      # Routine limitations
        ├── l4_limitations.json      # Trigger limitations
        └── parity_summary.json      # Migration parity
```

### run_manifest.json
```json
{
  "run_id": "20260114_143022",
  "start_time": "2026-01-14T14:30:22.123456",
  "end_time": "2026-01-14T14:30:45.789012",
  "duration_seconds": 23.67,
  "hana_version": "2.00.070.00",
  "schema": "SYSTEM",
  "privileges_summary": ["SELECT on SYS.VIEWS: OK", "CATALOG READ: OK"],
  "seed_hash": "abc123...",
  "results": {
    "a0_connectivity": "PASS",
    "a1_fixtures": "PASS",
    "b1_extraction": "PASS",
    ...
  }
}
```

---

## Test Coverage

### A) Environment Harness Prerequisites
- [x] A0) Connectivity + baseline
- [x] A1) Deterministic fixtures

### B) L2 (Views) Harness
- [x] B1) Extraction
- [x] B2) Emission (dependency order)
- [x] B3) Validation (presence + result parity)
- [x] B4) Limitations

### C) L3 (Routines) Harness
- [x] C1) Extraction
- [x] C2) Emission (dependency order)
- [x] C3) Behavioral validation
- [x] C4) Limitations

### D) L4 (Triggers) Harness
- [x] D1) Subset definition
- [x] D2) Extraction
- [x] D3) Emission
- [x] D4) Behavioral validation
- [x] D5) Limitations

### E) Bundle Requirements
- [x] E1) Validation report
- [x] E2) Parity summary

---

## Known Limitations

### L2 Views
- Calculation Views (HDI/repository artifacts) NOT supported
- Only catalog-based SQL views extracted
- Complex subqueries NOT validated

### L3 Routines
- AMDP (ABAP Managed Database Procedures) NOT supported
- Only SQLScript/SQL routines extracted
- Complex dependencies may not be fully resolved

### L4 Triggers
- Only BEFORE INSERT/UPDATE with simple normalization supported
- AFTER triggers NOT supported
- Triggers with DML operations NOT supported
- Triggers with loops/cursors NOT supported
- Triggers with conditional logic NOT supported

---

## Troubleshooting

### Connection Refused
```bash
# Wait for HANA to initialize (3-5 minutes)
docker logs saiql_hana_test | grep "Startup finished"

# Test connection manually
docker exec saiql_hana_test hdbsql -i 90 -d HXE -u SYSTEM -p SaiqlTest123 "SELECT 1 FROM DUMMY"
```

### Missing Privileges
```bash
# Check CATALOG READ
docker exec saiql_hana_test hdbsql -i 90 -d HXE -u SAIQL_L2L3L4_TEST -p 'L2L3L4Test123!' \
  "SELECT COUNT(*) FROM SYS.VIEWS WHERE SCHEMA_NAME = 'SYSTEM'"
```

### Fixtures Not Loaded
```bash
# Re-run setup script
./scripts/setup_hana_l2l3l4.sh
```

---

## Cleanup

```bash
# Stop and remove HANA container
docker stop saiql_hana_test
docker rm saiql_hana_test

# Remove runs (optional)
rm -rf tests/integration/hana_l2l3l4_harness/runs/*
```

---

## Apollo Standard Compliance

| Standard | Status |
|----------|--------|
| No smoke tests | COMPLIANT |
| No conditional passes | COMPLIANT |
| Clean state per run | COMPLIANT (fresh schema) |
| Deterministic tests | COMPLIANT |
| Unsupported objects reported | COMPLIANT |
| No secret leakage | COMPLIANT |
| 3x clean-state passes | PENDING (requires execution) |

---

## Proof Bar

Per `Tests_SAP_HANA_L2L3L4.md`:

All required suites must pass **3 consecutive times** from clean state.

To execute:
```bash
# Run 1
pytest tests/integration/test_hana_l2l3l4_harness.py -v

# Run 2 (clean state)
./scripts/setup_hana_l2l3l4.sh  # Reset fixtures
pytest tests/integration/test_hana_l2l3l4_harness.py -v

# Run 3 (clean state)
./scripts/setup_hana_l2l3l4.sh  # Reset fixtures
pytest tests/integration/test_hana_l2l3l4_harness.py -v
```

Each run produces a bundle in `runs/<run_id>/` for audit.
