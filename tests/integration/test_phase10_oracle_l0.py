#!/usr/bin/env python3
"""
SAIQL Phase 10 Oracle L0 Harness Tests

Validates Oracle adapter L0 capabilities:
- L0.1: List tables
- L0.2: Introspect schema
- L0.3: Extract data
- L0.4: Type mapping
- L0.5: Deterministic extraction

Proof-first approach: Tests written BEFORE adapter implementation.

Evidence:
- Oracle Free 23.5 container
- Fixture: /mnt/storage/DockerTests/oracle/fixtures/01_schema.sql
- 3 tables: customers, orders, type_test
"""

import pytest
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

class TestPhase10OracleL0Harness:
    """Oracle L0 harness tests"""

    @pytest.fixture(scope='class')
    def oracle_adapter(self):
        """Oracle adapter fixture"""
        from extensions.plugins.oracle_adapter import OracleAdapter

        config = {
            'user': 'saiql_user',
            'password': 'SaiqlTestPass123',
            'dsn': 'localhost:1522/FREEPDB1'
        }

        adapter = OracleAdapter(config)
        yield adapter
        adapter.close()

    def test_l0_1_list_tables(self, oracle_adapter):
        """
        L0.1: List tables from Oracle test database

        Requirement: get_tables() returns list of user tables
        Expected: At least ['customers', 'orders', 'type_test']
        """
        print("\n=== L0.1: Listing Oracle tables ===")

        tables = oracle_adapter.get_tables()

        print(f"Found {len(tables)} tables: {tables}")

        # Should include our test tables
        expected_tables = {'customers', 'orders', 'type_test'}
        found_tables = set(tables)

        assert expected_tables.issubset(found_tables), \
            f"Missing tables. Expected: {expected_tables}, Found: {found_tables}"

        print("✓ All expected tables found")

    def test_l0_2_introspect_customers_schema(self, oracle_adapter):
        """
        L0.2: Introspect 'customers' table schema

        Requirement: get_schema() returns column metadata
        Expected:
        - 6 columns: customer_id, email, name, created_at, credit_limit, is_active
        - Type info mapped via TypeRegistry
        - Nullable info present
        """
        print("\n=== L0.2: Introspecting 'customers' schema ===")

        schema = oracle_adapter.get_schema('customers')

        print(f"Found {len(schema['columns'])} columns")

        # Check expected columns present
        expected_columns = {
            'customer_id', 'email', 'name',
            'created_at', 'credit_limit', 'is_active'
        }
        found_columns = {col['name'] for col in schema['columns']}

        print(f"Expected columns: {expected_columns}")
        print(f"Found columns: {found_columns}")

        assert expected_columns == found_columns, \
            f"Column mismatch. Missing: {expected_columns - found_columns}, Extra: {found_columns - expected_columns}"

        # Verify type_info present (IR mapping)
        for col in schema['columns']:
            assert 'type_info' in col, f"Column {col['name']} missing type_info"
            assert col['type_info'] is not None, f"Column {col['name']} has null type_info"

        print("✓ Schema introspection successful")

    def test_l0_3_extract_customers_data(self, oracle_adapter):
        """
        L0.3: Extract data from 'customers' table

        Requirement: extract_data() returns all rows with stats
        Expected:
        - 3 customers (alice, bob, charlie)
        - Data dict with 'data' and 'stats' keys
        """
        print("\n=== L0.3: Extracting 'customers' data ===")

        result = oracle_adapter.extract_data('customers')

        assert 'data' in result, "Result missing 'data' key"
        assert 'stats' in result, "Result missing 'stats' key"

        data = result['data']
        stats = result['stats']

        print(f"Extracted {len(data)} rows")
        print(f"Stats: {stats}")

        assert len(data) == 3, f"Expected 3 customers, got {len(data)}"
        assert stats['total_rows'] == 3, f"Stats mismatch: {stats['total_rows']}"

        # Verify expected customers present
        emails = {row['email'] if 'email' in row else row.get('EMAIL') for row in data}
        expected_emails = {'alice@example.com', 'bob@example.com', 'charlie@example.com'}

        assert expected_emails.issubset(emails), \
            f"Missing customers. Expected: {expected_emails}, Found: {emails}"

        print("✓ Data extraction successful")

    def test_l0_4_type_mapping_validation(self, oracle_adapter):
        """
        L0.4: Verify type mapping for type_test table

        Requirement: TypeRegistry maps Oracle types to IR types
        Expected:
        - NUMBER → IR numeric type
        - VARCHAR2 → IR string type
        - TIMESTAMP → IR temporal type
        - BLOB/CLOB → IR binary/text types
        """
        print("\n=== L0.4: Validating type mapping ===")

        schema = oracle_adapter.get_schema('type_test')

        # Check we have various type families
        type_names = {col['type'].upper() for col in schema['columns']}

        print(f"Found types: {type_names}")

        # Oracle has these type families
        expected_type_families = ['NUMBER', 'VARCHAR2', 'TIMESTAMP', 'CLOB', 'BLOB']

        for type_family in expected_type_families:
            found = any(type_family in t for t in type_names)
            assert found, f"Missing type family: {type_family}"

        # All columns should have type_info
        for col in schema['columns']:
            assert col['type_info'] is not None, \
                f"Column {col['name']} has null type_info for type {col['type']}"

        print("✓ Type mapping validation successful")

    def test_l0_5_deterministic_extraction(self, oracle_adapter):
        """
        L0.5: Verify deterministic extraction (repeatability)

        Requirement: extract_data() returns rows in consistent order
        Expected: Two extractions yield same row order
        """
        print("\n=== L0.5: Testing deterministic extraction ===")

        result1 = oracle_adapter.extract_data('customers')
        result2 = oracle_adapter.extract_data('customers')

        # Compare row order (use customer_id or CUSTOMER_ID depending on case)
        def get_id(row):
            return row.get('customer_id') or row.get('CUSTOMER_ID')

        ids1 = [get_id(row) for row in result1['data']]
        ids2 = [get_id(row) for row in result2['data']]

        print(f"Run 1 IDs: {ids1}")
        print(f"Run 2 IDs: {ids2}")

        assert ids1 == ids2, "Row order should be deterministic across extractions"

        print("✓ Deterministic extraction verified")


class TestPhase10OracleL0Requirements:
    """Verify all L0 requirements met"""

    @pytest.fixture(scope='class')
    def oracle_adapter(self):
        """Oracle adapter fixture"""
        from extensions.plugins.oracle_adapter import OracleAdapter

        config = {
            'user': 'saiql_user',
            'password': 'SaiqlTestPass123',
            'dsn': 'localhost:1522/FREEPDB1'
        }

        adapter = OracleAdapter(config)
        yield adapter
        adapter.close()

    def test_all_l0_requirements_met(self, oracle_adapter):
        """
        Verify adapter implements all L0 methods

        Requirements:
        - get_tables() exists and callable
        - get_schema() exists and callable
        - extract_data() exists and callable
        """
        print("\n=== Verifying all L0 requirements ===")

        # Check methods exist
        assert hasattr(oracle_adapter, 'get_tables'), "Missing get_tables() method"
        assert hasattr(oracle_adapter, 'get_schema'), "Missing get_schema() method"
        assert hasattr(oracle_adapter, 'extract_data'), "Missing extract_data() method"

        # Check methods are callable
        assert callable(oracle_adapter.get_tables), "get_tables() not callable"
        assert callable(oracle_adapter.get_schema), "get_schema() not callable"
        assert callable(oracle_adapter.extract_data), "extract_data() not callable"

        print("✓ All L0 requirements met")
