#!/usr/bin/env python3
"""
SAIQL Phase 12 Validation Harness Tests

Validates Phase 12 validation infrastructure:
- Bundle schema versioning
- Fingerprint determinism
- Validation Report v2 structure
- Limitations Report v2 structure
- 3x deterministic run verification

Evidence:
- Uses Phase 11 fixtures for reference data
- Proves fingerprints are stable across runs
"""

import pytest
import json
import tempfile
import logging
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)

# Test fixtures path
SQLITE_FIXTURE = '/mnt/storage/DockerTests/sqlite/fixtures/01_schema.sql'


class TestPhase12SchemaVersioning:
    """Verify bundle and report schema versions are defined and stable."""

    def test_bundle_schema_version_defined(self):
        """Schema versions must be defined."""
        print("\n=== Bundle Schema Version ===")
        from core.validation.schemas import BUNDLE_SCHEMA_VERSION

        assert BUNDLE_SCHEMA_VERSION is not None
        assert isinstance(BUNDLE_SCHEMA_VERSION, str)
        assert len(BUNDLE_SCHEMA_VERSION) > 0

        print(f"  Bundle Schema Version: {BUNDLE_SCHEMA_VERSION}")
        print("✓ Bundle schema version defined")

    def test_report_schema_version_defined(self):
        """Report schema version must be defined."""
        print("\n=== Report Schema Version ===")
        from core.validation.schemas import REPORT_SCHEMA_VERSION

        assert REPORT_SCHEMA_VERSION is not None
        assert isinstance(REPORT_SCHEMA_VERSION, str)

        print(f"  Report Schema Version: {REPORT_SCHEMA_VERSION}")
        print("✓ Report schema version defined")

    def test_limitations_schema_version_defined(self):
        """Limitations schema version must be defined."""
        print("\n=== Limitations Schema Version ===")
        from core.validation.schemas import LIMITATIONS_SCHEMA_VERSION

        assert LIMITATIONS_SCHEMA_VERSION is not None
        assert isinstance(LIMITATIONS_SCHEMA_VERSION, str)

        print(f"  Limitations Schema Version: {LIMITATIONS_SCHEMA_VERSION}")
        print("✓ Limitations schema version defined")


class TestPhase12FingerprintDeterminism:
    """Verify fingerprints are deterministic across runs."""

    @pytest.fixture(scope='class')
    def sqlite_adapter(self):
        """SQLite adapter with Phase 11 fixture data."""
        from extensions.plugins.sqlite_adapter import SQLiteAdapter

        adapter = SQLiteAdapter(database=':memory:')
        fixture_sql = Path(SQLITE_FIXTURE).read_text()
        adapter.execute_script(fixture_sql)

        yield adapter
        adapter.close()

    def test_table_fingerprint_determinism(self, sqlite_adapter):
        """Same data must produce same fingerprint."""
        print("\n=== Table Fingerprint Determinism ===")
        from core.validation.fingerprint import FingerprintCalculator

        calc = FingerprintCalculator()

        # Extract data
        result = sqlite_adapter.extract_data('employees')
        rows = result['data']

        # Compute fingerprint 3 times
        fp1 = calc.compute_table_fingerprint('employees', rows)
        fp2 = calc.compute_table_fingerprint('employees', rows)
        fp3 = calc.compute_table_fingerprint('employees', rows)

        print(f"  Run 1: {fp1.fingerprint[:32]}...")
        print(f"  Run 2: {fp2.fingerprint[:32]}...")
        print(f"  Run 3: {fp3.fingerprint[:32]}...")

        assert fp1.fingerprint == fp2.fingerprint, "Fingerprints must match (run 1 vs 2)"
        assert fp2.fingerprint == fp3.fingerprint, "Fingerprints must match (run 2 vs 3)"
        assert fp1.row_count == 6, "employees should have 6 rows"

        print("✓ Table fingerprint is deterministic")

    def test_dataset_fingerprint_determinism(self, sqlite_adapter):
        """Dataset fingerprint must be deterministic."""
        print("\n=== Dataset Fingerprint Determinism ===")
        from core.validation.fingerprint import FingerprintCalculator

        calc = FingerprintCalculator()

        # Compute full dataset fingerprint 3 times
        fp1 = calc.compute_from_adapter(sqlite_adapter)
        fp2 = calc.compute_from_adapter(sqlite_adapter)
        fp3 = calc.compute_from_adapter(sqlite_adapter)

        print(f"  Run 1: {fp1.combined_fingerprint[:32]}... ({fp1.total_rows} rows)")
        print(f"  Run 2: {fp2.combined_fingerprint[:32]}... ({fp2.total_rows} rows)")
        print(f"  Run 3: {fp3.combined_fingerprint[:32]}... ({fp3.total_rows} rows)")

        assert fp1.combined_fingerprint == fp2.combined_fingerprint
        assert fp2.combined_fingerprint == fp3.combined_fingerprint
        assert fp1.total_tables == 5, "Should have 5 tables"
        assert fp1.total_rows == 23, "Should have 23 total rows"

        print("✓ Dataset fingerprint is deterministic")

    def test_fingerprint_changes_with_data(self, sqlite_adapter):
        """Fingerprint must change when data changes."""
        print("\n=== Fingerprint Sensitivity ===")
        from core.validation.fingerprint import FingerprintCalculator

        calc = FingerprintCalculator()

        # Get original fingerprint
        result1 = sqlite_adapter.extract_data('departments')
        rows1 = result1['data']
        fp1 = calc.compute_table_fingerprint('departments', rows1)

        # Modify data (add a row)
        rows2 = rows1 + [{'dept_id': 99, 'dept_name': 'Test', 'dept_code': 'TEST', 'budget': 0}]
        fp2 = calc.compute_table_fingerprint('departments', rows2)

        print(f"  Original: {fp1.fingerprint[:32]}... ({fp1.row_count} rows)")
        print(f"  Modified: {fp2.fingerprint[:32]}... ({fp2.row_count} rows)")

        assert fp1.fingerprint != fp2.fingerprint, "Fingerprint must change with data"
        assert fp2.row_count == fp1.row_count + 1

        print("✓ Fingerprint changes when data changes")


class TestPhase12ValidationReportV2:
    """Verify Validation Report v2 structure and determinism."""

    @pytest.fixture(scope='class')
    def sqlite_adapter(self):
        """SQLite adapter with Phase 11 fixture data."""
        from extensions.plugins.sqlite_adapter import SQLiteAdapter

        adapter = SQLiteAdapter(database=':memory:')
        fixture_sql = Path(SQLITE_FIXTURE).read_text()
        adapter.execute_script(fixture_sql)

        yield adapter
        adapter.close()

    def test_validation_report_structure(self, sqlite_adapter):
        """Report must have required sections."""
        print("\n=== Validation Report Structure ===")
        from core.validation.report_v2 import ValidationReportGenerator

        generator = ValidationReportGenerator(
            run_id="test_run_001",
            source_connector="SQLiteAdapter",
            target_connector="SQLiteAdapter"
        )

        # Add some data parity
        generator.add_data_parity("departments", 4, 4)
        generator.add_data_parity("employees", 6, 6)

        # Add type parity
        generator.add_type_parity("departments", [
            {"column_name": "dept_id", "source_type": "INTEGER", "ir_type": "INTEGER", "target_type": "INTEGER"}
        ])

        report = generator.generate()
        report_dict = report.to_dict()

        # Check required sections
        assert "schema_version" in report_dict
        assert "run_id" in report_dict
        assert "data_parity" in report_dict
        assert "type_parity" in report_dict
        assert "limitations" in report_dict
        assert "summary" in report_dict

        print(f"  Schema version: {report_dict['schema_version']}")
        print(f"  Run ID: {report_dict['run_id']}")
        print(f"  Tables in data_parity: {len(report_dict['data_parity'])}")

        print("✓ Validation report has required structure")

    def test_validation_report_determinism(self, sqlite_adapter):
        """Same inputs must produce same report."""
        print("\n=== Validation Report Determinism ===")
        from core.validation.report_v2 import ValidationReportGenerator

        def create_report():
            generator = ValidationReportGenerator(
                run_id="test_run_determinism",
                source_connector="SQLiteAdapter",
                target_connector="SQLiteAdapter"
            )
            generator.add_data_parity("departments", 4, 4, "fp1", "fp1")
            generator.add_data_parity("employees", 6, 6, "fp2", "fp2")
            generator.add_type_parity("departments", [
                {"column_name": "dept_id", "source_type": "INTEGER", "ir_type": "INTEGER", "target_type": "INTEGER"}
            ])
            generator.add_limitation("lossy_mapping", "column", "test.col", "Test limitation", "warning")
            return generator.generate()

        report1 = create_report()
        report2 = create_report()
        report3 = create_report()

        # Compare dict representations (excluding generated_at)
        dict1 = report1.to_dict()
        dict2 = report2.to_dict()
        dict3 = report3.to_dict()

        # Remove timestamps for comparison
        for d in [dict1, dict2, dict3]:
            d.pop('generated_at', None)

        json1 = json.dumps(dict1, sort_keys=True)
        json2 = json.dumps(dict2, sort_keys=True)
        json3 = json.dumps(dict3, sort_keys=True)

        print(f"  Report 1 size: {len(json1)} bytes")
        print(f"  Report 2 size: {len(json2)} bytes")
        print(f"  Report 3 size: {len(json3)} bytes")

        assert json1 == json2, "Reports must be identical (1 vs 2)"
        assert json2 == json3, "Reports must be identical (2 vs 3)"

        print("✓ Validation report is deterministic")

    def test_validation_report_save(self, sqlite_adapter):
        """Report must save to JSON and text files."""
        print("\n=== Validation Report Save ===")
        from core.validation.report_v2 import ValidationReportGenerator

        with tempfile.TemporaryDirectory() as tmpdir:
            generator = ValidationReportGenerator(
                run_id="test_save",
                source_connector="SQLiteAdapter",
                target_connector="SQLiteAdapter"
            )
            generator.add_data_parity("departments", 4, 4)

            paths = generator.save_report(tmpdir)

            assert Path(paths["validation_summary_json"]).exists()
            assert Path(paths["validation_report_txt"]).exists()
            assert Path(paths["limitations_json"]).exists()
            assert Path(paths["limitations_txt"]).exists()

            print(f"  JSON: {paths['validation_summary_json']}")
            print(f"  TXT: {paths['validation_report_txt']}")

        print("✓ Validation report saves correctly")


class TestPhase12LimitationsReport:
    """Verify Limitations Report v2 structure and determinism."""

    def test_limitations_report_structure(self):
        """Limitations report must have required sections."""
        print("\n=== Limitations Report Structure ===")
        from core.validation.schemas import LimitationsReport, Limitation

        report = LimitationsReport(
            unsupported_objects=[
                Limitation("unsupported_object", "trigger", "tr_test", "Triggers not supported", "warning")
            ],
            lossy_mappings=[
                Limitation("lossy_mapping", "column", "big_decimal", "Precision loss", "warning")
            ],
            behavior_differences=[],
            manual_steps=[
                Limitation("manual_step", "sequence", "seq_id", "Recreate sequence manually", "info")
            ]
        )

        report_dict = report.to_dict()

        assert "schema_version" in report_dict
        assert "unsupported_objects" in report_dict
        assert "lossy_mappings" in report_dict
        assert "behavior_differences" in report_dict
        assert "manual_steps" in report_dict

        print(f"  Schema version: {report_dict['schema_version']}")
        print(f"  Unsupported objects: {len(report_dict['unsupported_objects'])}")
        print(f"  Lossy mappings: {len(report_dict['lossy_mappings'])}")
        print(f"  Manual steps: {len(report_dict['manual_steps'])}")

        print("✓ Limitations report has required structure")

    def test_limitations_report_determinism(self):
        """Limitations must be sorted deterministically."""
        print("\n=== Limitations Report Determinism ===")
        from core.validation.schemas import LimitationsReport, Limitation

        # Create limitations in random order
        lims1 = [
            Limitation("unsupported_object", "trigger", "z_trigger", "Z trigger", "warning"),
            Limitation("unsupported_object", "trigger", "a_trigger", "A trigger", "warning"),
            Limitation("unsupported_object", "view", "my_view", "View not supported", "warning"),
        ]

        lims2 = [
            Limitation("unsupported_object", "view", "my_view", "View not supported", "warning"),
            Limitation("unsupported_object", "trigger", "a_trigger", "A trigger", "warning"),
            Limitation("unsupported_object", "trigger", "z_trigger", "Z trigger", "warning"),
        ]

        report1 = LimitationsReport(unsupported_objects=lims1, lossy_mappings=[], behavior_differences=[], manual_steps=[])
        report2 = LimitationsReport(unsupported_objects=lims2, lossy_mappings=[], behavior_differences=[], manual_steps=[])

        json1 = json.dumps(report1.to_dict(), sort_keys=True)
        json2 = json.dumps(report2.to_dict(), sort_keys=True)

        print(f"  Report 1: {json1[:100]}...")
        print(f"  Report 2: {json2[:100]}...")

        assert json1 == json2, "Same limitations in different order must produce same output"

        # Verify sort order
        sorted_objs = report1.to_dict()["unsupported_objects"]
        assert sorted_objs[0]["object_name"] == "a_trigger"
        assert sorted_objs[1]["object_name"] == "z_trigger"
        assert sorted_objs[2]["object_name"] == "my_view"

        print("✓ Limitations report is deterministic")

    def test_limitations_text_format(self):
        """Limitations text output must be readable."""
        print("\n=== Limitations Text Format ===")
        from core.validation.schemas import LimitationsReport, Limitation

        report = LimitationsReport(
            unsupported_objects=[
                Limitation("unsupported_object", "trigger", "audit_trigger", "Triggers not supported", "warning")
            ],
            lossy_mappings=[],
            behavior_differences=[],
            manual_steps=[
                Limitation("manual_step", "sequence", "order_seq", "Recreate sequence", "info")
            ]
        )

        text = report.to_text()

        assert "SAIQL LIMITATIONS REPORT" in text
        assert "UNSUPPORTED OBJECTS" in text
        assert "audit_trigger" in text
        assert "REQUIRED MANUAL STEPS" in text
        assert "order_seq" in text

        print(text[:500])
        print("✓ Limitations text format is readable")


class TestPhase12BundleManifest:
    """Verify bundle manifest structure."""

    def test_manifest_structure(self):
        """Manifest must have required fields."""
        print("\n=== Bundle Manifest Structure ===")
        from core.validation.schemas import BundleManifest

        manifest = BundleManifest(
            run_id="test_run_123",
            source_connector="PostgreSQLAdapter",
            target_connector="MySQLAdapter"
        )

        manifest_dict = manifest.to_dict()

        assert "bundle_version" in manifest_dict
        assert "run_id" in manifest_dict
        assert "created_at" in manifest_dict
        assert "file_checksums" in manifest_dict

        print(f"  Bundle version: {manifest_dict['bundle_version']}")
        print(f"  Run ID: {manifest_dict['run_id']}")

        print("✓ Bundle manifest has required structure")

    def test_manifest_json_output(self):
        """Manifest JSON must be valid and sorted."""
        print("\n=== Bundle Manifest JSON ===")
        from core.validation.schemas import BundleManifest

        manifest = BundleManifest(
            run_id="test_run_456",
            source_connector="SQLiteAdapter",
            target_connector="SQLiteAdapter",
            file_checksums={
                "data.json": "abc123",
                "schema.json": "def456"
            },
            dataset_fingerprint="combined_fp_789"
        )

        json_str = manifest.to_json()

        # Verify it's valid JSON
        parsed = json.loads(json_str)
        assert parsed["run_id"] == "test_run_456"
        assert parsed["dataset_fingerprint"] == "combined_fp_789"

        print(json_str[:300])
        print("✓ Bundle manifest JSON is valid")


class TestPhase12Requirements:
    """Verify all Phase 12 requirements are met."""

    def test_all_components_exist(self):
        """All required Phase 12 components must exist."""
        print("\n=== Phase 12 Components ===")

        # Import all components
        from core.validation.schemas import (
            BUNDLE_SCHEMA_VERSION,
            REPORT_SCHEMA_VERSION,
            LIMITATIONS_SCHEMA_VERSION,
            ValidationReportV2,
            LimitationsReport,
            BundleManifest,
            TableFingerprint,
            DatasetFingerprint
        )
        from core.validation.fingerprint import FingerprintCalculator, compute_file_checksum
        from core.validation.report_v2 import ValidationReportGenerator, compare_adapters

        components = [
            ("BUNDLE_SCHEMA_VERSION", BUNDLE_SCHEMA_VERSION),
            ("REPORT_SCHEMA_VERSION", REPORT_SCHEMA_VERSION),
            ("LIMITATIONS_SCHEMA_VERSION", LIMITATIONS_SCHEMA_VERSION),
            ("ValidationReportV2", ValidationReportV2),
            ("LimitationsReport", LimitationsReport),
            ("BundleManifest", BundleManifest),
            ("FingerprintCalculator", FingerprintCalculator),
            ("ValidationReportGenerator", ValidationReportGenerator),
        ]

        for name, component in components:
            assert component is not None, f"Missing component: {name}"
            print(f"  ✓ {name}")

        print("✓ All Phase 12 components exist")
