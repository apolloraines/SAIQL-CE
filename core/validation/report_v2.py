#!/usr/bin/env python3
"""
SAIQL Phase 12 - Validation Report v2 Generator

Generates comprehensive validation reports with:
- Data parity (row counts + fingerprints)
- Type parity (source -> IR -> target mappings)
- Constraint parity (L1 objects comparison)
- Limitations report

Author: Apollo & Claude
Version: 1.0.0
"""

import json
import logging
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
from pathlib import Path

from .schemas import (
    ValidationReportV2, TableTypeParity, TypeMapping,
    ConstraintParity, ParityStatus, LimitationsReport, Limitation
)
from .fingerprint import FingerprintCalculator, FingerprintConfig

logger = logging.getLogger(__name__)


class ValidationReportGenerator:
    """Generates Phase 12 validation reports."""

    def __init__(
        self,
        run_id: str,
        source_connector: str,
        target_connector: str,
        fingerprint_config: Optional[FingerprintConfig] = None
    ):
        self.run_id = run_id
        self.source_connector = source_connector
        self.target_connector = target_connector
        self.fingerprint_calc = FingerprintCalculator(fingerprint_config)

        # Collected data
        self._data_parity: Dict[str, Dict[str, Any]] = {}
        self._type_parity: List[TableTypeParity] = []
        self._constraint_parity: List[ConstraintParity] = []
        self._limitations = LimitationsReport(
            unsupported_objects=[],
            lossy_mappings=[],
            behavior_differences=[],
            manual_steps=[]
        )

    def add_data_parity(
        self,
        table_name: str,
        source_rows: int,
        target_rows: int,
        source_fingerprint: Optional[str] = None,
        target_fingerprint: Optional[str] = None
    ) -> None:
        """Add data parity check for a table."""
        status = "match" if source_rows == target_rows else "mismatch"

        if source_fingerprint and target_fingerprint:
            if source_fingerprint != target_fingerprint:
                status = "mismatch"

        self._data_parity[table_name] = {
            "source_rows": source_rows,
            "target_rows": target_rows,
            "source_fingerprint": source_fingerprint,
            "target_fingerprint": target_fingerprint,
            "status": status
        }

    def add_type_parity(
        self,
        table_name: str,
        mappings: List[Dict[str, Any]]
    ) -> None:
        """Add type parity mappings for a table."""
        type_mappings = []
        lossy_count = 0

        for m in mappings:
            tm = TypeMapping(
                column_name=m.get("column_name", ""),
                source_type=m.get("source_type", ""),
                ir_type=m.get("ir_type", ""),
                target_type=m.get("target_type", ""),
                is_lossy=m.get("is_lossy", False),
                lossy_reason=m.get("lossy_reason")
            )
            type_mappings.append(tm)
            if tm.is_lossy:
                lossy_count += 1

        self._type_parity.append(TableTypeParity(
            table_name=table_name,
            mappings=type_mappings,
            lossy_count=lossy_count
        ))

    def add_constraint_parity(
        self,
        table_name: str,
        source_constraints: Dict[str, Any],
        target_constraints: Dict[str, Any]
    ) -> None:
        """Add constraint (L1) parity for a table."""
        # Primary keys
        pk_source = source_constraints.get("primary_keys", [])
        pk_target = target_constraints.get("primary_keys", [])
        pk_status = self._compare_lists(pk_source, pk_target)

        # Unique constraints
        unique_source = source_constraints.get("unique_constraints", [])
        unique_target = target_constraints.get("unique_constraints", [])
        unique_status = self._compare_lists(unique_source, unique_target)

        # Foreign keys
        fk_source = source_constraints.get("foreign_keys", [])
        fk_target = target_constraints.get("foreign_keys", [])
        fk_status = self._compare_fks(fk_source, fk_target)

        # Indexes
        idx_source = source_constraints.get("indexes", [])
        idx_target = target_constraints.get("indexes", [])
        idx_status = self._compare_lists(idx_source, idx_target)

        # Identity
        identity_source = source_constraints.get("identity_column")
        identity_target = target_constraints.get("identity_column")
        if identity_source is None and identity_target is None:
            identity_status = ParityStatus.NOT_APPLICABLE
        elif identity_source == identity_target:
            identity_status = ParityStatus.MATCH
        else:
            identity_status = ParityStatus.MISMATCH

        self._constraint_parity.append(ConstraintParity(
            table_name=table_name,
            pk_source=pk_source,
            pk_target=pk_target,
            pk_status=pk_status,
            unique_source=unique_source,
            unique_target=unique_target,
            unique_status=unique_status,
            fk_source=fk_source,
            fk_target=fk_target,
            fk_status=fk_status,
            index_source=idx_source,
            index_target=idx_target,
            index_status=idx_status,
            identity_source=identity_source,
            identity_target=identity_target,
            identity_status=identity_status
        ))

    def _compare_lists(self, source: List, target: List) -> ParityStatus:
        """Compare two lists for parity."""
        if not source and not target:
            return ParityStatus.NOT_APPLICABLE
        source_set = set(str(x) for x in source)
        target_set = set(str(x) for x in target)
        if source_set == target_set:
            return ParityStatus.MATCH
        elif source_set and not target_set:
            return ParityStatus.SOURCE_ONLY
        elif target_set and not source_set:
            return ParityStatus.TARGET_ONLY
        else:
            return ParityStatus.MISMATCH

    def _compare_fks(self, source: List, target: List) -> ParityStatus:
        """Compare foreign key lists."""
        if not source and not target:
            return ParityStatus.NOT_APPLICABLE

        def normalize_fk(fk):
            if isinstance(fk, dict):
                return json.dumps(fk, sort_keys=True)
            return str(fk)

        source_set = set(normalize_fk(fk) for fk in source)
        target_set = set(normalize_fk(fk) for fk in target)

        if source_set == target_set:
            return ParityStatus.MATCH
        elif source_set and not target_set:
            return ParityStatus.SOURCE_ONLY
        elif target_set and not source_set:
            return ParityStatus.TARGET_ONLY
        else:
            return ParityStatus.MISMATCH

    def add_limitation(
        self,
        category: str,
        object_type: str,
        object_name: str,
        description: str,
        severity: str = "warning"
    ) -> None:
        """Add a limitation entry."""
        lim = Limitation(
            category=category,
            object_type=object_type,
            object_name=object_name,
            description=description,
            severity=severity
        )

        if category == "unsupported_object":
            self._limitations.unsupported_objects.append(lim)
        elif category == "lossy_mapping":
            self._limitations.lossy_mappings.append(lim)
        elif category == "behavior_difference":
            self._limitations.behavior_differences.append(lim)
        elif category == "manual_step":
            self._limitations.manual_steps.append(lim)
        else:
            logger.warning(f"Unknown limitation category: {category}")
            self._limitations.behavior_differences.append(lim)

    def generate(self) -> ValidationReportV2:
        """Generate the complete validation report."""
        # Calculate summary stats
        tables_checked = len(self._data_parity)
        tables_matched = sum(1 for p in self._data_parity.values() if p["status"] == "match")
        total_source = sum(p["source_rows"] for p in self._data_parity.values())
        total_target = sum(p["target_rows"] for p in self._data_parity.values())
        lossy_count = sum(tp.lossy_count for tp in self._type_parity)

        # Count constraint mismatches (MISMATCH, SOURCE_ONLY, and TARGET_ONLY all indicate drift)
        mismatch_statuses = {ParityStatus.MISMATCH, ParityStatus.SOURCE_ONLY, ParityStatus.TARGET_ONLY}
        constraint_mismatches = 0
        for cp in self._constraint_parity:
            if cp.pk_status in mismatch_statuses:
                constraint_mismatches += 1
            if cp.unique_status in mismatch_statuses:
                constraint_mismatches += 1
            if cp.fk_status in mismatch_statuses:
                constraint_mismatches += 1
            if cp.index_status in mismatch_statuses:
                constraint_mismatches += 1

        return ValidationReportV2(
            run_id=self.run_id,
            source_connector=self.source_connector,
            target_connector=self.target_connector,
            generated_at=datetime.now(timezone.utc).isoformat(),
            data_parity=self._data_parity,
            type_parity=self._type_parity,
            constraint_parity=self._constraint_parity,
            limitations=self._limitations,
            tables_checked=tables_checked,
            tables_matched=tables_matched,
            total_source_rows=total_source,
            total_target_rows=total_target,
            lossy_mappings_count=lossy_count,
            constraint_mismatches=constraint_mismatches
        )

    def save_report(
        self,
        output_dir: str,
        report: Optional[ValidationReportV2] = None
    ) -> Dict[str, str]:
        """
        Save validation report to files.

        Args:
            output_dir: Directory to save reports
            report: Report to save (generates if None)

        Returns:
            Dict with paths to generated files
        """
        if report is None:
            report = self.generate()

        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        paths = {}

        # Save JSON report
        json_path = output_path / "validation_summary.json"
        with open(json_path, 'w') as f:
            json.dump(report.to_dict(), f, indent=2, sort_keys=True)
        paths["validation_summary_json"] = str(json_path)

        # Save text report
        txt_path = output_path / "validation_report.txt"
        with open(txt_path, 'w') as f:
            f.write(report.to_text())
        paths["validation_report_txt"] = str(txt_path)

        # Save limitations JSON
        lim_json_path = output_path / "limitations.json"
        with open(lim_json_path, 'w') as f:
            json.dump(report.limitations.to_dict(), f, indent=2, sort_keys=True)
        paths["limitations_json"] = str(lim_json_path)

        # Save limitations text
        lim_txt_path = output_path / "limitations.txt"
        with open(lim_txt_path, 'w') as f:
            f.write(report.limitations.to_text())
        paths["limitations_txt"] = str(lim_txt_path)

        logger.info(f"Saved validation report to {output_dir}")
        return paths


def compare_adapters(
    source_adapter: Any,
    target_adapter: Any,
    run_id: str,
    tables: Optional[List[str]] = None,
    check_constraints: bool = True,
    fingerprint_config: Optional[FingerprintConfig] = None
) -> ValidationReportV2:
    """
    Compare source and target adapters and generate validation report.

    Args:
        source_adapter: Source database adapter
        target_adapter: Target database adapter
        run_id: Unique run identifier
        tables: Tables to compare (None = all common tables)
        check_constraints: Whether to check L1 constraint parity
        fingerprint_config: Fingerprint configuration

    Returns:
        ValidationReportV2
    """
    source_name = source_adapter.__class__.__name__
    target_name = target_adapter.__class__.__name__

    generator = ValidationReportGenerator(
        run_id=run_id,
        source_connector=source_name,
        target_connector=target_name,
        fingerprint_config=fingerprint_config
    )

    fingerprint_calc = FingerprintCalculator(fingerprint_config)

    # Get tables to compare
    if tables is None:
        source_tables = set(source_adapter.get_tables())
        target_tables = set(target_adapter.get_tables())
        tables = sorted(source_tables & target_tables)

        # Log tables that exist only on one side
        source_only = source_tables - target_tables
        target_only = target_tables - source_tables

        for t in source_only:
            generator.add_limitation(
                "behavior_difference", "table", t,
                "Table exists only in source", "warning"
            )
        for t in target_only:
            generator.add_limitation(
                "behavior_difference", "table", t,
                "Table exists only in target", "warning"
            )

    for table_name in tables:
        try:
            # Data parity
            source_data = source_adapter.extract_data(table_name)
            target_data = target_adapter.extract_data(table_name)

            source_rows = source_data.get('data', [])
            target_rows = target_data.get('data', [])

            # Compute fingerprints
            source_schema = source_adapter.get_schema(table_name)
            source_cols = [c['name'] for c in source_schema.get('columns', [])]

            source_fp = fingerprint_calc.compute_table_fingerprint(
                table_name, source_rows, source_cols
            )

            target_schema = target_adapter.get_schema(table_name)
            target_cols = [c['name'] for c in target_schema.get('columns', [])]

            target_fp = fingerprint_calc.compute_table_fingerprint(
                table_name, target_rows, target_cols
            )

            generator.add_data_parity(
                table_name,
                len(source_rows),
                len(target_rows),
                source_fp.fingerprint,
                target_fp.fingerprint
            )

            # Type parity
            type_mappings = []
            for src_col in source_schema.get('columns', []):
                col_name = src_col['name']
                src_type = src_col.get('type', 'unknown')
                ir_type = str(src_col.get('type_info', {}).get('ir_type', 'UNKNOWN'))

                # Find matching target column
                tgt_col = next(
                    (c for c in target_schema.get('columns', []) if c['name'] == col_name),
                    None
                )
                tgt_type = tgt_col.get('type', 'unknown') if tgt_col else 'MISSING'

                # Check for lossy mapping
                is_lossy = False
                lossy_reason = None

                if tgt_col is None:
                    is_lossy = True
                    lossy_reason = "Column missing in target"
                    generator.add_limitation(
                        "lossy_mapping", "column", f"{table_name}.{col_name}",
                        f"Column missing in target", "error"
                    )

                type_mappings.append({
                    "column_name": col_name,
                    "source_type": src_type,
                    "ir_type": ir_type,
                    "target_type": tgt_type,
                    "is_lossy": is_lossy,
                    "lossy_reason": lossy_reason
                })

            generator.add_type_parity(table_name, type_mappings)

            # Constraint parity (L1)
            if check_constraints:
                source_constraints = _get_constraints(source_adapter, table_name)
                target_constraints = _get_constraints(target_adapter, table_name)
                generator.add_constraint_parity(
                    table_name, source_constraints, target_constraints
                )

        except Exception as e:
            logger.error(f"Failed to compare table {table_name}: {e}")
            generator.add_limitation(
                "unsupported_object", "table", table_name,
                f"Comparison failed: {str(e)}", "error"
            )

    return generator.generate()


def _get_constraints(adapter: Any, table_name: str) -> Dict[str, Any]:
    """Extract L1 constraints from an adapter if methods exist."""
    constraints = {}

    if hasattr(adapter, 'get_primary_keys'):
        try:
            constraints['primary_keys'] = adapter.get_primary_keys(table_name)
        except Exception:
            constraints['primary_keys'] = []

    if hasattr(adapter, 'get_unique_constraints'):
        try:
            constraints['unique_constraints'] = adapter.get_unique_constraints(table_name)
        except Exception:
            constraints['unique_constraints'] = []

    if hasattr(adapter, 'get_foreign_keys'):
        try:
            constraints['foreign_keys'] = adapter.get_foreign_keys(table_name)
        except Exception:
            constraints['foreign_keys'] = []

    if hasattr(adapter, 'get_indexes'):
        try:
            indexes = adapter.get_indexes(table_name)
            constraints['indexes'] = [idx.get('name', str(idx)) for idx in indexes]
        except Exception:
            constraints['indexes'] = []

    return constraints
