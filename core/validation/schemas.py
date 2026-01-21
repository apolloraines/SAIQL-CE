#!/usr/bin/env python3
"""
SAIQL Phase 12 - Bundle and Report Schema Definitions

Versioned schemas for artifact bundles and validation reports.
These versions form part of the contract - changes require version bumps.

Author: Apollo & Claude
Version: 1.0.0
"""

from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional
from enum import Enum
from datetime import datetime, timezone
import json


# Schema versions - bump these when structure changes
BUNDLE_SCHEMA_VERSION = "1.0.0"
REPORT_SCHEMA_VERSION = "1.0.0"
LIMITATIONS_SCHEMA_VERSION = "1.0.0"


class ParityStatus(Enum):
    """Parity check outcomes."""
    MATCH = "match"
    MISMATCH = "mismatch"
    SOURCE_ONLY = "source_only"
    TARGET_ONLY = "target_only"
    NOT_APPLICABLE = "not_applicable"
    NOT_CHECKED = "not_checked"


@dataclass
class TableFingerprint:
    """Per-table fingerprint for data parity validation."""
    table_name: str
    row_count: int
    fingerprint: str  # SHA256 of deterministic row data
    column_count: int
    null_counts: Dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "table_name": self.table_name,
            "row_count": self.row_count,
            "fingerprint": self.fingerprint,
            "column_count": self.column_count,
            "null_counts": self.null_counts
        }


@dataclass
class DatasetFingerprint:
    """Aggregate fingerprint for entire dataset."""
    tables: List[TableFingerprint]
    total_rows: int
    total_tables: int
    combined_fingerprint: str  # SHA256 of all table fingerprints combined
    generated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": BUNDLE_SCHEMA_VERSION,
            "total_tables": self.total_tables,
            "total_rows": self.total_rows,
            "combined_fingerprint": self.combined_fingerprint,
            "generated_at": self.generated_at,
            "tables": [t.to_dict() for t in self.tables]
        }


@dataclass
class TypeMapping:
    """Single type mapping entry."""
    column_name: str
    source_type: str
    ir_type: str
    target_type: str
    is_lossy: bool = False
    lossy_reason: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        result = {
            "column_name": self.column_name,
            "source_type": self.source_type,
            "ir_type": self.ir_type,
            "target_type": self.target_type,
            "is_lossy": self.is_lossy
        }
        if self.lossy_reason:
            result["lossy_reason"] = self.lossy_reason
        return result


@dataclass
class TableTypeParity:
    """Type parity for a single table."""
    table_name: str
    mappings: List[TypeMapping]
    lossy_count: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "table_name": self.table_name,
            "mappings": [m.to_dict() for m in self.mappings],
            "lossy_count": self.lossy_count
        }


@dataclass
class ConstraintParity:
    """L1 constraint parity for a table."""
    table_name: str
    pk_source: List[str]
    pk_target: List[str]
    pk_status: ParityStatus
    unique_source: List[str]
    unique_target: List[str]
    unique_status: ParityStatus
    fk_source: List[Dict[str, str]]
    fk_target: List[Dict[str, str]]
    fk_status: ParityStatus
    index_source: List[str]
    index_target: List[str]
    index_status: ParityStatus
    identity_source: Optional[str] = None
    identity_target: Optional[str] = None
    identity_status: ParityStatus = ParityStatus.NOT_CHECKED

    def to_dict(self) -> Dict[str, Any]:
        return {
            "table_name": self.table_name,
            "primary_key": {
                "source": self.pk_source,
                "target": self.pk_target,
                "status": self.pk_status.value
            },
            "unique_constraints": {
                "source": self.unique_source,
                "target": self.unique_target,
                "status": self.unique_status.value
            },
            "foreign_keys": {
                "source": self.fk_source,
                "target": self.fk_target,
                "status": self.fk_status.value
            },
            "indexes": {
                "source": self.index_source,
                "target": self.index_target,
                "status": self.index_status.value
            },
            "identity": {
                "source": self.identity_source,
                "target": self.identity_target,
                "status": self.identity_status.value
            }
        }


@dataclass
class Limitation:
    """Single limitation entry."""
    category: str  # unsupported_object, lossy_mapping, behavior_difference, manual_step
    object_type: str
    object_name: str
    description: str
    severity: str  # info, warning, error

    def to_dict(self) -> Dict[str, Any]:
        return {
            "category": self.category,
            "object_type": self.object_type,
            "object_name": self.object_name,
            "description": self.description,
            "severity": self.severity
        }

    def __lt__(self, other):
        """Deterministic ordering: category, object_type, object_name."""
        return (self.category, self.object_type, self.object_name) < \
               (other.category, other.object_type, other.object_name)


@dataclass
class LimitationsReport:
    """Structured limitations report."""
    unsupported_objects: List[Limitation]
    lossy_mappings: List[Limitation]
    behavior_differences: List[Limitation]
    manual_steps: List[Limitation]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": LIMITATIONS_SCHEMA_VERSION,
            "unsupported_objects": sorted([l.to_dict() for l in self.unsupported_objects],
                                          key=lambda x: (x["object_type"], x["object_name"])),
            "lossy_mappings": sorted([l.to_dict() for l in self.lossy_mappings],
                                     key=lambda x: (x["object_type"], x["object_name"])),
            "behavior_differences": sorted([l.to_dict() for l in self.behavior_differences],
                                           key=lambda x: (x["object_type"], x["object_name"])),
            "manual_steps": sorted([l.to_dict() for l in self.manual_steps],
                                   key=lambda x: (x["object_type"], x["object_name"]))
        }

    def to_text(self) -> str:
        """Human-readable limitations report."""
        lines = [
            "=" * 60,
            "SAIQL LIMITATIONS REPORT",
            f"Schema Version: {LIMITATIONS_SCHEMA_VERSION}",
            "=" * 60,
            ""
        ]

        if self.unsupported_objects:
            lines.append("## UNSUPPORTED OBJECTS")
            lines.append("-" * 40)
            for lim in sorted(self.unsupported_objects):
                lines.append(f"  [{lim.severity.upper()}] {lim.object_type}: {lim.object_name}")
                lines.append(f"    {lim.description}")
            lines.append("")

        if self.lossy_mappings:
            lines.append("## LOSSY MAPPINGS")
            lines.append("-" * 40)
            for lim in sorted(self.lossy_mappings):
                lines.append(f"  [{lim.severity.upper()}] {lim.object_type}: {lim.object_name}")
                lines.append(f"    {lim.description}")
            lines.append("")

        if self.behavior_differences:
            lines.append("## BEHAVIOR DIFFERENCES")
            lines.append("-" * 40)
            for lim in sorted(self.behavior_differences):
                lines.append(f"  [{lim.severity.upper()}] {lim.object_type}: {lim.object_name}")
                lines.append(f"    {lim.description}")
            lines.append("")

        if self.manual_steps:
            lines.append("## REQUIRED MANUAL STEPS")
            lines.append("-" * 40)
            for lim in sorted(self.manual_steps):
                lines.append(f"  [ ] {lim.object_type}: {lim.object_name}")
                lines.append(f"      {lim.description}")
            lines.append("")

        if not any([self.unsupported_objects, self.lossy_mappings,
                    self.behavior_differences, self.manual_steps]):
            lines.append("No limitations detected.")

        return "\n".join(lines)


@dataclass
class ValidationReportV2:
    """Phase 12 Validation Report structure."""
    run_id: str
    source_connector: str
    target_connector: str
    generated_at: str

    # Data parity
    data_parity: Dict[str, Dict[str, Any]]  # table -> {source_rows, target_rows, status, fingerprints}

    # Type parity
    type_parity: List[TableTypeParity]

    # Constraint parity (L1)
    constraint_parity: List[ConstraintParity]

    # Limitations
    limitations: LimitationsReport

    # Summary
    tables_checked: int = 0
    tables_matched: int = 0
    total_source_rows: int = 0
    total_target_rows: int = 0
    lossy_mappings_count: int = 0
    constraint_mismatches: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": REPORT_SCHEMA_VERSION,
            "run_id": self.run_id,
            "source_connector": self.source_connector,
            "target_connector": self.target_connector,
            "generated_at": self.generated_at,
            "summary": {
                "tables_checked": self.tables_checked,
                "tables_matched": self.tables_matched,
                "total_source_rows": self.total_source_rows,
                "total_target_rows": self.total_target_rows,
                "lossy_mappings_count": self.lossy_mappings_count,
                "constraint_mismatches": self.constraint_mismatches
            },
            "data_parity": self.data_parity,
            "type_parity": [t.to_dict() for t in self.type_parity],
            "constraint_parity": [c.to_dict() for c in self.constraint_parity],
            "limitations": self.limitations.to_dict()
        }

    def to_text(self) -> str:
        """Human-readable validation report."""
        lines = [
            "=" * 70,
            "SAIQL VALIDATION REPORT v2",
            f"Schema Version: {REPORT_SCHEMA_VERSION}",
            "=" * 70,
            "",
            f"Run ID: {self.run_id}",
            f"Source: {self.source_connector}",
            f"Target: {self.target_connector}",
            f"Generated: {self.generated_at}",
            "",
            "=" * 70,
            "SUMMARY",
            "=" * 70,
            f"  Tables Checked: {self.tables_checked}",
            f"  Tables Matched: {self.tables_matched}",
            f"  Source Rows: {self.total_source_rows}",
            f"  Target Rows: {self.total_target_rows}",
            f"  Lossy Mappings: {self.lossy_mappings_count}",
            f"  Constraint Mismatches: {self.constraint_mismatches}",
            "",
            "=" * 70,
            "DATA PARITY",
            "=" * 70
        ]

        for table, parity in sorted(self.data_parity.items()):
            status = parity.get("status", "unknown")
            src = parity.get("source_rows", 0)
            tgt = parity.get("target_rows", 0)
            status_icon = "✓" if status == "match" else "✗"
            lines.append(f"  {status_icon} {table}: {src} -> {tgt} [{status}]")

        lines.extend(["", "=" * 70, "TYPE PARITY", "=" * 70])

        for table_parity in self.type_parity:
            lines.append(f"\n  Table: {table_parity.table_name}")
            lines.append(f"  Lossy mappings: {table_parity.lossy_count}")
            for mapping in table_parity.mappings:
                if mapping.is_lossy:
                    lines.append(f"    ! {mapping.column_name}: {mapping.source_type} -> {mapping.ir_type} -> {mapping.target_type}")
                    if mapping.lossy_reason:
                        lines.append(f"      Reason: {mapping.lossy_reason}")

        if self.constraint_parity:
            lines.extend(["", "=" * 70, "CONSTRAINT PARITY (L1)", "=" * 70])
            for cp in self.constraint_parity:
                lines.append(f"\n  Table: {cp.table_name}")
                lines.append(f"    PK: {cp.pk_status.value}")
                lines.append(f"    Unique: {cp.unique_status.value}")
                lines.append(f"    FK: {cp.fk_status.value}")
                lines.append(f"    Indexes: {cp.index_status.value}")

        lines.extend(["", "=" * 70, "LIMITATIONS", "=" * 70])
        lines.append(self.limitations.to_text())

        return "\n".join(lines)


@dataclass
class BundleManifest:
    """Artifact bundle manifest with integrity checksums."""
    bundle_version: str = BUNDLE_SCHEMA_VERSION
    run_id: str = ""
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    source_connector: str = ""
    target_connector: str = ""

    # File checksums (SHA256)
    file_checksums: Dict[str, str] = field(default_factory=dict)

    # Dataset fingerprint
    dataset_fingerprint: Optional[str] = None

    # Report digests
    validation_report_digest: Optional[str] = None
    limitations_report_digest: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "bundle_version": self.bundle_version,
            "run_id": self.run_id,
            "created_at": self.created_at,
            "source_connector": self.source_connector,
            "target_connector": self.target_connector,
            "file_checksums": self.file_checksums,
            "dataset_fingerprint": self.dataset_fingerprint,
            "validation_report_digest": self.validation_report_digest,
            "limitations_report_digest": self.limitations_report_digest
        }

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, sort_keys=True)
