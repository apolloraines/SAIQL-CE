#!/usr/bin/env python3
"""
SAIQL Phase 12 - Fingerprint Functions

Deterministic fingerprinting for data validation:
- Per-table fingerprints based on row data
- Dataset fingerprints aggregating table fingerprints
- Configurable for performance (sampling, column exclusions)

Author: Apollo & Claude
Version: 1.0.0

Fingerprint Method Documentation:
---------------------------------
1. Per-table fingerprint:
   - Rows are sorted by first column (or specified order_by)
   - Each row is serialized as JSON with sorted keys
   - Row hashes are computed via SHA256
   - All row hashes are concatenated and re-hashed
   - Result: SHA256 hex digest (64 chars)

2. Dataset fingerprint:
   - Table fingerprints are sorted alphabetically by table name
   - Fingerprints are concatenated with table names
   - Result is hashed via SHA256
"""

import hashlib
import json
import logging
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field

from .schemas import TableFingerprint, DatasetFingerprint

logger = logging.getLogger(__name__)


@dataclass
class FingerprintConfig:
    """Configuration for fingerprint computation."""
    # Performance controls
    sample_size: Optional[int] = None  # None = all rows, int = max rows to sample
    excluded_columns: List[str] = field(default_factory=list)  # Columns to skip

    # Ordering
    order_by: Optional[List[str]] = None  # Columns for deterministic ordering

    # Hash algorithm
    algorithm: str = "sha256"

    # Null handling
    null_representation: str = "__NULL__"


class FingerprintCalculator:
    """Calculates deterministic fingerprints for tables and datasets."""

    def __init__(self, config: Optional[FingerprintConfig] = None):
        self.config = config or FingerprintConfig()

    def _get_hasher(self):
        """Get hash function based on config."""
        if self.config.algorithm == "sha256":
            return hashlib.sha256
        elif self.config.algorithm == "md5":
            return hashlib.md5
        else:
            return hashlib.sha256

    def _serialize_value(self, value: Any) -> str:
        """Serialize a single value to string deterministically."""
        if value is None:
            return self.config.null_representation
        elif isinstance(value, bytes):
            return value.hex()
        elif isinstance(value, (dict, list)):
            return json.dumps(value, sort_keys=True, default=str)
        else:
            return str(value)

    def _serialize_row(self, row: Dict[str, Any], columns: List[str]) -> str:
        """Serialize a row to a deterministic string."""
        # Use only specified columns, in order
        filtered = {}
        for col in columns:
            if col not in self.config.excluded_columns:
                filtered[col] = self._serialize_value(row.get(col))

        # Sort by key for determinism
        return json.dumps(filtered, sort_keys=True)

    def _hash_string(self, data: str) -> str:
        """Hash a string and return hex digest."""
        hasher = self._get_hasher()
        return hasher(data.encode('utf-8')).hexdigest()

    def compute_table_fingerprint(
        self,
        table_name: str,
        rows: List[Dict[str, Any]],
        columns: Optional[List[str]] = None
    ) -> TableFingerprint:
        """
        Compute fingerprint for a table's data.

        Args:
            table_name: Name of the table
            rows: List of row dictionaries
            columns: Column names in order (if None, derived from first row)

        Returns:
            TableFingerprint with hash and statistics
        """
        if not rows:
            # Empty table fingerprint
            return TableFingerprint(
                table_name=table_name,
                row_count=0,
                fingerprint=self._hash_string(""),
                column_count=len(columns) if columns else 0,
                null_counts={}
            )

        # Derive columns from first row if not specified
        if columns is None:
            columns = sorted(rows[0].keys())

        # Determine sort columns for deterministic ordering
        if self.config.order_by:
            sort_cols = self.config.order_by
        else:
            # Default: sort by ALL non-excluded columns to ensure deterministic ordering
            # even when rows have duplicate values in the first column
            sort_cols = [c for c in columns if c not in self.config.excluded_columns]

        # Sort rows BEFORE sampling for deterministic fingerprints
        # (sampling unordered rows yields nondeterministic results)
        sorted_rows = rows
        if sort_cols:
            try:
                sorted_rows = sorted(
                    rows,
                    key=lambda r: tuple(
                        (r.get(c) is None, str(r.get(c, "")))
                        for c in sort_cols
                    )
                )
            except Exception as e:
                logger.warning(f"Could not sort rows for {table_name}: {e}")

        # Apply sample size limit AFTER sorting
        sample_rows = sorted_rows
        if self.config.sample_size and len(sorted_rows) > self.config.sample_size:
            sample_rows = sorted_rows[:self.config.sample_size]
            logger.debug(f"Sampling {self.config.sample_size} of {len(rows)} rows for {table_name}")

        # Compute null counts
        null_counts = {col: 0 for col in columns if col not in self.config.excluded_columns}
        for row in sample_rows:
            for col in null_counts:
                if row.get(col) is None:
                    null_counts[col] += 1

        # Hash each row
        row_hashes = []
        for row in sample_rows:
            row_str = self._serialize_row(row, columns)
            row_hashes.append(self._hash_string(row_str))

        # Combine all row hashes
        combined = "".join(row_hashes)
        fingerprint = self._hash_string(combined)

        return TableFingerprint(
            table_name=table_name,
            row_count=len(rows),  # Full count, not sample
            fingerprint=fingerprint,
            column_count=len([c for c in columns if c not in self.config.excluded_columns]),
            null_counts=null_counts
        )

    def compute_dataset_fingerprint(
        self,
        table_fingerprints: List[TableFingerprint]
    ) -> DatasetFingerprint:
        """
        Compute aggregate fingerprint for entire dataset.

        Args:
            table_fingerprints: List of per-table fingerprints

        Returns:
            DatasetFingerprint with combined hash
        """
        # Sort tables alphabetically for determinism
        sorted_tables = sorted(table_fingerprints, key=lambda t: t.table_name)

        # Combine: table_name:fingerprint for each
        combined_parts = []
        total_rows = 0

        for tf in sorted_tables:
            combined_parts.append(f"{tf.table_name}:{tf.fingerprint}")
            total_rows += tf.row_count

        combined_str = "|".join(combined_parts)
        combined_fingerprint = self._hash_string(combined_str)

        return DatasetFingerprint(
            tables=sorted_tables,
            total_rows=total_rows,
            total_tables=len(sorted_tables),
            combined_fingerprint=combined_fingerprint
        )

    def compute_from_adapter(
        self,
        adapter: Any,
        tables: Optional[List[str]] = None
    ) -> DatasetFingerprint:
        """
        Compute dataset fingerprint directly from a database adapter.

        Args:
            adapter: Database adapter with get_tables(), get_schema(), extract_data()
            tables: Specific tables to fingerprint (None = all)

        Returns:
            DatasetFingerprint
        """
        if tables is None:
            tables = adapter.get_tables()

        table_fingerprints = []

        for table_name in tables:
            try:
                # Get schema for column ordering
                schema = adapter.get_schema(table_name)
                columns = [c['name'] for c in schema.get('columns', [])]

                # Extract data
                result = adapter.extract_data(table_name, order_by=self.config.order_by)
                rows = result.get('data', [])

                # Compute fingerprint
                fp = self.compute_table_fingerprint(table_name, rows, columns)
                table_fingerprints.append(fp)

                logger.debug(f"Fingerprint for {table_name}: {fp.fingerprint[:16]}... ({fp.row_count} rows)")

            except Exception as e:
                logger.error(f"Failed to fingerprint table {table_name}: {e}")
                # Add empty fingerprint to maintain determinism
                table_fingerprints.append(TableFingerprint(
                    table_name=table_name,
                    row_count=0,
                    fingerprint=self._hash_string(f"ERROR:{str(e)}"),
                    column_count=0,
                    null_counts={}
                ))

        return self.compute_dataset_fingerprint(table_fingerprints)


def compute_file_checksum(filepath: str, algorithm: str = "sha256") -> str:
    """
    Compute checksum of a file.

    Args:
        filepath: Path to file
        algorithm: Hash algorithm (sha256, md5)

    Returns:
        Hex digest of file content
    """
    if algorithm == "sha256":
        hasher = hashlib.sha256()
    elif algorithm == "md5":
        hasher = hashlib.md5()
    else:
        hasher = hashlib.sha256()

    with open(filepath, 'rb') as f:
        while chunk := f.read(8192):
            hasher.update(chunk)

    return hasher.hexdigest()
