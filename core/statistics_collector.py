#!/usr/bin/env python3
"""
SAIQL Statistics Collector
===========================

Collects and maintains table statistics for cost-based query optimization.

Statistics collected:
- Row counts
- Column cardinality (distinct values)
- Data distribution (histograms)
- Index availability
- Average row size

Author: Apollo & Claude
Version: 1.0.0
"""

import logging
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
from collections import defaultdict
import json
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class ColumnStatistics:
    """Statistics for a single column"""
    column_name: str
    distinct_count: int = 0
    null_count: int = 0
    min_value: Any = None
    max_value: Any = None
    avg_length: float = 0.0
    most_common_values: List[Any] = field(default_factory=list)
    histogram: Dict[Any, int] = field(default_factory=dict)


@dataclass
class TableStatistics:
    """Statistics for a table"""
    table_name: str
    row_count: int = 0
    total_size_bytes: int = 0
    avg_row_size_bytes: float = 0.0
    columns: Dict[str, ColumnStatistics] = field(default_factory=dict)
    indexes: List[str] = field(default_factory=list)
    last_updated: str = ""
    
    def get_selectivity(self, column: str, value: Any) -> float:
        """
        Estimate selectivity of a condition (fraction of rows that match)
        
        Returns value between 0.0 and 1.0
        """
        if column not in self.columns:
            return 0.1  # Default guess
        
        col_stats = self.columns[column]
        
        if col_stats.distinct_count == 0:
            return 0.1
        
        # Simple selectivity: 1 / distinct_count
        # More sophisticated would use histograms
        return 1.0 / col_stats.distinct_count
    
    def estimate_join_size(self, other: 'TableStatistics', join_column: str) -> int:
        """
        Estimate the size of a join result
        
        Uses the formula: |R ⋈ S| ≈ (|R| * |S|) / max(distinct(R.key), distinct(S.key))
        """
        if self.row_count == 0 or other.row_count == 0:
            return 0
        
        # Get distinct counts for join column
        self_distinct = 1
        other_distinct = 1
        
        if join_column in self.columns:
            self_distinct = max(1, self.columns[join_column].distinct_count)
        
        if join_column in other.columns:
            other_distinct = max(1, other.columns[join_column].distinct_count)
        
        # Join size estimation
        max_distinct = max(self_distinct, other_distinct)
        estimated_size = (self.row_count * other.row_count) // max_distinct
        
        return estimated_size


class StatisticsCollector:
    """
    Collects and maintains statistics for query optimization
    """
    
    def __init__(self, storage_path: Optional[str] = None):
        self.storage_path = Path(storage_path) if storage_path else Path("stats")
        self.storage_path.mkdir(parents=True, exist_ok=True)
        
        self.table_stats: Dict[str, TableStatistics] = {}
        self._load_statistics()
    
    def collect_statistics(self, table_name: str, data: List[Dict[str, Any]]) -> TableStatistics:
        """
        Collect statistics from a dataset
        
        Args:
            table_name: Name of the table
            data: List of row dictionaries
            
        Returns:
            TableStatistics object
        """
        if not data:
            return TableStatistics(table_name=table_name)
        
        stats = TableStatistics(table_name=table_name)
        stats.row_count = len(data)
        
        # Calculate total size (approximate)
        total_size = sum(len(json.dumps(row)) for row in data[:100])  # Sample first 100
        stats.total_size_bytes = (total_size // min(100, len(data))) * len(data)
        stats.avg_row_size_bytes = stats.total_size_bytes / max(1, stats.row_count)
        
        # Collect column statistics
        if data:
            columns = data[0].keys()
            for column in columns:
                col_stats = self._collect_column_stats(column, data)
                stats.columns[column] = col_stats
        
        # Store timestamp
        from datetime import datetime
        stats.last_updated = datetime.now().isoformat()
        
        # Cache statistics
        self.table_stats[table_name] = stats
        self._save_statistics()
        
        logger.info(f"Collected statistics for {table_name}: {stats.row_count} rows, "
                   f"{len(stats.columns)} columns")
        
        return stats
    
    def _collect_column_stats(self, column_name: str, data: List[Dict[str, Any]]) -> ColumnStatistics:
        """Collect statistics for a single column"""
        stats = ColumnStatistics(column_name=column_name)

        values = []
        null_count = 0
        total_length = 0
        string_count = 0

        for row in data:
            value = row.get(column_name)
            if value is None:
                null_count += 1
            else:
                values.append(value)
                if isinstance(value, str):
                    total_length += len(value)
                    string_count += 1

        stats.null_count = null_count

        if values:
            # Distinct count - skip if values contain unhashable types (dict/list)
            try:
                unique_values = set(values)
                stats.distinct_count = len(unique_values)
            except TypeError:
                # Unhashable values (dict/list), estimate distinct count from sample
                stats.distinct_count = len(values)  # Upper bound estimate

            # Min/max
            try:
                stats.min_value = min(values)
                stats.max_value = max(values)
            except TypeError:
                # Values not comparable
                pass

            # Average length for strings (divide by string count, not all values)
            if string_count > 0:
                stats.avg_length = total_length / string_count

            # Most common values (top 10) - skip if values contain unhashable types
            try:
                value_counts = defaultdict(int)
                for v in values:
                    value_counts[v] += 1

                sorted_values = sorted(value_counts.items(), key=lambda x: x[1], reverse=True)
                stats.most_common_values = [v for v, _ in sorted_values[:10]]
            except TypeError:
                # Unhashable values, skip most common calculation
                pass
            
            # Simple histogram (for numeric values)
            if all(isinstance(v, (int, float)) for v in values[:100]):
                # Create 10 buckets
                min_val = stats.min_value
                max_val = stats.max_value
                if min_val != max_val:
                    bucket_size = (max_val - min_val) / 10
                    for v in values:
                        bucket = int((v - min_val) / bucket_size)
                        bucket = min(9, bucket)  # Cap at bucket 9
                        stats.histogram[bucket] = stats.histogram.get(bucket, 0) + 1
        
        return stats
    
    def get_statistics(self, table_name: str) -> Optional[TableStatistics]:
        """Get cached statistics for a table"""
        return self.table_stats.get(table_name)
    
    def estimate_join_cost(
        self,
        left_table: str,
        right_table: str,
        join_column: str,
        algorithm: str = "hash"
    ) -> Dict[str, Any]:
        """
        Estimate the cost of a join operation
        
        Returns:
            Dictionary with cost estimates (CPU, I/O, memory)
        """
        left_stats = self.get_statistics(left_table)
        right_stats = self.get_statistics(right_table)
        
        if not left_stats or not right_stats:
            # No statistics available, return default estimates
            return {
                "cpu_cost": 1000,
                "io_cost": 1000,
                "memory_cost": 1000,
                "estimated_rows": 1000,
                "confidence": "low"
            }
        
        # Estimate result size
        estimated_rows = left_stats.estimate_join_size(right_stats, join_column)
        
        # Calculate costs based on algorithm
        if algorithm == "hash":
            # Hash join: O(n + m)
            build_cost = left_stats.row_count * 2  # Build hash table
            probe_cost = right_stats.row_count * 1  # Probe
            cpu_cost = build_cost + probe_cost
            memory_cost = left_stats.total_size_bytes  # Hash table in memory
            io_cost = left_stats.row_count + right_stats.row_count  # Read both tables
            
        elif algorithm == "merge":
            # Merge join: O(n log n + m log m) if sorting needed
            sort_cost_left = left_stats.row_count * 2  # Simplified
            sort_cost_right = right_stats.row_count * 2
            merge_cost = left_stats.row_count + right_stats.row_count
            cpu_cost = sort_cost_left + sort_cost_right + merge_cost
            memory_cost = max(left_stats.total_size_bytes, right_stats.total_size_bytes)
            io_cost = left_stats.row_count + right_stats.row_count
            
        else:  # nested_loop
            # Nested loop: O(n * m)
            cpu_cost = left_stats.row_count * right_stats.row_count
            memory_cost = left_stats.avg_row_size_bytes  # Minimal memory
            io_cost = left_stats.row_count * right_stats.row_count
        
        return {
            "cpu_cost": cpu_cost,
            "io_cost": io_cost,
            "memory_cost": memory_cost,
            "estimated_rows": estimated_rows,
            "confidence": "high" if left_stats.row_count > 0 else "medium"
        }
    
    @staticmethod
    def _serialize_value(value: Any) -> Any:
        """Serialize a value for JSON storage with type tag to preserve exact type."""
        if value is None:
            return None
        # Store type tag to avoid ambiguity (e.g., string "00123" vs int 123)
        if isinstance(value, bool):  # Check bool before int (bool is subclass of int)
            return {"type": "bool", "value": value}
        if isinstance(value, int):
            return {"type": "int", "value": value}
        if isinstance(value, float):
            return {"type": "float", "value": value}
        if isinstance(value, str):
            return {"type": "str", "value": value}
        # Non-JSON-serializable types converted to string
        return {"type": "str", "value": str(value)}

    @staticmethod
    def _deserialize_value(value: Any) -> Any:
        """Deserialize a value from JSON storage using type tag."""
        if value is None:
            return None
        # Handle new format with type tag
        if isinstance(value, dict) and "type" in value and "value" in value:
            val = value["value"]
            typ = value["type"]
            if typ == "int":
                return int(val)
            if typ == "float":
                return float(val)
            if typ == "bool":
                return bool(val)
            return val  # str or unknown type
        # Legacy format: return as-is (already JSON-native type)
        return value

    def _save_statistics(self):
        """Save statistics to disk"""
        try:
            stats_file = self.storage_path / "table_stats.json"
            
            # Convert to serializable format
            stats_dict = {}
            for table_name, stats in self.table_stats.items():
                stats_dict[table_name] = {
                    "table_name": stats.table_name,
                    "row_count": stats.row_count,
                    "total_size_bytes": stats.total_size_bytes,
                    "avg_row_size_bytes": stats.avg_row_size_bytes,
                    "last_updated": stats.last_updated,
                    "columns": {
                        col_name: {
                            "column_name": col_stats.column_name,
                            "distinct_count": col_stats.distinct_count,
                            "null_count": col_stats.null_count,
                            "min_value": self._serialize_value(col_stats.min_value),
                            "max_value": self._serialize_value(col_stats.max_value),
                            "avg_length": col_stats.avg_length
                        }
                        for col_name, col_stats in stats.columns.items()
                    }
                }
            
            with open(stats_file, 'w') as f:
                json.dump(stats_dict, f, indent=2)
                
        except Exception as e:
            logger.warning(f"Failed to save statistics: {e}")
    
    def _load_statistics(self):
        """Load statistics from disk"""
        try:
            stats_file = self.storage_path / "table_stats.json"
            if not stats_file.exists():
                return
            
            with open(stats_file, 'r') as f:
                stats_dict = json.load(f)
            
            # Reconstruct statistics objects
            for table_name, table_data in stats_dict.items():
                stats = TableStatistics(
                    table_name=table_data["table_name"],
                    row_count=table_data["row_count"],
                    total_size_bytes=table_data["total_size_bytes"],
                    avg_row_size_bytes=table_data["avg_row_size_bytes"],
                    last_updated=table_data["last_updated"]
                )
                
                for col_name, col_data in table_data.get("columns", {}).items():
                    col_stats = ColumnStatistics(
                        column_name=col_data["column_name"],
                        distinct_count=col_data["distinct_count"],
                        null_count=col_data["null_count"],
                        min_value=self._deserialize_value(col_data.get("min_value")),
                        max_value=self._deserialize_value(col_data.get("max_value")),
                        avg_length=col_data["avg_length"]
                    )
                    stats.columns[col_name] = col_stats
                
                self.table_stats[table_name] = stats
            
            logger.info(f"Loaded statistics for {len(self.table_stats)} tables")
            
        except Exception as e:
            logger.warning(f"Failed to load statistics: {e}")


# Example usage
if __name__ == "__main__":
    # Test data
    users = [
        {"id": i, "name": f"User{i}", "age": 20 + (i % 50)}
        for i in range(1000)
    ]
    
    orders = [
        {"order_id": i, "user_id": i % 800, "amount": 100 + (i % 500)}
        for i in range(5000)
    ]
    
    # Collect statistics
    collector = StatisticsCollector()
    
    user_stats = collector.collect_statistics("users", users)
    order_stats = collector.collect_statistics("orders", orders)
    
    print(f"User Statistics:")
    print(f"  Rows: {user_stats.row_count}")
    print(f"  Columns: {len(user_stats.columns)}")
    print(f"  ID distinct: {user_stats.columns['id'].distinct_count}")
    
    print(f"\nOrder Statistics:")
    print(f"  Rows: {order_stats.row_count}")
    print(f"  user_id distinct: {order_stats.columns['user_id'].distinct_count}")
    
    # Estimate join
    cost = collector.estimate_join_cost("users", "orders", "id", "hash")
    print(f"\nJoin Cost Estimate (hash):")
    print(f"  CPU: {cost['cpu_cost']}")
    print(f"  Estimated rows: {cost['estimated_rows']}")
