#!/usr/bin/env python3
"""
SAIQL Join Engine - Advanced Join Algorithms
==============================================

Implements high-performance join algorithms for SAIQL:
- Hash Join: O(n+m) complexity for equi-joins
- Merge Join: Efficient for sorted data
- Nested Loop Join: Fallback for non-equi joins

Author: Apollo & Claude
Version: 1.0.0
Status: Production-Ready
"""

import logging
from typing import List, Dict, Any, Callable, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
from collections import defaultdict

logger = logging.getLogger(__name__)


class JoinType(Enum):
    """Supported join types"""
    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    FULL = "FULL"
    CROSS = "CROSS"


class JoinAlgorithm(Enum):
    """Join execution algorithms"""
    HASH = "hash"
    MERGE = "merge"
    NESTED_LOOP = "nested_loop"


@dataclass
class JoinStatistics:
    """Statistics collected during join execution"""
    algorithm_used: JoinAlgorithm
    build_time_ms: float
    probe_time_ms: float
    total_time_ms: float
    left_rows: int
    right_rows: int
    output_rows: int
    hash_table_size: int = 0
    collisions: int = 0


class HashJoinExecutor:
    """
    Hash Join Implementation
    
    Implements the classic hash join algorithm:
    1. Build Phase: Create hash table from smaller relation
    2. Probe Phase: Scan larger relation and find matches
    
    Time Complexity: O(n + m) average case
    Space Complexity: O(min(n, m)) for hash table
    """
    
    def __init__(self, join_type: JoinType = JoinType.INNER):
        self.join_type = join_type
        self.stats = None
        
    def execute(
        self,
        left_data: List[Dict[str, Any]],
        right_data: List[Dict[str, Any]],
        left_key: str,
        right_key: str,
        condition: Optional[Callable[[Dict, Dict], bool]] = None
    ) -> Tuple[List[Dict[str, Any]], JoinStatistics]:
        """
        Execute hash join
        
        Args:
            left_data: Left table rows
            right_data: Right table rows
            left_key: Join key column in left table
            right_key: Join key column in right table
            condition: Optional additional join condition
            
        Returns:
            Tuple of (result rows, statistics)
        """
        import time
        
        start_time = time.time()
        
        # Determine build and probe sides (build on smaller relation)
        if len(left_data) <= len(right_data):
            build_data, probe_data = left_data, right_data
            build_key, probe_key = left_key, right_key
            build_is_left = True
        else:
            build_data, probe_data = right_data, left_data
            build_key, probe_key = right_key, left_key
            build_is_left = False
        
        # Build Phase: Create hash table
        build_start = time.time()
        hash_table, collisions, build_null_indices = self._build_hash_table(build_data, build_key)
        build_time = (time.time() - build_start) * 1000

        # Probe Phase: Find matches
        probe_start = time.time()
        results = self._probe_hash_table(
            hash_table, probe_data, probe_key,
            build_is_left, build_data, build_null_indices, condition
        )
        probe_time = (time.time() - probe_start) * 1000
        
        total_time = (time.time() - start_time) * 1000
        
        # Collect statistics
        self.stats = JoinStatistics(
            algorithm_used=JoinAlgorithm.HASH,
            build_time_ms=build_time,
            probe_time_ms=probe_time,
            total_time_ms=total_time,
            left_rows=len(left_data),
            right_rows=len(right_data),
            output_rows=len(results),
            hash_table_size=len(hash_table),
            collisions=collisions
        )
        
        logger.info(f"Hash join completed: {len(results)} rows in {total_time:.2f}ms")
        
        return results, self.stats
    
    def _build_hash_table(
        self,
        data: List[Dict[str, Any]],
        key_column: str
    ) -> Tuple[Dict[Any, List[Tuple[int, Dict[str, Any]]]], int, List[int]]:
        """
        Build hash table from data.

        Returns:
            Tuple of (hash_table, collision_count, null_key_indices)
            - hash_table maps key -> list of (row_index, row) tuples
            - null_key_indices are row indices with NULL keys (for outer joins)
        """
        hash_table = defaultdict(list)
        collisions = 0
        null_key_indices = []

        for idx, row in enumerate(data):
            key_value = row.get(key_column)

            # SQL semantics: NULL keys don't match anything (NULL != NULL)
            if key_value is None:
                null_key_indices.append(idx)
                continue

            # Track collisions (multiple rows with same key)
            if key_value in hash_table:
                collisions += 1

            # Store (index, row) to track individual row matches
            hash_table[key_value].append((idx, row))

        return dict(hash_table), collisions, null_key_indices
    
    def _probe_hash_table(
        self,
        hash_table: Dict[Any, List[Tuple[int, Dict[str, Any]]]],
        probe_data: List[Dict[str, Any]],
        probe_key: str,
        build_is_left: bool,
        build_data: List[Dict[str, Any]],
        build_null_indices: List[int],
        condition: Optional[Callable[[Dict, Dict], bool]] = None
    ) -> List[Dict[str, Any]]:
        """
        Probe hash table to find matches.

        Tracks matched rows by index (not by key) to correctly handle
        duplicate keys where some rows match the condition and others don't.
        NULL keys in probe data are treated as unmatched (SQL semantics).
        """
        results = []
        # Track which build rows have been matched BY INDEX (not by key)
        matched_build_indices = set()

        for probe_row in probe_data:
            key_value = probe_row.get(probe_key)
            probe_matched = False

            # SQL semantics: NULL keys don't match anything
            if key_value is None:
                # Treat as unmatched for outer joins
                pass
            elif key_value in hash_table:
                # Found potential matches - check each build row
                for build_idx, build_row in hash_table[key_value]:
                    # Apply additional condition if provided
                    if condition is None or condition(build_row, probe_row):
                        # Merge rows (left, right order matters)
                        if build_is_left:
                            merged = self._merge_rows(build_row, probe_row)
                        else:
                            merged = self._merge_rows(probe_row, build_row)
                        results.append(merged)
                        probe_matched = True
                        matched_build_indices.add(build_idx)

            # Handle unmatched probe rows for outer joins
            if not probe_matched:
                if self.join_type == JoinType.LEFT and not build_is_left:
                    # Left outer join, probe is left: include unmatched left rows
                    results.append(self._merge_rows(probe_row, {}))
                elif self.join_type == JoinType.RIGHT and build_is_left:
                    # Right outer join, probe is right: include unmatched right rows
                    results.append(self._merge_rows({}, probe_row))
                elif self.join_type == JoinType.FULL:
                    # Full outer join: include unmatched probe rows
                    if build_is_left:
                        results.append(self._merge_rows({}, probe_row))
                    else:
                        results.append(self._merge_rows(probe_row, {}))

        # Handle unmatched build rows for outer joins (including NULL key rows)
        if self.join_type == JoinType.LEFT and build_is_left:
            # Left outer join, build is left: emit unmatched left (build) rows
            for key_value, build_entries in hash_table.items():
                for build_idx, build_row in build_entries:
                    if build_idx not in matched_build_indices:
                        results.append(self._merge_rows(build_row, {}))
            # Also emit build rows with NULL keys (never matched)
            for null_idx in build_null_indices:
                results.append(self._merge_rows(build_data[null_idx], {}))
        elif self.join_type == JoinType.RIGHT and not build_is_left:
            # Right outer join, build is right: emit unmatched right (build) rows
            for key_value, build_entries in hash_table.items():
                for build_idx, build_row in build_entries:
                    if build_idx not in matched_build_indices:
                        results.append(self._merge_rows({}, build_row))
            # Also emit build rows with NULL keys
            for null_idx in build_null_indices:
                results.append(self._merge_rows({}, build_data[null_idx]))
        elif self.join_type == JoinType.FULL:
            # Full outer join: emit unmatched build rows
            for key_value, build_entries in hash_table.items():
                for build_idx, build_row in build_entries:
                    if build_idx not in matched_build_indices:
                        if build_is_left:
                            results.append(self._merge_rows(build_row, {}))
                        else:
                            results.append(self._merge_rows({}, build_row))
            # Also emit build rows with NULL keys
            for null_idx in build_null_indices:
                if build_is_left:
                    results.append(self._merge_rows(build_data[null_idx], {}))
                else:
                    results.append(self._merge_rows({}, build_data[null_idx]))

        return results
    
    def _merge_rows(
        self, 
        left_row: Dict[str, Any], 
        right_row: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Merge two rows, handling column name conflicts
        """
        merged = {}
        
        # Add left row columns
        for key, value in left_row.items():
            merged[f"left_{key}"] = value
        
        # Add right row columns
        for key, value in right_row.items():
            merged[f"right_{key}"] = value
        
        return merged


class MergeJoinExecutor:
    """
    Merge Join Implementation

    Efficient for pre-sorted data or when sort cost is low.
    Scans both sorted inputs in parallel.

    Time Complexity: O(n + m) if data is sorted, O(n log n + m log m) with sorting
    Space Complexity: O(1) for merge, O(n + m) if sorting needed
    """

    def __init__(self, join_type: JoinType = JoinType.INNER):
        self.join_type = join_type

    def _merge_rows(self, left_row: Dict[str, Any], right_row: Dict[str, Any]) -> Dict[str, Any]:
        """Merge two rows with prefixed column names (consistent with HashJoin)"""
        merged = {}
        for key, value in left_row.items():
            merged[f"left_{key}"] = value
        for key, value in right_row.items():
            merged[f"right_{key}"] = value
        return merged

    def execute(
        self,
        left_data: List[Dict[str, Any]],
        right_data: List[Dict[str, Any]],
        left_key: str,
        right_key: str,
        condition: Optional[Callable[[Dict, Dict], bool]] = None
    ) -> Tuple[List[Dict[str, Any]], JoinStatistics]:
        """
        Execute merge join

        Assumes data is already sorted by join keys.
        If not sorted, will sort first.
        """
        import time

        start_time = time.time()

        # Sort data if needed
        left_sorted = sorted(left_data, key=lambda x: x.get(left_key, ''))
        right_sorted = sorted(right_data, key=lambda x: x.get(right_key, ''))

        # Track matched rows for outer joins
        left_matched = set()
        right_matched = set()

        # Merge phase
        results = []
        left_idx, right_idx = 0, 0

        while left_idx < len(left_sorted) and right_idx < len(right_sorted):
            left_row = left_sorted[left_idx]
            right_row = right_sorted[right_idx]

            left_val = left_row.get(left_key)
            right_val = right_row.get(right_key)

            if left_val == right_val:
                # Match found - handle potential duplicates
                right_start = right_idx
                while right_idx < len(right_sorted) and right_sorted[right_idx].get(right_key) == left_val:
                    right_row = right_sorted[right_idx]
                    # Apply additional condition if provided
                    if condition is None or condition(left_row, right_row):
                        merged = self._merge_rows(left_row, right_row)
                        results.append(merged)
                        left_matched.add(left_idx)
                        right_matched.add(right_idx)
                    right_idx += 1

                left_idx += 1
                right_idx = right_start  # Reset for next left row

            elif left_val < right_val:
                left_idx += 1
            else:
                right_idx += 1

        # Handle outer joins - emit unmatched rows
        if self.join_type in (JoinType.LEFT, JoinType.FULL):
            for i, row in enumerate(left_sorted):
                if i not in left_matched:
                    results.append(self._merge_rows(row, {}))

        if self.join_type in (JoinType.RIGHT, JoinType.FULL):
            for i, row in enumerate(right_sorted):
                if i not in right_matched:
                    results.append(self._merge_rows({}, row))

        total_time = (time.time() - start_time) * 1000

        stats = JoinStatistics(
            algorithm_used=JoinAlgorithm.MERGE,
            build_time_ms=0,
            probe_time_ms=total_time,
            total_time_ms=total_time,
            left_rows=len(left_data),
            right_rows=len(right_data),
            output_rows=len(results)
        )

        return results, stats


class NestedLoopJoinExecutor:
    """
    Nested Loop Join Implementation

    Fallback algorithm for non-equi joins or when other algorithms aren't applicable.

    Time Complexity: O(n * m)
    Space Complexity: O(1)
    """

    def __init__(self, join_type: JoinType = JoinType.INNER):
        self.join_type = join_type

    def _merge_rows(self, left_row: Dict[str, Any], right_row: Dict[str, Any]) -> Dict[str, Any]:
        """Merge two rows with prefixed column names (consistent with HashJoin)"""
        merged = {}
        for key, value in left_row.items():
            merged[f"left_{key}"] = value
        for key, value in right_row.items():
            merged[f"right_{key}"] = value
        return merged

    def execute(
        self,
        left_data: List[Dict[str, Any]],
        right_data: List[Dict[str, Any]],
        condition: Callable[[Dict, Dict], bool]
    ) -> Tuple[List[Dict[str, Any]], JoinStatistics]:
        """
        Execute nested loop join with arbitrary condition
        """
        import time

        start_time = time.time()
        results = []

        # Track matched rows for outer joins
        left_matched = set()
        right_matched = set()

        for i, left_row in enumerate(left_data):
            for j, right_row in enumerate(right_data):
                if condition(left_row, right_row):
                    merged = self._merge_rows(left_row, right_row)
                    results.append(merged)
                    left_matched.add(i)
                    right_matched.add(j)

        # Handle outer joins - emit unmatched rows
        if self.join_type in (JoinType.LEFT, JoinType.FULL):
            for i, row in enumerate(left_data):
                if i not in left_matched:
                    results.append(self._merge_rows(row, {}))

        if self.join_type in (JoinType.RIGHT, JoinType.FULL):
            for i, row in enumerate(right_data):
                if i not in right_matched:
                    results.append(self._merge_rows({}, row))

        total_time = (time.time() - start_time) * 1000

        stats = JoinStatistics(
            algorithm_used=JoinAlgorithm.NESTED_LOOP,
            build_time_ms=0,
            probe_time_ms=total_time,
            total_time_ms=total_time,
            left_rows=len(left_data),
            right_rows=len(right_data),
            output_rows=len(results)
        )

        return results, stats


class JoinExecutor:
    """
    Smart Join Executor
    
    Automatically selects the best join algorithm based on:
    - Table sizes
    - Join type
    - Available memory
    - Data characteristics
    """
    
    def __init__(self):
        self.hash_executor = HashJoinExecutor()
        self.merge_executor = MergeJoinExecutor()
        self.nested_loop_executor = NestedLoopJoinExecutor()
    
    def execute(
        self,
        left_data: List[Dict[str, Any]],
        right_data: List[Dict[str, Any]],
        left_key: str,
        right_key: str,
        join_type: JoinType = JoinType.INNER,
        condition: Optional[Callable[[Dict, Dict], bool]] = None
    ) -> Tuple[List[Dict[str, Any]], JoinStatistics]:
        """
        Execute join with automatic algorithm selection
        """
        # Select algorithm based on heuristics (pass join_type for consideration)
        algorithm = self._select_algorithm(left_data, right_data, join_type, condition)

        logger.info(f"Selected {algorithm.value} join for {len(left_data)} x {len(right_data)} rows")

        if algorithm == JoinAlgorithm.HASH:
            self.hash_executor.join_type = join_type
            return self.hash_executor.execute(left_data, right_data, left_key, right_key, condition)
        elif algorithm == JoinAlgorithm.MERGE:
            self.merge_executor.join_type = join_type
            return self.merge_executor.execute(left_data, right_data, left_key, right_key, condition)
        else:
            # Nested loop with combined condition
            self.nested_loop_executor.join_type = join_type

            def full_condition(l, r):
                # Check key equality
                if l.get(left_key) != r.get(right_key):
                    return False
                # Check additional condition
                if condition and not condition(l, r):
                    return False
                return True

            return self.nested_loop_executor.execute(left_data, right_data, full_condition)
    
    def _select_algorithm(
        self,
        left_data: List[Dict[str, Any]],
        right_data: List[Dict[str, Any]],
        join_type: JoinType = JoinType.INNER,
        condition: Optional[Callable] = None
    ) -> JoinAlgorithm:
        """
        Select best join algorithm based on data characteristics

        Considers:
        - Dataset sizes
        - Join type (INNER vs outer joins)
        - Additional conditions
        """
        left_size = len(left_data)
        right_size = len(right_data)
        total_size = left_size + right_size

        # For small datasets, nested loop is fine and handles all cases
        if total_size < 100:
            return JoinAlgorithm.NESTED_LOOP

        # For large datasets with equi-join (no extra condition), use hash join
        # Hash join handles all join types correctly
        if total_size > 1000 and condition is None:
            return JoinAlgorithm.HASH

        # For medium datasets without extra condition, merge join is efficient
        # (Now properly supports outer joins and conditions)
        if 100 <= total_size <= 1000 and condition is None:
            return JoinAlgorithm.MERGE

        # If there's a condition, prefer hash join (handles conditions)
        # or nested loop for smaller datasets
        if condition is not None:
            if total_size > 500:
                return JoinAlgorithm.HASH
            return JoinAlgorithm.NESTED_LOOP

        # Default to hash join for large datasets
        return JoinAlgorithm.HASH


# Example usage and testing
if __name__ == "__main__":
    # Test data
    users = [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
        {"id": 3, "name": "Charlie"}
    ]
    
    orders = [
        {"order_id": 101, "user_id": 1, "amount": 100},
        {"order_id": 102, "user_id": 2, "amount": 200},
        {"order_id": 103, "user_id": 1, "amount": 150},
        {"order_id": 104, "user_id": 4, "amount": 300}  # No matching user
    ]
    
    # Test hash join
    print("Testing Hash Join:")
    executor = JoinExecutor()
    results, stats = executor.execute(users, orders, "id", "user_id")
    
    print(f"Results: {len(results)} rows")
    for row in results:
        print(f"  {row}")
    
    print(f"\nStatistics:")
    print(f"  Algorithm: {stats.algorithm_used.value}")
    print(f"  Total time: {stats.total_time_ms:.2f}ms")
    print(f"  Build time: {stats.build_time_ms:.2f}ms")
    print(f"  Probe time: {stats.probe_time_ms:.2f}ms")
    print(f"  Hash table size: {stats.hash_table_size}")
    print(f"  Collisions: {stats.collisions}")
