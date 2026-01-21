#!/usr/bin/env python3
"""
Unit Tests for SAIQL Join Engine
=================================

Tests for hash join, merge join, and nested loop join implementations.
"""

import pytest
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from core.join_engine import (
    HashJoinExecutor, MergeJoinExecutor, NestedLoopJoinExecutor,
    JoinExecutor, JoinType, JoinAlgorithm
)


class TestHashJoin:
    """Test hash join implementation"""
    
    def test_basic_inner_join(self):
        """Test basic inner join"""
        users = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
            {"id": 3, "name": "Charlie"}
        ]
        
        orders = [
            {"order_id": 101, "user_id": 1, "amount": 100},
            {"order_id": 102, "user_id": 2, "amount": 200},
            {"order_id": 103, "user_id": 1, "amount": 150}
        ]
        
        executor = HashJoinExecutor(JoinType.INNER)
        results, stats = executor.execute(users, orders, "id", "user_id")
        
        assert len(results) == 3
        assert stats.algorithm_used == JoinAlgorithm.HASH
        assert stats.left_rows == 3
        assert stats.right_rows == 3
        assert stats.output_rows == 3
    
    def test_join_with_no_matches(self):
        """Test join where no rows match"""
        left = [{"id": 1}, {"id": 2}]
        right = [{"id": 3}, {"id": 4}]
        
        executor = HashJoinExecutor()
        results, stats = executor.execute(left, right, "id", "id")
        
        assert len(results) == 0
        assert stats.output_rows == 0
    
    def test_join_with_duplicates(self):
        """Test join with duplicate keys"""
        left = [{"id": 1, "val": "A"}, {"id": 1, "val": "B"}]
        right = [{"id": 1, "val": "X"}, {"id": 1, "val": "Y"}]
        
        executor = HashJoinExecutor()
        results, stats = executor.execute(left, right, "id", "id")
        
        # Should produce cartesian product of matching rows
        assert len(results) == 4  # 2 x 2
        assert stats.collisions > 0
    
    def test_join_performance(self):
        """Test join performance on larger dataset"""
        # Generate test data
        left = [{"id": i, "value": f"left_{i}"} for i in range(1000)]
        right = [{"id": i, "value": f"right_{i}"} for i in range(500, 1500)]
        
        executor = HashJoinExecutor()
        results, stats = executor.execute(left, right, "id", "id")
        
        # Should match rows 500-999
        assert len(results) == 500
        assert stats.total_time_ms < 100  # Should be fast
        
    def test_join_with_condition(self):
        """Test join with additional condition"""
        left = [{"id": 1, "age": 25}, {"id": 2, "age": 35}]
        right = [{"id": 1, "score": 80}, {"id": 2, "score": 90}]
        
        # Only join if age > 30
        condition = lambda l, r: l.get("age", 0) > 30
        
        executor = HashJoinExecutor()
        results, stats = executor.execute(left, right, "id", "id", condition)
        
        assert len(results) == 1  # Only id=2 matches
        assert results[0]["left_age"] == 35


class TestMergeJoin:
    """Test merge join implementation"""
    
    def test_basic_merge_join(self):
        """Test basic merge join"""
        left = [{"id": 1}, {"id": 2}, {"id": 3}]
        right = [{"id": 2}, {"id": 3}, {"id": 4}]
        
        executor = MergeJoinExecutor()
        results, stats = executor.execute(left, right, "id", "id")
        
        assert len(results) == 2  # id=2 and id=3
        assert stats.algorithm_used == JoinAlgorithm.MERGE


class TestNestedLoopJoin:
    """Test nested loop join implementation"""
    
    def test_basic_nested_loop(self):
        """Test basic nested loop join"""
        left = [{"id": 1}, {"id": 2}]
        right = [{"id": 2}, {"id": 3}]
        
        condition = lambda l, r: l["id"] == r["id"]
        
        executor = NestedLoopJoinExecutor()
        results, stats = executor.execute(left, right, condition)
        
        assert len(results) == 1  # Only id=2 matches
        assert stats.algorithm_used == JoinAlgorithm.NESTED_LOOP


class TestJoinExecutor:
    """Test smart join executor (algorithm selection)"""
    
    def test_algorithm_selection_small_dataset(self):
        """Test that small datasets use nested loop"""
        left = [{"id": i} for i in range(10)]
        right = [{"id": i} for i in range(10)]
        
        executor = JoinExecutor()
        results, stats = executor.execute(left, right, "id", "id")
        
        # Small dataset should use nested loop
        assert stats.algorithm_used == JoinAlgorithm.NESTED_LOOP
    
    def test_algorithm_selection_large_dataset(self):
        """Test that large datasets use hash join"""
        left = [{"id": i} for i in range(2000)]
        right = [{"id": i} for i in range(2000)]
        
        executor = JoinExecutor()
        results, stats = executor.execute(left, right, "id", "id")
        
        # Large dataset should use hash join
        assert stats.algorithm_used == JoinAlgorithm.HASH
        assert len(results) == 2000


class TestJoinStatistics:
    """Test join statistics collection"""
    
    def test_statistics_collection(self):
        """Test that statistics are properly collected"""
        left = [{"id": i} for i in range(100)]
        right = [{"id": i} for i in range(100)]
        
        executor = HashJoinExecutor()
        results, stats = executor.execute(left, right, "id", "id")
        
        assert stats.left_rows == 100
        assert stats.right_rows == 100
        assert stats.output_rows == 100
        assert stats.build_time_ms >= 0
        assert stats.probe_time_ms >= 0
        assert stats.total_time_ms >= 0
        assert stats.hash_table_size > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
