#!/usr/bin/env python3
"""
Unit Tests for B-Tree Index
============================

Comprehensive tests for B-Tree implementation.
"""

import pytest
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from core.btree import BTree, BTreeNode


class TestBTreeBasics:
    """Test basic B-Tree operations"""
    
    def test_create_btree(self):
        """Test B-Tree creation"""
        btree = BTree(order=5)
        assert len(btree) == 0
        assert btree.root is not None
    
    def test_insert_single(self):
        """Test inserting a single key"""
        btree = BTree(order=5)
        btree.insert(10, "ten")
        assert len(btree) == 1
        assert btree.search(10) == ["ten"]
    
    def test_insert_multiple(self):
        """Test inserting multiple keys"""
        btree = BTree(order=5)
        for i in range(10):
            btree.insert(i, f"value_{i}")
        
        assert len(btree) == 10
        for i in range(10):
            assert btree.search(i) == [f"value_{i}"]
    
    def test_insert_reverse_order(self):
        """Test inserting in reverse order"""
        btree = BTree(order=5)
        for i in range(10, 0, -1):
            btree.insert(i, f"value_{i}")
        
        assert len(btree) == 10
        # Verify all keys are present
        for i in range(1, 11):
            assert btree.search(i) == [f"value_{i}"]


class TestBTreeSearch:
    """Test B-Tree search operations"""
    
    def test_search_existing(self):
        """Test searching for existing keys"""
        btree = BTree(order=5)
        btree.insert(10, "ten")
        btree.insert(20, "twenty")
        btree.insert(30, "thirty")
        
        assert btree.search(10) == ["ten"]
        assert btree.search(20) == ["twenty"]
        assert btree.search(30) == ["thirty"]
    
    def test_search_nonexistent(self):
        """Test searching for non-existent keys"""
        btree = BTree(order=5)
        btree.insert(10, "ten")
        
        assert btree.search(5) is None
        assert btree.search(15) is None
        assert btree.search(100) is None
    
    def test_contains(self):
        """Test __contains__ operator"""
        btree = BTree(order=5)
        btree.insert(10, "ten")
        
        assert 10 in btree
        assert 5 not in btree


class TestBTreeRangeSearch:
    """Test B-Tree range query operations"""
    
    def test_range_search_basic(self):
        """Test basic range search"""
        btree = BTree(order=5)
        for i in range(1, 11):
            btree.insert(i, f"value_{i}")
        
        results = btree.range_search(3, 7)
        keys = [k for k, v in results]
        
        assert keys == [3, 4, 5, 6, 7]
    
    def test_range_search_empty(self):
        """Test range search with no results"""
        btree = BTree(order=5)
        btree.insert(10, "ten")
        btree.insert(20, "twenty")
        
        results = btree.range_search(11, 19)
        assert len(results) == 0
    
    def test_range_search_all(self):
        """Test range search that includes all keys"""
        btree = BTree(order=5)
        for i in range(5, 16):
            btree.insert(i, f"value_{i}")
        
        results = btree.range_search(0, 100)
        assert len(results) == 11


class TestBTreeDelete:
    """Test B-Tree deletion operations"""
    
    def test_delete_from_leaf(self):
        """Test deleting from leaf node"""
        btree = BTree(order=5)
        btree.insert(10, "ten")
        btree.insert(20, "twenty")
        btree.insert(30, "thirty")
        
        assert btree.delete(20) is True
        assert len(btree) == 2
        assert btree.search(20) is None
        assert btree.search(10) == ["ten"]
        assert btree.search(30) == ["thirty"]
    
    def test_delete_nonexistent(self):
        """Test deleting non-existent key"""
        btree = BTree(order=5)
        btree.insert(10, "ten")
        
        assert btree.delete(20) is False
        assert len(btree) == 1
    
    def test_delete_all(self):
        """Test deleting all keys"""
        btree = BTree(order=5)
        keys = [10, 20, 30, 40, 50]
        for key in keys:
            btree.insert(key, f"value_{key}")
        
        for key in keys:
            assert btree.delete(key) is True
        
        assert len(btree) == 0


class TestBTreeTraversal:
    """Test B-Tree traversal operations"""
    
    def test_traverse_ordered(self):
        """Test in-order traversal"""
        btree = BTree(order=5)
        keys = [30, 10, 50, 20, 40]
        for key in keys:
            btree.insert(key, f"value_{key}")
        
        traversed_keys = [k for k, v in btree.traverse()]
        assert traversed_keys == sorted(keys)
    
    def test_traverse_empty(self):
        """Test traversing empty tree"""
        btree = BTree(order=5)
        traversed = list(btree.traverse())
        assert len(traversed) == 0


class TestBTreeLargeDataset:
    """Test B-Tree with large datasets"""
    
    def test_insert_1000_keys(self):
        """Test inserting 1000 keys"""
        btree = BTree(order=5)
        for i in range(1000):
            btree.insert(i, f"value_{i}")
        
        assert len(btree) == 1000
        
        # Verify random samples
        for i in [0, 100, 500, 999]:
            assert btree.search(i) == [f"value_{i}"]
    
    def test_range_search_large(self):
        """Test range search on large dataset"""
        btree = BTree(order=5)
        for i in range(1000):
            btree.insert(i, f"value_{i}")
        
        results = btree.range_search(100, 200)
        assert len(results) == 101  # 100-200 inclusive
    
    def test_delete_from_large_tree(self):
        """Test deleting from large tree"""
        btree = BTree(order=5)
        for i in range(100):
            btree.insert(i, f"value_{i}")
        
        # Delete every other key
        for i in range(0, 100, 2):
            assert btree.delete(i) is True
        
        assert len(btree) == 50
        
        # Verify remaining keys
        for i in range(1, 100, 2):
            assert btree.search(i) == [f"value_{i}"]


class TestBTreePerformance:
    """Test B-Tree performance characteristics"""
    
    def test_search_performance(self):
        """Test that search is O(log n)"""
        import time
        
        btree = BTree(order=10)
        for i in range(10000):
            btree.insert(i, f"value_{i}")
        
        # Search should be fast even with 10K keys
        start = time.time()
        for _ in range(100):
            btree.search(5000)
        duration = time.time() - start
        
        assert duration < 0.1  # Should complete in < 100ms


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
