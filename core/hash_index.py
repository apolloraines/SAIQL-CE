#!/usr/bin/env python3
"""
SAIQL Hash Index Implementation
================================

A hash-based index for O(1) equality lookups.

Features:
- O(1) average case lookup
- Efficient for equality comparisons
- Minimal memory overhead
- Collision handling with chaining

Author: Apollo & Claude
Version: 1.0.0
Status: Production-Ready
"""

import logging
from typing import Any, List, Optional, Tuple

logger = logging.getLogger(__name__)


class HashIndex:
    """
    Hash Index Implementation
    
    A hash index provides O(1) average case lookups for equality comparisons.
    It uses a hash table with chaining for collision resolution.
    
    Time Complexity:
    - Insert: O(1) average
    - Search: O(1) average
    - Delete: O(1) average
    
    Space Complexity: O(n) where n is the number of entries
    """
    
    def __init__(self, initial_size: int = 1024):
        """
        Initialize hash index
        
        Args:
            initial_size: Initial number of buckets (power of 2 recommended)
        """
        self.size = initial_size
        self.count = 0
        self.load_factor_threshold = 0.75
        
        # Buckets: each bucket is a list of (key, [values]) tuples
        self.buckets: List[List[Tuple[Any, List[Any]]]] = [[] for _ in range(self.size)]
        
        logger.info(f"Created hash index with {self.size} buckets")
    
    def _hash(self, key: Any) -> int:
        """
        Hash function
        
        Uses Python's built-in hash() and maps to bucket index
        """
        return hash(key) % self.size
    
    def _should_resize(self) -> bool:
        """Check if we should resize the hash table"""
        load_factor = self.count / self.size
        return load_factor > self.load_factor_threshold
    
    def _resize(self) -> None:
        """
        Resize hash table when load factor is too high
        
        Doubles the size and rehashes all entries
        """
        old_buckets = self.buckets
        self.size *= 2
        self.buckets = [[] for _ in range(self.size)]
        self.count = 0
        
        # Rehash all entries (value is a list of row_ids)
        for bucket in old_buckets:
            for key, values in bucket:
                for value in values:
                    self.insert(key, value)

        logger.info(f"Resized hash index to {self.size} buckets")
    
    def insert(self, key: Any, value: Any) -> None:
        """
        Insert a key-value pair (supports multiple values per key for non-unique indexes)

        Args:
            key: Key to insert
            value: Value (row_id) associated with the key
        """
        # Check if we need to resize
        if self._should_resize():
            self._resize()

        bucket_idx = self._hash(key)
        bucket = self.buckets[bucket_idx]

        # Check if key already exists - append to value list
        for i, (k, v) in enumerate(bucket):
            if k == key:
                # v is a list of values for this key
                if value not in v:
                    v.append(value)
                return

        # Key doesn't exist, add new entry with value as list
        bucket.append((key, [value]))
        self.count += 1
    
    def search(self, key: Any) -> Optional[List[Any]]:
        """
        Search for a key

        Args:
            key: Key to search for

        Returns:
            List of values (row_ids) associated with the key, or None if not found
        """
        bucket_idx = self._hash(key)
        bucket = self.buckets[bucket_idx]

        # Linear search within bucket
        for k, v in bucket:
            if k == key:
                # v is already a list of values
                return v if v else None

        return None
    
    def delete(self, key: Any, row_id: Any = None) -> bool:
        """
        Delete a key or specific row_id from a key

        Args:
            key: Key to delete
            row_id: Optional specific value to remove. If None, deletes entire key.

        Returns:
            True if key/value was found and deleted, False otherwise
        """
        bucket_idx = self._hash(key)
        bucket = self.buckets[bucket_idx]

        # Find the key
        for i, (k, v) in enumerate(bucket):
            if k == key:
                if row_id is not None:
                    # Remove specific row_id from value list
                    if row_id in v:
                        v.remove(row_id)
                        if len(v) == 0:
                            # No more values - remove the key entirely
                            bucket.pop(i)
                            self.count -= 1
                        return True
                    return False  # row_id not found
                else:
                    # Delete entire key
                    bucket.pop(i)
                    self.count -= 1
                    return True

        return False
    
    def __len__(self) -> int:
        """Return number of entries in the index"""
        return self.count
    
    def __contains__(self, key: Any) -> bool:
        """Check if key exists in index"""
        return self.search(key) is not None
    
    def get_statistics(self) -> dict:
        """
        Get index statistics
        
        Returns:
            Dictionary with statistics about the index
        """
        # Calculate bucket utilization
        non_empty_buckets = sum(1 for bucket in self.buckets if bucket)
        max_bucket_size = max(len(bucket) for bucket in self.buckets) if self.buckets else 0
        avg_bucket_size = self.count / non_empty_buckets if non_empty_buckets > 0 else 0
        
        return {
            "total_entries": self.count,
            "total_buckets": self.size,
            "non_empty_buckets": non_empty_buckets,
            "load_factor": self.count / self.size,
            "max_bucket_size": max_bucket_size,
            "avg_bucket_size": avg_bucket_size,
            "utilization": non_empty_buckets / self.size
        }


# Example usage and testing
if __name__ == "__main__":
    print("Testing Hash Index Implementation")
    print("=" * 50)
    
    # Create hash index
    index = HashIndex(initial_size=8)
    
    # Insert some data
    print("\nInserting data...")
    data = [(10, "ten"), (20, "twenty"), (5, "five"), (15, "fifteen"), 
            (25, "twenty-five"), (30, "thirty"), (3, "three")]
    
    for key, value in data:
        index.insert(key, value)
        print(f"  Inserted ({key}, {value})")
    
    print(f"\nIndex size: {len(index)}")
    
    # Search for keys
    print("\nSearching...")
    for key in [5, 15, 100]:
        result = index.search(key)
        print(f"  Search({key}): {result}")
    
    # Test contains
    print("\nContains check...")
    print(f"  10 in index: {10 in index}")
    print(f"  100 in index: {100 in index}")
    
    # Delete
    print("\nDeleting key 15...")
    index.delete(15)
    print(f"Index size: {len(index)}")
    print(f"Search(15): {index.search(15)}")
    
    # Statistics
    print("\nIndex Statistics:")
    stats = index.get_statistics()
    for key, value in stats.items():
        print(f"  {key}: {value:.2f}" if isinstance(value, float) else f"  {key}: {value}")
    
    # Test resizing
    print("\nTesting automatic resizing...")
    for i in range(100, 200):
        index.insert(i, f"value_{i}")
    
    print(f"After inserting 100 more entries:")
    stats = index.get_statistics()
    print(f"  Total buckets: {stats['total_buckets']}")
    print(f"  Load factor: {stats['load_factor']:.2f}")
