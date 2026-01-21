#!/usr/bin/env python3
"""
SAIQL B-Tree Index Implementation
==================================

A balanced tree data structure for efficient range queries and ordered access.

Features:
- O(log n) search, insert, delete
- Range queries
- Ordered iteration
- Automatic balancing

Author: Apollo & Claude
Version: 1.0.0
Status: Production-Ready

Change Notes (2026-01-20):
- Fixed _delete_from_internal() to properly handle deletion from internal nodes
- Added _find_predecessor() helper for cleaner predecessor finding
- Fixed fallback logic that could silently corrupt the tree

Change Notes (2026-01-20 Round 2):
- Fixed _traverse_node() IndexError when node has zero keys
- Added _ensure_child_can_lose_key() for underflow prevention before deletion
- Added _borrow_from_left_sibling() and _borrow_from_right_sibling()
- Added _merge_children() for merging nodes when borrowing not possible

Change Notes (2026-01-20 Round 3):
- Fixed _ensure_child_can_lose_key() to return updated child index after merge
- Updated _delete_from_node() and _delete_from_internal() to use returned index
- Prevents wrong subtree deletion or IndexError when merging with left sibling

Change Notes (2026-01-20 Round 4):
- Fixed duplicate key handling: insert() now appends to value list for existing keys
- delete() now accepts optional row_id to remove specific value from duplicates
- Consistent duplicate policy: keys unique in tree, values stored as lists

Change Notes (2026-01-20 Round 5):
- Fixed insert to check for existing keys in internal nodes (not just leaves)
- Fixed row_id deletion for keys in internal nodes (was ignoring row_id)
- Updated return type annotations: _delete_from_node/internal return Union[str, bool]
"""

import logging
from typing import Any, List, Optional, Tuple, Iterator, Union
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class BTreeNode:
    """
    Node in a B-Tree
    
    A B-Tree node contains:
    - keys: Sorted list of keys
    - values: Corresponding values for each key
    - children: Child nodes (for internal nodes)
    - is_leaf: Whether this is a leaf node
    """
    order: int  # Maximum number of children
    keys: List[Any] = field(default_factory=list)
    values: List[Any] = field(default_factory=list)
    children: List['BTreeNode'] = field(default_factory=list)
    is_leaf: bool = True
    
    def is_full(self) -> bool:
        """Check if node is full"""
        return len(self.keys) >= self.order - 1
    
    def find_key_index(self, key: Any) -> int:
        """
        Find the index where key should be inserted
        Uses binary search for efficiency
        """
        left, right = 0, len(self.keys)
        while left < right:
            mid = (left + right) // 2
            if self.keys[mid] < key:
                left = mid + 1
            else:
                right = mid
        return left


class BTree:
    """
    B-Tree Index Implementation
    
    A B-Tree is a self-balancing tree data structure that maintains sorted data
    and allows searches, sequential access, insertions, and deletions in O(log n).
    
    Properties:
    - All leaves are at the same depth
    - Internal nodes have between ⌈m/2⌉ and m children (where m is the order)
    - Root has at least 2 children (unless it's a leaf)
    
    Time Complexity:
    - Search: O(log n)
    - Insert: O(log n)
    - Delete: O(log n)
    - Range Query: O(log n + k) where k is the number of results
    """
    
    def __init__(self, order: int = 5):
        """
        Initialize B-Tree
        
        Args:
            order: Maximum number of children per node (typically 3-7)
                  Higher order = fewer levels but more comparisons per node
        """
        if order < 3:
            raise ValueError("B-Tree order must be at least 3")
        
        self.order = order
        self.root = BTreeNode(order=order)
        self.size = 0
        
        logger.info(f"Created B-Tree with order {order}")
    
    def search(self, key: Any) -> Optional[List[Any]]:
        """
        Search for a key in the B-Tree
        
        Args:
            key: Key to search for
            
        Returns:
            List of values associated with the key, or None if not found
        """
        return self._search_node(self.root, key)
    
    def _search_node(self, node: BTreeNode, key: Any) -> Optional[List[Any]]:
        """Recursively search for key in node and its children"""
        # Find position where key might be
        idx = node.find_key_index(key)
        
        # Check if key is in this node
        if idx < len(node.keys) and node.keys[idx] == key:
            # Found it! Return the value(s) - values are stored as lists
            value = node.values[idx]
            return value if isinstance(value, list) else [value]
        
        # If leaf node and key not found, it doesn't exist
        if node.is_leaf:
            return None
        
        # Recurse to appropriate child
        return self._search_node(node.children[idx], key)
    
    def insert(self, key: Any, value: Any) -> None:
        """
        Insert a key-value pair into the B-Tree
        
        Args:
            key: Key to insert
            value: Value associated with the key
        """
        # If root is full, split it
        if self.root.is_full():
            old_root = self.root
            self.root = BTreeNode(order=self.order, is_leaf=False)
            self.root.children.append(old_root)
            self._split_child(self.root, 0)
        
        # Insert into the tree
        is_new_key = self._insert_non_full(self.root, key, value)
        if is_new_key:
            self.size += 1
    
    def _insert_non_full(self, node: BTreeNode, key: Any, value: Any) -> bool:
        """Insert into a node that is not full. Returns True if new key, False if appended to existing."""
        idx = node.find_key_index(key)

        # Check if key already exists at this position (works for both leaf and internal nodes)
        if idx < len(node.keys) and node.keys[idx] == key:
            # Key exists - append value to list
            existing = node.values[idx]
            if isinstance(existing, list):
                existing.append(value)
            else:
                node.values[idx] = [existing, value]
            return False  # Not a new key

        if node.is_leaf:
            # Insert new key into leaf node
            node.keys.insert(idx, key)
            node.values.insert(idx, [value])  # Store as list from the start
            return True  # New key
        else:
            # Insert into internal node - recurse to child
            if node.children[idx].is_full():
                # Child is full, split it first
                self._split_child(node, idx)
                # After split, check if key matches the promoted key
                if idx < len(node.keys) and node.keys[idx] == key:
                    # Key was promoted during split - append value
                    existing = node.values[idx]
                    if isinstance(existing, list):
                        existing.append(value)
                    else:
                        node.values[idx] = [existing, value]
                    return False
                # Otherwise, key might go to the right child
                if key > node.keys[idx]:
                    idx += 1

            return self._insert_non_full(node.children[idx], key, value)
    
    def _split_child(self, parent: BTreeNode, child_idx: int) -> None:
        """
        Split a full child node
        
        This is the key operation that maintains the B-Tree balance.
        When a node is full, we split it into two nodes and move the
        median key up to the parent.
        """
        full_child = parent.children[child_idx]
        mid_idx = len(full_child.keys) // 2
        
        # Create new node for right half
        new_child = BTreeNode(order=self.order, is_leaf=full_child.is_leaf)
        
        # Move right half of keys/values to new node
        new_child.keys = full_child.keys[mid_idx + 1:]
        new_child.values = full_child.values[mid_idx + 1:]
        
        # If not leaf, move right half of children too
        if not full_child.is_leaf:
            new_child.children = full_child.children[mid_idx + 1:]
            full_child.children = full_child.children[:mid_idx + 1]
        
        # Keep left half in original node
        median_key = full_child.keys[mid_idx]
        median_value = full_child.values[mid_idx]
        full_child.keys = full_child.keys[:mid_idx]
        full_child.values = full_child.values[:mid_idx]
        
        # Insert median into parent
        parent.keys.insert(child_idx, median_key)
        parent.values.insert(child_idx, median_value)
        parent.children.insert(child_idx + 1, new_child)
    
    def range_search(self, min_key: Any, max_key: Any) -> List[Tuple[Any, Any]]:
        """
        Search for all keys in a range [min_key, max_key]
        
        Args:
            min_key: Minimum key (inclusive)
            max_key: Maximum key (inclusive)
            
        Returns:
            List of (key, value) tuples in the range
        """
        results = []
        self._range_search_node(self.root, min_key, max_key, results)
        return results
    
    def _range_search_node(
        self, 
        node: BTreeNode, 
        min_key: Any, 
        max_key: Any, 
        results: List[Tuple[Any, Any]]
    ) -> None:
        """Recursively search for keys in range"""
        i = 0
        
        # Traverse through keys in this node
        while i < len(node.keys):
            # If not leaf, recurse to left child first
            if not node.is_leaf and node.keys[i] >= min_key:
                self._range_search_node(node.children[i], min_key, max_key, results)
            
            # Check if current key is in range
            if min_key <= node.keys[i] <= max_key:
                results.append((node.keys[i], node.values[i]))
            
            # If we've passed max_key, we're done
            if node.keys[i] > max_key:
                return
            
            i += 1
        
        # Check rightmost child if not leaf
        if not node.is_leaf:
            self._range_search_node(node.children[i], min_key, max_key, results)
    
    def delete(self, key: Any, row_id: Any = None) -> bool:
        """
        Delete a key (or specific value) from the B-Tree

        Args:
            key: Key to delete
            row_id: Optional specific value to remove. If None, deletes entire key.
                    If provided, removes only that value from the key's value list.

        Returns:
            True if key/value was found and deleted, False otherwise
        """
        result = self._delete_from_node(self.root, key, row_id)

        # If root is empty after deletion, make its only child the new root
        if len(self.root.keys) == 0 and not self.root.is_leaf:
            self.root = self.root.children[0]

        # Only decrement size if a KEY was removed (not just a value)
        if result == 'key':
            self.size -= 1

        return result is not False  # Return True if anything was deleted
    
    def _delete_from_node(self, node: BTreeNode, key: Any, row_id: Any = None) -> Union[str, bool]:
        """Recursively delete key from node. Returns 'key' if key removed, 'value' if value removed, False if not found."""
        idx = node.find_key_index(key)

        # Case 1: Key is in this node
        if idx < len(node.keys) and node.keys[idx] == key:
            # Handle row_id: remove specific value (works for both leaf and internal)
            if row_id is not None:
                values = node.values[idx]
                if isinstance(values, list) and row_id in values:
                    values.remove(row_id)
                    if len(values) == 0:
                        # No more values - remove the key entirely
                        if node.is_leaf:
                            node.keys.pop(idx)
                            node.values.pop(idx)
                            return 'key'
                        else:
                            # Internal node - need to use predecessor/successor replacement
                            return self._delete_from_internal(node, idx)
                    return 'value'  # Only value was removed, key still exists
                return False  # row_id not found

            # No row_id specified - delete entire key
            if node.is_leaf:
                node.keys.pop(idx)
                node.values.pop(idx)
                return 'key'  # Key was removed
            else:
                # Complex case: delete from internal node
                return self._delete_from_internal(node, idx)

        # Case 2: Key is not in this node
        if node.is_leaf:
            return False  # Key not found

        # Before recursing, ensure child can lose a key without underflow
        # Returns updated index (may change if merged with left sibling)
        idx = self._ensure_child_can_lose_key(node, idx)

        # Recurse to appropriate child
        return self._delete_from_node(node.children[idx], key, row_id)
    
    def _delete_from_internal(self, node: BTreeNode, idx: int) -> Union[str, bool]:
        """Delete key from internal node by replacing with predecessor or successor"""
        left_child = node.children[idx]
        min_keys = self._min_keys()

        # Case 1: Left child has more than min keys - use predecessor
        if len(left_child.keys) > min_keys:
            pred_key, pred_value = self._find_predecessor(left_child)
            if pred_key is not None:
                node.keys[idx] = pred_key
                node.values[idx] = pred_value
                child_idx = self._ensure_child_can_lose_key(node, idx)
                return self._delete_from_node(node.children[child_idx], pred_key)

        # Case 2: Right child has more than min keys - use successor
        if idx + 1 < len(node.children):
            right_child = node.children[idx + 1]
            if len(right_child.keys) > min_keys:
                succ_key, succ_value = self._find_successor(right_child)
                if succ_key is not None:
                    node.keys[idx] = succ_key
                    node.values[idx] = succ_value
                    child_idx = self._ensure_child_can_lose_key(node, idx + 1)
                    return self._delete_from_node(node.children[child_idx], succ_key)

        # Case 3: Both children at minimum - merge and recurse
        # The key at node.keys[idx] moves down into merged child
        key_to_delete = node.keys[idx]
        if idx + 1 < len(node.children):
            self._merge_children(node, idx)
            # Key moved down into node.children[idx], recurse to delete it
            return self._delete_from_node(node.children[idx], key_to_delete)

        # Fallback: use predecessor with underflow handling
        pred_key, pred_value = self._find_predecessor(left_child)
        if pred_key is not None:
            node.keys[idx] = pred_key
            node.values[idx] = pred_value
            child_idx = self._ensure_child_can_lose_key(node, idx)
            return self._delete_from_node(node.children[child_idx], pred_key)

        logger.warning(f"B-Tree deletion: could not find replacement key at index {idx}")
        return False

    def _find_predecessor(self, node: BTreeNode) -> Tuple[Any, Any]:
        """Find the predecessor (rightmost key) in a subtree"""
        current = node
        while not current.is_leaf and len(current.children) > 0:
            current = current.children[-1]

        if len(current.keys) > 0:
            return current.keys[-1], current.values[-1]
        return None, None

    def _find_successor(self, node: BTreeNode) -> Tuple[Any, Any]:
        """Find the successor (leftmost key) in a subtree"""
        current = node
        while not current.is_leaf and len(current.children) > 0:
            current = current.children[0]

        if len(current.keys) > 0:
            return current.keys[0], current.values[0]
        return None, None

    def _min_keys(self) -> int:
        """Return minimum number of keys a non-root node must have"""
        return (self.order + 1) // 2 - 1

    def _ensure_child_can_lose_key(self, parent: BTreeNode, child_idx: int) -> int:
        """
        Ensure child at child_idx has more than minimum keys before deletion.
        If child is at minimum, borrow from sibling or merge.

        Returns:
            Updated child index (may change if merged with left sibling)
        """
        child = parent.children[child_idx]
        min_keys = self._min_keys()

        # If child has more than minimum keys, it can safely lose one
        if len(child.keys) > min_keys:
            return child_idx

        # Try to borrow from left sibling
        if child_idx > 0:
            left_sibling = parent.children[child_idx - 1]
            if len(left_sibling.keys) > min_keys:
                self._borrow_from_left_sibling(parent, child_idx)
                return child_idx

        # Try to borrow from right sibling
        if child_idx < len(parent.children) - 1:
            right_sibling = parent.children[child_idx + 1]
            if len(right_sibling.keys) > min_keys:
                self._borrow_from_right_sibling(parent, child_idx)
                return child_idx

        # Can't borrow - must merge
        if child_idx > 0:
            # Merge with left sibling - child is now at child_idx - 1
            self._merge_children(parent, child_idx - 1)
            return child_idx - 1
        else:
            # Merge with right sibling - child stays at child_idx
            self._merge_children(parent, child_idx)
            return child_idx

    def _borrow_from_left_sibling(self, parent: BTreeNode, child_idx: int) -> None:
        """Borrow a key from left sibling through parent"""
        child = parent.children[child_idx]
        left_sibling = parent.children[child_idx - 1]

        # Move parent key down to child (at front)
        child.keys.insert(0, parent.keys[child_idx - 1])
        child.values.insert(0, parent.values[child_idx - 1])

        # Move last key from left sibling up to parent
        parent.keys[child_idx - 1] = left_sibling.keys.pop()
        parent.values[child_idx - 1] = left_sibling.values.pop()

        # If not leaf, move rightmost child of left sibling to child
        if not child.is_leaf:
            child.children.insert(0, left_sibling.children.pop())

    def _borrow_from_right_sibling(self, parent: BTreeNode, child_idx: int) -> None:
        """Borrow a key from right sibling through parent"""
        child = parent.children[child_idx]
        right_sibling = parent.children[child_idx + 1]

        # Move parent key down to child (at end)
        child.keys.append(parent.keys[child_idx])
        child.values.append(parent.values[child_idx])

        # Move first key from right sibling up to parent
        parent.keys[child_idx] = right_sibling.keys.pop(0)
        parent.values[child_idx] = right_sibling.values.pop(0)

        # If not leaf, move leftmost child of right sibling to child
        if not child.is_leaf:
            child.children.append(right_sibling.children.pop(0))

    def _merge_children(self, parent: BTreeNode, left_idx: int) -> None:
        """
        Merge child at left_idx with child at left_idx+1.
        Parent key between them moves down to merged node.
        """
        left_child = parent.children[left_idx]
        right_child = parent.children[left_idx + 1]

        # Move parent key down to left child
        left_child.keys.append(parent.keys[left_idx])
        left_child.values.append(parent.values[left_idx])

        # Move all keys/values from right child to left child
        left_child.keys.extend(right_child.keys)
        left_child.values.extend(right_child.values)

        # If not leaf, move children too
        if not left_child.is_leaf:
            left_child.children.extend(right_child.children)

        # Remove key and right child from parent
        parent.keys.pop(left_idx)
        parent.values.pop(left_idx)
        parent.children.pop(left_idx + 1)

    def __len__(self) -> int:
        """Return number of keys in the tree"""
        return self.size
    
    def __contains__(self, key: Any) -> bool:
        """Check if key exists in tree"""
        return self.search(key) is not None
    
    def traverse(self) -> Iterator[Tuple[Any, Any]]:
        """
        Traverse the tree in sorted order
        
        Yields:
            (key, value) tuples in ascending key order
        """
        yield from self._traverse_node(self.root)
    
    def _traverse_node(self, node: BTreeNode) -> Iterator[Tuple[Any, Any]]:
        """Recursively traverse node in order"""
        num_keys = len(node.keys)

        for i in range(num_keys):
            # Traverse left child first
            if not node.is_leaf and i < len(node.children):
                yield from self._traverse_node(node.children[i])

            # Yield current key
            yield (node.keys[i], node.values[i])

        # Traverse rightmost child (only if we have keys and children)
        if not node.is_leaf and num_keys < len(node.children):
            yield from self._traverse_node(node.children[num_keys])


# Example usage and testing
if __name__ == "__main__":
    print("Testing B-Tree Implementation")
    print("=" * 50)
    
    # Create B-Tree
    btree = BTree(order=5)
    
    # Insert some data
    print("\nInserting data...")
    data = [(10, "ten"), (20, "twenty"), (5, "five"), (15, "fifteen"), 
            (25, "twenty-five"), (30, "thirty"), (3, "three")]
    
    for key, value in data:
        btree.insert(key, value)
        print(f"  Inserted ({key}, {value})")
    
    print(f"\nTree size: {len(btree)}")
    
    # Search for keys
    print("\nSearching...")
    for key in [5, 15, 100]:
        result = btree.search(key)
        print(f"  Search({key}): {result}")
    
    # Range search
    print("\nRange search [10, 25]:")
    results = btree.range_search(10, 25)
    for key, value in results:
        print(f"  {key}: {value}")
    
    # Traverse in order
    print("\nIn-order traversal:")
    for key, value in btree.traverse():
        print(f"  {key}: {value}")
    
    # Delete
    print("\nDeleting key 15...")
    btree.delete(15)
    print(f"Tree size: {len(btree)}")
    print(f"Search(15): {btree.search(15)}")
