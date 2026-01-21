#!/usr/bin/env python3
"""
SAIQL Index Manager (CE Edition)
================================

Manages all indexes for SAIQL tables and coordinates index selection.
CE Edition: Only B-tree and Hash indexes available.
"""

from typing import Any, List, Optional, Dict, Tuple
from enum import Enum
from dataclasses import dataclass, field
from pathlib import Path

# Import logging (handle potential shadowing by local logging.py)
import logging
if hasattr(logging, 'getLogger'):
    logger = logging.getLogger(__name__)
elif hasattr(logging, 'logger'):
    logger = logging.logger
else:
    class DummyLogger:
        def info(self, msg, **kwargs): print(f"INFO: {msg}")
        def debug(self, msg, **kwargs): pass
        def warning(self, msg, **kwargs): print(f"WARN: {msg}")
        def error(self, msg, **kwargs): print(f"ERROR: {msg}")
    logger = DummyLogger()

# Import index structures
try:
    from .btree import BTree
    from .hash_index import HashIndex
except ImportError:
    from btree import BTree
    from hash_index import HashIndex


class IndexType(Enum):
    """Supported index types (CE Edition)"""
    BTREE = "btree"
    HASH = "hash"


@dataclass
class IndexDefinition:
    """Definition of an index"""
    index_name: str
    table_name: str
    column_name: str
    index_type: IndexType
    is_unique: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)


class Index:
    """Wrapper for an index structure"""

    def __init__(self, definition: IndexDefinition):
        self.definition = definition
        self.firewall = None

        if definition.index_type == IndexType.BTREE:
            self.structure = BTree(order=5)
        elif definition.index_type == IndexType.HASH:
            self.structure = HashIndex(initial_size=1024)
        else:
            raise ValueError(f"Unsupported index type: {definition.index_type}")

        logger.info(f"Created {definition.index_type.value} index: {definition.index_name}")

    def insert(self, key: Any, row_id: Any) -> None:
        """Insert a key-rowid pair into the index.

        Raises ValueError if index is unique and key already exists.
        """
        if self.definition.is_unique:
            existing = self.structure.search(key)
            if existing:
                raise ValueError(
                    f"Duplicate key '{key}' in unique index '{self.definition.index_name}'"
                )
        self.structure.insert(key, row_id)

    def set_firewall(self, firewall: Any) -> None:
        self.firewall = firewall
        if hasattr(self.structure, 'set_firewall'):
            self.structure.set_firewall(firewall)

    def bulk_insert(self, items: List[Tuple[Any, Any]]) -> None:
        """Optimized bulk insertion for better performance.

        Raises ValueError on first duplicate if index is unique.
        """
        for key, row_id in items:
            self.insert(key, row_id)  # Delegates to insert() which checks uniqueness

    def search(self, key: Any) -> List[Any]:
        return self.structure.search(key)

    def range_search(self, min_key: Any, max_key: Any) -> List[Any]:
        if hasattr(self.structure, 'range_search'):
            return self.structure.range_search(min_key, max_key)
        return []

    def delete(self, key: Any, row_id: Any = None) -> bool:
        return self.structure.delete(key, row_id)

    def save(self, dir_path: Path) -> None:
        """Save index to disk.

        NOTE: CE Edition - BTree/HashIndex do not implement save_bundle,
        so this is effectively a no-op. Indexes are rebuilt on startup.
        """
        if hasattr(self.structure, 'save_bundle'):
            self.structure.save_bundle(str(dir_path), self.definition.index_name)
        # CE Edition: no persistence - indexes rebuild on cold start

    @classmethod
    def load(cls, definition: IndexDefinition, dir_path: Path, expected_schema: str = None) -> Optional['Index']:
        """Load index from disk.

        NOTE: CE Edition - BTree/HashIndex do not implement load_bundle,
        so this always returns None. Indexes are rebuilt on startup.
        """
        # CE Edition: BTree/HashIndex lack load_bundle, so warm start is not available
        if definition.index_type == IndexType.BTREE:
            if hasattr(BTree, 'load_bundle'):
                loaded = BTree.load_bundle(str(dir_path), expected_schema)
                if loaded:
                    idx = cls.__new__(cls)
                    idx.definition = definition
                    idx.structure = loaded
                    idx.firewall = None
                    return idx
        elif definition.index_type == IndexType.HASH:
            if hasattr(HashIndex, 'load_bundle'):
                loaded = HashIndex.load_bundle(str(dir_path), expected_schema)
                if loaded:
                    idx = cls.__new__(cls)
                    idx.definition = definition
                    idx.structure = loaded
                    idx.firewall = None
                    return idx
        return None


class IndexManager:
    """Manages all indexes for a database"""

    def __init__(self, storage_path: Optional[Path] = None):
        self.indexes: Dict[str, Index] = {}
        self.storage_path = storage_path
        self._table_indexes: Dict[str, List[str]] = {}

    def _schema_fingerprint(self, table_name: str, column_name: str) -> str:
        return f"{table_name}:{column_name}:v1"

    def create_index(
        self,
        index_name: str,
        table_name: str,
        column_name: str,
        index_type: IndexType = IndexType.BTREE,
        is_unique: bool = False,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Index:
        """Create a new index"""
        if index_name in self.indexes:
            raise ValueError(f"Index {index_name} already exists")

        definition = IndexDefinition(
            index_name=index_name,
            table_name=table_name,
            column_name=column_name,
            index_type=index_type,
            is_unique=is_unique,
            metadata=metadata or {}
        )

        # Try to load from disk first (warm start)
        if self.storage_path:
            bundle_path = self.storage_path / f"{index_name}.idx"
            schema_fingerprint = self._schema_fingerprint(table_name, column_name)
            loaded = Index.load(definition, bundle_path, expected_schema=schema_fingerprint)
            if loaded:
                self.indexes[index_name] = loaded
                if table_name not in self._table_indexes:
                    self._table_indexes[table_name] = []
                self._table_indexes[table_name].append(index_name)
                logger.info(f"WARM START: Loaded index bundle {bundle_path}")
                return loaded
            else:
                logger.info(f"No valid index bundle at {bundle_path}; cold start")

        # Cold start: create new index
        index = Index(definition)
        self.indexes[index_name] = index

        if table_name not in self._table_indexes:
            self._table_indexes[table_name] = []
        self._table_indexes[table_name].append(index_name)

        return index

    def get_index(self, index_name: str) -> Optional[Index]:
        return self.indexes.get(index_name)

    def get_table_indexes(self, table_name: str) -> List[Index]:
        index_names = self._table_indexes.get(table_name, [])
        return [self.indexes[name] for name in index_names if name in self.indexes]

    def select_best_index(
        self,
        table_name: str,
        column_name: str,
        operation: str = "="
    ) -> Optional[Index]:
        """Select the best index for a query operation.

        CE Edition uses B-tree as default for versatility.
        """
        indexes = self.get_table_indexes(table_name)
        if not indexes:
            return None

        # Filter to indexes on the target column
        column_indexes = [
            idx for idx in indexes
            if idx.definition.column_name == column_name
        ]

        if not column_indexes:
            return None

        btree_indexes = [idx for idx in column_indexes if idx.definition.index_type == IndexType.BTREE]
        hash_indexes = [idx for idx in column_indexes if idx.definition.index_type == IndexType.HASH]

        # RULE 1: Range queries - B-tree
        if operation in ('>', '<', '>=', '<=', 'BETWEEN'):
            if btree_indexes:
                logger.debug(f"Selected B-tree for range query ({operation})")
                return btree_indexes[0]

        # RULE 2: Queries requiring ordering - B-tree
        if operation in ('ORDER BY', 'MIN', 'MAX'):
            if btree_indexes:
                logger.debug(f"Selected B-tree for ordered query")
                return btree_indexes[0]

        # RULE 3: Exact equality - Hash is optimal, B-tree fallback
        if operation == '=':
            if hash_indexes:
                logger.debug(f"Selected Hash for equality")
                return hash_indexes[0]
            if btree_indexes:
                logger.debug(f"Selected B-tree for equality (fallback)")
                return btree_indexes[0]

        # RULE 4: IN clause - Hash preferred
        if operation == 'IN':
            if hash_indexes:
                logger.debug(f"Selected Hash for IN clause")
                return hash_indexes[0]
            if btree_indexes:
                logger.debug(f"Selected B-tree for IN clause (fallback)")
                return btree_indexes[0]

        # RULE 5: Default - prefer B-tree for versatility
        if btree_indexes:
            logger.debug(f"Selected B-tree (default for operation: {operation})")
            return btree_indexes[0]

        if hash_indexes:
            logger.debug(f"Selected Hash (fallback)")
            return hash_indexes[0]

        return column_indexes[0] if column_indexes else None

    def drop_index(self, index_name: str) -> bool:
        if index_name not in self.indexes:
            return False

        index = self.indexes[index_name]
        table_name = index.definition.table_name

        del self.indexes[index_name]

        if table_name in self._table_indexes:
            self._table_indexes[table_name] = [
                name for name in self._table_indexes[table_name]
                if name != index_name
            ]

        logger.info(f"Dropped index: {index_name}")
        return True

    def create_hybrid_indexes(
        self,
        table_name: str,
        column_name: str,
        base_name: Optional[str] = None
    ) -> Tuple[Index, Index]:
        """Create both B-tree and Hash indexes for a column.

        This provides optimal performance for both range and equality queries.
        """
        if base_name is None:
            base_name = f"{table_name}_{column_name}"

        btree_idx = self.create_index(
            index_name=f"{base_name}_btree",
            table_name=table_name,
            column_name=column_name,
            index_type=IndexType.BTREE
        )

        hash_idx = self.create_index(
            index_name=f"{base_name}_hash",
            table_name=table_name,
            column_name=column_name,
            index_type=IndexType.HASH
        )

        logger.info(f"Created hybrid indexes (B-tree + Hash) on {table_name}.{column_name}")
        return btree_idx, hash_idx

    def save_all(self) -> None:
        """Save all indexes to disk"""
        if not self.storage_path:
            return
        self.storage_path.mkdir(parents=True, exist_ok=True)
        for index_name, index in self.indexes.items():
            bundle_path = self.storage_path / f"{index_name}.idx"
            index.save(bundle_path)

    def get_stats(self) -> Dict[str, Any]:
        """Get statistics about all indexes"""
        stats = {
            "total_indexes": len(self.indexes),
            "indexes_by_type": {},
            "indexes_by_table": {}
        }

        for index_name, index in self.indexes.items():
            idx_type = index.definition.index_type.value
            table = index.definition.table_name

            stats["indexes_by_type"][idx_type] = stats["indexes_by_type"].get(idx_type, 0) + 1
            stats["indexes_by_table"][table] = stats["indexes_by_table"].get(table, 0) + 1

        return stats
