#!/usr/bin/env python3
"""
QIPI-Lite — CE-safe deterministic text retrieval

Provides:
- SQLite FTS5 based full-text search
- Deterministic ranking with BM25 + doc_id tie-breaker
- Python-side text normalization for cross-platform consistency
- add_doc, delete_doc, search, rebuild operations

NOT included (CE restrictions):
- Advanced QIPI indexing
- Hybrid fusion
- Vector search
- GPU acceleration

Backend: SQLite FTS5 with 'unicode61' tokenizer
"""

import sqlite3
import unicodedata
import hashlib
import json
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field
from pathlib import Path
from contextlib import contextmanager


class QPILiteError(Exception):
    """Base exception for QIPI-Lite operations."""
    pass


class QIPILiteIndexError(QPILiteError):
    """Raised when index operations fail."""
    pass


@dataclass
class SearchResult:
    """Search result with score and metadata."""
    doc_id: str
    score: float
    content: str
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class QIPILiteConfig:
    """Configuration for QIPI-Lite index."""
    db_path: str = ":memory:"  # Default to in-memory
    table_name: str = "qipi_docs"


class QIPILite:
    """
    QIPI-Lite: Deterministic SQLite FTS5-based text retrieval.

    Features:
    - Python-side text normalization for cross-platform consistency
    - BM25 ranking with doc_id tie-breaker for determinism
    - Stable index hashing for proof bundles

    Example:
        >>> index = QIPILite()
        >>> index.add_doc("doc1", "Hello world", {"source": "test"})
        >>> results = index.search("hello")
        >>> print(results[0].doc_id)  # "doc1"
    """

    def __init__(self, config: Optional[QIPILiteConfig] = None):
        """
        Initialize QIPI-Lite index.

        Args:
            config: Optional configuration
        """
        self.config = config or QIPILiteConfig()
        self.db_path = self.config.db_path
        self.table_name = self.config.table_name
        self._conn: Optional[sqlite3.Connection] = None
        self._init_db()

    def _init_db(self) -> None:
        """Initialize database and FTS5 table."""
        self._conn = sqlite3.connect(self.db_path)
        self._conn.row_factory = sqlite3.Row

        # Create docs table for metadata
        self._conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.table_name}_docs (
                doc_id TEXT PRIMARY KEY,
                content TEXT NOT NULL,
                content_normalized TEXT NOT NULL,
                metadata_json TEXT DEFAULT '{{}}'
            )
        """)

        # Create FTS5 virtual table with 'unicode61' tokenizer
        # We normalize text in Python for cross-platform consistency
        self._conn.execute(f"""
            CREATE VIRTUAL TABLE IF NOT EXISTS {self.table_name}_fts
            USING fts5(
                doc_id,
                content_normalized,
                tokenize='unicode61'
            )
        """)

        self._conn.commit()

    @contextmanager
    def _transaction(self):
        """Context manager for transactions."""
        try:
            yield
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise

    def normalize_text(self, text: str) -> str:
        """
        Normalize text for deterministic indexing.

        Applies:
        - Unicode NFKC normalization
        - Lowercase conversion
        - Diacritic removal

        This ensures identical results across platforms regardless
        of SQLite's built-in tokenizer behavior.

        Args:
            text: Input text

        Returns:
            Normalized text
        """
        # NFKC normalization + lowercase
        text = unicodedata.normalize('NFKC', text.lower())

        # Remove diacritics
        text = ''.join(
            c for c in unicodedata.normalize('NFD', text)
            if unicodedata.category(c) != 'Mn'
        )

        return text

    def add_doc(self,
                doc_id: str,
                content: str,
                metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Add a document to the index.

        Args:
            doc_id: Unique document identifier
            content: Document content
            metadata: Optional metadata dictionary

        Raises:
            IndexError: If document already exists
        """
        normalized = self.normalize_text(content)
        metadata_json = json.dumps(metadata or {}, sort_keys=True)

        with self._transaction():
            # Check if exists
            existing = self._conn.execute(
                f"SELECT doc_id FROM {self.table_name}_docs WHERE doc_id = ?",
                (doc_id,)
            ).fetchone()

            if existing:
                raise QIPILiteIndexError(f"Document already exists: {doc_id}")

            # Insert into docs table
            self._conn.execute(
                f"""INSERT INTO {self.table_name}_docs
                    (doc_id, content, content_normalized, metadata_json)
                    VALUES (?, ?, ?, ?)""",
                (doc_id, content, normalized, metadata_json)
            )

            # Insert into FTS table
            self._conn.execute(
                f"""INSERT INTO {self.table_name}_fts (doc_id, content_normalized)
                    VALUES (?, ?)""",
                (doc_id, normalized)
            )

    def update_doc(self,
                   doc_id: str,
                   content: str,
                   metadata: Optional[Dict[str, Any]] = None) -> bool:
        """
        Update an existing document.

        Args:
            doc_id: Document identifier
            content: New content
            metadata: Optional new metadata

        Returns:
            True if document was updated

        Raises:
            IndexError: If document does not exist
        """
        normalized = self.normalize_text(content)
        metadata_json = json.dumps(metadata or {}, sort_keys=True)

        with self._transaction():
            # Verify document exists
            existing = self._conn.execute(
                f"SELECT doc_id FROM {self.table_name}_docs WHERE doc_id = ?",
                (doc_id,)
            ).fetchone()

            if not existing:
                raise QIPILiteIndexError(f"Document not found: {doc_id}")

            # Update docs table
            self._conn.execute(
                f"""UPDATE {self.table_name}_docs
                    SET content = ?, content_normalized = ?, metadata_json = ?
                    WHERE doc_id = ?""",
                (content, normalized, metadata_json, doc_id)
            )

            # Update FTS table
            self._conn.execute(
                f"""UPDATE {self.table_name}_fts
                    SET content_normalized = ?
                    WHERE doc_id = ?""",
                (normalized, doc_id)
            )

            return True

    def delete_doc(self, doc_id: str) -> bool:
        """
        Delete a document from the index.

        Args:
            doc_id: Document identifier

        Returns:
            True if document was deleted, False if not found
        """
        with self._transaction():
            # Delete from FTS first
            self._conn.execute(
                f"DELETE FROM {self.table_name}_fts WHERE doc_id = ?",
                (doc_id,)
            )

            # Delete from docs table
            cursor = self._conn.execute(
                f"DELETE FROM {self.table_name}_docs WHERE doc_id = ?",
                (doc_id,)
            )

            return cursor.rowcount > 0

    def search(self,
               query: str,
               top_k: int = 10,
               metadata_filter: Optional[Dict[str, Any]] = None) -> List[SearchResult]:
        """
        Search the index.

        Returns results ranked by BM25 with doc_id tie-breaker for determinism.

        Args:
            query: Search query
            top_k: Maximum results to return
            metadata_filter: Optional metadata filter (exact match, applied before limiting)

        Returns:
            List of SearchResult objects, ordered by score descending
        """
        # Normalize query
        normalized_query = self.normalize_text(query)

        if not normalized_query.strip():
            return []

        # FTS5 search with BM25 ranking
        # ORDER BY bm25() ASC because bm25() returns negative values
        # (more negative = better match)
        # Then by doc_id ASC for deterministic tie-breaking

        # When metadata_filter is provided, fetch more rows to ensure we can
        # return top_k results after filtering. Without knowing selectivity,
        # we fetch a larger batch and filter in Python.
        fetch_limit = top_k * 10 if metadata_filter else top_k

        sql = f"""
            SELECT
                d.doc_id,
                d.content,
                d.metadata_json,
                bm25({self.table_name}_fts) as score
            FROM {self.table_name}_fts f
            JOIN {self.table_name}_docs d ON f.doc_id = d.doc_id
            WHERE {self.table_name}_fts MATCH ?
            ORDER BY bm25({self.table_name}_fts) ASC, d.doc_id ASC
            LIMIT ?
        """

        cursor = self._conn.execute(sql, (normalized_query, fetch_limit))
        rows = cursor.fetchall()

        results = []
        for row in rows:
            metadata = json.loads(row['metadata_json'])

            # Apply metadata filter if provided
            if metadata_filter:
                if not all(metadata.get(k) == v for k, v in metadata_filter.items()):
                    continue

            results.append(SearchResult(
                doc_id=row['doc_id'],
                score=abs(row['score']),  # Convert to positive
                content=row['content'],
                metadata=metadata
            ))

            # Stop once we have enough filtered results
            if len(results) >= top_k:
                break

        return results

    def get_doc(self, doc_id: str) -> Optional[SearchResult]:
        """
        Get a document by ID.

        Args:
            doc_id: Document identifier

        Returns:
            SearchResult or None if not found
        """
        cursor = self._conn.execute(
            f"""SELECT doc_id, content, metadata_json
                FROM {self.table_name}_docs WHERE doc_id = ?""",
            (doc_id,)
        )
        row = cursor.fetchone()

        if not row:
            return None

        return SearchResult(
            doc_id=row['doc_id'],
            score=1.0,
            content=row['content'],
            metadata=json.loads(row['metadata_json'])
        )

    def count(self) -> int:
        """Return number of documents in index."""
        cursor = self._conn.execute(
            f"SELECT COUNT(*) FROM {self.table_name}_docs"
        )
        return cursor.fetchone()[0]

    def rebuild(self) -> None:
        """
        Rebuild the FTS index.

        Call this after bulk operations for optimal performance.
        """
        self._conn.execute(f"INSERT INTO {self.table_name}_fts({self.table_name}_fts) VALUES('rebuild')")
        self._conn.commit()

    def index_hash(self) -> str:
        """
        Compute deterministic hash of index contents.

        Returns:
            SHA256 hash of all documents (sorted by doc_id)
        """
        cursor = self._conn.execute(
            f"""SELECT doc_id, content, metadata_json
                FROM {self.table_name}_docs
                ORDER BY doc_id ASC"""
        )

        hasher = hashlib.sha256()
        for row in cursor:
            hasher.update(row['doc_id'].encode('utf-8'))
            hasher.update(row['content'].encode('utf-8'))
            hasher.update(row['metadata_json'].encode('utf-8'))

        return hasher.hexdigest()

    def export(self, path: Path) -> None:
        """
        Export index to JSON file.

        Args:
            path: Output path
        """
        cursor = self._conn.execute(
            f"""SELECT doc_id, content, metadata_json
                FROM {self.table_name}_docs
                ORDER BY doc_id ASC"""
        )

        docs = []
        for row in cursor:
            docs.append({
                'doc_id': row['doc_id'],
                'content': row['content'],
                'metadata': json.loads(row['metadata_json'])
            })

        output = {
            'version': '1.0',
            'count': len(docs),
            'hash': self.index_hash(),
            'documents': docs
        }

        path.write_text(json.dumps(output, indent=2, sort_keys=True))

    def close(self) -> None:
        """Close the database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


# Export public API
__all__ = [
    'QIPILite',
    'QIPILiteConfig',
    'QPILiteError',
    'QIPILiteIndexError',
    'SearchResult',
]
