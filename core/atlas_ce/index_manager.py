"""
Atlas CE Index Manager - Three-Lane Index System
=================================================

Manages metadata, lexical (BM25), and vector indexes for hybrid retrieval.
"""

import hashlib
import json
import math
import sqlite3
import unicodedata
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Any
import logging

try:
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:
    np = None
    NUMPY_AVAILABLE = False

from .lore_chunk import LoreChunk, ChunkMetadata, SecurityLevel

logger = logging.getLogger(__name__)


@dataclass
class IndexStats:
    """Statistics about the index."""
    total_chunks: int = 0
    total_documents: int = 0
    namespaces: Set[str] = field(default_factory=set)
    doc_types: Set[str] = field(default_factory=set)
    avg_chunk_length: float = 0.0


class MetadataIndex:
    """
    Metadata filtering index using inverted indexes.

    Supports filtering by: namespace, tenant, doc_type, security_level, language, tags.
    """

    def __init__(self):
        # field_name -> value -> set of chunk_ids
        self._index: Dict[str, Dict[str, Set[str]]] = defaultdict(lambda: defaultdict(set))
        self._chunks: Dict[str, ChunkMetadata] = {}

    def add(self, chunk_id: str, metadata: ChunkMetadata) -> None:
        """Add chunk metadata to index."""
        self._chunks[chunk_id] = metadata

        # Index each field
        self._index['namespace'][metadata.namespace].add(chunk_id)
        if metadata.tenant:
            self._index['tenant'][metadata.tenant].add(chunk_id)
        if metadata.doc_type:
            self._index['doc_type'][metadata.doc_type].add(chunk_id)
        self._index['security_level'][metadata.security_level.value].add(chunk_id)
        if metadata.language:
            self._index['language'][metadata.language].add(chunk_id)
        for tag in metadata.tags:
            self._index['tags'][tag].add(chunk_id)

    def remove(self, chunk_id: str) -> None:
        """Remove chunk from index."""
        if chunk_id not in self._chunks:
            return

        metadata = self._chunks[chunk_id]

        # Remove from each field index
        self._index['namespace'][metadata.namespace].discard(chunk_id)
        if metadata.tenant:
            self._index['tenant'][metadata.tenant].discard(chunk_id)
        if metadata.doc_type:
            self._index['doc_type'][metadata.doc_type].discard(chunk_id)
        self._index['security_level'][metadata.security_level.value].discard(chunk_id)
        if metadata.language:
            self._index['language'][metadata.language].discard(chunk_id)
        for tag in metadata.tags:
            self._index['tags'][tag].discard(chunk_id)

        del self._chunks[chunk_id]

    def filter(self, filters: Dict[str, Any], namespace: str) -> Set[str]:
        """
        Filter chunks by metadata.

        Logic: AND across fields, OR within multi-value fields.
        Namespace is always required.
        """
        # Start with all chunks in namespace
        result = self._index['namespace'].get(namespace, set()).copy()
        if not result:
            return set()

        for field_name, value in filters.items():
            if field_name == 'namespace':
                continue  # Already filtered

            if isinstance(value, list):
                # OR within field: union of all matching values
                field_matches = set()
                for v in value:
                    field_matches |= self._index[field_name].get(str(v), set())
                result &= field_matches
            else:
                # Single value filter
                result &= self._index[field_name].get(str(value), set())

            if not result:
                break  # Short-circuit

        return result


class LexicalIndex:
    """
    BM25-based lexical search using SQLite FTS5.

    Provides deterministic, cross-platform text search.
    """

    def __init__(self, db_path: Optional[Path] = None):
        self.db_path = db_path or ":memory:"
        self._conn: Optional[sqlite3.Connection] = None
        self._initialize_db()

    def _initialize_db(self) -> None:
        """Initialize SQLite FTS5 tables."""
        self._conn = sqlite3.connect(str(self.db_path))
        self._conn.execute("""
            CREATE VIRTUAL TABLE IF NOT EXISTS chunks_fts
            USING fts5(
                chunk_id,
                content_normalized,
                tokenize='unicode61'
            )
        """)
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS chunks_meta (
                chunk_id TEXT PRIMARY KEY,
                content TEXT,
                doc_length INTEGER
            )
        """)
        self._conn.commit()

    def _normalize_text(self, text: str) -> str:
        """Normalize text for deterministic indexing."""
        # NFKC normalization + lowercase
        text = unicodedata.normalize('NFKC', text.lower())
        # Remove diacritics
        text = ''.join(
            c for c in unicodedata.normalize('NFD', text)
            if unicodedata.category(c) != 'Mn'
        )
        return text

    def add(self, chunk_id: str, content: str) -> None:
        """Add chunk to lexical index."""
        normalized = self._normalize_text(content)
        doc_length = len(normalized.split())

        self._conn.execute(
            "INSERT OR REPLACE INTO chunks_fts (chunk_id, content_normalized) VALUES (?, ?)",
            (chunk_id, normalized)
        )
        self._conn.execute(
            "INSERT OR REPLACE INTO chunks_meta (chunk_id, content, doc_length) VALUES (?, ?, ?)",
            (chunk_id, content, doc_length)
        )
        self._conn.commit()

    def remove(self, chunk_id: str) -> None:
        """Remove chunk from lexical index."""
        self._conn.execute("DELETE FROM chunks_fts WHERE chunk_id = ?", (chunk_id,))
        self._conn.execute("DELETE FROM chunks_meta WHERE chunk_id = ?", (chunk_id,))
        self._conn.commit()

    def search(
        self,
        query: str,
        candidate_ids: Optional[Set[str]] = None,
        top_k: int = 100
    ) -> List[Tuple[str, float]]:
        """
        Search using BM25 ranking.

        Returns list of (chunk_id, bm25_score) tuples.
        """
        normalized_query = self._normalize_text(query)

        # Guard against empty/whitespace queries (FTS5 MATCH crashes on empty)
        if not normalized_query or not normalized_query.strip():
            logger.debug("Empty query, returning no lexical results")
            return []

        # FTS5 BM25 search
        try:
            cursor = self._conn.execute("""
                SELECT chunk_id, bm25(chunks_fts) as score
                FROM chunks_fts
                WHERE chunks_fts MATCH ?
                ORDER BY score
                LIMIT ?
            """, (normalized_query, top_k * 2))  # Get extra for filtering
        except Exception as e:
            logger.debug(f"FTS5 search error (query may be invalid): {e}")
            return []

        results = []
        for chunk_id, score in cursor:
            if candidate_ids is None or chunk_id in candidate_ids:
                # BM25 returns negative scores (lower is better), convert to positive
                results.append((chunk_id, -score))
                if len(results) >= top_k:
                    break

        return results

    def close(self) -> None:
        """Close database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None


class VectorIndex:
    """
    Vector similarity index using numpy.

    Stores normalized embeddings for cosine similarity search.
    """

    def __init__(self, embedding_dim: int = 384, embedding_model: str = "all-MiniLM-L6-v2"):
        self.embedding_dim = embedding_dim
        self.embedding_model = embedding_model
        self._vectors: Dict[str, Any] = {}  # chunk_id -> numpy array
        self._embedder = None

    def _get_embedder(self):
        """Lazy load sentence-transformers embedder."""
        if self._embedder is None:
            try:
                from sentence_transformers import SentenceTransformer
                self._embedder = SentenceTransformer(self.embedding_model)
                logger.info(f"Loaded embedding model: {self.embedding_model}")
            except ImportError:
                logger.warning("sentence-transformers not available; vector search disabled")
                return None
        return self._embedder

    def embed(self, text: str) -> Optional[Any]:
        """Generate embedding for text."""
        embedder = self._get_embedder()
        if embedder is None or not NUMPY_AVAILABLE:
            return None

        embedding = embedder.encode(text, show_progress_bar=False)
        # Normalize for cosine similarity
        norm = np.linalg.norm(embedding)
        if norm > 0:
            embedding = embedding / norm
        return embedding

    def add(self, chunk_id: str, embedding: Any) -> None:
        """Add pre-computed embedding to index."""
        if embedding is not None and NUMPY_AVAILABLE:
            # Ensure normalized
            embedding = np.array(embedding)
            norm = np.linalg.norm(embedding)
            if norm > 0:
                embedding = embedding / norm
            self._vectors[chunk_id] = embedding

    def add_text(self, chunk_id: str, content: str) -> None:
        """Compute and add embedding for text."""
        embedding = self.embed(content)
        if embedding is not None:
            self._vectors[chunk_id] = embedding

    def remove(self, chunk_id: str) -> None:
        """Remove chunk from vector index."""
        self._vectors.pop(chunk_id, None)

    def search(
        self,
        query: str,
        candidate_ids: Optional[Set[str]] = None,
        top_k: int = 50
    ) -> List[Tuple[str, float]]:
        """
        Search by vector similarity.

        Returns list of (chunk_id, similarity_score) tuples.
        """
        if not NUMPY_AVAILABLE or not self._vectors:
            return []

        query_embedding = self.embed(query)
        if query_embedding is None:
            return []

        # Compute similarities
        scores = []
        search_ids = candidate_ids if candidate_ids else self._vectors.keys()

        for chunk_id in search_ids:
            if chunk_id in self._vectors:
                similarity = float(np.dot(query_embedding, self._vectors[chunk_id]))
                # Normalize to 0-1 range (cosine similarity is -1 to 1)
                similarity = (similarity + 1) / 2
                scores.append((chunk_id, similarity))

        # Sort by similarity descending, chunk_id ascending for determinism
        scores.sort(key=lambda x: (-x[1], x[0]))
        return scores[:top_k]


class AtlasCEIndexManager:
    """
    Three-lane index manager for Atlas CE.

    Coordinates metadata, lexical, and vector indexes.
    """

    def __init__(
        self,
        namespace: str,
        embedding_model: str = "all-MiniLM-L6-v2",
        embedding_dim: int = 384,
        storage_path: Optional[Path] = None,
    ):
        self.namespace = namespace
        self.embedding_model = embedding_model
        self.embedding_dim = embedding_dim
        self.storage_path = storage_path

        # Initialize indexes
        db_path = storage_path / f"{namespace}_fts.db" if storage_path else None
        self.metadata_index = MetadataIndex()
        self.lexical_index = LexicalIndex(db_path)
        self.vector_index = VectorIndex(embedding_dim, embedding_model)

        # Chunk storage
        self._chunks: Dict[str, LoreChunk] = {}

        logger.info(f"AtlasCEIndexManager initialized for namespace: {namespace}")

    def add_chunk(self, chunk: LoreChunk) -> None:
        """Add a chunk to all indexes."""
        self._chunks[chunk.chunk_id] = chunk
        self.metadata_index.add(chunk.chunk_id, chunk.metadata)
        self.lexical_index.add(chunk.chunk_id, chunk.content)

        # Vector embedding (optional)
        if chunk.embedding:
            self.vector_index.add(chunk.chunk_id, chunk.embedding)
        else:
            self.vector_index.add_text(chunk.chunk_id, chunk.content)

    def update_chunk(self, chunk: LoreChunk) -> None:
        """Update an existing chunk."""
        self.remove_chunk(chunk.chunk_id)
        self.add_chunk(chunk)

    def remove_chunk(self, chunk_id: str) -> None:
        """Remove a chunk from all indexes."""
        self.metadata_index.remove(chunk_id)
        self.lexical_index.remove(chunk_id)
        self.vector_index.remove(chunk_id)
        self._chunks.pop(chunk_id, None)

    def get_chunk(self, chunk_id: str) -> Optional[LoreChunk]:
        """Retrieve a chunk by ID."""
        return self._chunks.get(chunk_id)

    def filter_by_metadata(self, filters: Optional[Dict[str, Any]] = None) -> Set[str]:
        """Filter chunks by metadata."""
        filters = filters or {}
        return self.metadata_index.filter(filters, self.namespace)

    def search_lexical(
        self,
        query: str,
        candidate_ids: Optional[Set[str]] = None,
        top_k: int = 100
    ) -> List[Tuple[str, float]]:
        """Search using BM25."""
        return self.lexical_index.search(query, candidate_ids, top_k)

    def search_vector(
        self,
        query: str,
        candidate_ids: Optional[Set[str]] = None,
        top_k: int = 50
    ) -> List[Tuple[str, float]]:
        """Search using vector similarity."""
        return self.vector_index.search(query, candidate_ids, top_k)

    def get_stats(self) -> IndexStats:
        """Get index statistics."""
        doc_ids = set()
        namespaces = set()
        doc_types = set()
        total_length = 0

        for chunk in self._chunks.values():
            doc_ids.add(chunk.doc_id)
            namespaces.add(chunk.metadata.namespace)
            if chunk.metadata.doc_type:
                doc_types.add(chunk.metadata.doc_type)
            total_length += len(chunk.content)

        return IndexStats(
            total_chunks=len(self._chunks),
            total_documents=len(doc_ids),
            namespaces=namespaces,
            doc_types=doc_types,
            avg_chunk_length=total_length / max(len(self._chunks), 1),
        )

    def get_dataset_hash(self) -> str:
        """Compute deterministic hash of all chunks for drift detection."""
        chunk_ids = sorted(self._chunks.keys())
        content = json.dumps(chunk_ids, sort_keys=True)
        return hashlib.sha256(content.encode()).hexdigest()[:16]

    def close(self) -> None:
        """Close resources."""
        self.lexical_index.close()
