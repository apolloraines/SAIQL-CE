"""
Atlas CE Engine - Main Orchestrator
====================================

Coordinates ingestion, indexing, and retrieval for Atlas CE RAG system.
"""

import time
from pathlib import Path
from typing import Dict, List, Optional, Any
import logging

from . import AtlasCEConfig
from .lore_chunk import LoreChunk
from .index_manager import AtlasCEIndexManager
from .ingest import IngestOrchestrator, IngestResult
from .retriever import HybridRetriever, FusionConfig, RetrievalTrace

logger = logging.getLogger(__name__)


class AtlasCE:
    """
    Atlas CE - Community Edition Semantic RAG Engine.

    Provides:
    - Document ingestion with chunking
    - Three-lane hybrid retrieval
    - Citation support for grounding

    CE Limitations:
    - Text/Markdown only (no PDF/DOCX)
    - No vision/OCR
    - No citation enforcement
    - No confidence thresholds
    """

    def __init__(self, config: AtlasCEConfig):
        """
        Initialize Atlas CE engine.

        Args:
            config: AtlasCEConfig with namespace, embedding settings, etc.
        """
        self.config = config
        self.namespace = config.namespace

        # Initialize storage path
        self.storage_path: Optional[Path] = None
        if config.storage_path:
            self.storage_path = Path(config.storage_path)
            self.storage_path.mkdir(parents=True, exist_ok=True)

        # Initialize components
        self._index_manager = AtlasCEIndexManager(
            namespace=config.namespace,
            embedding_model=config.embedding_model,
            embedding_dim=config.embedding_dim,
            storage_path=self.storage_path,
        )

        self._ingest = IngestOrchestrator(
            namespace=config.namespace,
            chunk_size=config.chunk_size,
            chunk_overlap=config.chunk_overlap,
            storage_path=self.storage_path,
        )

        self._retriever = HybridRetriever(
            index_manager=self._index_manager,
            fusion_config=FusionConfig(),
            cache_size=config.cache_size,
        )

        # Statistics
        self._stats = {
            'documents_ingested': 0,
            'chunks_created': 0,
            'queries_executed': 0,
            'created_at': time.time(),
        }

        logger.info(f"Atlas CE initialized: namespace={config.namespace}")

    def ingest_text(
        self,
        content: str,
        source_uri: str,
        title: str = "",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> List[LoreChunk]:
        """
        Ingest text content into the RAG system.

        Args:
            content: Text content to ingest
            source_uri: Source identifier (file path or URL)
            title: Document title
            metadata: Additional metadata (doc_type, tags, security_level, etc.)

        Returns:
            List of created LoreChunk objects
        """
        chunks, result = self._ingest.ingest_text(
            content=content,
            source_uri=source_uri,
            title=title,
            metadata=metadata,
        )

        # Delete old chunks on re-ingest (prevents stale content accumulation)
        if result.old_chunk_ids:
            for chunk_id in result.old_chunk_ids:
                self._index_manager.remove_chunk(chunk_id)
            logger.debug(f"Removed {len(result.old_chunk_ids)} old chunks for re-ingest")

        # Add new chunks to index
        for chunk in chunks:
            self._index_manager.add_chunk(chunk)

        # Update stats
        self._stats['documents_ingested'] += 1
        self._stats['chunks_created'] += len(chunks)

        logger.info(
            f"Ingested '{source_uri}': {len(chunks)} chunks "
            f"(new={result.is_new_document}, old_removed={len(result.old_chunk_ids)})"
        )

        return chunks

    def ingest_file(
        self,
        file_path: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> List[LoreChunk]:
        """
        Ingest a file into the RAG system.

        CE Edition: Only .txt and .md files supported.

        Args:
            file_path: Path to file
            metadata: Additional metadata

        Returns:
            List of created LoreChunk objects
        """
        chunks, result = self._ingest.ingest_file(
            file_path=Path(file_path),
            metadata=metadata,
        )

        # Delete old chunks on re-ingest (prevents stale content accumulation)
        if result.old_chunk_ids:
            for chunk_id in result.old_chunk_ids:
                self._index_manager.remove_chunk(chunk_id)
            logger.debug(f"Removed {len(result.old_chunk_ids)} old chunks for re-ingest")

        # Add new chunks to index
        for chunk in chunks:
            self._index_manager.add_chunk(chunk)

        # Update stats
        self._stats['documents_ingested'] += 1
        self._stats['chunks_created'] += len(chunks)

        return chunks

    def query(
        self,
        query_text: str,
        top_k: int = 5,
        filters: Optional[Dict[str, Any]] = None,
        include_scores: bool = False,
    ) -> List[LoreChunk]:
        """
        Query the RAG system for relevant chunks.

        Args:
            query_text: Search query
            top_k: Number of results to return
            filters: Metadata filters (doc_type, tags, etc.)
            include_scores: If True, return (chunk, score) tuples

        Returns:
            List of LoreChunk objects (or tuples with scores)
        """
        results = self._retriever.retrieve(
            query=query_text,
            top_k=top_k,
            filters=filters,
        )

        self._stats['queries_executed'] += 1

        if include_scores:
            return results
        else:
            return [chunk for chunk, score in results]

    def delete_document(self, source_uri: str) -> int:
        """
        Delete a document and its chunks.

        Args:
            source_uri: Source identifier of document to delete

        Returns:
            Number of chunks deleted
        """
        # Get chunks to delete
        doc_id = self._ingest._compute_doc_id(source_uri)
        chunks_to_delete = [
            chunk_id for chunk_id, chunk in self._index_manager._chunks.items()
            if chunk.doc_id == doc_id
        ]

        # Remove from index
        for chunk_id in chunks_to_delete:
            self._index_manager.remove_chunk(chunk_id)

        # Remove from ingest state
        self._ingest.delete_document(source_uri)

        logger.info(f"Deleted document '{source_uri}': {len(chunks_to_delete)} chunks")
        return len(chunks_to_delete)

    def get_chunk(self, chunk_id: str) -> Optional[LoreChunk]:
        """Get a specific chunk by ID."""
        return self._index_manager.get_chunk(chunk_id)

    def get_last_trace(self) -> Optional[RetrievalTrace]:
        """Get the trace from the last query operation."""
        return self._retriever.get_last_trace()

    def get_stats(self) -> Dict[str, Any]:
        """Get engine statistics."""
        index_stats = self._index_manager.get_stats()
        ingest_stats = self._ingest.get_stats()
        cache_stats = self._retriever.get_cache_stats()

        return {
            'namespace': self.namespace,
            'edition': 'ce',
            'enabled': self.config.enabled,
            'documents_ingested': self._stats['documents_ingested'],
            'chunks_created': self._stats['chunks_created'],
            'chunks_indexed': index_stats.total_chunks,
            'queries_executed': self._stats['queries_executed'],
            'uptime_seconds': time.time() - self._stats['created_at'],
            'index': {
                'total_chunks': index_stats.total_chunks,
                'total_documents': index_stats.total_documents,
                'avg_chunk_length': index_stats.avg_chunk_length,
            },
            'ingest': ingest_stats,
            'cache': cache_stats,
            'config': {
                'embedding_model': self.config.embedding_model,
                'chunk_size': self.config.chunk_size,
                'chunk_overlap': self.config.chunk_overlap,
            },
        }

    def clear_cache(self) -> None:
        """Clear the query cache."""
        self._retriever.clear_cache()

    def close(self) -> None:
        """Close resources and cleanup."""
        self._index_manager.close()
        logger.info(f"Atlas CE closed: namespace={self.namespace}")
