"""
Atlas CE - Community Edition Semantic RAG
==========================================

Lightweight RAG (Retrieval-Augmented Generation) for SAIQL Community Edition.

Provides:
- Three-lane hybrid retrieval (metadata + BM25 + vector)
- LoreChunk with citations for source attribution
- Incremental document ingestion
- Deterministic, reproducible results

NOT included (Enterprise features):
- Vision/CLIP embeddings
- OCR extraction
- PDF/DOCX/HTML extraction (text/markdown only)
- Citation enforcement
- Confidence thresholds
- Proof bundles with secret scanning
- Advanced safety with redaction

Dependencies:
- sentence-transformers (Apache 2.0) - for embeddings
- numpy (BSD) - for vector operations
- SQLite FTS5 (public domain) - for lexical search
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Dict, Any
import logging

logger = logging.getLogger(__name__)

__all__ = [
    'AtlasCEConfig',
    'AtlasCE',
    'LoreChunk',
    'ChunkCitation',
    'ChunkMetadata',
    'enable_atlas_ce',
    'get_atlas_ce',
    'is_atlas_ce_enabled',
]


@dataclass
class AtlasCEConfig:
    """Configuration for Atlas CE RAG system."""

    enabled: bool = False
    namespace: str = "atlas_ce_default"

    # Embedding settings
    embedding_model: str = "all-MiniLM-L6-v2"  # 384-dim, Apache 2.0 license
    embedding_dim: int = 384

    # Chunking settings
    chunk_size: int = 512
    chunk_overlap: int = 64

    # Safety (simplified in CE)
    safety_enabled: bool = False  # Off by default in CE

    # Persistence
    storage_path: Optional[Path] = None

    # Retrieval settings
    default_top_k: int = 5
    cache_size: int = 100  # LRU cache for queries

    def __post_init__(self):
        if self.storage_path and isinstance(self.storage_path, str):
            self.storage_path = Path(self.storage_path)


# Module-level singleton
_atlas_ce_instance: Optional['AtlasCE'] = None
_atlas_ce_config: Optional[AtlasCEConfig] = None


def enable_atlas_ce(config: AtlasCEConfig) -> None:
    """Enable Atlas CE with the given configuration."""
    global _atlas_ce_config, _atlas_ce_instance
    _atlas_ce_config = config
    _atlas_ce_instance = None  # Reset instance
    if config.enabled:
        logger.info(f"Atlas CE enabled with namespace: {config.namespace}")


def get_atlas_ce() -> 'AtlasCE':
    """Get the Atlas CE singleton instance."""
    global _atlas_ce_instance, _atlas_ce_config

    if _atlas_ce_config is None or not _atlas_ce_config.enabled:
        raise RuntimeError("Atlas CE is not enabled. Call enable_atlas_ce() first.")

    if _atlas_ce_instance is None:
        from .atlas_engine import AtlasCE
        _atlas_ce_instance = AtlasCE(_atlas_ce_config)

    return _atlas_ce_instance


def is_atlas_ce_enabled() -> bool:
    """Check if Atlas CE is enabled."""
    return _atlas_ce_config is not None and _atlas_ce_config.enabled


# Lazy imports to avoid circular dependencies
def __getattr__(name: str):
    if name == 'AtlasCE':
        from .atlas_engine import AtlasCE
        return AtlasCE
    elif name == 'LoreChunk':
        from .lore_chunk import LoreChunk
        return LoreChunk
    elif name == 'ChunkCitation':
        from .lore_chunk import ChunkCitation
        return ChunkCitation
    elif name == 'ChunkMetadata':
        from .lore_chunk import ChunkMetadata
        return ChunkMetadata
    raise AttributeError(f"module 'atlas_ce' has no attribute '{name}'")
