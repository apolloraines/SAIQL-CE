"""
Atlas CE Retriever - Hybrid Three-Lane Retrieval
================================================

Deterministic hybrid retrieval combining metadata, lexical (BM25), and vector search.
"""

from dataclasses import dataclass, field
from functools import lru_cache
from typing import Dict, List, Optional, Set, Tuple, Any
import logging

from .lore_chunk import LoreChunk
from .index_manager import AtlasCEIndexManager

logger = logging.getLogger(__name__)


@dataclass
class FusionConfig:
    """Fusion weights for hybrid retrieval."""
    w_meta: float = 0.10   # Metadata match bonus
    w_lex: float = 0.25    # BM25 lexical score
    w_vec: float = 0.65    # Vector similarity score

    def validate(self) -> bool:
        """Ensure weights sum to 1.0."""
        return abs(self.w_meta + self.w_lex + self.w_vec - 1.0) < 0.001


@dataclass
class RetrievalTrace:
    """Audit trail of a retrieval operation."""
    query: str
    filters: Optional[Dict[str, Any]]
    metadata_candidates: int = 0
    lexical_shortlist: int = 0
    vector_refined: int = 0
    final_results: int = 0
    fusion_weights: Dict[str, float] = field(default_factory=dict)
    top_scores: List[Tuple[str, float]] = field(default_factory=list)
    cache_hit: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            'query': self.query,
            'filters': self.filters,
            'metadata_candidates': self.metadata_candidates,
            'lexical_shortlist': self.lexical_shortlist,
            'vector_refined': self.vector_refined,
            'final_results': self.final_results,
            'fusion_weights': self.fusion_weights,
            'top_scores': self.top_scores,
            'cache_hit': self.cache_hit,
        }


class HybridRetriever:
    """
    Three-lane hybrid retrieval with deterministic fusion.

    Pipeline:
    1. Metadata filter → candidate_ids
    2. BM25 lexical search → lexical_scores
    3. Vector similarity search → vector_scores
    4. Weighted fusion → final_scores
    5. Sort by (-score, chunk_id) for determinism
    """

    def __init__(
        self,
        index_manager: AtlasCEIndexManager,
        fusion_config: Optional[FusionConfig] = None,
        cache_size: int = 100,
    ):
        self.index = index_manager
        self.fusion = fusion_config or FusionConfig()
        self.cache_size = cache_size

        # Query cache
        self._cache: Dict[str, List[Tuple[LoreChunk, float]]] = {}
        self._cache_order: List[str] = []

        # Last trace for debugging
        self._last_trace: Optional[RetrievalTrace] = None

        if not self.fusion.validate():
            logger.warning("Fusion weights do not sum to 1.0")

    def _cache_key(self, query: str, top_k: int, filters: Optional[Dict]) -> str:
        """Generate cache key."""
        filter_str = str(sorted(filters.items())) if filters else ""
        return f"{query}|{top_k}|{filter_str}"

    def _cache_get(self, key: str) -> Optional[List[Tuple[LoreChunk, float]]]:
        """Get from LRU cache."""
        return self._cache.get(key)

    def _cache_put(self, key: str, value: List[Tuple[LoreChunk, float]]) -> None:
        """Put in LRU cache with eviction."""
        if key in self._cache:
            self._cache_order.remove(key)
        elif len(self._cache) >= self.cache_size:
            # Evict oldest
            oldest = self._cache_order.pop(0)
            del self._cache[oldest]

        self._cache[key] = value
        self._cache_order.append(key)

    def retrieve(
        self,
        query: str,
        top_k: int = 5,
        filters: Optional[Dict[str, Any]] = None,
        use_cache: bool = True,
    ) -> List[Tuple[LoreChunk, float]]:
        """
        Retrieve relevant chunks for a query.

        Args:
            query: Search query
            top_k: Number of results to return
            filters: Metadata filters (doc_type, tags, etc.)
            use_cache: Whether to use query cache

        Returns:
            List of (chunk, score) tuples sorted by relevance
        """
        # Initialize trace
        trace = RetrievalTrace(
            query=query,
            filters=filters,
            fusion_weights={
                'w_meta': self.fusion.w_meta,
                'w_lex': self.fusion.w_lex,
                'w_vec': self.fusion.w_vec,
            },
        )

        # Check cache
        cache_key = self._cache_key(query, top_k, filters)
        if use_cache:
            cached = self._cache_get(cache_key)
            if cached is not None:
                trace.cache_hit = True
                trace.final_results = len(cached)
                self._last_trace = trace
                return cached

        # Stage 1: Metadata filtering
        candidate_ids = self.index.filter_by_metadata(filters)
        trace.metadata_candidates = len(candidate_ids)

        if not candidate_ids:
            self._last_trace = trace
            return []

        # Stage 2: Lexical (BM25) search
        lexical_results = self.index.search_lexical(
            query,
            candidate_ids,
            top_k=100  # Get more for fusion
        )
        trace.lexical_shortlist = len(lexical_results)

        # Build score maps
        lexical_scores: Dict[str, float] = {}
        if lexical_results:
            max_lex = max(score for _, score in lexical_results)
            for chunk_id, score in lexical_results:
                lexical_scores[chunk_id] = score / max_lex if max_lex > 0 else 0

        # Get shortlist for vector search
        shortlist_ids = set(lexical_scores.keys()) if lexical_scores else candidate_ids

        # Stage 3: Vector similarity search
        vector_results = self.index.search_vector(
            query,
            shortlist_ids,
            top_k=50
        )
        trace.vector_refined = len(vector_results)

        vector_scores: Dict[str, float] = {}
        for chunk_id, score in vector_results:
            vector_scores[chunk_id] = score

        # Stage 4: Weighted fusion
        all_chunk_ids = set(lexical_scores.keys()) | set(vector_scores.keys())
        fused_scores: List[Tuple[str, float]] = []

        for chunk_id in all_chunk_ids:
            # Metadata score: 1.0 if in candidates, 0.0 otherwise
            meta_score = 1.0 if chunk_id in candidate_ids else 0.0

            # Get individual scores (default 0)
            lex_score = lexical_scores.get(chunk_id, 0.0)
            vec_score = vector_scores.get(chunk_id, 0.0)

            # Compute weighted fusion
            final_score = (
                self.fusion.w_meta * meta_score +
                self.fusion.w_lex * lex_score +
                self.fusion.w_vec * vec_score
            )

            fused_scores.append((chunk_id, final_score))

        # Stage 5: Deterministic sort (score DESC, chunk_id ASC)
        fused_scores.sort(key=lambda x: (-x[1], x[0]))

        # Get top_k results
        results: List[Tuple[LoreChunk, float]] = []
        for chunk_id, score in fused_scores[:top_k]:
            chunk = self.index.get_chunk(chunk_id)
            if chunk:
                results.append((chunk, score))

        # Update trace
        trace.final_results = len(results)
        trace.top_scores = [(chunk_id, score) for chunk_id, score in fused_scores[:top_k]]
        self._last_trace = trace

        # Cache results
        if use_cache:
            self._cache_put(cache_key, results)

        logger.debug(
            f"Retrieved {len(results)} chunks for query '{query[:50]}...' "
            f"(meta={trace.metadata_candidates}, lex={trace.lexical_shortlist}, "
            f"vec={trace.vector_refined})"
        )

        return results

    def get_last_trace(self) -> Optional[RetrievalTrace]:
        """Get the trace from the last retrieval operation."""
        return self._last_trace

    def clear_cache(self) -> None:
        """Clear the query cache."""
        self._cache.clear()
        self._cache_order.clear()

    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        return {
            'size': len(self._cache),
            'max_size': self.cache_size,
            'utilization': len(self._cache) / self.cache_size if self.cache_size > 0 else 0,
        }
