"""
CE Mode Grounding - Basic Output Validation
============================================

Provides informational grounding status without enforcement.

CE Edition differences from Enterprise:
- Citations are GENERATED but not ENFORCED
- No confidence thresholds
- No automatic refusal on low confidence
- Grounding status is informational only

Enterprise Edition provides:
- Citation enforcement (responses rejected without sources)
- Configurable confidence thresholds
- Automatic refusal when evidence insufficient
- Advanced validation rules
"""

from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Any
import logging

logger = logging.getLogger(__name__)

__all__ = [
    'GroundedResponseCE',
    'CEModeGrounding',
    'GroundingStatus',
]


class GroundingStatus:
    """Grounding status constants."""
    GROUNDED = "GROUNDED"       # 3+ evidence chunks
    PARTIAL = "PARTIAL"         # 1-2 evidence chunks
    UNGROUNDED = "UNGROUNDED"   # No evidence


@dataclass
class GroundedResponseCE:
    """
    CE grounding result - informational only, not enforced.

    Unlike Enterprise Edition, CE does not reject responses
    based on grounding status. The status is provided for
    informational purposes and downstream processing.
    """
    answer_text: str
    source_query: str
    source_data: List[Dict[str, Any]] = field(default_factory=list)
    citations: List[str] = field(default_factory=list)
    chunk_count: int = 0
    grounding_status: str = GroundingStatus.UNGROUNDED

    # CE Edition: These are informational only
    # Enterprise would have: confidence_score, enforcement_action, refusal_reason

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary."""
        return asdict(self)

    @property
    def is_grounded(self) -> bool:
        """Check if response has any grounding."""
        return self.grounding_status != GroundingStatus.UNGROUNDED

    @property
    def citation_string(self) -> str:
        """Get citations as comma-separated string."""
        return ", ".join(self.citations) if self.citations else "(no citations)"

    def to_loretoken(self) -> str:
        """Format as LoreToken for context injection."""
        citations_xml = "\n".join(f"  <cite>{c}</cite>" for c in self.citations)
        return f"""<grounded status="{self.grounding_status}" chunks="{self.chunk_count}">
<answer>{self.answer_text}</answer>
<citations>
{citations_xml}
</citations>
</grounded>"""


class CEModeGrounding:
    """
    CE Mode Grounding - Basic output validation.

    Provides:
    - Result citation generation
    - Simple evidence linking
    - Informational grounding status

    NOT included (Enterprise features):
    - Citation enforcement
    - Confidence thresholds
    - Automatic refusal on low confidence
    - Advanced validation rules
    """

    def __init__(self, atlas: Optional[Any] = None):
        """
        Initialize CE Mode Grounding.

        Args:
            atlas: Optional AtlasCE instance for retrieval
        """
        self.atlas = atlas
        self._stats = {
            'total_checks': 0,
            'grounded': 0,
            'partial': 0,
            'ungrounded': 0,
        }

        logger.info("CE Mode Grounding initialized (informational only)")

    def ground_response(
        self,
        response_text: str,
        query: str,
        retrieved_chunks: Optional[List[Any]] = None,
    ) -> GroundedResponseCE:
        """
        Link response to evidence (informational only).

        CE Edition does NOT enforce grounding - the status is
        informational and the response is always returned.

        Args:
            response_text: The response to ground
            query: Original query
            retrieved_chunks: List of LoreChunk objects from retrieval

        Returns:
            GroundedResponseCE with informational status
        """
        self._stats['total_checks'] += 1

        # Extract citations from chunks
        citations = []
        source_data = []

        if retrieved_chunks:
            for chunk in retrieved_chunks:
                # Handle both LoreChunk objects and dicts
                if hasattr(chunk, 'citation_string'):
                    citations.append(chunk.citation_string)
                    source_data.append(chunk.to_dict() if hasattr(chunk, 'to_dict') else {})
                elif isinstance(chunk, dict):
                    if 'citation_string' in chunk:
                        citations.append(chunk['citation_string'])
                    elif 'source_uri' in chunk:
                        citations.append(chunk['source_uri'])
                    source_data.append(chunk)

        # Determine grounding status (informational only)
        chunk_count = len(citations)
        if chunk_count >= 3:
            status = GroundingStatus.GROUNDED
            self._stats['grounded'] += 1
        elif chunk_count >= 1:
            status = GroundingStatus.PARTIAL
            self._stats['partial'] += 1
        else:
            status = GroundingStatus.UNGROUNDED
            self._stats['ungrounded'] += 1

        logger.debug(
            f"Grounding check: status={status}, citations={chunk_count}"
        )

        return GroundedResponseCE(
            answer_text=response_text,
            source_query=query,
            source_data=source_data,
            citations=citations,
            chunk_count=chunk_count,
            grounding_status=status,
        )

    def ground_with_retrieval(
        self,
        response_text: str,
        query: str,
        top_k: int = 5,
        filters: Optional[Dict[str, Any]] = None,
    ) -> GroundedResponseCE:
        """
        Retrieve evidence and ground response in one call.

        Requires atlas instance to be configured.

        Args:
            response_text: The response to ground
            query: Query for retrieval
            top_k: Number of chunks to retrieve
            filters: Metadata filters for retrieval

        Returns:
            GroundedResponseCE with retrieved evidence
        """
        if self.atlas is None:
            logger.warning("Atlas not configured; returning ungrounded response")
            return self.ground_response(response_text, query, None)

        # Retrieve evidence
        chunks = self.atlas.query(query, top_k=top_k, filters=filters)

        # Ground with retrieved chunks
        return self.ground_response(response_text, query, chunks)

    def get_stats(self) -> Dict[str, Any]:
        """Get grounding statistics."""
        total = self._stats['total_checks']
        return {
            'edition': 'ce',
            'mode': 'informational',  # CE does not enforce
            'total_checks': total,
            'grounded': self._stats['grounded'],
            'partial': self._stats['partial'],
            'ungrounded': self._stats['ungrounded'],
            'grounding_rate': (
                (self._stats['grounded'] + self._stats['partial']) / total
                if total > 0 else 0.0
            ),
        }


# Convenience function for simple grounding
def ground(
    response_text: str,
    query: str,
    evidence: Optional[List[Any]] = None,
) -> GroundedResponseCE:
    """
    Simple grounding function (stateless).

    Args:
        response_text: Response to ground
        query: Original query
        evidence: List of evidence chunks

    Returns:
        GroundedResponseCE
    """
    grounding = CEModeGrounding()
    return grounding.ground_response(response_text, query, evidence)
