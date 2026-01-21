"""
LoreChunk - Semantic Data Unit for Atlas CE
============================================

Content-addressable chunks with full citation support for RAG retrieval.
"""

import hashlib
import json
import time
from dataclasses import dataclass, field, asdict
from typing import Optional, Dict, Any, List
from enum import Enum


class SecurityLevel(Enum):
    """Security classification for chunks."""
    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"


@dataclass
class ChunkCitation:
    """Precise source attribution for a chunk."""

    start_offset: int = 0      # Character offset from document start
    end_offset: int = 0        # Character offset of chunk end
    start_line: int = 1        # Line number (1-indexed)
    end_line: int = 1          # End line number
    start_byte: int = 0        # Byte offset for binary safety
    end_byte: int = 0          # End byte offset

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ChunkCitation':
        return cls(**data)

    def __str__(self) -> str:
        return f"lines {self.start_line}-{self.end_line}"


@dataclass
class ChunkMetadata:
    """Rich metadata for chunk filtering."""

    namespace: str = "atlas_ce_default"
    tenant: Optional[str] = None
    doc_type: Optional[str] = None  # markdown, code, config, etc.
    security_level: SecurityLevel = SecurityLevel.PUBLIC
    language: Optional[str] = None  # en, es, code:python, etc.
    tags: List[str] = field(default_factory=list)
    custom: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d['security_level'] = self.security_level.value
        return d

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ChunkMetadata':
        if 'security_level' in data and isinstance(data['security_level'], str):
            data['security_level'] = SecurityLevel(data['security_level'])
        return cls(**data)


@dataclass
class LoreChunk:
    """
    Content-addressable semantic unit for RAG retrieval.

    Each chunk carries complete citation information for source attribution.
    The chunk_id is content-addressable (derived from content hash).
    """

    # Identity
    doc_id: str = ""           # Stable: hash(namespace + source_uri)
    chunk_id: str = ""         # Content-addressable: hash(doc_id + index + content_hash)
    chunk_index: int = 0       # Position within document

    # Source
    source_uri: str = ""       # Original file path or URL
    title: str = ""            # Section heading or document title

    # Content
    content: str = ""          # Text content
    content_hash: str = ""     # SHA256 for change detection

    # Attribution
    citation: ChunkCitation = field(default_factory=ChunkCitation)
    metadata: ChunkMetadata = field(default_factory=ChunkMetadata)

    # Versioning
    version: int = 1           # Monotonic version counter
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)

    # Embedding (optional, computed on demand)
    embedding: Optional[List[float]] = None

    def __post_init__(self):
        # Compute hashes if not provided
        if not self.content_hash and self.content:
            self.content_hash = self._compute_content_hash()
        if not self.doc_id and self.source_uri:
            self.doc_id = self._compute_doc_id()
        if not self.chunk_id and self.doc_id:
            self.chunk_id = self._compute_chunk_id()

    def _compute_content_hash(self) -> str:
        """SHA256 hash of content for change detection."""
        return hashlib.sha256(self.content.encode('utf-8')).hexdigest()[:16]

    def _compute_doc_id(self) -> str:
        """Stable document ID from namespace + source_uri."""
        key = f"{self.metadata.namespace}:{self.source_uri}"
        return hashlib.sha256(key.encode('utf-8')).hexdigest()[:16]

    def _compute_chunk_id(self) -> str:
        """Content-addressable chunk ID."""
        key = f"{self.doc_id}:{self.chunk_index}:{self.content_hash}"
        return hashlib.sha256(key.encode('utf-8')).hexdigest()[:16]

    @property
    def citation_string(self) -> str:
        """Human-readable citation (e.g., 'docs/readme.md:10-25')."""
        if self.citation.start_line == self.citation.end_line:
            return f"{self.source_uri}:{self.citation.start_line}"
        return f"{self.source_uri}:{self.citation.start_line}-{self.citation.end_line}"

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary."""
        return {
            'doc_id': self.doc_id,
            'chunk_id': self.chunk_id,
            'chunk_index': self.chunk_index,
            'source_uri': self.source_uri,
            'title': self.title,
            'content': self.content,
            'content_hash': self.content_hash,
            'citation': self.citation.to_dict(),
            'metadata': self.metadata.to_dict(),
            'version': self.version,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'embedding': self.embedding,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'LoreChunk':
        """Deserialize from dictionary."""
        data = data.copy()
        if 'citation' in data and isinstance(data['citation'], dict):
            data['citation'] = ChunkCitation.from_dict(data['citation'])
        if 'metadata' in data and isinstance(data['metadata'], dict):
            data['metadata'] = ChunkMetadata.from_dict(data['metadata'])
        return cls(**data)

    @classmethod
    def create(
        cls,
        source_uri: str,
        namespace: str,
        chunk_index: int,
        title: str,
        content: str,
        citation: ChunkCitation,
        metadata_overrides: Optional[Dict[str, Any]] = None,
    ) -> 'LoreChunk':
        """Factory method to create a new chunk with proper initialization."""
        metadata = ChunkMetadata(namespace=namespace)

        if metadata_overrides:
            if 'doc_type' in metadata_overrides:
                metadata.doc_type = metadata_overrides['doc_type']
            if 'language' in metadata_overrides:
                metadata.language = metadata_overrides['language']
            if 'tags' in metadata_overrides:
                metadata.tags = metadata_overrides['tags']
            if 'security_level' in metadata_overrides:
                level = metadata_overrides['security_level']
                if isinstance(level, str):
                    metadata.security_level = SecurityLevel(level)
                else:
                    metadata.security_level = level
            if 'custom' in metadata_overrides:
                metadata.custom = metadata_overrides['custom']
            if 'tenant' in metadata_overrides:
                metadata.tenant = metadata_overrides['tenant']

        return cls(
            source_uri=source_uri,
            chunk_index=chunk_index,
            title=title,
            content=content,
            citation=citation,
            metadata=metadata,
        )

    def with_new_version(
        self,
        new_content: str,
        new_citation: Optional[ChunkCitation] = None,
    ) -> 'LoreChunk':
        """Create a new version of this chunk with updated content."""
        return LoreChunk(
            doc_id=self.doc_id,
            chunk_index=self.chunk_index,
            source_uri=self.source_uri,
            title=self.title,
            content=new_content,
            citation=new_citation or self.citation,
            metadata=self.metadata,
            version=self.version + 1,
            created_at=self.created_at,
            embedding=None,  # Reset embedding for new content
        )

    def to_loretoken(self) -> str:
        """Format as LoreToken for LLM context injection."""
        return f"""<lore>
<meta>ns:{self.metadata.namespace} doc:{self.doc_id} chunk:{self.chunk_id} v:{self.version}</meta>
<title>{self.title}</title>
<src>{self.source_uri}</src>
<citation>{self.citation_string}</citation>
<content>
{self.content}
</content>
</lore>"""

    def __repr__(self) -> str:
        return f"LoreChunk(id={self.chunk_id[:8]}, src={self.source_uri}, lines={self.citation.start_line}-{self.citation.end_line})"
