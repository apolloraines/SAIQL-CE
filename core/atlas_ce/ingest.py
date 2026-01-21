"""
Atlas CE Ingestion - Document Processing and Chunking
=====================================================

Handles incremental document ingestion with hash-based change detection.
CE Edition: Text and Markdown only (no PDF/DOCX/HTML).
"""

import hashlib
import json
import re
import time
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
import logging

from .lore_chunk import LoreChunk, ChunkCitation, ChunkMetadata

logger = logging.getLogger(__name__)


@dataclass
class IngestResult:
    """Result of an ingestion operation."""
    doc_id: str
    source_uri: str
    chunks_created: int = 0
    chunks_updated: int = 0
    chunks_unchanged: int = 0
    chunks_deleted: int = 0
    is_new_document: bool = True
    content_hash: str = ""
    timestamp: str = ""
    old_chunk_ids: List[str] = field(default_factory=list)  # Chunks to delete on re-ingest

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class DocumentState:
    """Persisted state for incremental ingestion."""
    doc_id: str
    source_uri: str
    content_hash: str
    chunk_count: int
    chunk_ids: List[str] = field(default_factory=list)  # Track chunk IDs for deletion on re-ingest
    last_ingested: float = 0.0


class IngestOrchestrator:
    """
    Handles document ingestion with incremental updates.

    CE Edition limitations:
    - Text and Markdown only (no PDF/DOCX extraction)
    - No OCR or vision processing
    - Basic paragraph-aware chunking
    """

    def __init__(
        self,
        namespace: str,
        chunk_size: int = 512,
        chunk_overlap: int = 64,
        storage_path: Optional[Path] = None,
    ):
        self.namespace = namespace
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.storage_path = storage_path

        # Document state for incremental ingestion
        self._doc_states: Dict[str, DocumentState] = {}

        # Load persisted state
        if storage_path:
            self._load_state()

    def _compute_doc_id(self, source_uri: str) -> str:
        """Compute stable document ID."""
        key = f"{self.namespace}:{source_uri}"
        return hashlib.sha256(key.encode('utf-8')).hexdigest()[:16]

    def _compute_content_hash(self, content: str) -> str:
        """Compute content hash for change detection."""
        return hashlib.sha256(content.encode('utf-8')).hexdigest()[:16]

    def ingest_text(
        self,
        content: str,
        source_uri: str,
        title: str = "",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Tuple[List[LoreChunk], IngestResult]:
        """
        Ingest text content.

        Args:
            content: Text content to ingest
            source_uri: Source identifier (file path or URL)
            title: Document title
            metadata: Additional metadata (doc_type, tags, etc.)

        Returns:
            Tuple of (list of chunks, ingest result)
        """
        doc_id = self._compute_doc_id(source_uri)
        content_hash = self._compute_content_hash(content)

        # Check for unchanged document
        existing_state = self._doc_states.get(doc_id)
        if existing_state and existing_state.content_hash == content_hash:
            logger.debug(f"Document unchanged: {source_uri}")
            return [], IngestResult(
                doc_id=doc_id,
                source_uri=source_uri,
                chunks_unchanged=existing_state.chunk_count,
                is_new_document=False,
                content_hash=content_hash,
                timestamp=time.strftime('%Y-%m-%dT%H:%M:%SZ'),
            )

        # Get old chunk IDs for deletion (if re-ingesting)
        old_chunk_ids = existing_state.chunk_ids if existing_state else []

        # Chunk the content
        chunks = self._chunk_text(content, source_uri, title, metadata)

        # Track new chunk IDs
        new_chunk_ids = [chunk.chunk_id for chunk in chunks]

        # Update state with new chunk IDs
        self._doc_states[doc_id] = DocumentState(
            doc_id=doc_id,
            source_uri=source_uri,
            content_hash=content_hash,
            chunk_count=len(chunks),
            chunk_ids=new_chunk_ids,
            last_ingested=time.time(),
        )

        # Persist state
        if self.storage_path:
            self._save_state()

        is_new = existing_state is None
        result = IngestResult(
            doc_id=doc_id,
            source_uri=source_uri,
            chunks_created=len(chunks) if is_new else 0,
            chunks_updated=len(chunks) if not is_new else 0,
            is_new_document=is_new,
            content_hash=content_hash,
            timestamp=time.strftime('%Y-%m-%dT%H:%M:%SZ'),
            old_chunk_ids=old_chunk_ids,  # Chunks to delete on re-ingest
        )

        logger.info(f"Ingested {len(chunks)} chunks from {source_uri}")
        return chunks, result

    def ingest_file(
        self,
        file_path: Path,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Tuple[List[LoreChunk], IngestResult]:
        """
        Ingest a file.

        CE Edition: Only .txt and .md files supported.
        """
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        suffix = file_path.suffix.lower()
        if suffix not in ('.txt', '.md', '.markdown'):
            raise ValueError(
                f"Unsupported file type: {suffix}. "
                "CE Edition supports only .txt and .md files."
            )

        content = file_path.read_text(encoding='utf-8')
        title = file_path.stem

        # Auto-detect doc_type
        if metadata is None:
            metadata = {}
        if 'doc_type' not in metadata:
            metadata['doc_type'] = 'markdown' if suffix in ('.md', '.markdown') else 'text'

        return self.ingest_text(content, str(file_path), title, metadata)

    def _chunk_text(
        self,
        content: str,
        source_uri: str,
        title: str,
        metadata: Optional[Dict[str, Any]],
    ) -> List[LoreChunk]:
        """
        Split text into chunks with paragraph awareness.

        Strategy:
        1. Split by double newlines (paragraphs)
        2. Merge small paragraphs up to chunk_size
        3. Split large paragraphs at sentence boundaries

        Citations use exact offsets/lines from original content boundaries.
        """
        chunks = []
        paragraphs = self._split_paragraphs(content)

        # Track current chunk being built
        current_paras = []  # List of paragraph dicts in current chunk
        chunk_index = 0

        for para in paragraphs:
            para_text = para['text']

            # Calculate current chunk text length
            current_text_len = sum(len(p['text']) for p in current_paras)
            if current_paras:
                current_text_len += 2 * (len(current_paras) - 1)  # "\n\n" separators

            if current_text_len + len(para_text) + (2 if current_paras else 0) <= self.chunk_size:
                # Add paragraph to current chunk
                current_paras.append(para)
            else:
                # Save current chunk and start new one
                if current_paras:
                    chunk = self._create_chunk_from_paras(
                        current_paras, source_uri, title, metadata, chunk_index
                    )
                    chunks.append(chunk)
                    chunk_index += 1

                # Handle large paragraphs
                if len(para_text) > self.chunk_size:
                    # Split at sentence boundaries with exact spans
                    sentences = self._split_sentences_with_spans(
                        para_text, para['start_offset'], content
                    )
                    sentence_chunks = self._merge_sentence_spans(sentences, self.chunk_size)

                    # Create chunks from merged sentence spans
                    for sent_span in sentence_chunks:
                        chunk = self._create_chunk(
                            content=sent_span['text'],
                            source_uri=source_uri,
                            title=title,
                            metadata=metadata,
                            chunk_index=chunk_index,
                            start_offset=sent_span['start_offset'],
                            end_offset=sent_span['end_offset'],
                            start_line=sent_span['start_line'],
                            end_line=sent_span['end_line'],
                        )
                        chunks.append(chunk)
                        chunk_index += 1

                    current_paras = []
                else:
                    current_paras = [para]

        # Don't forget the last chunk
        if current_paras:
            chunk = self._create_chunk_from_paras(
                current_paras, source_uri, title, metadata, chunk_index
            )
            chunks.append(chunk)

        return chunks

    def _create_chunk_from_paras(
        self,
        paras: List[Dict[str, Any]],
        source_uri: str,
        title: str,
        metadata: Optional[Dict[str, Any]],
        chunk_index: int,
    ) -> LoreChunk:
        """Create a chunk from a list of paragraph dicts with exact boundaries."""
        # Build chunk text by joining paragraph texts
        chunk_text = "\n\n".join(p['text'] for p in paras)

        # Get exact boundaries from first and last paragraphs
        start_offset = paras[0]['start_offset']
        end_offset = paras[-1]['end_offset']
        start_line = paras[0]['start_line']
        end_line = paras[-1]['end_line']

        return self._create_chunk(
            content=chunk_text,
            source_uri=source_uri,
            title=title,
            metadata=metadata,
            chunk_index=chunk_index,
            start_offset=start_offset,
            end_offset=end_offset,
            start_line=start_line,
            end_line=end_line,
        )

    def _split_paragraphs(self, content: str) -> List[Dict[str, Any]]:
        """
        Split content into paragraphs with exact span boundaries.

        Returns list of dicts with:
            - text: paragraph content (stripped for display)
            - start_offset: exact start position in original content
            - end_offset: exact end position in original content
            - start_line: line number at start
            - end_line: line number at end

        All positions reference the ORIGINAL content exactly.
        """
        paragraphs = []

        # Find all paragraph separators (one or more blank lines)
        separator_pattern = re.compile(r'\n[ \t]*\n')

        # Track positions
        prev_end = 0

        for match in separator_pattern.finditer(content):
            sep_start = match.start()
            sep_end = match.end()

            # Extract paragraph span before this separator
            if prev_end < sep_start:
                para_raw = content[prev_end:sep_start]
                if para_raw.strip():
                    # Find actual content boundaries (skip leading/trailing whitespace)
                    first_nonws = len(para_raw) - len(para_raw.lstrip())
                    last_nonws = len(para_raw.rstrip())

                    actual_start = prev_end + first_nonws
                    actual_end = prev_end + last_nonws

                    paragraphs.append({
                        'text': para_raw.strip(),
                        'start_offset': actual_start,
                        'end_offset': actual_end,
                        'start_line': content[:actual_start].count('\n') + 1,
                        'end_line': content[:actual_end].count('\n') + 1,
                    })

            prev_end = sep_end

        # Handle last paragraph after final separator
        if prev_end < len(content):
            para_raw = content[prev_end:]
            if para_raw.strip():
                first_nonws = len(para_raw) - len(para_raw.lstrip())
                last_nonws = len(para_raw.rstrip())

                actual_start = prev_end + first_nonws
                actual_end = prev_end + last_nonws

                paragraphs.append({
                    'text': para_raw.strip(),
                    'start_offset': actual_start,
                    'end_offset': actual_end,
                    'start_line': content[:actual_start].count('\n') + 1,
                    'end_line': content[:actual_end].count('\n') + 1,
                })

        # Handle case with no separators (single paragraph)
        if not paragraphs and content.strip():
            first_nonws = len(content) - len(content.lstrip())
            last_nonws = len(content.rstrip())

            paragraphs.append({
                'text': content.strip(),
                'start_offset': first_nonws,
                'end_offset': last_nonws,
                'start_line': content[:first_nonws].count('\n') + 1,
                'end_line': content[:last_nonws].count('\n') + 1,
            })

        return paragraphs

    def _split_sentences_with_spans(
        self, text: str, base_offset: int, content: str
    ) -> List[Dict[str, Any]]:
        """
        Split text into sentences with exact spans from original content.

        Args:
            text: The paragraph text to split
            base_offset: Offset of this paragraph in original content
            content: The original full content (for line number calculation)

        Returns:
            List of dicts with: text, start_offset, end_offset, start_line, end_line
        """
        sentences = []

        # Find sentence boundaries using regex
        # Match sentence-ending punctuation followed by whitespace
        pattern = re.compile(r'(?<=[.!?])(\s+)')

        prev_end = 0
        for match in pattern.finditer(text):
            # Sentence is from prev_end to match.start() (punctuation boundary)
            # match.start() is at the whitespace, so it's already after the punctuation
            sent_start = prev_end
            sent_end = match.start()  # End at punctuation, not whitespace

            sent_text = text[sent_start:sent_end].strip()
            if sent_text:
                abs_start = base_offset + sent_start
                abs_end = base_offset + sent_end
                sentences.append({
                    'text': sent_text,
                    'start_offset': abs_start,
                    'end_offset': abs_end,
                    'start_line': content[:abs_start].count('\n') + 1,
                    'end_line': content[:abs_end].count('\n') + 1,
                })

            prev_end = match.end()  # Skip past the whitespace

        # Handle last sentence (or only sentence if no matches)
        if prev_end < len(text):
            sent_text = text[prev_end:].strip()
            if sent_text:
                abs_start = base_offset + prev_end
                abs_end = base_offset + len(text.rstrip())
                sentences.append({
                    'text': sent_text,
                    'start_offset': abs_start,
                    'end_offset': abs_end,
                    'start_line': content[:abs_start].count('\n') + 1,
                    'end_line': content[:abs_end].count('\n') + 1,
                })

        # If no sentences found (no sentence-ending punctuation), treat whole text as one
        if not sentences and text.strip():
            sentences.append({
                'text': text.strip(),
                'start_offset': base_offset,
                'end_offset': base_offset + len(text.rstrip()),
                'start_line': content[:base_offset].count('\n') + 1,
                'end_line': content[:base_offset + len(text.rstrip())].count('\n') + 1,
            })

        return sentences

    def _merge_sentence_spans(
        self, sentences: List[Dict[str, Any]], max_size: int
    ) -> List[Dict[str, Any]]:
        """
        Merge sentence spans into chunks up to max_size.

        Returns list of dicts with merged text and span from first to last sentence.
        """
        if not sentences:
            return []

        chunks = []
        current_sents = []
        current_len = 0

        for sent in sentences:
            sent_len = len(sent['text'])
            # Account for space between sentences when joining
            added_len = sent_len + (1 if current_sents else 0)

            if current_len + added_len <= max_size:
                current_sents.append(sent)
                current_len += added_len
            else:
                if current_sents:
                    chunks.append(self._combine_sentence_spans(current_sents))
                current_sents = [sent]
                current_len = sent_len

        if current_sents:
            chunks.append(self._combine_sentence_spans(current_sents))

        return chunks

    def _combine_sentence_spans(self, sents: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Combine multiple sentence spans into one chunk span."""
        # Join texts with single space (normalized for chunk content)
        combined_text = ' '.join(s['text'] for s in sents)

        # Span covers from first sentence start to last sentence end
        return {
            'text': combined_text,
            'start_offset': sents[0]['start_offset'],
            'end_offset': sents[-1]['end_offset'],
            'start_line': sents[0]['start_line'],
            'end_line': sents[-1]['end_line'],
        }

    def _create_chunk(
        self,
        content: str,
        source_uri: str,
        title: str,
        metadata: Optional[Dict[str, Any]],
        chunk_index: int,
        start_offset: int,
        end_offset: int,
        start_line: int,
        end_line: int,
    ) -> LoreChunk:
        """Create a LoreChunk with exact citation boundaries."""
        citation = ChunkCitation(
            start_offset=start_offset,
            end_offset=end_offset,
            start_line=start_line,
            end_line=end_line,
            start_byte=start_offset,
            end_byte=end_offset,
        )

        return LoreChunk.create(
            source_uri=source_uri,
            namespace=self.namespace,
            chunk_index=chunk_index,
            title=title or source_uri,
            content=content,
            citation=citation,
            metadata_overrides=metadata,
        )

    def delete_document(self, source_uri: str) -> int:
        """Delete a document's state. Returns chunk count that was tracked."""
        doc_id = self._compute_doc_id(source_uri)
        state = self._doc_states.pop(doc_id, None)

        if self.storage_path:
            self._save_state()

        return state.chunk_count if state else 0

    def _save_state(self) -> None:
        """Persist document states to disk."""
        if not self.storage_path:
            return

        self.storage_path.mkdir(parents=True, exist_ok=True)
        state_file = self.storage_path / f"{self.namespace}_ingest_state.json"

        data = {
            doc_id: {
                'doc_id': state.doc_id,
                'source_uri': state.source_uri,
                'content_hash': state.content_hash,
                'chunk_count': state.chunk_count,
                'chunk_ids': state.chunk_ids,  # Track for re-ingest cleanup
                'last_ingested': state.last_ingested,
            }
            for doc_id, state in self._doc_states.items()
        }

        state_file.write_text(json.dumps(data, indent=2))
        logger.debug(f"Saved ingest state: {len(data)} documents")

    def _load_state(self) -> None:
        """Load document states from disk."""
        if not self.storage_path:
            return

        state_file = self.storage_path / f"{self.namespace}_ingest_state.json"
        if not state_file.exists():
            return

        try:
            data = json.loads(state_file.read_text())
            for doc_id, state_data in data.items():
                self._doc_states[doc_id] = DocumentState(**state_data)
            logger.debug(f"Loaded ingest state: {len(data)} documents")
        except Exception as e:
            logger.warning(f"Failed to load ingest state: {e}")

    def get_stats(self) -> Dict[str, Any]:
        """Get ingestion statistics."""
        return {
            'namespace': self.namespace,
            'documents_tracked': len(self._doc_states),
            'total_chunks': sum(s.chunk_count for s in self._doc_states.values()),
            'chunk_size': self.chunk_size,
            'chunk_overlap': self.chunk_overlap,
        }
