# SAIQL Semantic RAG

> **Retrieval built for governed execution, not just best-effort context.**

---

## The Problem with Standard RAG

Most RAG stacks are **"vector search + prompt stuffing."** You embed chunks, retrieve top-k by similarity, and hope the model uses them correctly.

That works — until it doesn't:

| Failure Mode | What Happens |
|--------------|--------------|
| **Wrong chunks** | Semantically similar but factually irrelevant content retrieved |
| **Missing constraints** | No enforcement of business rules or access controls |
| **Prompt injection** | Malicious instructions hidden in retrieved documents |
| **Brittle relevance** | Small query changes produce wildly different results |
| **No audit trail** | Can't explain why an answer was generated |
| **Confident hallucinations** | Model invents facts when retrieval misses |

---

## How SAIQL Semantic RAG (Atlas) is Different

SAIQL Semantic RAG optimizes for **evidence you can trust, explain, and audit** — not just "get relevant text."

```
Standard RAG:  Query → Embed → Vector Search → Top-K → Stuff Prompt → Hope

SAIQL RAG:     Query → Parse Intent → Three-Lane Retrieval → Proof Bundle → Grounded Response
                              ↓
                    [Metadata] [BM25] [Vector]
                              ↓
                    Fusion → Safety Check → Citation
```

---

## Five Key Differences

### 1. Structured Intent, Not Just Similarity

| Standard RAG | SAIQL Semantic RAG |
|--------------|-------------------|
| Retrieves by embedding distance only | Normalizes request into **symbolic/typed intent** |
| "Find similar text" | "What is being asked? What constraints apply? What evidence is required?" |
| Many "close but wrong" results | Fewer irrelevant chunks, better constraint adherence |

SAIQL first understands *what you're asking* before searching — resulting in precision over recall.

---

### 2. Proof-First Retrieval (Auditable Evidence)

Instead of returning opaque "top-k" results, SAIQL produces an **artifact bundle** documenting the entire retrieval decision:

| Artifact | Contents |
|----------|----------|
| **Retrieved** | What chunks were selected and their scores |
| **Match Signals** | Why each chunk matched (metadata, lexical, semantic) |
| **Exclusions** | What was filtered out and why |
| **Confidence** | Score distributions and coverage gaps |
| **Citations** | Source URIs with line numbers for verification |

This makes retrieval **debuggable and reviewable** — essential for enterprise and regulated workflows where "trust me" isn't acceptable.

---

### 3. Injection-Aware Grounding

| Standard RAG | SAIQL Semantic RAG |
|--------------|-------------------|
| Treats retrieved text as trustworthy | Treats retrieval as **untrusted input** |
| Vulnerable to document-based attacks | Separates facts from instructions |
| No content inspection | Flags policy/role conflicts |
| Blindly includes all results | Can refuse or quarantine malicious passages |

**Retrieval prompt injection** — where a document contains instructions that hijack the model — is a real attack vector. SAIQL's safety layer inspects retrieved content before it reaches the model.

---

### 4. Deterministic Mode When You Need It

Many RAG stacks are non-deterministic:
- Ranking shifts between runs
- Chunking produces different boundaries
- Model variability affects results

SAIQL supports a **deterministic, repeatable retrieval path**:

```
Same Query + Same Corpus = Same Results
```

| Use Case | Why Determinism Matters |
|----------|------------------------|
| **Testing** | Assertions on retrieval behavior |
| **Regression** | Detect when index changes affect outputs |
| **Change Control** | Audit trail of what changed and when |
| **Compliance** | Reproducible results for regulators |

Determinism is configurable — enable it when you need reproducibility, disable it when you want ranking diversity.

---

### 5. Better Failure Behavior

| Standard RAG | SAIQL Semantic RAG |
|--------------|-------------------|
| Hallucinates confidently when retrieval misses | Returns **"insufficient evidence"** with explicit gaps |
| No distinction between proven and guessed | Separates **"what I can prove"** from **"what I can guess"** |
| User doesn't know what's missing | Clear next steps: index more, add sources, adjust constraints |

When SAIQL can't find sufficient evidence, it tells you:
- What it searched for
- What it found (partial matches)
- What's missing (coverage gaps)
- What you could do about it

**Fewer confident wrong answers. Clearer paths forward.**

---

## Three-Lane Hybrid Retrieval

SAIQL uses three parallel retrieval lanes, then fuses results:

| Lane | Method | Strength |
|------|--------|----------|
| **Metadata** | Inverted index filtering | Exact matches on namespace, tags, doc_type, dates |
| **Lexical (BM25)** | SQLite FTS5 | Keyword matching, acronyms, exact phrases |
| **Vector** | Sentence embeddings | Semantic similarity, paraphrase detection |

### Fusion Weights

```python
w_meta = 0.10   # Metadata match bonus
w_lex  = 0.25   # BM25 lexical score
w_vec  = 0.65   # Vector similarity
```

Results are sorted by `(-score, chunk_id)` for deterministic ordering.

---

## CE vs Enterprise

| Feature | Community Edition (Atlas CE) | Enterprise (Atlas) |
|---------|------------------------------|-------------------|
| **Three-lane retrieval** | Yes | Yes |
| **Deterministic mode** | Yes | Yes |
| **Citation generation** | Yes | Yes + Enforcement |
| **Grounding status** | Informational | Enforced (can refuse) |
| **Document types** | Text, Markdown | +PDF, DOCX, HTML, Images |
| **Safety scanning** | Basic | Advanced (redaction, quarantine) |
| **Proof bundles** | Partial | Full audit trail |
| **Confidence thresholds** | Not enforced | Configurable minimums |
| **Serialization** | LoreToken-Lite | LoreToken™ (3 formats: Symbolic, Standard, Ultra) |

---

## Quick Example

```python
from core.atlas_ce import AtlasCE, AtlasCEConfig, enable_atlas_ce
from core.grounding_ce import CEModeGrounding

# Initialize
config = AtlasCEConfig(enabled=True, namespace="docs")
enable_atlas_ce(config)
atlas = AtlasCE(config)

# Ingest documents
atlas.ingest_text(
    content="SAIQL is a semantic query language for AI applications.",
    source_uri="docs/intro.md",
    title="Introduction"
)

# Query with three-lane retrieval
chunks = atlas.query("What is SAIQL?", top_k=5)

# Ground response with citations
grounding = CEModeGrounding(atlas)
result = grounding.ground_response(
    response_text="SAIQL is a semantic query language.",
    query="What is SAIQL?",
    retrieved_chunks=chunks
)

print(f"Status: {result.grounding_status}")  # GROUNDED, PARTIAL, or UNGROUNDED
print(f"Citations: {result.citations}")       # ['docs/intro.md:1-1']
```

---

## Summary

| Standard RAG | SAIQL Semantic RAG |
|--------------|-------------------|
| Vector similarity only | Three-lane hybrid (metadata + BM25 + vector) |
| Opaque results | Proof bundles with full audit trail |
| Trusts retrieved content | Treats retrieval as untrusted input |
| Non-deterministic | Deterministic mode available |
| Hallucinates on miss | Explicit "insufficient evidence" |
| "Get relevant text" | "Retrieve evidence you can trust, explain, and audit" |

---

> *SAIQL Semantic RAG: Because "probably relevant" isn't good enough for production AI.*
