# SAIQL Community Edition Update: 2025-12-14

## Summary

This update documents the features included in SAIQL Community Edition that were developed as part of the broader SAIQL project. CE includes the Semantic Firewall and Quality Mode features.

## 1. Semantic Firewall (Security)
**Status: INCLUDED IN CE**

A fast, regex-based semantic firewall protects SAIQL from common AI-specific threats.

### Key Features

- **Pre-Prompt Guard**: Blocks prompt injection attacks (e.g., "Ignore previous instructions") and system prompt extraction attempts before they reach the execution engine.
- **Pre-Retrieval Guard**: Scans retrieval queries for malicious patterns.
- **Post-Output Guard**: Automatically redacts sensitive information (like API keys matching `sk-*`) from the final response.
- **Zero-Latency Design**: Uses compiled regex patterns for minimal performance impact.

### Components

- `core/semantic_firewall.py`: Core logic and rules engine
- `config/semantic_firewall_rules.json`: Configurable threat patterns and actions

## 2. Quality Mode (Grounding)
**Status: INCLUDED IN CE**

Quality Mode enforces strict grounding in SAIQL responses, preventing hallucinations by refusing to answer queries without sufficient evidence.

### Key Features

- **Grounding Guard**: Validates retrieved evidence against configurable thresholds (`min_chunks`, `min_score`).
- **Hallucination Prevention**: Returns an `UNGROUNDED` status and refusal message if evidence is insufficient.
- **Citation Enforcement**: Ensures grounded responses include citations linked to source documents.
- **Output Contract**: Standardized response format with confidence scores and evidence snippets.

### Components

- `core/grounding_guard.py`: Logic for grounding validation and output formatting
- `config/quality_guard_rules.json`: Configuration for grounding thresholds

## 3. System Doctor (Diagnostics)
**Status: INCLUDED IN CE**

System diagnostics for health checks and environment validation.

### Components

- `tools/system_doctor.py`: System health checks

## CE-Specific Notes

The following features documented in the original Full Edition update are **NOT included** in Community Edition:

- Full QIPI Persistence (CE uses QIPI-Lite / SQLite FTS5 instead)
- Truth Benchmarks harness (`benchmarks/` directory)
- Hybrid BM25 + Vector retrieval (vector search removed)
- SAIQL Doctor CLI tool (`saiql-doctor`)
- `core/qipi_index.py` (full probabilistic indexing)

## Next Steps

CE is ready for production use with:
- Core SAIQL engine
- All 10 database adapters
- Semantic Firewall protection
- Quality Mode grounding
- LoreToken-Lite and QIPI-Lite
