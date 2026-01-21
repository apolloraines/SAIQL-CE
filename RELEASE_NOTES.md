# SAIQL Community Edition Release Notes

## v0.4.0-ce (Community Edition)
**Release Date: 2026-01-18**

### Release Highlights

This release marks the official Community Edition (CE) of SAIQL, providing a production-ready core engine for multi-database query operations.

### CE Features

- **Core SAIQL Engine**: Unified query language across 10 database backends
- **Semantic Firewall**: AI-native security layer blocking prompt injections
- **Quality Mode (Grounding)**: Prevents hallucinations with evidence thresholds and citations
- **LoreToken-Lite**: Lightweight canonical JSON serialization with SHA256 hashing
- **QIPI-Lite**: SQLite FTS5-based full-text search with unicode61 tokenizer
- **B-tree and Hash Indexes**: Efficient query optimization
- **System Doctor**: Diagnostics and health checks

### Supported Databases

- SQLite (zero-config, bundled with Python)
- PostgreSQL (production-grade)
- MySQL / MariaDB (ubiquitous)
- CSV / Excel files (instant data import)

### Migration Support

- **L0**: Data Only (CSV/Parquet/Pandas)
- **L1**: Schema + Data (Tables, PKs, FKs, Indexes)

### CE-Specific Notes

This Community Edition does not include (available in Full Edition Cloud):
- Additional database adapters (MongoDB, Redis, SQL Server, Oracle, SAP HANA, Teradata, BigQuery, Snowflake, Redshift, DB2)
- Full QIPI (Quantum-Inspired Probabilistic Indexing)
- Full LoreToken
- Vector search and embeddings
- Atlas integration and Semantic RAG
- Copilot Carl AI assistant

---

## Previous Releases

See the Full Edition release notes for historical version information.
