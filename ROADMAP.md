# SAIQL Community Edition Roadmap & Known Limitations

**Current Version**: `v0.4.0-ce` (Community Edition)
**Status**: Public Alpha

---

## Known Limitations

As an Alpha release, SAIQL CE is a **Production-Ready Core Engine**. Developers should be aware of:

### Migration Capability Levels

CE supports **L0-L1** migrations only.

| Level | Scope | Status | Notes |
| :--- | :--- | :--- | :--- |
| **L0** | **Data Only** | Supported | CSV/Parquet/Pandas -> SAIQL |
| **L1** | **Schema + Data** | Supported | Tables, PKs, FKs, Indexes, Data |
| **L2** | **Views** | Not Supported | Views are ignored during introspection |
| **L3** | **Routines** | Not Supported | Stored Procs / Functions ignored |
| **L4** | **Triggers** | Not Supported | Triggers ignored |

### Persistence

- **Data**: Fully persistent via SQLite/PostgreSQL backends
- **Index**: B-tree and Hash indexes with standard persistence
- **Text Search**: QIPI-Lite (SQLite FTS5) for full-text search

### Concurrency

- Thread-safe for reads
- Writes use file locking (SQLite WAL mode)
- Suitable for typical workloads

### Architecture

- Single-node engine
- Network accessible via REST API

---

## CE Feature Set

### Included in Community Edition

- Core SAIQL engine and query language
- 4 database adapters:
  - SQLite (zero-config, bundled with Python)
  - PostgreSQL (production-grade)
  - MySQL / MariaDB (ubiquitous)
  - CSV / Excel files (instant data import)
- B-tree and Hash indexing
- LoreToken-Lite (lightweight data serialization)
- QIPI-Lite (SQLite FTS5-based text search)
- Semantic Firewall (injection protection)
- Quality Mode / Grounding Guard
- System Doctor (diagnostics)

### Not Included in CE (Full Edition Cloud Only)

- Additional database adapters (MongoDB, Redis, SQL Server, Oracle, SAP HANA, Teradata, BigQuery, Snowflake, Redshift, DB2)
- Full QIPI (Quantum-Inspired Probabilistic Indexing)
- Full LoreToken
- Vector search and embeddings
- Atlas integration
- Semantic RAG pipeline
- Copilot Carl (AI assistant)

---

## Development Roadmap

### Phase 1: Core Engine (COMPLETED)

- [x] Multi-database adapter support
- [x] SAIQL query language implementation
- [x] B-tree and Hash indexing
- [x] Core architecture

### Phase 2: CE Stability (COMPLETED)

- [x] LoreToken-Lite implementation
- [x] QIPI-Lite (FTS5) implementation
- [x] CE edition gating
- [x] Semantic Firewall
- [x] Quality Mode / Grounding

### Phase 3: Convert-First & Enterprise (IN PROGRESS)

- [ ] Convert-First Contract: Schema/Data Migration vs Query Translation
- [ ] Enterprise Sources: SQL Server / Oracle Source Adapters (L1)
- [ ] File Lane: Excel/CSV -> SAIQL/Postgres conversion
- [ ] Audit Bundles: Automated report generation

### Phase 4: Future Enhancements

- [ ] Enhanced monitoring and observability
- [ ] Performance optimizations
- [ ] Additional adapter support

---

## Contributing

We welcome contributions! See `CONTRIBUTING.md` or check the Issues tab.
