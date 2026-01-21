# 🔮 Planned Features & Roadmap

**Goal**: Document known gaps compared to mature engines and outline the future roadmap for SAIQL.

This document serves as a "Wish List" and "Gap Analysis" for contributors and architects.

---

## 1. Missing Core Features (The "Gap")
*Features present in PostgreSQL/MySQL/Oracle that SAIQL does not yet have.*

### 🚧 Advanced SQL Analytics
-   **CTEs (Common Table Expressions)**: `WITH` clauses for recursive or complex queries.
-   **Window Functions**: `OVER()`, `RANK()`, `PARTITION BY` for analytical reporting.
-   **Stored Procedures**: Server-side logic (PL/pgSQL equivalent). Currently, logic must live in the client or Python extensions.

### 🌐 High Availability & Distribution
-   **Native Clustering**: Raft/Paxos implementation for multi-node consensus. Currently, SAIQL is single-node (Standalone) or relies on the backend DB (Translation).
-   **Replication**: Built-in primary-replica streaming.
-   **Sharding**: Automatic horizontal partitioning of data across nodes.

### 🛡️ Data Governance
-   **Row-Level Security (RLS)**: Granular access control at the row level based on user attributes.
-   **Point-in-Time Recovery (PITR)**: Tooling to restore the database to a specific second using WAL archives.

### 🧩 Data Types
-   **Native JSONB**: Efficient binary JSON storage and indexing (currently stored as TEXT).
-   **Geospatial**: Native Point/Polygon types and spatial indexes (PostGIS equivalent).

---

## 2. Enhancement Ideas (The "Vision")
*Ideas to leverage SAIQL's AI-native architecture for unique advantages.*

### ☁️ Cloud-Native Storage Tiering
-   **S3-Backed SSTables**: Automatically offload "cold" data (L3 buckets) to S3/GCS object storage while keeping hot data on NVMe.
-   **Serverless Scale-to-Zero**: Fast startup allows for Lambda-like on-demand instances.

### 🧠 Optimization
-   **Auto-Tuning Agent**: An internal agent that watches query patterns and auto-adjusts cache sizes, bloom filter bits, and compaction strategies.

### 🔌 Extensibility
-   **WASM Plugins**: Run custom functions (UDFs) written in Rust/Go/C++ safely inside the engine sandbox.
-   **Native CDC (Change Data Capture)**: Expose the WAL as a structured stream (Kafka-compatible) for event-driven architectures.

### 🖥️ Interface
-   **Web Admin Console**: A lightweight, single-binary web UI (served by the API) for monitoring, query visualization, and config management.
-   **Official Drivers**: Native clients for Go, Rust, Node.js, and Java (currently Python-centric).

---

## 3. Contributing
Want to build one of these? Check `CONTRIBUTING.md` and open an RFC on the issue tracker.
