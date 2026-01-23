<img width="1075" height="605" alt="image" src="https://github.com/user-attachments/assets/46c0178e-3726-4183-b9a3-50df233aee28" />

# SAIQL Community Edition

**Semantic Artificial Intelligence Query Language**
*A Database System Built for AI*

[![License](https://img.shields.io/badge/license-OLL--CE-blue.svg)](LICENSE.md)
[![Python](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/)

---

## About SAIQL

**SAIQL** (Semantic Artificial Intelligence Query Language) is a database system designed for AI from the ground up. It's three things:

- **A Transpiler** — Symbolic queries → validated → native SQL for any backend
- **A Converter** — Move schemas and queries between databases with controlled diffs
- **A Database Engine** — SAIQL's own IR and execution path for AI-native speed

***CE defaults to SQLite as its backend. Enterprise uses Lorecore, our AI-native storage engine.***

```
Traditional: AI generates raw SQL → injection risks, dialect errors, hallucinations
SAIQL:       AI generates symbolic query → validated → transpiled → executed safely
```

### The AI-Database Problem

When AI agents need to query databases, the traditional approach is problematic:

- **Injection Risks**: Raw SQL generation opens the door to prompt injection attacks
- **Dialect Errors**: AI must know the exact SQL flavor for each database
- **Unvalidated Queries**: No semantic layer to catch malicious or malformed requests
- **No Grounding**: AI responses about data can't be verified against actual results

### How SAIQL Solves It

SAIQL provides a **semantic firewall** between AI and your data:

- **Symbolic Syntax**: Compact, unambiguous format that AI models can reliably generate
- **Semantic Validation**: Queries are validated against schema and security rules before execution
- **Database Agnostic**: AI doesn't need to know if it's talking to PostgreSQL or SQLite
- **Grounded Responses**: Query results can be validated and cited (Full Edition)

Example — an AI agent querying user data:
```
*[users]::name,email|status='active'>>oQ
```
This symbolic query is validated, transpiled to native SQL, and executed safely.

### Universal Query Language

Beyond AI applications, SAIQL eliminates friction when working with multiple databases. Instead of learning SQL dialects for PostgreSQL, MySQL, SQLite, and others — you write one query that runs everywhere.

- **No Dialect Lock-in**: Same query syntax across all supported databases
- **Easy Migration**: Switch backends without rewriting queries
- **One Language**: Standardize your team on a single query format

### How SAIQL Works

SAIQL provides a **symbolic query language** that compiles down to native SQL for your target database. The engine handles:

1. **Query Translation**: SAIQL syntax → optimized native SQL
2. **Type Mapping**: Automatic conversion between database type systems
3. **Schema Migration**: L0 (data) and L1 (schema + data) migrations between backends

### Who It's For

- **AI/Agent Developers** building LLM applications that need safe, validated database access
- **AI Application Teams** wanting to ground model responses in real data
- **Developers** building applications that need database flexibility
- **Data Engineers** migrating between database platforms
- **Teams** standardizing on one query language across projects

---

## SAIQL Community Edition

The **Community Edition (CE)** is the self-hosted, open version of SAIQL with everything you need to get started.

### Supported Databases

- SQLite (zero-config, bundled with Python)
- PostgreSQL (production-grade)
- MySQL / MariaDB (ubiquitous)
- CSV / Excel files (instant data import)

### Key Features

- **Unified Query Syntax**: One language for all your databases
- **Semantic Firewall**: Security preventing prompt injection
- **Multi-Backend Support**: Switch databases without rewriting queries
- **B-tree and Hash Indexes**: Efficient query optimization
- **LoreToken-Lite**: Lightweight data serialization
- **QIPI-Lite**: SQLite FTS5-based text search

---

## Quick Start

### Prerequisites
- Python 3.8+
- pip

### Installation

```bash
# 1. Clone the repository
git clone https://github.com/yourusername/SAIQL-CE.git
cd SAIQL-CE

# 2. Create virtual environment
python3 -m venv venv
source venv/bin/activate

# 3. Install CE dependencies
pip install -r requirements-ce.txt
```

### Basic Usage

```python
from core.engine import SAIQLEngine

# Initialize SAIQL
engine = SAIQLEngine()

# Execute queries
result = engine.execute("*10[users]::name,email>>oQ")
print(result.data)
```

### CLI Usage

```bash
# Check version
python saiql.py --version

# Execute a query
python saiql.py --query "*[users]::*>>oQ"

# Show statistics
python saiql.py --stats
```

---

## SAIQL Query Syntax

SAIQL uses a symbolic query language:

```
*10[users]::name,email>>oQ     # Select 10 users
*[products]::*|price>100>>oQ   # Filter by price
=J[users+orders]::*>>oQ        # Join tables
```

### Symbols

| Symbol | Meaning |
|--------|---------|
| `*` | SELECT |
| `=` | Operation modifier |
| `J` | JOIN |
| `[]` | Table reference |
| `::` | Column selector |
| `|` | WHERE clause |
| `>>` | Output modifier |
| `oQ` | Output as query result |

---

## Project Structure

```
SAIQL-CE/
  core/           # Core engine components
  config/         # Configuration
  interface/      # API server
  adapters/       # Database adapters
  tests/          # Test suite
  docs/           # Documentation
```

---

## Database Adapters

Each adapter supports L0-L1 migration levels (Data + Schema):

| Database | Status | Notes |
|----------|--------|-------|
| SQLite | Complete | Zero-config, great for dev/demos |
| PostgreSQL | Complete | Recommended for production |
| MySQL / MariaDB | Complete | Wide compatibility |
| CSV / Excel | Complete | File-based data import |

---

## Contributing

We welcome contributions. Please see [CONTRIBUTING.md](CONTRIBUTING.md).

---

## License

SAIQL is licensed under the Open Lore License (OLL). See [LICENSE.md](LICENSE.md).

---

## Community Edition vs Full Edition

<table>
<thead>
<tr>
<th width="18%">Feature</th>
<th width="41%">Community Edition</th>
<th width="41%">Full Edition</th>
</tr>
</thead>
<tbody>
<tr>
<td><strong>Core Engine</strong></td>
<td>SAIQL transpiler, converter, engine</td>
<td>Same + cloud deployment</td>
</tr>
<tr>
<td><strong>Default Backend</strong></td>
<td>SQLite</td>
<td>Lorecore (AI-native storage)</td>
</tr>
<tr>
<td><strong>Database Adapters</strong></td>
<td>SQLite, PostgreSQL, MySQL, CSV/Excel</td>
<td>+MongoDB, Redis, SQL Server, Oracle, SAP HANA, Teradata, BigQuery, Snowflake, Redshift, DB2</td>
</tr>
<tr>
<td><strong>Indexing & Search</strong></td>
<td>B-tree, Hash, FTS5</td>
<td>+QIPI (proprietary high-speed indexing)</td>
</tr>
<tr>
<td><strong>Compression</strong></td>
<td>LoreToken-Lite</td>
<td>LoreToken Full (3 formats: Symbolic, Standard, Ultra)</td>
</tr>
<tr>
<td><strong>Semantic Firewall</strong></td>
<td>Full (injection protection, output scanning)</td>
<td>Full (injection protection, output scanning)</td>
</tr>
<tr>
<td><strong>RAG</strong></td>
<td>Atlas CE (Coming Soon)</td>
<td>Atlas Semantic RAG</td>
</tr>
<tr>
<td><strong>Grounding</strong></td>
<td>CE Mode (Coming Soon)</td>
<td>Enterprise Mode (citation enforcement, confidence thresholds)</td>
</tr>
<tr>
<td><strong>License</strong></td>
<td>OLL-CE (free for on-prem)</td>
<td>Commercial</td>
</tr>
</tbody>
</table>

### Full Edition Highlights (Proprietary)

**QIPI** *(proprietary)* — High-speed indexing that beats B-trees on 4/5 operations. 10x faster on hot data.

**Lorecore** *(proprietary)* — Agent-native storage with stream-based semantics optimized for AI memory patterns: conversation history, state management, event streams.

**Atlas** *(proprietary)* — Semantic RAG with three retrieval lanes (metadata, BM25 lexical, vector), safety matrix, grounding enforcement, and proof bundles for compliance.

**LoreToken** *(proprietary)* — Three format specifications for different efficiency/readability trade-offs: Symbolic (2.6x compression), Standard (1.5x), Ultra (human-readable). Data compressed via LoreToken before GPU processing uses up to 70% less compute.



