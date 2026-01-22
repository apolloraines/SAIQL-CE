# SAIQL Database Conversion Disclaimer

> **Database conversion is not magic.**

---

## Critical Safety Guarantee

> **SAIQL NEVER modifies your source database.**
>
> During conversion and migration operations, SAIQL operates in **read-only mode** on the originating database. Your source data remains completely untouched throughout the entire process—from analysis through execution. All writes occur exclusively on the target/destination database, and only after you explicitly approve the migration plan.

---

## Overview

Databases are inherently different. **"Any-DB to Any-DB"** does not mean 100% lossless, 100% automatic conversion for every schema, feature, and vendor-specific behavior.

SAIQL's goal is to make migrations **safer** and **more predictable** by proving what will and won't convert cleanly—*before* anything is written.

---

## Core Principles

### 1. Proof-First, Not Promise-First

SAIQL generates explicit artifacts that show exactly what changes in the destination:

| Artifact | Purpose |
|----------|---------|
| **Diffs** | Line-by-line comparison of source vs. target schema |
| **Mappings** | Type and constraint translation tables |
| **Warnings** | Explicit flags for lossy or unsupported conversions |
| **Compatibility Reports** | Feature-by-feature analysis of migration viability |

If a conversion is lossy, **SAIQL will say so up front.**

### 2. Read-Only Analysis

The source database is treated as **read-only** during analysis. Your original data is never modified during the proof phase. You review the migration plan and decide whether the tradeoffs are acceptable before applying any write operations.

### 3. Explicit Over Implicit

If SAIQL cannot confidently prove correctness for a feature, it will:
- Mark it as **unsupported**
- Require an **explicit override** with acknowledgment
- Generate a **warning** in the migration report

Silent data loss is never acceptable. Informed decisions are.

---

## What "Lossy" Means

"Lossy" refers to **representation or constraint changes**, not silent deletion. Examples include:

| Category | Example |
|----------|---------|
| **Precision/Scale** | `DECIMAL(38,10)` → `DECIMAL(28,8)` (target DB limitation) |
| **String Length** | `VARCHAR(MAX)` → `TEXT` (unbounded, but different semantics) |
| **Collation/Encoding** | `utf8mb4_unicode_ci` → `en_US.UTF-8` (sorting differences) |
| **Temporal Precision** | `DATETIME2(7)` → `TIMESTAMP(6)` (nanosecond truncation) |
| **Identity/Sequences** | `IDENTITY(1,1)` → `SERIAL` (restart behavior differs) |
| **Functions/Triggers** | `GETDATE()` → `NOW()` (timezone handling varies) |
| **Stored Procedures** | Vendor-specific PL/SQL, T-SQL, PL/pgSQL dialects |
| **Constraints** | `CHECK` constraints with vendor-specific functions |

SAIQL documents every lossy transformation so you can make informed decisions.

---

## Success Depends on Scope

| Migration Type | Expected Outcome |
|----------------|------------------|
| **Schema-only** | Typically clean migration |
| **Common data types** | High compatibility across platforms |
| **Standard SQL (ANSI)** | Excellent portability |
| **Vendor-specific features** | May require adapters, transforms, or manual intervention |
| **Proprietary extensions** | Often unsupported; requires explicit handling |

### What Migrates Cleanly

- Standard table definitions (`CREATE TABLE`)
- Primary keys, foreign keys, unique constraints
- Common indexes (B-tree, unique)
- Basic data types (`INT`, `VARCHAR`, `DATE`, `BOOLEAN`)
- Simple views and queries

### What May Require Attention

- Materialized views
- Partitioned tables
- Full-text search indexes
- Spatial/geographic data types
- Custom data types and domains
- Triggers and stored procedures
- Database-specific functions
- Replication and clustering configurations

---

## Your Responsibilities

Before executing any migration:

1. **Review the migration report** — Understand all warnings and lossy transformations
2. **Test in a non-production environment** — Validate data integrity and application compatibility
3. **Back up your source database** — SAIQL doesn't modify source data, but backups are always prudent
4. **Validate application behavior** — Ensure your application handles any schema differences
5. **Plan for rollback** — Have a recovery strategy if issues are discovered post-migration

---

## Limitations

SAIQL does **not** guarantee:

- Zero data loss for all possible schemas
- Perfect semantic equivalence across all database vendors
- Automatic handling of proprietary features
- Performance parity between source and target databases
- Compatibility with undocumented or deprecated features

SAIQL **does** guarantee:

- Transparent reporting of all detected incompatibilities
- Read-only analysis that never modifies source data
- Explicit documentation of every transformation applied
- Fail-safe behavior when proof of correctness cannot be established

---

## Summary

| Principle | Implementation |
|-----------|----------------|
| **Source Protection** | Source database is NEVER modified—read-only access only |
| **Transparency** | Every change is documented before execution |
| **Safety** | All writes go to target database only, after explicit approval |
| **Control** | You approve every migration plan before any writes occur |
| **Honesty** | Limitations are stated clearly, not hidden |

---

## Legal Notice

SAIQL is provided "as-is" without warranty of any kind, express or implied. The developers and contributors are not liable for any data loss, corruption, or damages arising from the use of this software. Users are solely responsible for validating migration results and maintaining appropriate backups.

**Always test migrations in a non-production environment before applying to production systems.**

---

*For questions, issues, or feature requests, visit the [SAIQL GitHub repository](https://github.com/apolloraines/SAIQL-CE).*
