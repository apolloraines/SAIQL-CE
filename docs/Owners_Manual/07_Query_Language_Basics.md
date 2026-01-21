# 🗣️ SAIQL Query Language Basics

**Goal**: Master the syntax of Semantic AI Query Language.

SAIQL supports standard SQL operations across multiple database backends.

---

## 1. Standard SQL Support
SAIQL supports a subset of ANSI SQL for standard operations.

```sql
-- Select
SELECT id, name FROM users WHERE active = true;

-- Insert
INSERT INTO users (id, name, active) VALUES (1, 'Alice', true);

-- Update
UPDATE users SET active = false WHERE id = 1;

-- Delete
DELETE FROM users WHERE id = 1;
```

---

### Best Practices
- **Quoting**: Use single quotes `'` for string literals.
- **Case Sensitivity**: Keywords are case-insensitive (`SELECT` vs `select`), but identifiers may be case-sensitive depending on the backend.
- **Comments**: Use `--` for single-line comments.

### Next Step
- **[08_Storage_Backends.md](./08_Storage_Backends.md)**: Understand where your data lives.
