# 💻 CLI Guide

**Goal**: Master the SAIQL Command Line Interface.

The CLI is the primary tool for managing SAIQL, executing queries, and handling data.

---

## 1. Basic Usage
The CLI is located at `bin/saiql.py`.

```bash
# Show help
python3 bin/saiql.py --help

# Check version
python3 bin/saiql.py --version
```

## 2. Interactive Mode (REPL)
Launch a shell to run queries interactively.

```bash
python3 bin/saiql.py --interactive
```

**Commands inside REPL**:
- Type your query and press Enter.
- `exit` or `quit`: Exit the shell.
- `history`: Show query history.

## 3. Executing Queries
Run a single query directly from the command line.

```bash
python3 bin/saiql.py --query "SELECT * FROM users"
python3 bin/saiql.py --query "SELECT * FROM products WHERE price > 100"
```

## 4. Loading Data
Load a specific database file.

```bash
python3 bin/saiql.py --load data/my_database.db
```

## 5. Cache Management
Warm up the cache for faster performance.

```bash
python3 bin/saiql.py --warm-cache
```

## 6. Debug Mode
Enable verbose logging for troubleshooting.

```bash
python3 bin/saiql.py --debug --query "SELECT * FROM users"
```

---

### Common Issues
- **ImportError**: Ensure you are in the root directory (`SAIQL.DEV`) and your virtual environment is active.
- **Permission Denied**: Check file permissions on the database file.
- **Default DB Path**: Set `SAIQL_DEFAULT_DB` to point CLI to your database (e.g., migrated `data/saiql_store.db`).

### Next Step
- **[06_API_Guide.md](./06_API_Guide.md)**: Learn about the REST API.
