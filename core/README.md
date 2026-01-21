# SAIQL Core Components

Welcome to the SAIQL core engine! This directory contains the canonical implementation of the SAIQL query processing pipeline.

## 🚀 Quick Start

**For most use cases, use the frontend:**

```python
from core.frontend import tokenize, parse, compile
from core.frontend import SAIQLLexer, SAIQLParser, SAIQLCompiler

# Tokenize a query
tokens = tokenize("*10[users]::name,email>>oQ")

# Parse to AST
ast = parse("*10[users]::name,email>>oQ")

# Compile to SQL
sql = compile(ast, dialect='postgresql')
```

## 📁 File Guide

### Core Pipeline (Use These!)

| File | Purpose | When to Import |
|------|---------|----------------|
| **frontend.py** 👈 | **Unified public API** | **Start here!** |
| **lexer.py** | Tokenization (SQL + SAIQL) | Direct lexer control needed |
| **parser.py** | AST construction | Direct parser control needed |
| **compiler.py** | Optimization + code generation | Custom compilation |
| **runtime.py** | Query execution | Direct execution control |
| **engine.py** | High-level orchestration | Full engine initialization |

### Supporting Components (CE)

- **database_manager.py** - Multi-backend support (SQLite, PostgreSQL, MySQL, File)
- **index_manager.py** - B-tree and Hash indexing
- **qipi_lite.py** - SQLite FTS5 text search
- **loretoken_lite.py** - Lightweight data serialization
- **semantic_firewall.py** - Query safety and injection protection
- **grounding_guard.py** - Quality mode / hallucination prevention

### Advanced

- **execution_planner.py** - Query optimization
- **join_engine.py** - JOIN algorithms

## 🏗️ Architecture

```
Query Flow:
──────────

SAIQL String
    ↓
[Lexer] → Tokens
    ↓
[Parser] → AST
    ↓
[Compiler] → Optimized IR + SQL
    ↓
[Runtime] → Execute
    ↓
Results
```

## 🎯 Import Patterns

### ✅ Recommended

```python
# Use the frontend for standard operations
from core.frontend import tokenize, parse, compile

# Or import core classes directly
from core import SAIQLLexer, SAIQLParser, SAIQLEngine
```

### ⚠️ Advanced (If you know what you're doing)

```python
# Direct component access for customization
from core.lexer import SAIQLLexer, TokenType
from core.parser import SAIQLParser, ASTNode, NodeType
from core.compiler import SAIQLCompiler, OptimizationLevel
```

### ❌ Don't Do This

```python
# WRONG - These are deprecated experimental versions
from core.core_lexer import SAIQLLexer  # NO!
from core.legacy.experimental.core_parser import SAIQLParser  # NO!
```

## 🧪 Testing

```bash
# Run core component tests
pytest tests/unit/ -v

# Run integration tests
pytest tests/integration/ -v
```

## 📚 Learn More

- **ROADMAP.md** - Development roadmap
- **docs/** - Full documentation
- **examples/** - Usage examples

## 🔧 Development

### Adding a New Operator

1. Add token in `lexer.py`
2. Add AST node in `parser.py` (if needed)
3. Add implementation in `runtime.py`
4. Add tests in `tests/`

### Testing Your Changes

```python
# Quick validation
python -c "from core.frontend import *; print('OK')"

# Run test suite
pytest tests/ -v
```

---

**Questions?** See the main README.md or open an issue.
