# Contributing to SAIQL

Thank you for your interest in contributing to SAIQL! This document provides guidelines and instructions for contributors.

For detailed development guides, see [docs/developer_guide/contributing.md](docs/developer_guide/contributing.md).

## Quick Start

### Prerequisites
- Python 3.11 or higher
- Git

### Setup Development Environment

```bash
# Clone the repository
git clone https://github.com/apolloraines/SAIQL.git
cd SAIQL

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install in development mode with all dependencies
pip install -e ".[dev,all]"
```

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=core --cov=storage

# Run specific test file
pytest tests/unit/test_engine.py

# Run storage engine tests
python storage/test_storage.py
```

### Running Benchmarks

```bash
cd benchmarks

# Quick benchmark
python quick_benchmark.py

# Full LSM vs PostgreSQL benchmark (requires PostgreSQL)
python lsm_vs_postgresql.py

# QIPI vs B-tree benchmark
python qipi_vs_btree.py
```

### Code Quality

```bash
# Lint with ruff
ruff check core/ storage/

# Type check with mypy
mypy core/ storage/

# Format with black
black core/ storage/
```

## Development Workflow

1. **Fork** the repository
2. **Create** a feature branch (`git checkout -b feature/amazing-feature`)
3. **Make** your changes
4. **Test** your changes (`pytest`)
5. **Commit** your changes (`git commit -m 'Add amazing feature'`)
6. **Push** to your fork (`git push origin feature/amazing-feature`)
7. **Open** a Pull Request

## Code Style

- Follow PEP 8 guidelines
- Use type hints where possible
- Write docstrings for public functions/classes
- Keep line length ≤ 120 characters
- Use meaningful variable names

## Testing Guidelines

- Write tests for new features
- Maintain or improve code coverage
- Test edge cases and error conditions
- Use descriptive test names

## Pull Request Guidelines

- **Title**: Clear, concise description of changes
- **Description**: Explain what, why, and how
- **Tests**: Include tests for new functionality
- **Documentation**: Update docs if needed
- **Changelog**: Add entry to RELEASE_NOTES.md

## Questions?

- Open an issue for bugs or feature requests
- Join discussions in GitHub Discussions
- Email: apollo@saiql.ai

## License

By contributing, you agree that your contributions will be licensed under the Open Lore License v1.0.
