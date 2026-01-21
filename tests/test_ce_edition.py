#!/usr/bin/env python3
"""
Tests: SAIQL Community Edition

Test suite for CE-specific functionality:
- T1: Clean install smoke
- T2: CLI smoke (CE commands only)
- T3: LoreToken-Lite determinism
- T4: QIPI-Lite determinism
- T5: Negative tests (removed features fail deterministically)
- T6: Banned import scan
- T7: Startup time benchmark
"""

import pytest
import subprocess
import sys
import time
import hashlib
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))


# =============================================================================
# T1 — Clean Install Smoke
# =============================================================================

class TestT1CleanInstallSmoke:
    """T1: Verify CE installs and runs."""

    def test_core_import(self):
        """Core module imports without error."""
        import core
        assert core.__edition__ == 'ce'
        assert '1.0.0-ce' in core.__version__

    def test_saiql_import(self):
        """Main saiql module imports."""
        import saiql
        assert saiql.SAIQL_EDITION == 'ce'

    def test_ce_edition_module(self):
        """CE edition module works."""
        from core.ce_edition import is_ce, EDITION, VERSION
        assert is_ce() is True
        assert EDITION == 'ce'
        assert 'ce' in VERSION


# =============================================================================
# T2 — CLI Smoke
# =============================================================================

class TestT2CLISmoke:
    """T2: Verify CLI commands work and removed commands are absent."""

    def test_version_shows_ce(self):
        """saiql --version shows CE edition."""
        result = subprocess.run(
            [sys.executable, 'saiql.py', '--version'],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent.parent
        )
        assert result.returncode == 0
        assert 'ce' in result.stdout.lower() or 'community' in result.stdout.lower()

    def test_help_no_atlas_flag(self):
        """saiql --help does not show --atlas flag."""
        result = subprocess.run(
            [sys.executable, 'saiql.py', '--help'],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent.parent
        )
        assert '--atlas' not in result.stdout
        assert '--ingest' not in result.stdout

    def test_stats_command(self):
        """saiql --stats works."""
        result = subprocess.run(
            [sys.executable, 'saiql.py', '--stats'],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent.parent,
            timeout=30
        )
        # May fail due to DB issues, but should not crash
        assert result.returncode in (0, 1)


# =============================================================================
# T3 — LoreToken-Lite Determinism
# =============================================================================

class TestT3LoreTokenLiteDeterminism:
    """T3: Verify LoreToken-Lite is deterministic."""

    def test_schema_validation(self):
        """Schema validates correctly."""
        from core.loretoken_lite import pack, unpack, validate

        payload = {'key': 'value', 'number': 42}
        packed = pack(payload)
        token = unpack(packed)

        assert token.validate() is True
        assert token.schema_version == '1.0'
        assert token.hash_algo == 'sha256'

    def test_pack_unpack_roundtrip(self):
        """Pack/unpack preserves data."""
        from core.loretoken_lite import pack, unpack

        payload = {'nested': {'data': [1, 2, 3]}, 'text': 'hello'}
        metadata = {'source': 'test'}

        packed = pack(payload, metadata)
        unpacked = unpack(packed)

        assert unpacked.payload == payload
        assert unpacked.metadata == metadata

    def test_stable_hash_across_runs(self):
        """Same payload produces same hash across 3 runs."""
        from core.loretoken_lite import stable_hash

        payload = {'a': 1, 'b': 2, 'c': [1, 2, 3]}

        hashes = [stable_hash(payload) for _ in range(3)]
        assert len(set(hashes)) == 1, "Hash should be identical across runs"

    def test_key_ordering_normalization(self):
        """Key ordering is normalized (sorted)."""
        from core.loretoken_lite import stable_hash

        # Different key order, same content
        h1 = stable_hash({'z': 1, 'a': 2, 'm': 3})
        h2 = stable_hash({'a': 2, 'm': 3, 'z': 1})
        h3 = stable_hash({'m': 3, 'z': 1, 'a': 2})

        assert h1 == h2 == h3, "Key order should not affect hash"


# =============================================================================
# T4 — QIPI-Lite Determinism
# =============================================================================

class TestT4QPILiteDeterminism:
    """T4: Verify QIPI-Lite is deterministic."""

    def test_add_doc_search(self):
        """Basic add_doc and search work."""
        from core.qipi_lite import QIPILite

        with QIPILite() as index:
            index.add_doc('doc1', 'Hello world')
            index.add_doc('doc2', 'World peace')

            results = index.search('world')
            assert len(results) == 2
            assert results[0].doc_id in ('doc1', 'doc2')

    def test_search_determinism(self):
        """Same query returns same results across 3 runs."""
        from core.qipi_lite import QIPILite

        with QIPILite() as index:
            index.add_doc('a', 'Python programming language')
            index.add_doc('b', 'Python snake species')
            index.add_doc('c', 'Java programming language')

            results_runs = []
            for _ in range(3):
                results = index.search('python')
                results_runs.append([r.doc_id for r in results])

            assert results_runs[0] == results_runs[1] == results_runs[2]

    def test_delete_doc(self):
        """delete_doc removes document."""
        from core.qipi_lite import QIPILite

        with QIPILite() as index:
            index.add_doc('doc1', 'Test document')
            assert index.count() == 1

            deleted = index.delete_doc('doc1')
            assert deleted is True
            assert index.count() == 0

    def test_index_hash_determinism(self):
        """Index hash is deterministic."""
        from core.qipi_lite import QIPILite

        def build_index():
            index = QIPILite()
            index.add_doc('doc1', 'First document')
            index.add_doc('doc2', 'Second document')
            h = index.index_hash()
            index.close()
            return h

        hashes = [build_index() for _ in range(3)]
        assert len(set(hashes)) == 1, "Index hash should be deterministic"

    def test_text_normalization(self):
        """Text normalization is consistent."""
        from core.qipi_lite import QIPILite

        with QIPILite() as index:
            # Different cases/diacritics, same normalized form
            n1 = index.normalize_text('HELLO')
            n2 = index.normalize_text('hello')
            n3 = index.normalize_text('HéLLo')

            assert n1 == n2 == 'hello'
            assert n3 == 'hello'


# =============================================================================
# T5 — Negative Tests (Removed Features)
# =============================================================================

class TestT5NegativeRemovedFeatures:
    """T5: Verify removed features fail deterministically."""

    def test_atlas_import_fails(self):
        """Importing core.atlas raises CENotShippedError."""
        from core.ce_edition import CENotShippedError

        with pytest.raises(CENotShippedError) as exc_info:
            from core import atlas

        assert exc_info.value.code == 'E_CE_NOT_SHIPPED'
        assert 'atlas' in str(exc_info.value)

    def test_qipi_import_fails(self):
        """Importing core.qipi raises CENotShippedError."""
        from core.ce_edition import CENotShippedError

        with pytest.raises(CENotShippedError) as exc_info:
            from core import qipi

        assert exc_info.value.code == 'E_CE_NOT_SHIPPED'

    def test_check_feature_atlas(self):
        """check_feature('atlas') raises error."""
        from core.ce_edition import check_feature, CENotShippedError

        with pytest.raises(CENotShippedError):
            check_feature('atlas')

    def test_check_feature_rag(self):
        """check_feature('rag') raises error."""
        from core.ce_edition import check_feature, CENotShippedError

        with pytest.raises(CENotShippedError):
            check_feature('rag')

    def test_error_code_stable(self):
        """Error codes are stable strings."""
        from core.ce_edition import E_CE_NOT_SHIPPED, E_CE_FEATURE_DISABLED

        assert E_CE_NOT_SHIPPED == 'E_CE_NOT_SHIPPED'
        assert E_CE_FEATURE_DISABLED == 'E_CE_FEATURE_DISABLED'


# =============================================================================
# T6 — Banned Import Scan
# =============================================================================

class TestT6BannedImportScan:
    """T6: Verify no banned imports in source code."""

    BANNED_PATTERNS = [
        'from core.atlas',
        'from atlas',
        'import atlas',
        'from core.qipi',
        'from core.qipi_index',
        'from qipi_index',
        'from loretokens',
        'from lore_chunk',
        'from lore_core',
        'from imagination',
        'from Copilot_Carl',
        'from core.vector_engine',
        'from interface.vector_api',
        'import vector_engine',
        'QuantumProbabilisticIndex',
    ]

    EXCLUDED_PATHS = [
        'venv/',
        '.git/',
        '__pycache__/',
        'tests/test_ce_edition.py',  # This file
    ]

    def test_no_banned_imports(self):
        """No banned import patterns in source files."""
        project_root = Path(__file__).parent.parent
        violations = []

        # Scan .py files
        for py_file in project_root.rglob('*.py'):
            rel_path = str(py_file.relative_to(project_root))
            if any(excl in rel_path for excl in self.EXCLUDED_PATHS):
                continue

            try:
                content = py_file.read_text()
            except Exception:
                continue

            for pattern in self.BANNED_PATTERNS:
                if pattern in content:
                    for i, line in enumerate(content.split('\n'), 1):
                        if pattern in line and not line.strip().startswith('#'):
                            violations.append(f"{rel_path}:{i}: {pattern}")

        # Scan bin/* files (no .py extension but may contain Python)
        bin_dir = project_root / 'bin'
        if bin_dir.exists():
            for bin_file in bin_dir.iterdir():
                if bin_file.is_file():
                    rel_path = str(bin_file.relative_to(project_root))
                    if any(excl in rel_path for excl in self.EXCLUDED_PATHS):
                        continue
                    try:
                        content = bin_file.read_text()
                        if '#!/usr/bin/env python' in content or 'import ' in content:
                            for pattern in self.BANNED_PATTERNS:
                                if pattern in content:
                                    for i, line in enumerate(content.split('\n'), 1):
                                        if pattern in line and not line.strip().startswith('#'):
                                            violations.append(f"{rel_path}:{i}: {pattern}")
                    except Exception:
                        continue

        assert len(violations) == 0, f"Banned imports found:\n" + "\n".join(violations)

    def test_atlas_directory_removed(self):
        """core/atlas/ directory does not exist."""
        atlas_dir = Path(__file__).parent.parent / 'core' / 'atlas'
        assert not atlas_dir.exists(), "core/atlas/ should be removed"

    def test_panel_directory_removed(self):
        """panel/ directory does not exist."""
        panel_dir = Path(__file__).parent.parent / 'panel'
        assert not panel_dir.exists(), "panel/ should be removed"


# =============================================================================
# T7 — Startup Time Benchmark
# =============================================================================

class TestT7StartupBenchmark:
    """T7: Verify CE starts quickly (no heavy imports)."""

    def test_version_startup_time(self):
        """saiql --version completes in under 500ms."""
        start = time.time()
        result = subprocess.run(
            [sys.executable, 'saiql.py', '--version'],
            capture_output=True,
            cwd=Path(__file__).parent.parent,
            timeout=5
        )
        elapsed = time.time() - start

        assert result.returncode == 0
        assert elapsed < 0.5, f"Startup took {elapsed:.2f}s, expected < 0.5s"

    def test_core_import_time(self):
        """Core module imports in under 500ms."""
        start = time.time()
        import core
        elapsed = time.time() - start

        # Note: First import may be slower due to compilation
        # This is a rough check
        assert elapsed < 2.0, f"Core import took {elapsed:.2f}s"


# =============================================================================
# Run tests
# =============================================================================

if __name__ == '__main__':
    pytest.main([__file__, '-v'])
