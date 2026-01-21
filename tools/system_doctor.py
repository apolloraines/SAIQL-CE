#!/usr/bin/env python3
"""
SAIQL System Doctor
Performs environment checks and can apply safe, reversible fixes when --fix is passed.
"""

import argparse
import hashlib
import json
import shutil
import sys
import time
from pathlib import Path
from typing import Dict, List, Tuple


DEFAULT_MODEL = "all-MiniLM-L6-v2"
MODELS_DIR = Path("models")
INDEXES_DIR = Path("indexes")
HYBRID_CONFIG = Path("config/hybrid_config.json")
HYBRID_EVAL_LOG = Path("logs/hybrid_eval.json")


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def load_metadata(meta_path: Path) -> Dict:
    with open(meta_path, "r", encoding="utf-8") as f:
        return json.load(f)


def rebuild_index_dir(index_dir: Path) -> Tuple[bool, str]:
    try:
        if index_dir.exists():
            ts = int(time.time())
            target = index_dir.with_name(index_dir.name + ".corrupt")
            if target.exists():
                target = target.with_name(target.name + f".{ts}")
            index_dir.rename(target)
        return True, f"Marked index {index_dir} for rebuild"
    except Exception as exc:
        return False, f"Failed to mark {index_dir} for rebuild: {exc}"


def check_indexes(apply_fix: bool) -> List[str]:
    messages = []
    if not INDEXES_DIR.exists():
        messages.append("Indexes dir not found; skipping checksum checks.")
        return messages

    for meta_path in INDEXES_DIR.rglob("metadata.json"):
        try:
            index_root = meta_path.parent
            # Skip already-quarantined directories
            if str(index_root).endswith(".corrupt") or ".corrupt." in index_root.name:
                messages.append(f"{index_root}: skipped (already quarantined)")
                continue

            metadata = load_metadata(meta_path)
            
            # QIPI Bundle Check
            if "format_version" in metadata:
                if metadata["format_version"] != 1:
                     messages.append(f"{index_root}: unsupported format_version {metadata['format_version']}")
                     # Don't auto-fix version mismatch, might be valid upgrade
                     continue
            
            data_path = index_root / "data.bin"
            recorded = metadata.get("checksum")
            if not recorded or not data_path.exists():
                messages.append(f"{index_root}: missing data.bin/checksum; rebuild needed.")
                if apply_fix:
                    ok, msg = rebuild_index_dir(index_root)
                    messages.append(msg)
                continue
            actual = sha256_file(data_path)
            if actual != recorded:
                messages.append(f"{index_root}: checksum mismatch ({recorded} != {actual})")
                if apply_fix:
                    ok, msg = rebuild_index_dir(index_root)
                    messages.append(msg)
            else:
                messages.append(f"{index_root}: checksum OK")

            # Schema fingerprint
            if "schema_fingerprint" in metadata:
                messages.append(f"{index_root}: schema_fingerprint={metadata.get('schema_fingerprint')}")
            else:
                messages.append(f"{index_root}: schema_fingerprint missing")
                
            # WAL Check
            wal_path = index_root / "wal.log"
            if wal_path.exists():
                size = wal_path.stat().st_size
                messages.append(f"{index_root}: WAL present (size={size} bytes)")
            else:
                messages.append(f"{index_root}: WAL missing (clean shutdown?)")
        except Exception as exc:
            index_root = meta_path.parent
            messages.append(f"{index_root}: error reading metadata ({exc})")
            if apply_fix:
                ok, msg = rebuild_index_dir(index_root)
                messages.append(msg)
    return messages


def ensure_model(apply_fix: bool) -> List[str]:
    messages = []
    model_path = MODELS_DIR / DEFAULT_MODEL
    if model_path.exists():
        messages.append(f"Model present: {model_path}")
        return messages

    messages.append(f"Model missing: {model_path}")
    if not apply_fix:
        return messages

    try:
        from sentence_transformers import SentenceTransformer

        model_path.parent.mkdir(parents=True, exist_ok=True)
        start = time.perf_counter()
        SentenceTransformer(DEFAULT_MODEL, cache_folder=str(model_path.parent))
        elapsed = time.perf_counter() - start
        messages.append(f"Downloaded model {DEFAULT_MODEL} in {elapsed:.2f}s")
    except Exception as exc:
        messages.append(f"Failed to download model {DEFAULT_MODEL}: {exc}")
    return messages


def load_hybrid_config() -> Dict:
    if HYBRID_CONFIG.exists():
        try:
            with open(HYBRID_CONFIG, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {
        "rerank_enabled": False,
        "rerank_timeout_ms": 50,
        "rerank_model_path": "",
        "max_rerank_k": 50,
    }


def save_hybrid_config(cfg: Dict):
    HYBRID_CONFIG.parent.mkdir(parents=True, exist_ok=True)
    with open(HYBRID_CONFIG, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2)


def handle_latency_gate(apply_fix: bool) -> List[str]:
    messages = []
    cfg = load_hybrid_config()
    gate_failed = False
    if HYBRID_EVAL_LOG.exists():
        try:
            with open(HYBRID_EVAL_LOG, "r", encoding="utf-8") as f:
                eval_data = json.load(f)
            gate_failed = bool(eval_data.get("current", {}).get("drop_pct", 0) > 10)
            if gate_failed:
                messages.append(f"Hybrid eval gate failed: drop_pct={eval_data.get('current', {}).get('drop_pct'):.2f}%")
        except Exception as exc:
            messages.append(f"Could not read hybrid eval log: {exc}")

    if gate_failed and cfg.get("rerank_enabled", False):
        messages.append("Reranker is enabled and TPS gate failed.")
        if apply_fix:
            cfg["rerank_enabled"] = False
            save_hybrid_config(cfg)
            messages.append("Auto-disabled reranker in hybrid_config.json")
    else:
        messages.append("Rerank gate OK or reranker already disabled.")
    return messages


def ensure_logs_dir():
    Path("logs").mkdir(exist_ok=True)


def doctor(apply_fix: bool) -> List[str]:
    ensure_logs_dir()
    messages = []
    messages.append("== Index checks ==")
    messages.extend(check_indexes(apply_fix))
    messages.append("\n== Models ==")
    messages.extend(ensure_model(apply_fix))
    messages.append("\n== Hybrid latency gate ==")
    messages.extend(handle_latency_gate(apply_fix))
    return messages


def parse_args():
    parser = argparse.ArgumentParser(description="SAIQL system doctor")
    parser.add_argument("--fix", action="store_true", help="Apply safe fixes instead of only reporting")
    return parser.parse_args()


def main():
    args = parse_args()
    messages = doctor(apply_fix=args.fix)
    mode = "FIX" if args.fix else "REPORT"
    print(f"SAIQL Doctor ({mode})")
    print("-" * 40)
    for msg in messages:
        print(msg)


if __name__ == "__main__":
    main()
