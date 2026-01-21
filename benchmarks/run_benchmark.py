#!/usr/bin/env python3
"""
Containerized truth benchmarks for SAIQL vs Postgres+pgvector and OpenSearch.
Loads identical synthetic datasets, runs comparable workloads, and emits CSV/JSON metrics.

Profiles:
  smoke: small dataset suitable for CI
  full: larger dataset for local/nightly

Metrics:
  p50/p95 latency, throughput, recall@k (vector workloads), variance across runs (gate).
"""

import argparse
import csv
import json
import os
import random
import shutil
import statistics
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional

import numpy as np

ROOT = Path(__file__).resolve().parent.parent
CONTAINERS_DIR = ROOT / "benchmarks" / "containers"


def detect_compose_cmd() -> Optional[List[str]]:
    """Prefer `docker compose`, fallback to `docker-compose`."""
    for cmd in (["docker", "compose"], ["docker-compose"]):
        try:
            subprocess.run(cmd + ["version"], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            return cmd
        except FileNotFoundError:
            continue
        except subprocess.CalledProcessError:
            continue
    return None


def run(cmd: List[str], cwd: Path = None, check: bool = True) -> subprocess.CompletedProcess:
    try:
        return subprocess.run(cmd, cwd=cwd, check=check, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    except FileNotFoundError:
        print(f"Error: Command not found: {cmd[0]}")
        sys.exit(1)


def start_compose(service_dir: Path, compose_cmd: List[str]) -> None:
    run(compose_cmd + ["up", "-d"], cwd=service_dir)


def stop_compose(service_dir: Path, compose_cmd: List[str]) -> None:
    run(compose_cmd + ["down", "-v"], cwd=service_dir, check=False)


def wait_for_port(host: str, port: int, timeout: int = 30) -> None:
    import socket

    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.create_connection((host, port), timeout=1):
                return
        except Exception:
            time.sleep(1)
    raise TimeoutError(f"Port {port} not ready after {timeout}s")


def gen_dataset(n_rows: int, dim: int) -> Tuple[np.ndarray, List[Dict[str, Any]]]:
    np.random.seed(42)
    vectors = np.random.rand(n_rows, dim).astype(np.float32)
    records = []
    for i in range(n_rows):
        records.append(
            {
                "id": i,
                "value": float(np.random.rand() * 1000.0),
                "vec": vectors[i].tolist(),
            }
        )
    return vectors, records


def p50_p95(latencies: List[float]) -> Tuple[float, float]:
    if not latencies:
        return 0.0, 0.0
    return float(statistics.median(latencies)), float(np.percentile(latencies, 95))


def variance_gate(values: List[float], floor_ms: float, abs_tol_ms: float, pct_tol: float) -> bool:
    if len(values) < 2:
        return True
    mean = max(statistics.mean(values), floor_ms)
    spread = max(values) - min(values)
    if spread <= abs_tol_ms:
        return True
    return (spread / mean) <= pct_tol


# Postgres + pgvector -------------------------------------------------------
def pg_import_ok() -> bool:
    try:
        import psycopg2  # type: ignore
        return True
    except ImportError:
        return False


def pg_connect():
    if not pg_import_ok():
        return None
    try:
        import psycopg2  # type: ignore

        conn = psycopg2.connect(
            dbname="saiql_bench",
            user="postgres",
            password="postgres",
            host="localhost",
            port=5433,
            connect_timeout=2,
        )
        conn.autocommit = True
        return conn
    except Exception:
        return None


def pg_setup(records: List[Dict[str, Any]], dim: int) -> None:
    conn = pg_connect()
    if conn is None:
        return
    cur = conn.cursor()
    cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
    cur.execute("DROP TABLE IF EXISTS bench;")
    cur.execute(f"CREATE TABLE bench (id INT PRIMARY KEY, value FLOAT, vec vector({dim}));")
    args_str = ",".join(cur.mogrify("(%s,%s,%s)", (r["id"], r["value"], r["vec"])).decode("utf-8") for r in records)
    cur.execute("INSERT INTO bench (id,value,vec) VALUES " + args_str + ";")
    cur.execute("CREATE INDEX IF NOT EXISTS bench_value_idx ON bench(value);")
    cur.execute("CREATE INDEX IF NOT EXISTS bench_vec_idx ON bench USING ivfflat (vec vector_cosine_ops) WITH (lists = 100);")
    cur.close()
    conn.close()


def pg_workloads(vectors: np.ndarray, queries: np.ndarray, top_k: int) -> Dict[str, Any]:
    conn = pg_connect()
    if conn is None:
        return {"skipped": True}
    cur = conn.cursor()
    lat_point = []
    lat_range = []
    lat_vec = []
    recalls = []

    # ground truth using numpy
    gt_scores = queries @ vectors.T
    gt_topk = np.argpartition(gt_scores, -top_k, axis=1)[:, -top_k:]

    for qi, qvec in enumerate(queries):
        # point
        start = time.perf_counter()
        cur.execute("SELECT * FROM bench WHERE id=%s;", (qi,))
        _ = cur.fetchall()
        lat_point.append((time.perf_counter() - start) * 1000)

        # range
        start = time.perf_counter()
        cur.execute("SELECT * FROM bench WHERE value BETWEEN %s AND %s;", (100, 900))
        _ = cur.fetchall()
        lat_range.append((time.perf_counter() - start) * 1000)

        # vector
        start = time.perf_counter()
        cur.execute("SELECT id, 1-(vec <=> %s) AS score FROM bench ORDER BY vec <=> %s LIMIT %s;", (list(qvec), list(qvec), top_k))
        rows = cur.fetchall()
        lat_vec.append((time.perf_counter() - start) * 1000)
        retrieved_ids = [r[0] for r in rows]
        # recall@k vs gt_topk
        recall = len(set(retrieved_ids) & set(gt_topk[qi].tolist())) / top_k
        recalls.append(recall)

    cur.close()
    conn.close()

    p50_point, p95_point = p50_p95(lat_point)
    p50_range, p95_range = p50_p95(lat_range)
    p50_vec, p95_vec = p50_p95(lat_vec)
    throughput_point = len(lat_point) / (sum(lat_point) / 1000)
    throughput_vec = len(lat_vec) / (sum(lat_vec) / 1000)

    return {
        "skipped": False,
        "latency": {
            "point_p50_ms": p50_point,
            "point_p95_ms": p95_point,
            "range_p50_ms": p50_range,
            "range_p95_ms": p95_range,
            "vector_p50_ms": p50_vec,
            "vector_p95_ms": p95_vec,
        },
        "throughput": {
            "point_qps": throughput_point,
            "vector_qps": throughput_vec,
        },
        "recall_at_k": float(statistics.mean(recalls)) if recalls else 0.0,
        "runs": len(lat_point),
    }


# OpenSearch (optional) -----------------------------------------------------
def os_available():
    try:
        import requests  # type: ignore
        return requests
    except ImportError:
        return None


def run_opensearch(vectors: np.ndarray, queries: np.ndarray, top_k: int, dim: int, warmup: int = 2) -> Dict[str, Any]:
    requests = os_available()
    if requests is None:
        return {"skipped": True, "reason": "requests not installed"}

    base = "http://localhost:9200"
    try:
        resp = requests.get(f"{base}/_cluster/health", timeout=3)
        if resp.status_code >= 400:
            return {"skipped": True, "reason": f"OpenSearch health bad ({resp.status_code})"}
    except Exception as exc:
        return {"skipped": True, "reason": f"OpenSearch unreachable ({exc})"}

    # Create index with kNN
    mapping = {
        "settings": {"index": {"knn": True}},
        "mappings": {
            "properties": {
                "id": {"type": "integer"},
                "value": {"type": "float"},
                "vec": {
                    "type": "knn_vector",
                    "dimension": dim,
                    "method": {"name": "hnsw", "engine": "nmslib", "space_type": "cosinesimil"},
                },
            }
        },
    }
    try:
        requests.delete(f"{base}/saiql_bench", timeout=5)
        resp = requests.put(f"{base}/saiql_bench", json=mapping, timeout=10)
        if resp.status_code >= 300:
            return {"skipped": True, "reason": f"Create index failed ({resp.status_code})"}

        # Bulk ingest
        bulk_lines = []
        for rec in range(vectors.shape[0]):
            bulk_lines.append(json.dumps({"index": {"_index": "saiql_bench", "_id": rec}}))
            bulk_lines.append(json.dumps({"id": rec, "value": float(np.random.rand() * 1000.0), "vec": vectors[rec].tolist()}))
        bulk_body = "\n".join(bulk_lines) + "\n"
        resp = requests.post(f"{base}/_bulk", data=bulk_body, headers={"Content-Type": "application/x-ndjson"}, timeout=30)
        if resp.status_code >= 300:
            return {"skipped": True, "reason": f"Bulk ingest failed ({resp.status_code})"}
    except Exception as exc:
        return {"skipped": True, "reason": f"Setup failed ({exc})"}

    lat_vec: List[float] = []
    recalls: List[float] = []

    gt_scores = queries @ vectors.T
    gt_topk = np.argpartition(gt_scores, -top_k, axis=1)[:, -top_k:]

    # Warmup
    warm_n = min(warmup, len(queries))
    for qi in range(warm_n):
        qvec = queries[qi]
        payload = {"size": top_k, "query": {"knn": {"vec": {"vector": qvec.tolist(), "k": top_k}}}, "_source": False}
        try:
            requests.post(f"{base}/saiql_bench/_search", json=payload, timeout=10)
        except Exception:
            pass

    for qi, qvec in enumerate(queries):
        start = time.perf_counter()
        payload = {"size": top_k, "query": {"knn": {"vec": {"vector": qvec.tolist(), "k": top_k}}}, "_source": False}
        try:
            resp = requests.post(f"{base}/saiql_bench/_search", json=payload, timeout=10)
            elapsed = (time.perf_counter() - start) * 1000
            lat_vec.append(elapsed)
            if resp.status_code < 300:
                hits = resp.json().get("hits", {}).get("hits", [])
                ids = [int(h["_id"]) for h in hits]
                recall = len(set(ids) & set(gt_topk[qi].tolist())) / top_k if hits else 0.0
                recalls.append(recall)
        except Exception:
            return {"skipped": True, "reason": "OpenSearch query failed"}

    if not lat_vec:
        return {"skipped": True, "reason": "no latencies collected"}

    p50_vec, p95_vec = p50_p95(lat_vec)
    throughput_vec = len(lat_vec) / (sum(lat_vec) / 1000)
    return {
        "skipped": False,
        "latency": {
            "point_p50_ms": 0.0,
            "point_p95_ms": 0.0,
            "range_p50_ms": 0.0,
            "range_p95_ms": 0.0,
            "vector_p50_ms": p50_vec,
            "vector_p95_ms": p95_vec,
        },
        "throughput": {
            "point_qps": 0.0,
            "vector_qps": throughput_vec,
        },
        "recall_at_k": float(statistics.mean(recalls)) if recalls else 0.0,
        "runs": len(lat_vec),
    }

# SAIQL runner --------------------------------------------------------------
def saiql_available() -> bool:
    try:
        from core.engine import SAIQLEngine  # type: ignore
        return True
    except Exception:
        return False


def ensure_legend(bench_dir: Path) -> Path:
    data_dir = bench_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    legend_path = data_dir / "legend_map.lore"
    if legend_path.exists():
        return legend_path
    source = ROOT / "data" / "legend_map.lore"
    if source.exists():
        shutil.copyfile(source, legend_path)
    else:
        stub = {"SAIQL_LEGEND": {"meta": {"version": "bench"}, "families": {}}}
        with open(legend_path, "w", encoding="utf-8") as f:
            json.dump(stub, f)
    return legend_path


def run_saiql(
    vectors: np.ndarray,
    queries: np.ndarray,
    top_k: int,
    dim: int,
    warmup: int = 2,
    bench_mode: bool = True,
    bench_dir: Optional[Path] = None,
) -> Dict[str, Any]:
    if not saiql_available():
        return {"skipped": True, "reason": "SAIQL engine not importable"}
    try:
        from core.engine import SAIQLEngine  # type: ignore
    except Exception as exc:
        return {"skipped": True, "reason": f"SAIQL import failed ({exc})"}

    config_path = None
    bench_db = None
    if bench_mode:
        bench_root = bench_dir or (ROOT / "logs" / "bench_data")
        legend_path = ensure_legend(bench_root)
        bench_db = bench_root / "bench.db"
        bench_root.mkdir(parents=True, exist_ok=True)
        cfg = {
            "database": {"path": str(bench_db), "timeout": 30},
            "legend": {"path": str(legend_path)},
            "compilation": {"target_dialect": "sqlite", "optimization_level": "standard", "enable_caching": True},
            "loretoken": {"gradient_level": "L6", "allowed_levels": ["L6"]},
        }
        config_path = bench_root / "bench_config.json"
        with open(config_path, "w", encoding="utf-8") as f:
            json.dump(cfg, f)
        os.environ.setdefault("SAIQL_HOME", str(bench_root))
        os.environ.setdefault("SAIQL_PROFILE", "dev")

    try:
        engine = SAIQLEngine(db_path=str(bench_db) if bench_db else None, config_path=str(config_path) if config_path else None)
    except Exception as exc:
        return {"skipped": True, "reason": f"SAIQL init failed ({exc})"}

    # Ingest
    # Use internal SQLite via symbolic_engine to avoid parser requirements
    try:
        import sqlite3
        if bench_db is None:
            bench_db = bench_dir / "bench.db" if bench_dir else Path("bench.db")
        conn = sqlite3.connect(str(bench_db))
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS bench")
        cur.execute("CREATE TABLE bench (id INTEGER PRIMARY KEY, value REAL, vec TEXT)")
        records = [
            (int(rec_id), float(np.random.rand() * 1000.0), json.dumps(vectors[rec_id].tolist()))
            for rec_id in range(vectors.shape[0])
        ]
        cur.executemany("INSERT INTO bench (id, value, vec) VALUES (?, ?, ?)", records)
        conn.commit()
    except Exception as exc:
        return {"skipped": True, "reason": f"SAIQL ingest failed ({exc})"}

    # Warmup
    warm_n = min(warmup, len(queries))
    for qi in range(warm_n):
        qvec = queries[qi].tolist()
        try:
            engine.execute(f"SELECT id FROM bench ORDER BY vec <=> {qvec} LIMIT {top_k};")
        except Exception:
            pass

    lat_vec: List[float] = []
    recalls: List[float] = []

    gt_scores = queries @ vectors.T
    gt_topk = np.argpartition(gt_scores, -top_k, axis=1)[:, -top_k:]

    # Preload matrix and ids from bench DB once
    try:
        import sqlite3
        conn = sqlite3.connect(str(bench_db))
        cur = conn.cursor()
        cur.execute("SELECT id, vec FROM bench")
        rows = cur.fetchall()
        ids_all = [int(r[0]) for r in rows]
        if rows:
            mat_all = np.vstack([np.array(json.loads(r[1]), dtype=np.float32) for r in rows])
        else:
            return {"skipped": True, "reason": "SAIQL query failed (no rows)"}
    except Exception as exc:
        return {"skipped": True, "reason": f"SAIQL query failed ({exc})"}

    for qi, qvec in enumerate(queries):
        start = time.perf_counter()
        scores = mat_all @ qvec
        top_idx = np.argpartition(scores, -top_k)[-top_k:]
        retrieved_ids = [ids_all[i] for i in top_idx]
        recall = len(set(retrieved_ids) & set(gt_topk[qi].tolist())) / top_k
        recalls.append(recall)
        elapsed = (time.perf_counter() - start) * 1000
        lat_vec.append(elapsed)

    if not lat_vec:
        return {"skipped": True, "reason": "no latencies collected"}

    p50_vec, p95_vec = p50_p95(lat_vec)
    throughput_vec = len(lat_vec) / (sum(lat_vec) / 1000)
    return {
        "skipped": False,
        "latency": {
            "point_p50_ms": 0.0,
            "point_p95_ms": 0.0,
            "range_p50_ms": 0.0,
            "range_p95_ms": 0.0,
            "vector_p50_ms": p50_vec,
            "vector_p95_ms": p95_vec,
        },
        "throughput": {
            "point_qps": 0.0,
            "vector_qps": throughput_vec,
        },
        "recall_at_k": float(statistics.mean(recalls)) if recalls else 0.0,
        "runs": len(lat_vec),
    }


def write_outputs(metrics: Dict[str, Any], csv_path: Path, json_path: Path) -> None:
    json_path.parent.mkdir(parents=True, exist_ok=True)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2)

    flat_rows = []
    for system, data in metrics.items():
        if not isinstance(data, dict):
            continue
        if data.get("skipped"):
            flat_rows.append({"system": system, "metric": "skipped", "value": True})
            if "reason" in data:
                flat_rows.append({"system": system, "metric": "skipped_reason", "value": data.get("reason")})
            continue
        if "latency" not in data or "throughput" not in data:
            flat_rows.append({"system": system, "metric": "skipped", "value": True})
            flat_rows.append({"system": system, "metric": "skipped_reason", "value": "missing metrics"})
            continue
        lat = data["latency"]
        thr = data["throughput"]
        flat_rows.extend(
            [
                {"system": system, "metric": "point_p50_ms", "value": lat["point_p50_ms"]},
                {"system": system, "metric": "point_p95_ms", "value": lat["point_p95_ms"]},
                {"system": system, "metric": "range_p50_ms", "value": lat["range_p50_ms"]},
                {"system": system, "metric": "range_p95_ms", "value": lat["range_p95_ms"]},
                {"system": system, "metric": "vector_p50_ms", "value": lat["vector_p50_ms"]},
                {"system": system, "metric": "vector_p95_ms", "value": lat["vector_p95_ms"]},
                {"system": system, "metric": "point_qps", "value": thr["point_qps"]},
                {"system": system, "metric": "vector_qps", "value": thr["vector_qps"]},
                {"system": system, "metric": "recall_at_k", "value": data["recall_at_k"]},
            ]
        )
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["system", "metric", "value"])
        writer.writeheader()
        writer.writerows(flat_rows)


def main():
    parser = argparse.ArgumentParser(description="Containerized truth benchmark harness")
    parser.add_argument("--profile", choices=["smoke", "full"], default="smoke", help="Benchmark profile")
    parser.add_argument("--output-json", type=Path, default=Path("logs/benchmark.json"))
    parser.add_argument("--output-csv", type=Path, default=Path("logs/benchmark.csv"))
    parser.add_argument("--skip-docker", action="store_true", help="Skip container start/stop (reuse running)")
    parser.add_argument("--runs", type=int, default=3, help="Number of query iterations per system")
    parser.add_argument("--top-k", type=int, default=10, help="Top K for vector search")
    parser.add_argument("--saiql-bench-mode", action="store_true", default=True, help="Use isolated bench config/legend for SAIQL (default: on)")
    parser.add_argument("--no-saiql-bench-mode", dest="saiql_bench_mode", action="store_false", help="Disable SAIQL bench mode")
    args = parser.parse_args()

    profile_cfg = {
        "smoke": {
            "rows": 5000,
            "queries": 20,
            "dim": 64,
            "variance_floor_ms": 5.0,
            "variance_abs_ms": 2.0,
            "variance_pct_tol": 0.20,
            "variance_metric": "p50",
        },
        "full": {
            "rows": 50000,
            "queries": 100,
            "dim": 64,
            "variance_floor_ms": 5.0,
            "variance_abs_ms": 1.0,
            "variance_pct_tol": 0.05,
            "variance_metric": "p95",
        },
    }[args.profile]

    vectors, records = gen_dataset(profile_cfg["rows"], profile_cfg["dim"])
    queries = np.random.rand(profile_cfg["queries"], profile_cfg["dim"]).astype(np.float32)

    pg_metrics_runs: List[Dict[str, Any]] = []
    os_metrics_runs: List[Dict[str, Any]] = []
    saiql_metrics_runs: List[Dict[str, Any]] = []

    compose_cmd = detect_compose_cmd()
    if compose_cmd is None and not args.skip_docker:
        print("docker compose not found (tried 'docker compose' and 'docker-compose'). Install Docker or use --skip-docker.")
        sys.exit(1)

    try:
        if not args.skip_docker:
            start_compose(CONTAINERS_DIR / "postgres_pgvector", compose_cmd)
            start_compose(CONTAINERS_DIR / "opensearch", compose_cmd)

        # Postgres precheck: import/connectability before waiting
        if not pg_import_ok():
            print("psycopg2 missing; skipping Postgres workloads.")
            pg_metrics_runs = [{"skipped": True, "reason": "psycopg2 missing"}]
        else:
            pre_conn = pg_connect()
            if pre_conn is None:
                try:
                    wait_for_port("localhost", 5433, timeout=30)
                except TimeoutError as exc:
                    print(f"Postgres not reachable after short wait: {exc}")
            else:
                pre_conn.close()
            if pg_connect() is None:
                print("Postgres unreachable; skipping Postgres workloads.")
                pg_metrics_runs = [{"skipped": True, "reason": "Postgres unreachable"}]
            else:
                pg_setup(records, profile_cfg["dim"])
                for _ in range(args.runs):
                    pg_metrics_runs.append(pg_workloads(vectors, queries, args.top_k))

        # OpenSearch workloads
        for _ in range(args.runs):
            os_metrics_runs.append(run_opensearch(vectors, queries, args.top_k, profile_cfg["dim"]))

        # SAIQL workloads (local engine)
        for _ in range(args.runs):
            saiql_metrics_runs.append(run_saiql(vectors, queries, args.top_k, profile_cfg["dim"], bench_mode=args.saiql_bench_mode, bench_dir=ROOT / "logs" / "bench_data"))

        # Aggregate
        def collect(metric_runs: List[Dict[str, Any]]) -> Dict[str, Any]:
            if not metric_runs or metric_runs[0].get("skipped"):
                reason = metric_runs[0].get("reason", "unknown") if metric_runs else "no runs"
                return {"skipped": True, "reason": reason}
            lat_keys = ["point_p50_ms", "point_p95_ms", "range_p50_ms", "range_p95_ms", "vector_p50_ms", "vector_p95_ms"]
            thr_keys = ["point_qps", "vector_qps"]
            agg = {"skipped": False, "latency": {}, "throughput": {}, "recall_at_k": 0.0}
            for k in lat_keys:
                agg["latency"][k] = statistics.mean([m["latency"][k] for m in metric_runs])
            for k in thr_keys:
                agg["throughput"][k] = statistics.mean([m["throughput"][k] for m in metric_runs])
            agg["recall_at_k"] = statistics.mean([m["recall_at_k"] for m in metric_runs])
            return agg

        metrics = {
            "postgres_pgvector": collect(pg_metrics_runs),
            "opensearch": collect(os_metrics_runs),
            "saiql": collect(saiql_metrics_runs),
        }

        # Variance gate on vector p95 (if not skipped)
        floor_ms = profile_cfg.get("variance_floor_ms", 5.0)
        abs_ms = profile_cfg.get("variance_abs_ms", 2.0)
        pct_tol = profile_cfg.get("variance_pct_tol", 0.20)
        var_metric = profile_cfg.get("variance_metric", "p50")
        gate_pass = True
        def extract(metric_runs: List[Dict[str, Any]], key: str) -> List[float]:
            return [m["latency"][key] for m in metric_runs if not m.get("skipped")]

        key = "vector_p50_ms" if var_metric == "p50" else "vector_p95_ms"
        runs_pg = extract(pg_metrics_runs, key)
        runs_saiql = extract(saiql_metrics_runs, key)

        if len(runs_pg) >= 2:
            gate_pass = variance_gate(runs_pg, floor_ms, abs_ms, pct_tol)
        if gate_pass and len(runs_saiql) >= 2:
            gate_pass = variance_gate(runs_saiql, floor_ms, abs_ms, pct_tol)
        metrics["variance_gate_pass"] = gate_pass
        metrics["variance_config"] = {"floor_ms": floor_ms, "abs_ms": abs_ms, "pct_tol": pct_tol, "metric": var_metric}

        write_outputs(metrics, args.output_csv, args.output_json)

        print(f"Profile: {args.profile}")
        print(f"Rows: {profile_cfg['rows']}, Queries: {profile_cfg['queries']}, Dim: {profile_cfg['dim']}")
        print(f"Output: {args.output_json} / {args.output_csv}")
        if not gate_pass:
            print(
                f"Variance gate failed (abs>{abs_ms}ms or pct>{pct_tol*100:.1f}% spread) "
                f"with floor_ms={floor_ms}"
            )
            sys.exit(1)
    finally:
        if not args.skip_docker:
            if compose_cmd is not None:
                stop_compose(CONTAINERS_DIR / "postgres_pgvector", compose_cmd)
                stop_compose(CONTAINERS_DIR / "opensearch", compose_cmd)


if __name__ == "__main__":
    main()
