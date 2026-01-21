#!/usr/bin/env python3
"""
Hybrid Retrieval Synthetic TPS Evaluator
----------------------------------------

Measures effective tokens-per-second impact of hybrid retrieval (vector + optional rerank)
using a synthetic workload. Fails if EffectiveTPS drops by >10% vs vector-only baseline.

Formula (Antigravity):
  EffectiveTPS = tokens / (retrieval_time + (tokens / generation_rate))
Where retrieval_time = retrieval_latency + rerank_latency (rerank_latency=0 if disabled).
"""

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Dict, Tuple

import numpy as np


def compute_effective_tps(retrieval_time_s: float, tokens: int, gen_rate: float) -> float:
    """Compute EffectiveTPS given retrieval latency and generation parameters."""
    gen_time = tokens / gen_rate
    return tokens / (retrieval_time_s + gen_time)


def measure_vector_search(matrix: np.ndarray, query: np.ndarray, top_k: int) -> Tuple[float, np.ndarray]:
    """Measure latency of vector similarity search."""
    start = time.perf_counter()
    scores = np.dot(matrix, query)
    top_idx = np.argpartition(scores, -top_k)[-top_k:]
    # Force ordering to simulate realistic post-processing
    _ = scores[top_idx][np.argsort(-scores[top_idx])]
    end = time.perf_counter()
    return end - start, top_idx


def measure_rerank(top_vectors: np.ndarray, query: np.ndarray, iterations: int, sleep_ms: float) -> float:
    """Simulate rerank cost via lightweight compute or optional sleep."""
    if top_vectors.size == 0:
        return 0.0

    start = time.perf_counter()

    # Simple compute loop to approximate CPU cost
    score = 0.0
    for i in range(iterations):
        idx = i % top_vectors.shape[0]
        score += float(np.dot(top_vectors[idx], query))

    if sleep_ms > 0:
        time.sleep(sleep_ms / 1000.0)

    end = time.perf_counter()
    return end - start


def run_trial(matrix: np.ndarray, query: np.ndarray, top_k: int, rerank: bool, rerank_iters: int, rerank_sleep_ms: float) -> Dict[str, float]:
    """Run a single baseline/current trial and return latencies."""
    retrieval_time, top_idx = measure_vector_search(matrix, query, top_k)
    rerank_time = 0.0
    if rerank:
        rerank_time = measure_rerank(matrix[top_idx], query, rerank_iters, rerank_sleep_ms)

    return {
        "retrieval_time_s": retrieval_time,
        "rerank_time_s": rerank_time,
        "total_time_s": retrieval_time + rerank_time,
    }


def aggregate(times):
    """Compute average of a list of floats."""
    return sum(times) / max(len(times), 1)


def main():
    parser = argparse.ArgumentParser(description="Hybrid retrieval EffectiveTPS evaluator")
    parser.add_argument("--docs", type=int, default=50000, help="Synthetic document count")
    parser.add_argument("--vector-dim", type=int, default=384, help="Vector dimension")
    parser.add_argument("--top-k", type=int, default=50, help="Top K for retrieval")
    parser.add_argument("--runs", type=int, default=5, help="Number of runs per mode")
    parser.add_argument("--tokens", type=int, default=500, help="Assumed response length in tokens")
    parser.add_argument("--gen-rate", type=float, default=50.0, help="Generation rate (tokens/sec)")
    parser.add_argument("--rerank", action="store_true", help="Enable rerank simulation for current pipeline")
    parser.add_argument("--rerank-iters", type=int, default=2000, help="Compute iterations to simulate rerank cost")
    parser.add_argument("--rerank-sleep-ms", type=float, default=0.0, help="Optional additional sleep to simulate rerank latency")
    parser.add_argument("--seed", type=int, default=13, help="RNG seed for reproducibility")
    parser.add_argument("--output-json", type=Path, help="Write metrics JSON to path")
    args = parser.parse_args()

    np.random.seed(args.seed)
    matrix = np.random.rand(args.docs, args.vector_dim).astype(np.float32)

    baseline_times = []
    baseline_totals = []
    current_times = []
    current_totals = []
    rerank_times = []

    for _ in range(args.runs):
        query = np.random.rand(args.vector_dim).astype(np.float32)

        # Baseline: vector-only
        base_trial = run_trial(matrix, query, args.top_k, rerank=False, rerank_iters=0, rerank_sleep_ms=0.0)
        baseline_times.append(base_trial["retrieval_time_s"])
        baseline_totals.append(base_trial["total_time_s"])

        # Current: vector + optional rerank
        curr_trial = run_trial(
            matrix,
            query,
            args.top_k,
            rerank=args.rerank,
            rerank_iters=args.rerank_iters if args.rerank else 0,
            rerank_sleep_ms=args.rerank_sleep_ms if args.rerank else 0.0,
        )
        current_times.append(curr_trial["retrieval_time_s"])
        current_totals.append(curr_trial["total_time_s"])
        rerank_times.append(curr_trial["rerank_time_s"])

    tokens = args.tokens
    gen_rate = args.gen_rate

    baseline_retrieval = aggregate(baseline_totals)
    current_retrieval = aggregate(current_totals)
    current_rerank = aggregate(rerank_times)

    baseline_tps = compute_effective_tps(baseline_retrieval, tokens, gen_rate)
    current_tps = compute_effective_tps(current_retrieval, tokens, gen_rate)

    drop_pct = 0.0
    if baseline_tps > 0:
        drop_pct = max(0.0, (baseline_tps - current_tps) / baseline_tps * 100)

    pass_gate = drop_pct <= 10.0

    metrics = {
        "docs": args.docs,
        "vector_dim": args.vector_dim,
        "top_k": args.top_k,
        "runs": args.runs,
        "tokens": tokens,
        "gen_rate": gen_rate,
        "baseline": {
            "avg_retrieval_time_s": baseline_retrieval,
            "effective_tps": baseline_tps,
        },
        "current": {
            "avg_retrieval_time_s": current_retrieval,
            "avg_rerank_time_s": current_rerank,
            "effective_tps": current_tps,
            "drop_pct": drop_pct,
            "pass": pass_gate,
        },
    }

    print("=== Hybrid Retrieval Synthetic TPS ===")
    print(f"Docs: {args.docs}, dim: {args.vector_dim}, top_k: {args.top_k}, runs: {args.runs}")
    print(f"Tokens: {tokens}, gen_rate: {gen_rate} tok/s")
    print(f"Baseline EffectiveTPS: {baseline_tps:.2f} (avg retrieval {baseline_retrieval*1000:.2f} ms)")
    print(
        f"Current EffectiveTPS: {current_tps:.2f} "
        f"(avg retrieval {current_retrieval*1000:.2f} ms, rerank {current_rerank*1000:.2f} ms)"
    )
    print(f"Drop: {drop_pct:.2f}% -> {'PASS' if pass_gate else 'FAIL'} (must be <= 10%)")

    if args.output_json:
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        with open(args.output_json, "w", encoding="utf-8") as f:
            json.dump(metrics, f, indent=2)
        print(f"Wrote metrics to {args.output_json}")

    if not pass_gate:
        sys.exit(1)


if __name__ == "__main__":
    main()
