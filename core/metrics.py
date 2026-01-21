#!/usr/bin/env python3
"""
SAIQL Unified Metrics System
Aggregates operational metrics from all subsystems.
"""

from dataclasses import dataclass, field, asdict
from typing import Dict, Any
import time
import json
import threading

@dataclass
class QIPIMetrics:
    total_lookups: int = 0
    l1_hits: int = 0
    l3_hits: int = 0
    bloom_rejections: int = 0
    buckets_scanned: int = 0
    buckets_skipped: int = 0

@dataclass
class BackendMetrics:
    queries_executed: int = 0
    rows_returned: int = 0
    errors: int = 0
    avg_latency_ms: float = 0.0

@dataclass
class EngineMetrics:
    uptime_seconds: float = 0.0
    active_sessions: int = 0
    cache_hit_rate: float = 0.0
    total_queries: int = 0

@dataclass
class SAIQLMetrics:
    """Unified metrics snapshot"""
    timestamp: float = field(default_factory=time.time)
    engine: EngineMetrics = field(default_factory=EngineMetrics)
    qipi: QIPIMetrics = field(default_factory=QIPIMetrics)
    backends: Dict[str, BackendMetrics] = field(default_factory=dict)
    
    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2)

class MetricsCollector:
    """Central collector for all SAIQL metrics (thread-safe)"""

    def __init__(self):
        self._lock = threading.Lock()
        self._start_time = time.time()
        self.qipi_stats = QIPIMetrics()
        self.backend_stats: Dict[str, BackendMetrics] = {}
        self.engine_stats = EngineMetrics()

    def update_qipi(self, stats: Any):
        """Update from QPIStats object (thread-safe)"""
        with self._lock:
            self.qipi_stats.total_lookups = stats.total_lookups
            self.qipi_stats.l1_hits = stats.l1_hits
            self.qipi_stats.l3_hits = stats.l3_hits
            self.qipi_stats.bloom_rejections = stats.bloom_rejections
            self.qipi_stats.buckets_scanned = getattr(stats, 'buckets_scanned', 0)
            self.qipi_stats.buckets_skipped = getattr(stats, 'buckets_skipped', 0)

    def record_backend_query(self, backend: str, latency_ms: float, rows: int, error: bool = False):
        """Record a backend query (thread-safe)"""
        with self._lock:
            if backend not in self.backend_stats:
                self.backend_stats[backend] = BackendMetrics()

            m = self.backend_stats[backend]
            m.queries_executed += 1
            m.rows_returned += rows
            if error:
                m.errors += 1

            # Moving average for latency
            alpha = 0.1
            m.avg_latency_ms = (alpha * latency_ms) + ((1 - alpha) * m.avg_latency_ms)

    def get_snapshot(self) -> SAIQLMetrics:
        """Get a consistent snapshot of metrics (thread-safe)"""
        with self._lock:
            self.engine_stats.uptime_seconds = time.time() - self._start_time
            return SAIQLMetrics(
                engine=self.engine_stats,
                qipi=self.qipi_stats,
                backends=dict(self.backend_stats)  # Copy to avoid mutation
            )

# Global singleton for easy access
metrics = MetricsCollector()
