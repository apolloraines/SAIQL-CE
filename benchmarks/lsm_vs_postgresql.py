#!/usr/bin/env python3
"""
SAIQL LSM vs PostgreSQL Benchmark
Comprehensive performance comparison
"""

import time
import sys
import tempfile
import shutil
from pathlib import Path

# Add SAIQL to path
saiql_root = Path(__file__).parent.parent
sys.path.insert(0, str(saiql_root))

from storage import LSMEngine

# Try to import PostgreSQL
try:
    import psycopg2
    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False
    print("⚠️  PostgreSQL not available - install with: pip install psycopg2-binary")


def benchmark_lsm_engine(num_records=100000):
    """Benchmark SAIQL LSM Engine"""
    print(f"\n{'='*60}")
    print(f"SAIQL LSM Engine Benchmark ({num_records:,} records)")
    print(f"{'='*60}\n")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        engine = LSMEngine(data_dir=tmpdir, memtable_size_mb=256)
        
        # Benchmark: Bulk Insert
        print("📝 Bulk Insert...")
        start = time.time()
        
        for i in range(num_records):
            key = f"user:{i:08d}"
            value = f"name:User{i},age:{20 + (i % 60)},email:user{i}@example.com".encode()
            engine.put(key, value)
        
        insert_time = time.time() - start
        inserts_per_sec = num_records / insert_time
        
        print(f"  Time: {insert_time:.2f}s")
        print(f"  Rate: {inserts_per_sec:,.0f} inserts/sec")
        
        # Benchmark: Point Lookups (Random)
        print("\n🔍 Point Lookups (Random)...")
        import random
        lookup_keys = [f"user:{random.randint(0, num_records-1):08d}" for _ in range(10000)]
        
        start = time.time()
        for key in lookup_keys:
            value = engine.get(key)
            assert value is not None
        
        lookup_time = time.time() - start
        lookups_per_sec = len(lookup_keys) / lookup_time
        
        print(f"  Time: {lookup_time:.2f}s")
        print(f"  Rate: {lookups_per_sec:,.0f} lookups/sec")
        
        # Benchmark: Range Scans
        print("\n📊 Range Scans...")
        num_scans = 100
        scan_limit = 1000
        
        start = time.time()
        for i in range(num_scans):
            start_key = f"user:{i*1000:08d}"
            end_key = f"user:{(i+1)*1000:08d}"
            results = engine.scan(start_key=start_key, end_key=end_key, limit=scan_limit)
        
        scan_time = time.time() - start
        scans_per_sec = num_scans / scan_time
        
        print(f"  Time: {scan_time:.2f}s")
        print(f"  Rate: {scans_per_sec:.0f} scans/sec")
        print(f"  Avg: {scan_time/num_scans*1000:.2f}ms per scan")
        
        # Stats
        stats = engine.get_stats()
        print(f"\n📈 Statistics:")
        print(f"  Total Size: {stats['total_size_mb']:.2f} MB")
        print(f"  MemTable: {stats['memtable_size_mb']:.2f} MB ({stats['memtable_keys']:,} keys)")
        print(f"  L1 SSTables: {stats['l1_sstables']} ({stats['l1_size_mb']:.2f} MB)")
        print(f"  L2 SSTables: {stats['l2_sstables']} ({stats['l2_size_mb']:.2f} MB)")
        print(f"  L3 SSTables: {stats['l3_sstables']} ({stats['l3_size_mb']:.2f} MB)")
        print(f"  Flushes: {stats['flushes']}")
        print(f"  Compactions: {stats['compactions']}")
        
        engine.close()
        
        return {
            'insert_rate': inserts_per_sec,
            'lookup_rate': lookups_per_sec,
            'scan_rate': scans_per_sec,
            'total_size_mb': stats['total_size_mb']
        }


def benchmark_postgresql(num_records=100000):
    """Benchmark PostgreSQL"""
    if not POSTGRES_AVAILABLE:
        return None
    
    print(f"\n{'='*60}")
    print(f"PostgreSQL Benchmark ({num_records:,} records)")
    print(f"{'='*60}\n")
    
    # Connect to PostgreSQL (assumes local instance)
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="postgres",
            user="postgres",
            password="postgres"
        )
        conn.autocommit = False
    except Exception as e:
        print(f"⚠️  Could not connect to PostgreSQL: {e}")
        print("   Skipping PostgreSQL benchmark")
        return None
    
    cursor = conn.cursor()
    
    # Create test table
    cursor.execute("DROP TABLE IF EXISTS benchmark_users")
    cursor.execute("""
        CREATE TABLE benchmark_users (
            key VARCHAR(255) PRIMARY KEY,
            value TEXT
        )
    """)
    cursor.execute("CREATE INDEX idx_key ON benchmark_users(key)")
    conn.commit()
    
    # Benchmark: Bulk Insert
    print("📝 Bulk Insert...")
    start = time.time()
    
    for i in range(num_records):
        key = f"user:{i:08d}"
        value = f"name:User{i},age:{20 + (i % 60)},email:user{i}@example.com"
        cursor.execute("INSERT INTO benchmark_users (key, value) VALUES (%s, %s)", (key, value))
        
        if i % 1000 == 0:
            conn.commit()
    
    conn.commit()
    insert_time = time.time() - start
    inserts_per_sec = num_records / insert_time
    
    print(f"  Time: {insert_time:.2f}s")
    print(f"  Rate: {inserts_per_sec:,.0f} inserts/sec")
    
    # Benchmark: Point Lookups (Random)
    print("\n🔍 Point Lookups (Random)...")
    import random
    lookup_keys = [f"user:{random.randint(0, num_records-1):08d}" for _ in range(10000)]
    
    start = time.time()
    for key in lookup_keys:
        cursor.execute("SELECT value FROM benchmark_users WHERE key = %s", (key,))
        result = cursor.fetchone()
        assert result is not None
    
    lookup_time = time.time() - start
    lookups_per_sec = len(lookup_keys) / lookup_time
    
    print(f"  Time: {lookup_time:.2f}s")
    print(f"  Rate: {lookups_per_sec:,.0f} lookups/sec")
    
    # Benchmark: Range Scans
    print("\n📊 Range Scans...")
    num_scans = 100
    
    start = time.time()
    for i in range(num_scans):
        start_key = f"user:{i*1000:08d}"
        end_key = f"user:{(i+1)*1000:08d}"
        cursor.execute(
            "SELECT key, value FROM benchmark_users WHERE key >= %s AND key < %s LIMIT 1000",
            (start_key, end_key)
        )
        results = cursor.fetchall()
    
    scan_time = time.time() - start
    scans_per_sec = num_scans / scan_time
    
    print(f"  Time: {scan_time:.2f}s")
    print(f"  Rate: {scans_per_sec:.0f} scans/sec")
    print(f"  Avg: {scan_time/num_scans*1000:.2f}ms per scan")
    
    # Get table size
    cursor.execute("""
        SELECT pg_size_pretty(pg_total_relation_size('benchmark_users'))
    """)
    size_str = cursor.fetchone()[0]
    
    print(f"\n📈 Statistics:")
    print(f"  Total Size: {size_str}")
    
    # Cleanup
    cursor.execute("DROP TABLE benchmark_users")
    conn.commit()
    cursor.close()
    conn.close()
    
    return {
        'insert_rate': inserts_per_sec,
        'lookup_rate': lookups_per_sec,
        'scan_rate': scans_per_sec
    }


def print_comparison(lsm_results, pg_results):
    """Print comparison table"""
    print(f"\n{'='*60}")
    print("PERFORMANCE COMPARISON")
    print(f"{'='*60}\n")
    
    if pg_results is None:
        print("⚠️  PostgreSQL results not available")
        return
    
    print(f"{'Operation':<20} {'SAIQL LSM':<20} {'PostgreSQL':<20} {'Speedup':<10}")
    print(f"{'-'*70}")
    
    # Inserts
    lsm_insert = lsm_results['insert_rate']
    pg_insert = pg_results['insert_rate']
    insert_speedup = lsm_insert / pg_insert
    print(f"{'Bulk Insert':<20} {lsm_insert:>15,.0f}/s {pg_insert:>15,.0f}/s {insert_speedup:>8.1f}x")
    
    # Lookups
    lsm_lookup = lsm_results['lookup_rate']
    pg_lookup = pg_results['lookup_rate']
    lookup_speedup = lsm_lookup / pg_lookup
    print(f"{'Point Lookup':<20} {lsm_lookup:>15,.0f}/s {pg_lookup:>15,.0f}/s {lookup_speedup:>8.1f}x")
    
    # Scans
    lsm_scan = lsm_results['scan_rate']
    pg_scan = pg_results['scan_rate']
    scan_speedup = lsm_scan / pg_scan
    print(f"{'Range Scan':<20} {lsm_scan:>15,.0f}/s {pg_scan:>15,.0f}/s {scan_speedup:>8.1f}x")
    
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}\n")
    print(f"  SAIQL LSM is {insert_speedup:.1f}x faster at bulk inserts")
    print(f"  SAIQL LSM is {lookup_speedup:.1f}x faster at point lookups")
    print(f"  SAIQL LSM is {scan_speedup:.1f}x faster at range scans")
    print(f"\n  Average speedup: {(insert_speedup + lookup_speedup + scan_speedup) / 3:.1f}x")


if __name__ == "__main__":
    print("="*60)
    print("SAIQL LSM vs PostgreSQL Benchmark")
    print("="*60)
    
    # Run benchmarks
    lsm_results = benchmark_lsm_engine(num_records=100000)
    pg_results = benchmark_postgresql(num_records=100000)
    
    # Print comparison
    if pg_results:
        print_comparison(lsm_results, pg_results)
    
    print("\n✅ Benchmark complete!")
