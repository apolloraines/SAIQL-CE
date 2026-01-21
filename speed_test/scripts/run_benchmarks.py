#!/usr/bin/env python3
import json
import os
import subprocess
import time
from datetime import datetime
from pathlib import Path

import psycopg2

QUERY = "SELECT * FROM backhistory WHERE symbol='BTC-USD' ORDER BY \"timestamp\" DESC LIMIT 100;"

SAIQL_DB = os.environ.get('SAIQL_DB')
PG_URL = os.environ.get('PG_URL')
if not SAIQL_DB or not PG_URL:
    raise SystemExit("Set SAIQL_DB and PG_URL before running this script")

ROOT = Path(__file__).resolve().parents[1]
RESULT_DIR = ROOT / 'results'
RESULT_DIR.mkdir(parents=True, exist_ok=True)


def run_saiql(invocation: int) -> float:
    cmd = [
        os.environ.get('SAIQL_CLI', 'saiql'),
        '--warm-cache',
        '--quiet',
        '-d',
        SAIQL_DB,
        '-q',
        QUERY,
    ]
    start = time.perf_counter()
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return time.perf_counter() - start


def run_postgres(invocation: int) -> float:
    start = time.perf_counter()
    with psycopg2.connect(PG_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(QUERY)
            cur.fetchall()
    return time.perf_counter() - start


def redact_url(url: str) -> str:
    """Redact credentials from database URL for safe logging/storage."""
    import re
    # Redact password in URLs like postgresql://user:password@host:port/db
    return re.sub(r'(://[^:]+:)[^@]+(@)', r'\1***REDACTED***\2', url)

results = {
    'timestamp': datetime.utcnow().isoformat() + 'Z',
    'saiql': [],
    'postgres': [],
    'query': QUERY,
    'saiql_db': SAIQL_DB,
    'pg_url': redact_url(PG_URL),  # Redact credentials before storing
}

# first + warm repeat for each engine
for i in range(2):
    results['saiql'].append(run_saiql(i))
for i in range(2):
    results['postgres'].append(run_postgres(i))

output_path = RESULT_DIR / f"benchmark_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.json"
output_path.write_text(json.dumps(results, indent=2))

first_saiql, second_saiql = results['saiql']
first_pg, second_pg = results['postgres']

print("SAIQL vs PostgreSQL (sample dataset)")
print("-------------------------------------")
print(f"SAIQL cold  : {first_saiql:.6f} s")
print(f"SAIQL warm  : {second_saiql:.6f} s")
print(f"Postgres #1 : {first_pg:.6f} s")
print(f"Postgres #2 : {second_pg:.6f} s")
if second_pg > 0:
    print(f"Warm speedup: {second_pg/second_saiql:.1f}x")
print(f"Results saved to {output_path}")
