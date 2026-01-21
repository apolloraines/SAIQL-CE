#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="${SCRIPT_DIR}/../data"
CSV="${DATA_DIR}/backhistory_sample.csv"

: "${PGDATABASE:?Set PG* env vars (PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE)}"

psql <<'SQL'
CREATE TABLE IF NOT EXISTS backhistory (
    symbol       TEXT,
    "timestamp"  BIGINT,
    open         NUMERIC,
    high         NUMERIC,
    low          NUMERIC,
    close        NUMERIC,
    volume       NUMERIC,
    granularity  INTEGER
);
SQL

psql -c 'TRUNCATE backhistory;'
psql -c "\\copy backhistory FROM '${CSV}' WITH (FORMAT csv, HEADER true);"
psql -c 'CREATE INDEX IF NOT EXISTS backhistory_symbol_idx ON backhistory(symbol);'
psql -c 'CREATE INDEX IF NOT EXISTS backhistory_timestamp_idx ON backhistory("timestamp");'

echo "PostgreSQL dataset prepared."
