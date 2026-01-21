# SAIQL vs PostgreSQL Speed Test Harness

This folder contains a reproducible (and self-documenting) benchmark that compares the SAIQL Echo engine with a stock PostgreSQL instance using the same dataset. The trimmed sample (~5k rows) is derived from the production `backhistory` table under `/mnt/nova_memory/nova_saiql.db` (~45 GB) so that others can reproduce the workflow without hauling the full file.

> **Important:** Live benchmarking from this sandbox cannot reach PostgreSQL due to socket/network restrictions. We therefore ship the latest manual results captured on the host in `results/benchmark_manual.json`. The harness and scripts remain here so you can rerun on any machine with access to Postgres.

## Directory Layout

```
speed_test/
├── README.md
├── data/
│   ├── backhistory_sample.csv   # CSV used for PostgreSQL
│   └── backhistory_sample.saiql # Equivalent SAIQL text dataset
├── scripts/
│   ├── prepare_postgres.sh      # Loads the CSV into backhistory table + indexes
│   ├── build_saiql.sh           # Copies the sample .saiql into results/
│   └── run_benchmarks.py        # Calls both engines and records timings (requires PG access)
├── results/
│   ├── backhistory_local.saiql  # Local copy built by build_saiql.sh (gitignored)
│   └── benchmark_manual.json    # Reference timings from the production dataset
├── .gitignore                  # Ignore benchmark artifacts
└── speed_summary.md            # Human-friendly write-up
```

## Requirements

- Python 3.10+
- `psql` client + access to a PostgreSQL server (`PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGDATABASE`)
- SAIQL Echo CLI on your PATH (repo root already includes `bin/saiql`)

## 1. Load PostgreSQL

```
cd speed_test/scripts
export PGHOST=localhost PGPORT=5432 PGUSER=postgres PGPASSWORD=postgres PGDATABASE=saiql_bench
./prepare_postgres.sh
```

What the script does:
1. Ensures `backhistory` table exists
2. Truncates it
3. Imports `data/backhistory_sample.csv` via `\copy`
4. Adds indexes on `symbol` and `timestamp`

## 2. Prepare the SAIQL file

```
./build_saiql.sh ../data/backhistory_sample.saiql ../results/backhistory_local.saiql
```

This yields a working `.saiql` copy in `results/` so tests don’t mutate the canonical sample.

## 3. Run the benchmark (when PG is reachable)

```
cd ..
export SAIQL_DB=$PWD/results/backhistory_local.saiql
export PG_URL="postgresql://postgres:postgres@localhost:5432/saiql_bench"
python3 scripts/run_benchmarks.py
```

The script measures two invocations for each engine (cold + warm) and writes JSON under `results/`. If you can’t reach Postgres inside this repo (as in our sandbox), run the commands directly on the host or copy the dataset to another machine.

## Reference Results

`results/benchmark_manual.json` captures the canonical benchmark we use internally (full dataset under `/mnt/nova_memory/nova_saiql.db`, not included here). Summary:

- SAIQL cache hydrate from pickle: ~0.40 s (one-time per process)
- First query after load: ~0.00061 s (610 µs)
- Warm repeats: ~0.000012 s (12 µs) → ~220× faster than our local Postgres baseline (~0.107 s)
- 500-iteration loop: ~0.00048 s total (~0.95 µs/query)

These numbers are for the larger dataset; the 5k-row sample will run even faster. Your numbers will vary with hardware, dataset size, and cache warmth.

## Notes

- The production `.saiql` file is ~45 GB and lives at `/mnt/nova_memory/nova_saiql.db/trader/backhistory.saiql`; we cannot ship it in the repo.
- If you do run the full benchmark, feel free to drop the JSON into `speed_test/results/` (gitignored) so others can compare.
- `speed_summary.md` tracks the narrative behind these numbers (Alpha 616×, Echo regression, 220× warm recovery, etc.).

Happy benchmarking!
