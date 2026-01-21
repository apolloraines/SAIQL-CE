#!/usr/bin/env bash
set -euo pipefail
SRC=${1:-../data/backhistory_sample.saiql}
DST=${2:-../results/backhistory_local.saiql}
mkdir -p "$(dirname "$DST")"
cp "$SRC" "$DST"
chmod 600 "$DST"
echo "SAIQL sample copied to $DST"
