#!/usr/bin/env bash
set -e

RAW_DIR="data/raw"

for f in "$RAW_DIR"/*.csv; do
  echo "=============================="
  echo "FILE: $f"
  echo "--- First 3 lines ---"
  head -n 3 "$f"
  echo "--- Column count ---"
  head -n 1 "$f" | awk -F',' '{print NF " columns"}'
done
