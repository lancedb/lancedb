#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors
#
# Runs the Lance HNSW primitive vs hnswlib across memtable sizes (100k/500k/1M),
# capturing insert/query throughput, recall, peak RSS (/usr/bin/time -v) and CPU
# counters (perf stat, if available). Designed for the parity benchmark loop.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
# Cargo writes to the workspace target dir at the repo root; resolve it robustly.
REPO_ROOT="$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel 2>/dev/null)"
REPO_ROOT="${REPO_ROOT:-$(cd "$SCRIPT_DIR/../../../../.." && pwd -P)}"
HNSWLIB_DIR="${HNSWLIB_DIR:-$HOME/oss/hnswlib}"
# Honor CARGO_TARGET_DIR so haswell vs target-cpu=native builds can coexist.
TARGET_DIR="${CARGO_TARGET_DIR:-$REPO_ROOT/target}"
OUT_DIR="${OUT_DIR:-$TARGET_DIR/parity_suite}"

SIZES="${SIZES:-100000 500000 1000000}"
DIM="${DIM:-1024}"
QUERIES="${QUERIES:-5000}"
QUERY_REPEATS="${QUERY_REPEATS:-20}"
# Sustained measurement: when > 0, write rebuilds the graph and read loops the
# query workload for this many seconds, reporting steady-state (not burst).
INSERT_SECONDS="${INSERT_SECONDS:-0}"
QUERY_SECONDS="${QUERY_SECONDS:-0}"
TRUTH_QUERIES="${TRUTH_QUERIES:-200}"
K="${K:-10}"
M="${M:-12}"
EF_CONSTRUCTION="${EF_CONSTRUCTION:-64}"
EF_SEARCH="${EF_SEARCH:-64}"
THREADS="${THREADS:-$(getconf _NPROCESSORS_ONLN 2>/dev/null || sysctl -n hw.ncpu)}"
SEED="${SEED:-100}"
CLUSTERS="${CLUSTERS:-4096}"
NOISE="${NOISE:-0.05}"

if [ ! -d "$HNSWLIB_DIR/hnswlib" ]; then
    echo "ERROR: HNSWLIB_DIR must point to a hnswlib checkout, got: $HNSWLIB_DIR" >&2
    exit 1
fi

mkdir -p "$OUT_DIR" "$TARGET_DIR/release"

# Locate /usr/bin/time (GNU time, for peak RSS). Fall back to no wrapper.
TIME_BIN=""
if [ -x /usr/bin/time ]; then
    TIME_BIN="/usr/bin/time"
fi
echo "=== Building Lance HNSW benchmark (release) ==="
cargo bench -p lance --bench mem_wal_hnsw_bench --no-run 2>&1 | tail -3
LANCE_BIN="$(find "$TARGET_DIR/release/deps" -maxdepth 1 -type f -perm -111 -name 'mem_wal_hnsw_bench-*' | sort | tail -n 1)"

echo "=== Building hnswlib benchmark (g++ -O3 -march=native) ==="
g++ -std=c++17 -O3 -march=native -DNDEBUG -pthread \
    -I "$HNSWLIB_DIR" \
    "$SCRIPT_DIR/mem_wal_hnswlib_bench.cpp" \
    -o "$TARGET_DIR/release/hnswlib_bench"
HNSWLIB_BIN="$TARGET_DIR/release/hnswlib_bench"

run_one() {
    local impl="$1" bin="$2" rows="$3"
    local tag="${impl}_r${rows}"
    local args=(
        --rows "$rows" --dim "$DIM" --queries "$QUERIES" --truth-queries "$TRUTH_QUERIES"
        --k "$K" --m "$M" --ef-construction "$EF_CONSTRUCTION" --ef-search "$EF_SEARCH"
        --threads "$THREADS" --seed "$SEED" --clusters "$CLUSTERS" --noise "$NOISE"
        --query-repeats "$QUERY_REPEATS"
        --insert-seconds "$INSERT_SECONDS" --query-seconds "$QUERY_SECONDS"
    )
    local out="$OUT_DIR/${tag}.out"
    local timef="$OUT_DIR/${tag}.time"
    echo "--- run $tag ---"
    if [ -n "$TIME_BIN" ]; then
        "$TIME_BIN" -v "$bin" "${args[@]}" >"$out" 2>"$timef" || cat "$timef"
    else
        "$bin" "${args[@]}" >"$out" 2>&1
    fi
    grep -E '^(bench|result)' "$out" || true
    if [ -f "$timef" ]; then
        grep -E 'Maximum resident set size|Elapsed \(wall|Percent of CPU' "$timef" || true
    fi
}

echo "=== Parity suite: sizes=[$SIZES] dim=$DIM threads=$THREADS ==="
for rows in $SIZES; do
    run_one lance "$LANCE_BIN" "$rows"
    run_one hnswlib "$HNSWLIB_BIN" "$rows"
done

echo "=== SUMMARY (json lines) ==="
grep -h '^{' "$OUT_DIR"/*.out 2>/dev/null || true
echo "=== peak RSS (KB) ==="
for f in "$OUT_DIR"/*.time; do
    [ -f "$f" ] || continue
    rss=$(grep 'Maximum resident set size' "$f" | grep -oE '[0-9]+' | head -1)
    printf '%-28s %s\n' "$(basename "$f" .time)" "${rss:-NA}"
done
echo "=== results written to $OUT_DIR ==="
