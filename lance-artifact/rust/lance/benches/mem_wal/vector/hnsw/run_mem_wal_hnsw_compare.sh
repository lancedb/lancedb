#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd -P)"
HNSWLIB_DIR="${HNSWLIB_DIR:-$HOME/oss/hnswlib}"

ROWS="${ROWS:-1000000}"
DIM="${DIM:-1024}"
QUERIES="${QUERIES:-1000}"
TRUTH_QUERIES="${TRUTH_QUERIES:-100}"
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

mkdir -p "$REPO_ROOT/target/release"

echo "=== Building Lance HNSW benchmark ==="
cargo bench -p lance --bench mem_wal_hnsw_bench --no-run

echo "=== Building hnswlib benchmark ==="
g++ -std=c++17 -O3 -march=native -DNDEBUG -pthread \
    -I "$HNSWLIB_DIR" \
    "$SCRIPT_DIR/mem_wal_hnswlib_bench.cpp" \
    -o "$REPO_ROOT/target/release/hnswlib_bench"

COMMON_ARGS=(
    --rows "$ROWS"
    --dim "$DIM"
    --queries "$QUERIES"
    --truth-queries "$TRUTH_QUERIES"
    --k "$K"
    --m "$M"
    --ef-construction "$EF_CONSTRUCTION"
    --ef-search "$EF_SEARCH"
    --threads "$THREADS"
    --seed "$SEED"
    --clusters "$CLUSTERS"
    --noise "$NOISE"
)

echo "=== Running Lance HNSW ==="
"$(find "$REPO_ROOT/target/release/deps" -maxdepth 1 -type f -perm -111 -name 'mem_wal_hnsw_bench-*' | sort | tail -n 1)" "${COMMON_ARGS[@]}"

echo "=== Running hnswlib ==="
"$REPO_ROOT/target/release/hnswlib_bench" "${COMMON_ARGS[@]}"
