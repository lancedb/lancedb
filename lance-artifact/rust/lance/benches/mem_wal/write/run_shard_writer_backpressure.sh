#!/usr/bin/env bash
# Centralized MemWAL ShardWriter backpressure sweep — drives the
# `mem_wal_shard_writer_backpressure` bench for BOTH the vector and the
# FTS index. The methodology (paced async ingest, WAL-queue sampling,
# skip-close) is identical; the only difference is `--index-type`, so the
# vector and FTS numbers are directly comparable.
#
# Usage:
#   INDEX_TYPE=fts    rust/lance/benches/mem_wal/write/run_shard_writer_backpressure.sh [run_id]
#   INDEX_TYPE=vector rust/lance/benches/mem_wal/write/run_shard_writer_backpressure.sh [run_id]
#
# Finds the max sustainable async-indexed throughput: the highest paced
# target where puts never block (slow>=1s == 0) and the WAL flush queue
# does not accumulate (tail queue delta ~0).
#
# Env:
#   DATASET_PREFIX  scratch dataset location (default <tmpdir>/mem-wal-backpressure)

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
REPO_ROOT="$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel)"
cd "$REPO_ROOT"

INDEX_TYPE="${INDEX_TYPE:-fts}"
RUN_ID="${1:-bp-${INDEX_TYPE}-$(date -u +%Y%m%dT%H%M%SZ)}"
DATASET_PREFIX="${DATASET_PREFIX:-${TMPDIR:-/tmp}/mem-wal-backpressure}"

# Shared knobs — identical to the HNSW vector backpressure sweep so the
# two index types are measured the same way.
ROWS="${ROWS:-500000}"
SEED_ROWS="${SEED_ROWS:-100000}"
BATCH_ROWS="${BATCH_ROWS:-1000}"
VECTOR_DIM="${VECTOR_DIM:-1024}"
THREADS="${THREADS:-64}"
TOKIO_THREADS="${TOKIO_THREADS:-64}"
MAX_MEMTABLE_SIZE="${MAX_MEMTABLE_SIZE:-17179869184}"          # 16 GiB
MAX_UNFLUSHED_BYTES="${MAX_UNFLUSHED_BYTES:-34359738368}"      # 32 GiB
MAX_WAL_BUFFER_SIZE="${MAX_WAL_BUFFER_SIZE:-52428800}"        # 50 MiB
MAX_WAL_FLUSH_INTERVAL_MS="${MAX_WAL_FLUSH_INTERVAL_MS:-0}"
SAMPLE_INTERVAL_MS="${SAMPLE_INTERVAL_MS:-1000}"
CONFIG_TIMEOUT="${CONFIG_TIMEOUT:-2400}"

# Paced async-indexed target sweep (rows/s). FTS indexing is heavier than
# IVF/PQ so its sustainable ceiling is lower; the vector sweep can push
# the same script higher via TARGETS=...
case "$INDEX_TYPE" in
    fts)    TARGETS_DEFAULT="500 1000 1500 2000 2500 3000 0" ;;
    vector) TARGETS_DEFAULT="2000 4000 6000 8000 10000 0" ;;
    *) echo "INDEX_TYPE must be fts|vector" >&2; exit 1 ;;
esac
TARGETS="${TARGETS:-$TARGETS_DEFAULT}"

CALLS=$(( ROWS / BATCH_ROWS ))
LOCAL_DIR="$REPO_ROOT/target/mem-wal-backpressure-results/${RUN_ID}"
mkdir -p "$LOCAL_DIR"

BIN="$(find target/release/deps -maxdepth 1 -type f -perm -111 \
    -name 'mem_wal_shard_writer_backpressure-*' ! -name '*.d' -printf '%T@ %p\n' 2>/dev/null | sort -nr | head -1 | cut -d' ' -f2-)"
if [ -z "$BIN" ]; then
    cargo bench -p lance --bench mem_wal_shard_writer_backpressure --no-run
    BIN="$(find target/release/deps -maxdepth 1 -type f -perm -111 \
        -name 'mem_wal_shard_writer_backpressure-*' ! -name '*.d' -printf '%T@ %p\n' 2>/dev/null | sort -nr | head -1 | cut -d' ' -f2-)"
fi
echo "index_type=$INDEX_TYPE  bin=$BIN  run_id=$RUN_ID  rows=$ROWS"

for tgt in $TARGETS; do
    if [ "$tgt" = "0" ]; then label="async_idx_unpaced"; paced=()
    else label="async_idx_t${tgt}"; paced=(--target-rows-per-sec "$tgt"); fi
    out="$LOCAL_DIR/${label}.json"; log="$LOCAL_DIR/${label}.log"
    echo ">>> $label"
    if [ -f "$out" ]; then echo "    already done"; continue; fi
    timeout "$CONFIG_TIMEOUT" "$BIN" --bench \
        --mode async_idx --index-type "$INDEX_TYPE" --schema-shape fineweb \
        --uri "$DATASET_PREFIX/$RUN_ID/bp_${label}" \
        --seed-rows "$SEED_ROWS" --batch-rows "$BATCH_ROWS" --calls "$CALLS" \
        --vector-dim "$VECTOR_DIM" \
        --max-memtable-size "$MAX_MEMTABLE_SIZE" \
        --max-unflushed-memtable-bytes "$MAX_UNFLUSHED_BYTES" \
        --max-wal-buffer-size "$MAX_WAL_BUFFER_SIZE" \
        --max-wal-flush-interval-ms "$MAX_WAL_FLUSH_INTERVAL_MS" \
        --sample-interval-ms "$SAMPLE_INTERVAL_MS" \
        --threads "$THREADS" --tokio-threads "$TOKIO_THREADS" \
        --skip-close \
        "${paced[@]}" --output "$out" > "$log" 2>&1
    rc=$?
    if [ "$rc" -eq 124 ]; then echo "    !!! TIMED OUT"
    elif [ "$rc" -ne 0 ]; then echo "    !!! failed rc=$rc"
    else echo "    ok"; fi
    [ -f "$out" ] && aws s3 cp "$out" "$DATASET_PREFIX/$RUN_ID/results/${label}.json" >/dev/null 2>&1
    aws s3 cp "$log" "$DATASET_PREFIX/$RUN_ID/results/${label}.log" >/dev/null 2>&1
done

echo ""
echo "=== backpressure summary (index_type=$INDEX_TYPE) ==="
python3 - "$LOCAL_DIR" "$ROWS" <<'PY'
import glob, json, os, sys
d, rows = sys.argv[1], int(sys.argv[2])
print(f"{'cell':22s} {'target':>8s} {'rows/s':>9s} {'MB/s':>8s} {'p99_ms':>10s} "
      f"{'slow>=1s':>9s} {'wal_q_end':>10s} {'wal_q_max':>10s} {'q_tail_delta':>13s}")
for p in sorted(glob.glob(os.path.join(d, "*.json"))):
    try: r = json.load(open(p))
    except Exception: continue
    name = os.path.basename(p)[:-5]
    mem = r.get("final_memtable_stats") or {}
    samples = r.get("samples") or []
    q_end = int(mem.get("wal_queue_pending_rows") or 0)
    q_max = max([s.get("wal_queue_pending_rows") or 0 for s in samples], default=0)
    puts = [s for s in samples if s.get("phase") == "puts"
            and isinstance(s.get("wal_queue_pending_rows"), int)]
    tail = puts[-5:]
    q_tail = (tail[-1]["wal_queue_pending_rows"] - tail[0]["wal_queue_pending_rows"]
              if len(tail) >= 2 else 0)
    tgt = r.get("target_rows_per_sec") or 0
    print(f"{name:22s} {tgt:>8.0f} {r.get('throughput_rows_per_sec',0):>9.0f} "
          f"{r.get('throughput_mb_per_sec',0):>8.2f} {r.get('p99_ms',0):>10.2f} "
          f"{r.get('slow_puts_1s',0):>9d} {q_end:>10d} {q_max:>10d} {q_tail:>13d}")
PY
echo ""
echo "results: $LOCAL_DIR  +  $DATASET_PREFIX/$RUN_ID/results/"
