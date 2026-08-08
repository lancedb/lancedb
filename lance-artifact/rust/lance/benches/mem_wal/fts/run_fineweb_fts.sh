#!/usr/bin/env bash
# Driver for the FineWeb FTS benchmark panel.
#
# Write panel : 12 configs = 4 modes (async/sync × idx/no-idx) × 3 memtable
#               sizes (100k / 500k / 1M). Each config ingests 1M rows.
# Read panel  : 6 configs  = 2 indexed modes × 3 memtable sizes. Each
#               ingests `size` rows into an auto-flush-disabled MemTable,
#               times the FTS queries, flushes, and replays on disk.
#
# Every config runs as its own process under a `timeout` watchdog, so a
# hang costs one timeout window, not days. When DATASET_PREFIX points at
# object storage, each result.json is also uploaded there.
#
# Usage: rust/lance/benches/mem_wal/fts/run_fineweb_fts.sh [run_id]
#
# Env:
#   DATASET_PREFIX  scratch dataset location (default <tmpdir>/mem-fts-fineweb)
#   CACHE_DIR       FineWeb shard download cache (default <tmpdir>/lance-fineweb-cache)

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
REPO_ROOT="$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel)"
cd "$REPO_ROOT"

RUN_ID="${1:-$(date -u +%Y%m%dT%H%M%SZ)}"
DATASET_PREFIX="${DATASET_PREFIX:-${TMPDIR:-/tmp}/mem-fts-fineweb}"
SEED_ROWS="${SEED_ROWS:-1000000}"
BATCH_ROWS="${BATCH_ROWS:-1000}"
CALLS="${CALLS:-1000}"
CACHE_DIR="${CACHE_DIR:-${TMPDIR:-/tmp}/lance-fineweb-cache}"
CONFIG_TIMEOUT="${CONFIG_TIMEOUT:-3600}"

LOCAL_DIR="$REPO_ROOT/target/fineweb-fts-results/${RUN_ID}"
mkdir -p "$LOCAL_DIR" "$CACHE_DIR"

BIN="$(find target/release/deps -maxdepth 1 -type f -perm -111 -name 'mem_wal_fineweb_fts-*' ! -name '*.d' -printf '%T@ %p\n' 2>/dev/null | sort -nr | head -1 | cut -d' ' -f2-)"
if [ -z "$BIN" ]; then
    echo "building bench binary..."
    cargo bench -p lance --bench mem_wal_fineweb_fts --no-run
    BIN="$(find target/release/deps -maxdepth 1 -type f -perm -111 -name 'mem_wal_fineweb_fts-*' ! -name '*.d' -printf '%T@ %p\n' 2>/dev/null | sort -nr | head -1 | cut -d' ' -f2-)"
fi
echo "bench binary: $BIN"
echo "run id:       $RUN_ID"
echo ""

run_one() {
    local name="$1"; shift
    local out="$LOCAL_DIR/${name}.json"
    local log="$LOCAL_DIR/${name}.log"
    echo ">>> $name"
    if [ -f "$out" ]; then
        echo "    already done, skipping"
        return
    fi
    timeout "$CONFIG_TIMEOUT" "$BIN" --bench "$@" --output "$out" > "$log" 2>&1
    local rc=$?
    if [ "$rc" -eq 124 ]; then
        echo "    !!! TIMED OUT after ${CONFIG_TIMEOUT}s"
    elif [ "$rc" -ne 0 ]; then
        echo "    !!! failed rc=$rc (see $log)"
    else
        echo "    ok"
    fi
    [ -f "$out" ] && aws s3 cp "$out" "$DATASET_PREFIX/$RUN_ID/results/${name}.json" >/dev/null 2>&1
    aws s3 cp "$log" "$DATASET_PREFIX/$RUN_ID/results/${name}.log" >/dev/null 2>&1
}

# ---- write panel: 4 modes × 3 sizes ----
for mode in async_noidx async_idx sync_noidx sync_idx; do
    for sz in 100000 500000 1000000; do
        case "$sz" in
            1000000) tag=1M ;;
            500000)  tag=500k ;;
            *)       tag=100k ;;
        esac
        run_one "write_${mode}_mt${tag}" \
            --phase write --mode "$mode" \
            --uri "$DATASET_PREFIX/$RUN_ID/w_${mode}_mt${tag}" \
            --seed-rows "$SEED_ROWS" --batch-rows "$BATCH_ROWS" --calls "$CALLS" \
            --max-memtable-rows "$sz" --cache-dir "$CACHE_DIR"
    done
done

# ---- read panel: 2 indexed modes × 3 sizes ----
for mode in async_idx sync_idx; do
    for sz in 100000 500000 1000000; do
        case "$sz" in
            1000000) tag=1M ;;
            500000)  tag=500k ;;
            *)       tag=100k ;;
        esac
        run_one "read_${mode}_mt${tag}" \
            --phase read --mode "$mode" \
            --uri "$DATASET_PREFIX/$RUN_ID/r_${mode}_mt${tag}" \
            --seed-rows "$SEED_ROWS" --batch-rows "$BATCH_ROWS" \
            --read-rows "$sz" --cache-dir "$CACHE_DIR"
    done
done

echo ""
echo "=== summary ==="
python3 - "$LOCAL_DIR" <<'PY'
import glob, json, os, sys
d = sys.argv[1]
print(f"{'config':28s} {'rows/s':>10} {'put_p99_ms':>11} {'mt_p95_ms':>10} {'cons_mean':>10}")
for p in sorted(glob.glob(os.path.join(d, "*.json"))):
    try:
        r = json.load(open(p))
    except Exception as e:
        print(f"  bad {p}: {e}"); continue
    name = os.path.basename(p)[:-5]
    if r.get("phase") == "write":
        print(f"{name:28s} {r['throughput_rows_per_sec']:>10.0f} {r['put_p99_ms']:>11.2f} {'-':>10} {'-':>10}")
    else:
        print(f"{name:28s} {'-':>10} {'-':>11} {r['mt_latency_p95_ms']:>10.3f} {r['consistency_mean']:>10.3f}")
PY
echo ""
echo "results: $LOCAL_DIR  +  $DATASET_PREFIX/$RUN_ID/results/"
