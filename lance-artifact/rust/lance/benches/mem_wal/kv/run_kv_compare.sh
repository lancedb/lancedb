#!/usr/bin/env bash
# KV point-lookup comparison driver — Lance MemTable vs RocksDB.
# Sibling of run_fts_compare.sh (Lance FTS vs Lucene).
#
# Sweeps `mem_wal_kv_point_lookup` over a set of MemTable sizes. For each size
# it runs the Lance and RocksDB arms in *separate processes* (clean per-engine
# peak RSS), then prints build/read/RSS side by side. A single mixed query set
# (hits + guaranteed misses) is used per size, identical across engines via a
# fixed seed in the bench itself.
#
# Usage:
#   rust/lance/benches/mem_wal/kv/run_kv_compare.sh [run_id]
#
# Env:
#   SIZES         row-count sweep (default "100000 500000 1000000")
#   VALUE_SIZE    payload bytes per row (default 100)
#   QUERIES       point lookups per size (default 5000)
#   MISS_RATIO    fraction of lookups that miss (default 0.5)
#   THREADS       reader threads for the N-thread QPS run (default nproc)
#   BATCH_ROWS    rows per write batch (default 1000)
#   WORK          scratch dir for datasets/DBs (default <tmpdir>/kv_compare/<run_id>)
#   CONFIG_TIMEOUT  per-config seconds (default 3600)

set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
REPO_ROOT="$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel)"
cd "$REPO_ROOT"

RUN_ID="${1:-kv-compare-$(date -u +%Y%m%dT%H%M%SZ)}"
SIZES="${SIZES:-100000 500000 1000000}"
VALUE_SIZE="${VALUE_SIZE:-100}"
QUERIES="${QUERIES:-5000}"
MISS_RATIO="${MISS_RATIO:-0.5}"
THREADS="${THREADS:-$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 8)}"
BATCH_ROWS="${BATCH_ROWS:-1000}"
WORK="${WORK:-${TMPDIR:-/tmp}/kv_compare/$RUN_ID}"
CONFIG_TIMEOUT="${CONFIG_TIMEOUT:-3600}"
RESULT_DIR="$REPO_ROOT/target/kv-compare-results/$RUN_ID"
mkdir -p "$WORK" "$RESULT_DIR"

BENCH=mem_wal_kv_point_lookup
echo "=== Building $BENCH (--features bench-rocksdb) ==="
# Drop stale binaries so the freshest build is unambiguous.
rm -f "$REPO_ROOT"/target/release/deps/${BENCH}-*
cargo bench -p lance --bench "$BENCH" --features bench-rocksdb --no-run || {
    echo "ERROR: build failed" >&2; exit 1; }
BIN="$(find "$REPO_ROOT/target/release/deps" -maxdepth 1 -type f -perm -111 \
    -name "${BENCH}-*" ! -name '*.d' -printf '%T@ %p\n' \
    | sort -nr | head -1 | cut -d' ' -f2-)"
echo "bench binary: $BIN"
echo "run id:       $RUN_ID"
echo "sizes:        $SIZES  value_size=$VALUE_SIZE queries=$QUERIES miss_ratio=$MISS_RATIO threads=$THREADS"
echo ""

run_engine() {  # $1=engine $2=rows $3=tag
    local engine="$1" rows="$2" tag="$3"
    local name="${engine}_${tag}"
    local out="$RESULT_DIR/${name}.json"
    local log="$RESULT_DIR/${name}.log"
    local uri="$WORK/${name}"
    if [ -f "$out" ]; then
        echo ">>> $name (already done, skipping)"; return 0
    fi
    echo ">>> $name (rows=$rows)"
    rm -rf "$uri"
    timeout "$CONFIG_TIMEOUT" "$BIN" --bench \
        --engine "$engine" --rows "$rows" --value-size "$VALUE_SIZE" \
        --queries "$QUERIES" --miss-ratio "$MISS_RATIO" --threads "$THREADS" \
        --batch-rows "$BATCH_ROWS" --uri "$uri" --output "$out" > "$log" 2>&1
    local rc=$?
    rm -rf "$uri"
    if [ "$rc" -eq 124 ]; then
        echo "    !!! TIMED OUT after ${CONFIG_TIMEOUT}s (see $log)"; return 1
    elif [ "$rc" -ne 0 ]; then
        echo "    !!! failed rc=$rc (see $log)"; return 1
    fi
    echo "    ok"
}

for rows in $SIZES; do
    case "$rows" in
        1000000) tag=1M ;;
        500000)  tag=500k ;;
        100000)  tag=100k ;;
        *)       tag="$rows" ;;
    esac
    # Separate processes => clean per-engine peak RSS.
    run_engine lance   "$rows" "$tag"
    run_engine rocksdb "$rows" "$tag"
done

echo ""
echo "=== summary ==="
python3 - "$RESULT_DIR" <<'PY'
import glob, json, os, sys
d = sys.argv[1]
rows_map = {}
for p in sorted(glob.glob(os.path.join(d, "*.json"))):
    try:
        r = json.load(open(p))
    except Exception as e:
        print(f"  bad {p}: {e}"); continue
    for res in r.get("results", []):
        rows_map.setdefault(res["rows"], {})[res["engine"]] = res

hdr = (f"{'rows':>9} {'engine':>8} {'write_rows/s':>13} {'rd_p50_us':>10} "
       f"{'rd_p95_us':>10} {'rd_p99_us':>10} {'qps_1t':>9} {'qps_nt':>10} "
       f"{'rss_load_mb':>12} {'rss_peak_mb':>12} {'rd_cpu_s':>9}")
print(hdr)
for rows in sorted(rows_map):
    for engine in ("lance", "rocksdb"):
        res = rows_map[rows].get(engine)
        if not res:
            continue
        print(f"{rows:>9} {engine:>8} {res['write_rows_per_s']:>13} "
              f"{res['read_p50_us']:>10} {res['read_p95_us']:>10} {res['read_p99_us']:>10} "
              f"{res['read_qps_1t']:>9} {res['read_qps_nt']:>10} "
              f"{res['rss_after_load_mb']:>12} {res['peak_rss_mb']:>12} {res['read_cpu_s']:>9}")
    # ratios
    l = rows_map[rows].get("lance"); g = rows_map[rows].get("rocksdb")
    if l and g:
        def sd(a, b): return (a / b) if b else float('nan')
        print(f"{rows:>9} {'ratio':>8} "
              f"{sd(l['write_rows_per_s'], g['write_rows_per_s']):>12.2f}x "
              f"{sd(l['read_p50_us'], g['read_p50_us']):>9.2f}x "
              f"{sd(l['read_p95_us'], g['read_p95_us']):>9.2f}x "
              f"{sd(l['read_p99_us'], g['read_p99_us']):>9.2f}x "
              f"{sd(l['read_qps_1t'], g['read_qps_1t']):>8.2f}x "
              f"{sd(l['read_qps_nt'], g['read_qps_nt']):>9.2f}x "
              f"{sd(l['rss_after_load_mb'], g['rss_after_load_mb']):>11.2f}x")
print("\n(write/qps ratio >1 = lance faster; p50/rss ratio <1 = lance better)")
PY
echo ""
echo "results: $RESULT_DIR"
