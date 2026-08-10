#!/usr/bin/env bash
# FTS comparison driver — Lance FtsMemIndex vs Apache Lucene.
# Sibling of run_mem_wal_hnsw_compare.sh (Lance HNSW vs hnswlib).
#
# Builds both benches, generates one shared FineWeb corpus + query set per
# size, runs each impl in Run A (pre-tokenized) and Run B (native
# analyzers), and prints build/query/recall side by side.
#
# Usage: rust/lance/benches/mem_wal/fts/run_fts_compare.sh [run_id]
#
# Env:
#   SIZES        doc-count sweep (default "100000 500000 1000000")
#   K            top-k (default 10)
#   THREADS      query threads for the multi-thread QPS run
#   LUCENE_CP    pre-built Lucene classpath; if set, the Lucene build is skipped
#   LUCENE_DIR   Lucene source checkout — built when LUCENE_CP is unset
#   JAVA_HOME    JDK 25 home; if unset the script searches common locations
#   CACHE_DIR    FineWeb shard download cache (default <tmpdir>/lance-fineweb-cache)

set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
REPO_ROOT="$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel)"
cd "$REPO_ROOT"

RUN_ID="${1:-fts-compare-$(date -u +%Y%m%dT%H%M%SZ)}"
SIZES="${SIZES:-100000 500000 1000000}"
K="${K:-10}"
THREADS="${THREADS:-$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 8)}"
CACHE_DIR="${CACHE_DIR:-${TMPDIR:-/tmp}/lance-fineweb-cache}"
WORK="${WORK:-${TMPDIR:-/tmp}/fts_compare/$RUN_ID}"
RESULT_DIR="$REPO_ROOT/target/fts-compare-results/$RUN_ID"
mkdir -p "$WORK" "$RESULT_DIR" "$CACHE_DIR"

# ---- locate JDK 25 ----
if [ -z "${JAVA_HOME:-}" ]; then
    for cand in /usr/lib/jvm/java-25-* /usr/lib/jvm/jdk-25* /opt/jdk-25* \
                "$HOME/.sdkman/candidates/java/25"*; do
        [ -x "$cand/bin/java" ] && JAVA_HOME="$cand" && break
    done
fi
if [ -z "${JAVA_HOME:-}" ] || [ ! -x "$JAVA_HOME/bin/java" ]; then
    echo "ERROR: JDK 25 not found; set JAVA_HOME" >&2; exit 1
fi
JAVA="$JAVA_HOME/bin/java"
JAVAC="$JAVA_HOME/bin/javac"
echo "JDK: $($JAVA -version 2>&1 | head -1)"

# ---- build Lucene classpath ----
if [ -z "${LUCENE_CP:-}" ]; then
    if [ -z "${LUCENE_DIR:-}" ]; then
        echo "ERROR: set LUCENE_CP (prebuilt jars) or LUCENE_DIR (source checkout)" >&2
        exit 1
    fi
    echo "=== Building Lucene jars ($LUCENE_DIR) ==="
    ( cd "$LUCENE_DIR" && JAVA_HOME="$JAVA_HOME" ./gradlew -q \
        :lucene:core:jar :lucene:analysis:common:jar ) || {
        echo "ERROR: Lucene build failed" >&2; exit 1; }
    CORE_JAR="$(find "$LUCENE_DIR/lucene/core/build/libs" -name 'lucene-core-*.jar' | head -1)"
    ANALYSIS_JAR="$(find "$LUCENE_DIR/lucene/analysis/common/build/libs" -name 'lucene-analysis-common-*.jar' | head -1)"
    LUCENE_CP="$CORE_JAR:$ANALYSIS_JAR"
fi
echo "Lucene classpath: $LUCENE_CP"

# ---- build the Lance bench ----
echo "=== Building Lance FTS bench ==="
# Drop stale binaries so the freshest build is unambiguous, then pick the
# most-recently-modified one — a lexical sort by build hash can otherwise
# pick a stale binary after a dependency change rotates the hash.
rm -f "$REPO_ROOT"/target/release/deps/mem_wal_fts_bench-*
cargo bench -p lance --bench mem_wal_fts_bench --no-run
LANCE_BIN="$(find "$REPO_ROOT/target/release/deps" -maxdepth 1 -type f -perm -111 \
    -name 'mem_wal_fts_bench-*' ! -name '*.d' -printf '%T@ %p\n' \
    | sort -nr | head -1 | cut -d' ' -f2-)"
echo "Lance bench: $LANCE_BIN"

# ---- compile the Lucene bench ----
echo "=== Compiling Lucene FTS bench ==="
"$JAVAC" -cp "$LUCENE_CP" -d "$WORK" \
    "$SCRIPT_DIR/LuceneFtsBench.java" \
    || { echo "ERROR: javac failed" >&2; exit 1; }

mutual_overlap() {  # $1=topk file A  $2=topk file B  $3=k
    python3 - "$1" "$2" "$3" <<'PY'
import sys
a, b, k = sys.argv[1], sys.argv[2], int(sys.argv[3])
la = [set(l.split()) for l in open(a)]
lb = [set(l.split()) for l in open(b)]
n = min(len(la), len(lb))
if n == 0:
    print("nan"); sys.exit()
tot = sum(len(la[i] & lb[i]) / max(len(la[i] | lb[i]), 1) for i in range(n))
print(f"{tot / n:.4f}")
PY
}

# Optional apples-to-apples toggles:
#   POSITIONS=0        index without token positions on BOTH sides (term-only,
#                      no phrase) — Lance with_position=false vs Lucene
#                      DOCS_AND_FREQS.
#   IMMUTABLE=1        Lance reads only immutable frozen segments (Lucene model).
#   FREEZE_THRESHOLD=n keep a large mutable tail (n docs per freeze) so the
#                      tail-read path is exercised — needed to measure tail-skip.
#   NO_TAIL_SKIP=1     disable Lance's block-max tail pruning (A/B the optimization).
LANCE_FLAGS=""
LUCENE_FLAGS=""
if [ "${POSITIONS:-1}" = "0" ]; then
    LANCE_FLAGS="$LANCE_FLAGS --no-positions"
    LUCENE_FLAGS="$LUCENE_FLAGS --no-positions"
fi
if [ "${IMMUTABLE:-0}" = "1" ]; then
    LANCE_FLAGS="$LANCE_FLAGS --immutable"
fi
if [ -n "${FREEZE_THRESHOLD:-}" ]; then
    LANCE_FLAGS="$LANCE_FLAGS --freeze-threshold $FREEZE_THRESHOLD"
fi
if [ "${NO_TAIL_SKIP:-0}" = "1" ]; then
    LANCE_FLAGS="$LANCE_FLAGS --no-tail-skip"
fi
echo "lance flags:'$LANCE_FLAGS'  lucene flags:'$LUCENE_FLAGS'"

echo ""
for SIZE in $SIZES; do
    DIR="$WORK/n$SIZE"
    mkdir -p "$DIR"
    echo "############ corpus size = $SIZE ############"
    echo "--- generating shared corpus + queries ---"
    "$LANCE_BIN" --bench gen --docs "$SIZE" --out-dir "$DIR" \
        --cache-dir "$CACHE_DIR" --k "$K" > "$RESULT_DIR/gen_n$SIZE.log" 2>&1 || {
        echo "  !!! gen failed (see gen_n$SIZE.log)"; continue; }

    for RUN in a b; do
        echo "--- run $RUN: lance ---"
        "$LANCE_BIN" --bench bench --in-dir "$DIR" --run "$RUN" --k "$K" \
            --threads "$THREADS" $LANCE_FLAGS | tee "$RESULT_DIR/lance_n${SIZE}_run${RUN}.txt" \
            | grep '^{' > "$RESULT_DIR/lance_n${SIZE}_run${RUN}.json"
        echo "--- run $RUN: lucene ---"
        "$JAVA" -cp "$LUCENE_CP:$WORK" LuceneFtsBench --in-dir "$DIR" --run "$RUN" \
            --k "$K" --threads "$THREADS" $LUCENE_FLAGS | tee "$RESULT_DIR/lucene_n${SIZE}_run${RUN}.txt" \
            | grep '^{' > "$RESULT_DIR/lucene_n${SIZE}_run${RUN}.json"
        ov="$(mutual_overlap "$DIR/lance_fts_run${RUN}_topk.txt" \
                             "$DIR/lucene_run${RUN}_topk.txt" "$K")"
        echo "    lance<->lucene mutual top-$K overlap (run $RUN) = $ov"
        echo "$ov" > "$RESULT_DIR/overlap_n${SIZE}_run${RUN}.txt"
    done
    echo ""
done

echo "=== summary ==="
python3 - "$RESULT_DIR" "$K" <<'PY'
import glob, json, os, sys
d, k = sys.argv[1], sys.argv[2]
print(f"{'size':>9} {'run':>4} {'impl':>10} {'build_dps':>11} {'q_p50_us':>10} "
      f"{'q_p95_us':>10} {'qps_1t':>9} {'qps_nt':>10} {'term_rec':>9} {'phr_rec':>9} {'or_rec':>9}")
for p in sorted(glob.glob(os.path.join(d, "*_n*_run*.json"))):
    try: r = json.load(open(p))
    except Exception: continue
    print(f"{r['docs']:>9} {r['run']:>4} {r['impl']:>10} {r['build_docs_per_s']:>11.0f} "
          f"{r['q_p50_us']:>10.1f} {r['q_p95_us']:>10.1f} {r['qps_1t']:>9.0f} "
          f"{r['qps_nt']:>10.0f} {r['term_recall_at_k']:>9.3f} {r['phrase_recall_at_k']:>9.3f} "
          f"{r.get('or_recall_at_k', float('nan')):>9.3f}")
for p in sorted(glob.glob(os.path.join(d, "overlap_*.txt"))):
    name = os.path.basename(p)[:-4]
    print(f"  {name} = {open(p).read().strip()}")
PY
echo ""
echo "results: $RESULT_DIR"
