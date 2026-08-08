#!/usr/bin/env bash
# Copy-on-write __manifest commit benchmark sweep panel.
#
# Drives `cargo run --release --example manifest_bench` across a panel of:
#   - bootstrap manifest sizes (rows already in __manifest)
#   - inline scalar indices on vs off
#   - continuous commit (single process, N commits) and
#     concurrent commit (C processes, steady TPS over a fixed duration)
#
# Each run is isolated: a "golden" manifest is bootstrapped once per (size, index)
# and server-side-copied to a fresh S3 prefix per run, so every run starts at exactly
# the bootstrapped size. Results are written as JSONL (one BenchResult per line) and
# summarised to CSV. The sweep is resumable: completed runs are skipped.
#
# Usage:
#   S3_BASE=s3://jack-devland-build/manifest-cow-bench/$(date -u +%Y%m%dT%H%M%SZ) \
#     ./manifest_commit_sweep.sh
#
# Env knobs (defaults match the requested panel):
#   SIZES, CONCURRENCY, INLINE_VARIANTS, CONT_OPS, CONC_DURATION_SECS,
#   AWS_REGION, OUT_DIR, BIN
#
# Resilient by design: a single failed run is logged and skipped rather than aborting
# the sweep, and re-running fills the gaps (completed runs are detected and skipped).
set -uo pipefail

RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
S3_BASE="${S3_BASE:?set S3_BASE, e.g. s3://jack-devland-build/manifest-cow-bench/$RUN_ID}"
AWS_REGION="${AWS_REGION:-us-east-1}"
export AWS_REGION AWS_DEFAULT_REGION="$AWS_REGION"

REPO_ROOT="${REPO_ROOT:-$HOME/oss/lance}"
BIN="${BIN:-$REPO_ROOT/target/release/examples/manifest_bench}"
OUT_DIR="${OUT_DIR:-$HOME/manifest_cow_bench_${RUN_ID}}"
RESULTS="$OUT_DIR/results.jsonl"
PROGRESS="$OUT_DIR/progress.log"
mkdir -p "$OUT_DIR"

SIZES=(${SIZES:-1000 2000 5000 10000 20000 50000 100000 200000 500000 1000000})
CONCURRENCY=(${CONCURRENCY:-10 20 50 100 120 150 200})
INLINE_VARIANTS=(${INLINE_VARIANTS:-true false})
CONT_OPS="${CONT_OPS:-100}"
CONC_DURATION_SECS="${CONC_DURATION_SECS:-30}"
STORAGE_OPT=(--storage-option "aws_region=${AWS_REGION}")

log() { printf '%s %s\n' "$(date -u +%H:%M:%S)" "$*" | tee -a "$PROGRESS"; }

# Skip a run if its tag already appears in results.jsonl (resume support).
done_already() { grep -q "\"bench_tag\":\"$1\"" "$RESULTS" 2>/dev/null; }

# Append a result line, tagging it so reruns can resume and we can pivot later.
record() {
    local tag="$1"; shift
    # shellcheck disable=SC2016
    python3 -c 'import json,sys; d=json.load(sys.stdin); d["bench_tag"]=sys.argv[1]; print(json.dumps(d))' \
        "$tag" >> "$RESULTS"
}

s3_copy() { aws s3 cp --recursive --quiet "$1" "$2" --region "$AWS_REGION"; }
s3_rm()   { aws s3 rm --recursive --quiet "$1" --region "$AWS_REGION" || true; }

# Backstops for unattended runs: cap any single run and clear leaked worker processes
# (a killed coordinator can orphan its worker children) before the next run.
RUN_TIMEOUT="${RUN_TIMEOUT:-1200}"
clear_stragglers() { pkill -f 'examples/manifest_bench worker' 2>/dev/null || true; sleep 1; }

for inline in "${INLINE_VARIANTS[@]}"; do
  for rows in "${SIZES[@]}"; do
    golden="${S3_BASE}/golden/inline_${inline}_rows_${rows}"
    boot_tag="boot_inline_${inline}_rows_${rows}"

    if ! done_already "$boot_tag"; then
      log "BOOTSTRAP inline=$inline rows=$rows -> $golden"
      s3_rm "$golden"
      if "$BIN" seed-large --root "$golden" --count "$rows" \
          --inline-optimization "$inline" "${STORAGE_OPT[@]}"; then
        echo "{\"bench_tag\":\"$boot_tag\"}" >> "$RESULTS"
      else
        log "BOOTSTRAP FAILED inline=$inline rows=$rows (skipping this size)"
        continue
      fi
    else
      log "skip bootstrap $boot_tag (done)"
    fi

    # ---- Continuous: single process, CONT_OPS commits ----
    cont_tag="cont_inline_${inline}_rows_${rows}"
    if ! done_already "$cont_tag"; then
      run_prefix="${S3_BASE}/run/${cont_tag}"
      log "CONTINUOUS inline=$inline rows=$rows ops=$CONT_OPS"
      clear_stragglers
      s3_copy "$golden" "$run_prefix"
      timeout "$RUN_TIMEOUT" "$BIN" run --root "$run_prefix" --operation write-create-namespace \
        --concurrency 1 --operations "$CONT_OPS" --initial-entries "$rows" \
        --inline-optimization "$inline" "${STORAGE_OPT[@]}" \
        2>>"$PROGRESS" | while read -r line; do record "$cont_tag" <<<"$line"; done
      s3_rm "$run_prefix"
    else
      log "skip continuous $cont_tag (done)"
    fi

    # ---- Concurrent: C processes, steady TPS over CONC_DURATION_SECS ----
    for c in "${CONCURRENCY[@]}"; do
      conc_tag="conc_inline_${inline}_rows_${rows}_c_${c}"
      if done_already "$conc_tag"; then log "skip concurrent $conc_tag (done)"; continue; fi
      run_prefix="${S3_BASE}/run/${conc_tag}"
      log "CONCURRENT inline=$inline rows=$rows c=$c dur=${CONC_DURATION_SECS}s"
      clear_stragglers
      s3_copy "$golden" "$run_prefix"
      timeout "$RUN_TIMEOUT" "$BIN" run --root "$run_prefix" --operation write-create-namespace \
        --concurrency "$c" --duration-secs "$CONC_DURATION_SECS" --initial-entries "$rows" \
        --inline-optimization "$inline" "${STORAGE_OPT[@]}" \
        2>>"$PROGRESS" | while read -r line; do record "$conc_tag" <<<"$line"; done
      s3_rm "$run_prefix"
    done
  done
done

# ---- Summarise to CSV ----
CSV="$OUT_DIR/summary.csv"
python3 - "$RESULTS" "$CSV" <<'PY'
import json, sys, csv
rows = []
with open(sys.argv[1]) as f:
    for line in f:
        d = json.loads(line)
        if "throughput_ops_per_sec" not in d:
            continue  # bootstrap marker
        mode = "continuous" if d["duration_secs"] == 0 else "concurrent"
        rows.append({
            "mode": mode, "variant": d["variant"], "initial_entries": d["initial_entries"],
            "concurrency": d["concurrency"], "duration_secs": d["duration_secs"],
            "ops": d["total_operations"], "errors": d["errors"],
            "tps": round(d["throughput_ops_per_sec"], 3),
            "avg_ms": round(d["avg_latency_ms"], 2), "p50_ms": round(d["p50_latency_ms"], 2),
            "p90_ms": round(d["p90_latency_ms"], 2), "p99_ms": round(d["p99_latency_ms"], 2),
        })
rows.sort(key=lambda r: (r["mode"], r["variant"], r["initial_entries"], r["concurrency"]))
with open(sys.argv[2], "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(rows[0].keys()) if rows else [])
    w.writeheader(); w.writerows(rows)
print(f"wrote {len(rows)} rows to {sys.argv[2]}")
PY

log "SWEEP COMPLETE. Results: $RESULTS  Summary: $CSV"
s3_rm "${S3_BASE}/golden" "${S3_BASE}/run" 2>/dev/null || true
