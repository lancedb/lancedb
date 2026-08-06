# `__manifest` commit benchmark

Measures how fast the copy-on-write directory catalog commits `__manifest` mutations as
the manifest scales, with the inline scalar indices on or off.

The catalog commits every mutation by rewriting the whole `__manifest` (copy-on-write)
and atomically writing a new manifest version. This benchmark characterises:

- **Continuous commit** — a single process commits `N` times into a manifest already
  holding `rows` entries (per-commit latency + throughput).
- **Concurrent commit** — `C` processes commit continuously for a fixed duration against
  a manifest of `rows` entries (steady, contended TPS).

## Binary: `examples/manifest_bench.rs`

```
manifest_bench seed-large --root <uri> --count <rows> --inline-optimization <true|false> \
    [--storage-option aws_region=us-east-1]
manifest_bench run --root <uri> --operation write-create-namespace \
    --concurrency 1 --operations 100 --initial-entries <rows> --inline-optimization <bool>   # continuous
manifest_bench run --root <uri> --operation write-create-namespace \
    --concurrency 50 --duration-secs 30 --initial-entries <rows> --inline-optimization <bool> # concurrent
```

- `seed-large` bootstraps a manifest to `count` rows by writing the Lance dataset
  directly (O(rows) once) and then triggering one CoW rewrite so the on-disk state
  matches the steady catalog form (single fragment; inline indices when enabled).
- `run` spawns `--concurrency` worker subprocesses. With `--operations` it runs a fixed
  commit budget (continuous); with `--duration-secs` each worker commits until the
  deadline (steady TPS). It prints one JSON `BenchResult` per concurrency level with
  throughput and p50/p90/p99 latency.
- The committed operation (`--operation`) defaults to `write-create-namespace`, the
  cheapest pure-`__manifest` mutation (no table data). `write-create-table` /
  `write-declare-table` are also available.

S3 requires the default `dir-aws` feature (on by default) and AWS credentials in the
environment; pass `--storage-option aws_region=<region>`.

## Sweep panel: `benches/manifest_commit_sweep.sh`

Runs the full panel — sizes × {inline index, no index} × {continuous, concurrent×C} —
with per-run S3-copy isolation (each run starts at exactly the bootstrapped size),
JSONL results, a `summary.csv`, and resume support.

```bash
cargo build --release --example manifest_bench -p lance-namespace-impls
S3_BASE=s3://<bucket>/manifest-cow-bench/$(date -u +%Y%m%dT%H%M%SZ) \
  rust/lance-namespace-impls/benches/manifest_commit_sweep.sh
```

Default panel (override via env): `SIZES="1000 2000 5000 10000 20000 50000 100000 200000
500000 1000000"`, `CONCURRENCY="10 20 50 100 120 150 200"`, `INLINE_VARIANTS="true false"`,
`CONT_OPS=100`, `CONC_DURATION_SECS=30`. Results land in `$OUT_DIR` (default
`~/manifest_cow_bench_<RUN_ID>`).

## Representative results

EC2 `c7i.48xlarge`, S3 `us-east-1`, op `write-create-namespace`. The catalog is a
single-writer-throughput system: per-commit cost scales ~O(rows) and throughput does **not**
scale with concurrency (every commit is a serialized `__manifest` version bump).

Continuous (1 process, 100 commits), ops/s — inline index vs no index:

| rows | inline | no index |
|---:|---:|---:|
| 1,000 | 2.0 | 3.5 |
| 100,000 | 1.1 | 2.1 |
| 1,000,000 | 0.34 | 0.53 |

Concurrent steady TPS is flat across C=10..200 (e.g. inline @100k ≈ 1.4–1.5 ops/s at every C;
@1M ≈ 0.3 ops/s). Conflicts that exceed the retry budget surface as errors and grow with C
(≈0 at C≤20, climbing at C≥100) — the contention ceiling, not data loss. No-index commits run
~1.5–2× faster (no per-commit index build) at the cost of unindexed reads.
