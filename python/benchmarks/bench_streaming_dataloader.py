#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Benchmark for StreamingDataset throughput.

Sweeps prefetch_chunk from 1 to 16384 to show how amortising the per-request
overhead scales.  Each row at each chunk size is timed via the real
StreamingDataset so the numbers reflect production code.

Run with:
    cd python
    uv run --extra tests benchmarks/bench_streaming_dataloader.py

Optional env vars:
    BENCH_NUM_ROWS   — total rows in the table (default 49152 = 24 × 2048)
    BENCH_NUM_SPLITS — number of splits (default 24)
    BENCH_STEPS      — round-robin cycles to time per chunk size (default 100)
    BENCH_ROW_BYTES  — approximate bytes per row padded with a binary column
                       (default 4096, mimics a small embedding/image patch)
"""

import os
import time
import tempfile

import pyarrow as pa
import lancedb

from lancedb.streaming import StreamingDataset

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

NUM_SPLITS = int(os.environ.get("BENCH_NUM_SPLITS", 24))
# Default: 2048 rows per split so every chunk size up to 16Ki has ≥1 full
# chunk (except 16Ki itself which gets a single full-split fetch — still valid).
NUM_ROWS = int(os.environ.get("BENCH_NUM_ROWS", NUM_SPLITS * 2048))
STEPS = int(os.environ.get("BENCH_STEPS", 100))
ROW_BYTES = int(os.environ.get("BENCH_ROW_BYTES", 4096))

assert NUM_ROWS % NUM_SPLITS == 0, "NUM_ROWS must be divisible by NUM_SPLITS"

CHUNK_SIZES = [1, 4, 16, 64, 256, 1024, 4096, 16384]


# ---------------------------------------------------------------------------
# Table helpers
# ---------------------------------------------------------------------------

def make_table(db_path: str) -> lancedb.table.Table:
    db = lancedb.connect(db_path)
    payload = b"x" * ROW_BYTES
    data = pa.table(
        {
            "id": pa.array(range(NUM_ROWS), type=pa.int32()),
            "payload": pa.array([payload] * NUM_ROWS, type=pa.large_binary()),
        }
    )
    return db.create_table("bench", data, mode="overwrite")


# ---------------------------------------------------------------------------
# Timing
# ---------------------------------------------------------------------------

def bench_chunk(table, chunk_size: int, steps: int) -> tuple[int, float]:
    """Return (rows_drained, elapsed_seconds) for one timed run."""
    total_rows = steps * NUM_SPLITS
    ds = StreamingDataset(
        table, num_splits=NUM_SPLITS, shuffle_seed=42, prefetch_chunk=chunk_size
    )
    count = 0
    t0 = time.perf_counter()
    for _ in ds:
        count += 1
        if count >= total_rows:
            break
    return count, time.perf_counter() - t0


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    rows_per_split = NUM_ROWS // NUM_SPLITS
    print("Benchmark config:")
    print(
        f"  NUM_ROWS={NUM_ROWS}  NUM_SPLITS={NUM_SPLITS}  "
        f"rows/split={rows_per_split}  STEPS={STEPS}  ROW_BYTES={ROW_BYTES}"
    )
    print(f"  ~{NUM_ROWS * ROW_BYTES / 1024 / 1024:.1f} MB total table size")
    print()

    with tempfile.TemporaryDirectory() as tmp:
        print("Creating table...", flush=True)
        table = make_table(tmp)

        print(f"\n{'chunk':>6}  {'rows':>6}  {'elapsed':>8}  {'rows/s':>10}  {'ms/step':>9}")
        print("-" * 52)

        for chunk in CHUNK_SIZES:
            # Warm-up pass (one step's worth of rows)
            warmup_ds = StreamingDataset(
                table, num_splits=NUM_SPLITS, shuffle_seed=42, prefetch_chunk=chunk
            )
            warmup_count = 0
            for _ in warmup_ds:
                warmup_count += 1
                if warmup_count >= NUM_SPLITS:
                    break

            drained, elapsed = bench_chunk(table, chunk, STEPS)
            rows_per_sec = drained / elapsed if elapsed > 0 else float("inf")
            ms_per_step = elapsed / STEPS * 1000

            print(
                f"{chunk:>6}  {drained:>6}  {elapsed:>7.3f}s  "
                f"{rows_per_sec:>10.0f}  {ms_per_step:>8.1f}ms"
            )

    print()
    print("Done.")


if __name__ == "__main__":
    main()
