# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import time

import lancedb
from lancedb.permutation import (
    Permutation,
    PermutationMetrics,
    Permutations,
    permutation_builder,
)


def test_permutation_metrics_accumulates():
    """PermutationMetrics correctly accumulates bytes and timings."""
    m = PermutationMetrics()
    m.add_io(1000, 0.5)
    m.add_io(2000, 0.5)
    m.add_transform_time(0.25)
    m.add_transform_time(0.25)
    m.add_consumption_time(0.1)
    m.add_consumption_time(0.1)

    s = m.summary()
    assert s["total_bytes"] == 3000
    assert s["batch_count"] == 2
    assert s["io_bytes_per_sec"] == 3000 / 1.0  # 3000 bytes / 1s
    assert s["transform_bytes_per_sec"] == 3000 / 0.5
    assert s["consumption_bytes_per_sec"] == 3000 / 0.2


def test_permutation_metrics_zero_time():
    """
    summary() returns 0.0 when no time has been recorded (avoids ZeroDivisionError).
    """
    m = PermutationMetrics()
    s = m.summary()
    assert s["io_bytes_per_sec"] == 0.0
    assert s["transform_bytes_per_sec"] == 0.0
    assert s["consumption_bytes_per_sec"] == 0.0
    assert s["total_bytes"] == 0
    assert s["batch_count"] == 0


def test_permutation_metrics_repr():
    """__repr__ runs without error and contains key labels."""
    m = PermutationMetrics()
    m.add_io(1024 * 1024, 1.0)
    r = repr(m)
    assert "I/O:" in r
    assert "Transform:" in r
    assert "Consumption:" in r
    assert "MB/s" in r


def test_with_observability_identity(tmp_path):
    """Iterating with observability=True populates metrics."""
    db = lancedb.connect(str(tmp_path))
    tbl = db.create_table("obs", data=[{"x": i} for i in range(200)])

    perm = Permutation.identity(tbl).with_batch_size(50).with_observability()

    assert perm.metrics is not None
    assert perm.metrics.total_bytes == 0  # nothing consumed yet

    for _batch in perm:
        pass

    s = perm.metrics.summary()
    assert s["total_bytes"] > 0
    assert s["io_bytes_per_sec"] > 0
    assert s["transform_bytes_per_sec"] > 0
    # consumption_bytes_per_sec may be very high if the consumer does
    # nothing, but it should still be > 0 because time.perf_counter()
    # never returns exactly the same value twice on modern hardware.
    assert s["consumption_bytes_per_sec"] > 0
    assert s["batch_count"] > 0


def test_with_observability_disabled(tmp_path):
    """with_observability(False) sets metrics to None."""
    db = lancedb.connect(str(tmp_path))
    tbl = db.create_table("obs_off", data=[{"x": i} for i in range(50)])

    perm = (
        Permutation.identity(tbl)
        .with_batch_size(10)
        .with_observability(True)
        .with_observability(False)
    )
    assert perm.metrics is None


def test_observability_not_in_pickle(tmp_path):
    """Metrics are ephemeral and are NOT preserved across pickle."""
    import pickle

    db = lancedb.connect(str(tmp_path))
    tbl = db.create_table("pkl", data=[{"x": i} for i in range(50)])
    perm = Permutation.identity(tbl).with_batch_size(10).with_observability()

    # Consume a few batches to populate metrics
    for _batch in perm:
        pass
    assert perm.metrics.total_bytes > 0

    # Round-trip through pickle
    data = pickle.dumps(perm)
    restored = pickle.loads(data)
    assert restored.metrics is None


def test_observability_with_permutation_splits(tmp_path):
    """Observability works correctly when using Permutations with splits."""
    db = lancedb.connect(str(tmp_path))
    tbl = db.create_table("split", data=[{"x": i} for i in range(100)])

    perm_tbl = (
        permutation_builder(tbl)
        .split_random(ratios=[0.8, 0.2], split_names=["train", "test"])
        .shuffle()
        .execute()
    )

    permutations = Permutations(tbl, perm_tbl)
    train = permutations["train"].with_batch_size(10).with_observability()

    for _batch in train:
        time.sleep(0.001)  # simulate a small consumer workload

    s = train.metrics.summary()
    assert s["total_bytes"] > 0
    assert s["io_bytes_per_sec"] > 0
    assert s["batch_count"] > 0
