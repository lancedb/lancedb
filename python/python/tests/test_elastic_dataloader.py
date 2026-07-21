# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Tests for elastic dataloader properties.

Two properties are verified:

1. **Elastic determinism** — for a fixed (num_splits, shuffle_seed, epoch), the
   set of samples that forms each global training step is identical regardless of
   the training topology (world_size).  This mirrors the MDS guarantee described
   in dataloader.md: "the global batch at a given step will always contain the
   same samples."

2. **Resumability** — after calling state_dict() and load_state_dict(), a fresh
   dataset continues from exactly where the previous run stopped: no sample is
   skipped, no sample is repeated.  The checkpoint is topology-independent (it
   captures per-split consumption counts), so resumption works even when
   world_size changes between runs.

Tests use explicit rank / world_size constructor parameters rather than
torch.distributed, so they run in a single process without a distributed process
group.  The import guard below causes the whole module to be skipped when
lancedb.streaming does not yet exist, which keeps CI green while the
implementation is in progress.

Parameters used throughout:
  NUM_ROWS        = 120   (divisible by all tested world_sizes and num_splits)
  NUM_SPLITS      = 12    (divides cleanly by world_sizes 1,2,3,4,6,12)
  GLOBAL_BATCH_SIZE = 12  (= num_splits → each split contributes 1 sample/step)
  STEPS_PER_EPOCH = 10    (= NUM_ROWS / GLOBAL_BATCH_SIZE)
"""

import dataclasses
import logging
from unittest.mock import patch

import lancedb
import pyarrow as pa
import pytest

torch = pytest.importorskip("torch")
streaming = pytest.importorskip("lancedb.streaming")
StreamingDataset = streaming.StreamingDataset

# ---------------------------------------------------------------------------
# Dataset parameters
# ---------------------------------------------------------------------------

NUM_ROWS = 120
NUM_SPLITS = 12
GLOBAL_BATCH_SIZE = NUM_SPLITS  # one sample per split per step
STEPS_PER_EPOCH = NUM_ROWS // GLOBAL_BATCH_SIZE  # = 10
SHUFFLE_SEED = 42

# world_sizes compatible with NUM_SPLITS (i.e. NUM_SPLITS % world_size == 0)
COMPATIBLE_WORLD_SIZES = [1, 2, 3, 4, 6, 12]

# Parameters for the "large global batch" tests where global_batch_size is a
# multiple of num_splits rather than equal to it.  Using 3× gives 3 samples per
# split per step, which exercises the path where each rank drains more than one
# sample per split per global step.
LARGE_GLOBAL_BATCH_SIZE = 36  # = 3 * NUM_SPLITS
LARGE_NUM_ROWS = 360  # must be divisible by LARGE_GLOBAL_BATCH_SIZE (360/36=10)

# (world_size, num_workers) pairs used in multi-worker tests.
# Constraint: num_splits % (world_size * num_workers) == 0, i.e. world_size *
# num_workers must divide NUM_SPLITS (12).
MULTI_WORKER_TOPOLOGIES = [
    (1, 2),
    (1, 3),
    (1, 4),
    (2, 2),
    (2, 3),
]


@dataclasses.dataclass
class FakeWorkerInfo:
    """Stand-in for torch.utils.data.WorkerInfo, accepted by worker_info_override.

    Mirrors the attributes that StreamingDataset reads from the real WorkerInfo:
      id          — this worker's index (0-based)
      num_workers — total workers for this rank's DataLoader
    """

    id: int
    num_workers: int


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def lance_table(tmp_path):
    """A simple LanceDB table with integer IDs 0 .. NUM_ROWS-1."""
    db = lancedb.connect(tmp_path)
    return db.create_table("data", pa.table({"id": list(range(NUM_ROWS))}))


@pytest.fixture
def lance_table_large(tmp_path):
    """A larger table for tests where global_batch_size > num_splits.

    Uses LARGE_NUM_ROWS rows.
    """
    db = lancedb.connect(tmp_path)
    return db.create_table("data", pa.table({"id": list(range(LARGE_NUM_ROWS))}))


# ---------------------------------------------------------------------------
# Simulation helpers
# ---------------------------------------------------------------------------


def _make_dataset(
    table,
    rank: int,
    world_size: int,
    *,
    num_splits: int = NUM_SPLITS,
    shuffle_seed: int = SHUFFLE_SEED,
    epoch: int = 0,
    worker_info_override: FakeWorkerInfo | None = None,
) -> StreamingDataset:
    """Create a StreamingDataset configured for a specific rank in a simulated
    distributed run.  rank and world_size are passed explicitly so we do not
    need an actual torch.distributed process group.

    Pass worker_info_override to simulate a specific DataLoader worker without
    spawning real worker processes.
    """
    return StreamingDataset(
        table,
        num_splits=num_splits,
        shuffle_seed=shuffle_seed,
        epoch=epoch,
        rank=rank,
        world_size=world_size,
        worker_info_override=worker_info_override,
    )


def _collect_global_batches(
    table,
    world_size: int,
    *,
    num_splits: int = NUM_SPLITS,
    global_batch_size: int = GLOBAL_BATCH_SIZE,
    shuffle_seed: int = SHUFFLE_SEED,
    epoch: int = 0,
) -> list[frozenset[int]]:
    """Drain a full epoch and return one frozenset of sample IDs per global step.

    Each rank is represented by one StreamingDataset.  A "global step" consumes
    (global_batch_size // world_size) samples from every rank, so the global batch
    at step k is the union of all per-rank micro-batches at that step.  frozensets
    are used so comparisons are order-independent within a batch.

    global_batch_size must be a multiple of num_splits (each split contributes
    global_batch_size // num_splits samples per step).

    Raises AssertionError if the per-rank iterators exhaust at different times,
    which would indicate a padding/truncation bug in the implementation.
    """
    assert num_splits % world_size == 0, (
        f"world_size={world_size} does not divide num_splits={num_splits}"
    )
    assert global_batch_size % num_splits == 0, (
        f"global_batch_size={global_batch_size} is not a multiple of "
        f"num_splits={num_splits}"
    )
    micro = global_batch_size // world_size  # samples per rank per global step

    datasets = [
        _make_dataset(
            table,
            rank,
            world_size,
            num_splits=num_splits,
            shuffle_seed=shuffle_seed,
            epoch=epoch,
        )
        for rank in range(world_size)
    ]
    iters = [iter(ds) for ds in datasets]

    _STOP = object()
    global_batches: list[frozenset[int]] = []
    while True:
        step_samples: set[int] = set()
        exhausted = 0
        for it in iters:
            for _ in range(micro):
                val = next(it, _STOP)
                if val is _STOP:
                    exhausted += 1
                    break
                step_samples.add(val["id"])
        if exhausted == len(iters):
            # All iterators exhausted at the same step — end of epoch.
            break
        assert exhausted == 0, (
            "Rank iterators exhausted at different steps. "
            "StreamingDataset must produce equal-length sequences across ranks."
        )
        global_batches.append(frozenset(step_samples))

    return global_batches


def _advance_and_checkpoint(
    table,
    world_size: int,
    steps: int,
    *,
    num_splits: int = NUM_SPLITS,
    global_batch_size: int = GLOBAL_BATCH_SIZE,
    shuffle_seed: int = SHUFFLE_SEED,
    epoch: int = 0,
) -> tuple[list[frozenset[int]], dict]:
    """Run `steps` global steps, then snapshot the dataset state.

    Returns (batches_consumed, checkpoint) where checkpoint is the state_dict
    from rank 0's dataset.  Because state is per-split (not per-rank), any
    rank's state_dict can be used to resume on any compatible topology.
    """
    assert num_splits % world_size == 0
    assert global_batch_size % num_splits == 0
    micro = global_batch_size // world_size

    datasets = [
        _make_dataset(
            table,
            rank,
            world_size,
            num_splits=num_splits,
            shuffle_seed=shuffle_seed,
            epoch=epoch,
        )
        for rank in range(world_size)
    ]
    iters = [iter(ds) for ds in datasets]

    seen: list[frozenset[int]] = []
    for _ in range(steps):
        step_samples: set[int] = set()
        for it in iters:
            for _ in range(micro):
                step_samples.add(next(it)["id"])
        seen.append(frozenset(step_samples))

    checkpoint = datasets[0].state_dict()
    return seen, checkpoint


def _resume_and_collect(
    table,
    world_size: int,
    checkpoint: dict,
    *,
    num_splits: int = NUM_SPLITS,
    global_batch_size: int = GLOBAL_BATCH_SIZE,
    shuffle_seed: int = SHUFFLE_SEED,
    epoch: int = 0,
) -> list[frozenset[int]]:
    """Create fresh datasets, load checkpoint, and drain to epoch end.

    world_size may differ from the run that produced the checkpoint,
    exercising the elastic-resume path.
    """
    assert num_splits % world_size == 0
    assert global_batch_size % num_splits == 0
    micro = global_batch_size // world_size

    datasets = [
        _make_dataset(
            table,
            rank,
            world_size,
            num_splits=num_splits,
            shuffle_seed=shuffle_seed,
            epoch=epoch,
        )
        for rank in range(world_size)
    ]
    for ds in datasets:
        ds.load_state_dict(checkpoint)

    iters = [iter(ds) for ds in datasets]
    _STOP2 = object()
    remaining: list[frozenset[int]] = []
    while True:
        step_samples: set[int] = set()
        exhausted = 0
        for it in iters:
            for _ in range(micro):
                val = next(it, _STOP2)
                if val is _STOP2:
                    exhausted += 1
                    break
                step_samples.add(val["id"])
        if exhausted == len(iters):
            break
        assert exhausted == 0
        remaining.append(frozenset(step_samples))

    return remaining


# ---------------------------------------------------------------------------
# Elastic determinism tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("world_size", COMPATIBLE_WORLD_SIZES)
def test_elastic_det_full_coverage(lance_table, world_size):
    """Every sample ID appears exactly once across all steps of an epoch."""
    batches = _collect_global_batches(lance_table, world_size)
    all_seen = sorted(sid for b in batches for sid in b)
    assert all_seen == list(range(NUM_ROWS)), (
        f"world_size={world_size}: expected IDs 0..{NUM_ROWS - 1} but got {all_seen}"
    )


@pytest.mark.parametrize("world_size", COMPATIBLE_WORLD_SIZES)
def test_elastic_det_correct_step_count(lance_table, world_size):
    """Number of global steps equals NUM_ROWS // GLOBAL_BATCH_SIZE."""
    batches = _collect_global_batches(lance_table, world_size)
    assert len(batches) == STEPS_PER_EPOCH, (
        f"world_size={world_size}: expected {STEPS_PER_EPOCH} steps, got {len(batches)}"
    )


@pytest.mark.parametrize("world_size", COMPATIBLE_WORLD_SIZES)
def test_elastic_det_no_intra_batch_duplicates(lance_table, world_size):
    """Within a single global batch, every sample ID is distinct."""
    batches = _collect_global_batches(lance_table, world_size)
    for step, batch in enumerate(batches):
        assert len(batch) == GLOBAL_BATCH_SIZE, (
            f"world_size={world_size} step {step}: "
            f"expected {GLOBAL_BATCH_SIZE} samples, got {len(batch)}"
        )


def test_elastic_det_same_batches_across_world_sizes(lance_table):
    """The global batch at each step is identical for every compatible world_size.

    This is the core elastic-determinism guarantee from the design doc.
    """
    reference = _collect_global_batches(lance_table, world_size=1)
    for ws in COMPATIBLE_WORLD_SIZES[1:]:
        batches = _collect_global_batches(lance_table, world_size=ws)
        assert len(batches) == len(reference), (
            f"world_size={ws} produced {len(batches)} steps; "
            f"world_size=1 produced {len(reference)}"
        )
        for step, (ref_batch, batch) in enumerate(zip(reference, batches)):
            assert ref_batch == batch, (
                f"Global batch at step {step} differs between world_size=1 and "
                f"world_size={ws}.\n"
                f"  world_size=1  → {sorted(ref_batch)}\n"
                f"  world_size={ws} → {sorted(batch)}"
            )


@pytest.mark.parametrize("world_size", COMPATIBLE_WORLD_SIZES)
def test_elastic_det_reproducible(lance_table, world_size):
    """Two independent runs with the same parameters produce identical epochs."""
    batches_a = _collect_global_batches(lance_table, world_size)
    batches_b = _collect_global_batches(lance_table, world_size)
    assert batches_a == batches_b, (
        f"world_size={world_size}: second run produced different batches"
    )


@pytest.mark.parametrize("epoch_a,epoch_b", [(0, 1), (1, 2), (0, 2)])
def test_elastic_det_different_epochs_differ(lance_table, epoch_a, epoch_b):
    """Different epochs use different shuffles and therefore produce different
    step sequences.  (Both cover all samples; they just appear in different order.)
    """
    batches_a = _collect_global_batches(lance_table, world_size=1, epoch=epoch_a)
    batches_b = _collect_global_batches(lance_table, world_size=1, epoch=epoch_b)
    # Same set of samples overall...
    assert sorted(sid for b in batches_a for sid in b) == list(range(NUM_ROWS))
    assert sorted(sid for b in batches_b for sid in b) == list(range(NUM_ROWS))
    # ...but a different step-by-step ordering.
    assert batches_a != batches_b, (
        f"Epoch {epoch_a} and epoch {epoch_b} produced identical step sequences; "
        "shuffle_seed should incorporate the epoch number."
    )


def test_elastic_det_different_seeds_differ(lance_table):
    """Different shuffle seeds produce different sample orderings."""
    batches_seed0 = _collect_global_batches(lance_table, world_size=1, shuffle_seed=0)
    batches_seed1 = _collect_global_batches(lance_table, world_size=1, shuffle_seed=1)
    assert batches_seed0 != batches_seed1, (
        "Different shuffle_seed values must produce different sample orderings."
    )


# ---------------------------------------------------------------------------
# Resumability tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "checkpoint_at_step", [0, 1, 4, STEPS_PER_EPOCH - 1, STEPS_PER_EPOCH]
)
def test_resumability_same_world_size(lance_table, checkpoint_at_step):
    """Resuming on the same world_size produces the same global batches as a
    reference full-epoch run, split at the checkpoint step.

    Combined (before + after resume):
    - No sample is repeated.
    - No sample is skipped.
    - Each global batch matches the reference exactly.
    """
    world_size = 2
    reference = _collect_global_batches(lance_table, world_size)

    seen_before, checkpoint = _advance_and_checkpoint(
        lance_table, world_size, checkpoint_at_step
    )
    seen_after = _resume_and_collect(lance_table, world_size, checkpoint)

    # Correct total number of steps.
    assert len(seen_before) + len(seen_after) == STEPS_PER_EPOCH, (
        f"Expected {STEPS_PER_EPOCH} total steps; "
        f"got {len(seen_before)} + {len(seen_after)}"
    )

    # Each pre-checkpoint batch matches the reference.
    for step, (ref, got) in enumerate(zip(reference, seen_before)):
        assert ref == got, (
            f"Pre-checkpoint batch {step} doesn't match reference.\n"
            f"  reference: {sorted(ref)}\n"
            f"  got:       {sorted(got)}"
        )

    # Each post-resume batch matches the reference (continuing from
    # checkpoint_at_step).
    for offset, (ref, got) in enumerate(
        zip(reference[checkpoint_at_step:], seen_after)
    ):
        step = checkpoint_at_step + offset
        assert ref == got, (
            f"Post-resume batch {step} doesn't match reference.\n"
            f"  reference: {sorted(ref)}\n"
            f"  got:       {sorted(got)}"
        )

    # Full coverage: every sample seen exactly once.
    all_seen = sorted(sid for b in seen_before + seen_after for sid in b)
    assert all_seen == list(range(NUM_ROWS))


@pytest.mark.parametrize(
    "initial_world_size,resume_world_size",
    [
        (1, 2),
        (1, 4),
        (1, 12),
        (2, 1),
        (2, 4),
        (4, 2),
        (4, 1),
        (3, 6),
        (6, 3),
        (12, 1),
    ],
)
def test_resumability_elastic_world_size_change(
    lance_table, initial_world_size, resume_world_size
):
    """Resuming on a *different* world_size (elastic resume) produces the same
    global batches as the reference epoch for both the pre- and post-checkpoint
    phases.  No sample is skipped or repeated.

    This is the central elastic-resume guarantee: because state is stored
    per-split (not per-rank), the checkpoint is topology-independent.
    """
    checkpoint_at_step = 4
    reference = _collect_global_batches(lance_table, world_size=initial_world_size)

    seen_before, checkpoint = _advance_and_checkpoint(
        lance_table, initial_world_size, checkpoint_at_step
    )
    seen_after = _resume_and_collect(lance_table, resume_world_size, checkpoint)

    # Pre-checkpoint batches match reference.
    for step, (ref, got) in enumerate(zip(reference, seen_before)):
        assert ref == got, (
            f"Pre-checkpoint batch {step} differs "
            f"(initial_world_size={initial_world_size}).\n"
            f"  reference: {sorted(ref)}\n"
            f"  got:       {sorted(got)}"
        )

    # Post-resume batches match reference (elastic determinism also holds
    # for the resumed phase with the new world_size).
    for offset, (ref, got) in enumerate(
        zip(reference[checkpoint_at_step:], seen_after)
    ):
        step = checkpoint_at_step + offset
        assert ref == got, (
            f"Post-resume batch {step} differs "
            f"(resume_world_size={resume_world_size}).\n"
            f"  reference: {sorted(ref)}\n"
            f"  got:       {sorted(got)}"
        )

    # No overlap and full coverage.
    before_ids = {sid for b in seen_before for sid in b}
    after_ids = {sid for b in seen_after for sid in b}
    assert before_ids.isdisjoint(after_ids), (
        f"Samples seen before and after resume overlap: "
        f"{sorted(before_ids & after_ids)}"
    )
    assert before_ids | after_ids == set(range(NUM_ROWS)), (
        f"Not all samples covered. Missing: "
        f"{sorted(set(range(NUM_ROWS)) - (before_ids | after_ids))}"
    )


def test_resumability_state_dict_is_topology_independent(lance_table):
    """state_dict() captures per-split consumption, so it must be identical
    across all ranks at the same training step.

    If ranks produce different state dicts, resumption with a different
    world_size would give inconsistent results depending on which rank's
    checkpoint was saved.
    """
    checkpoint_at_step = 4
    world_size = 4
    micro = NUM_SPLITS // world_size

    datasets = [
        _make_dataset(lance_table, rank, world_size) for rank in range(world_size)
    ]
    iters = [iter(ds) for ds in datasets]

    for _ in range(checkpoint_at_step):
        for it in iters:
            for _ in range(micro):
                next(it)

    state_dicts = [ds.state_dict() for ds in datasets]
    for rank in range(1, world_size):
        assert state_dicts[rank] == state_dicts[0], (
            f"state_dict from rank {rank} differs from rank 0.\n"
            f"  rank 0:    {state_dicts[0]}\n"
            f"  rank {rank}: {state_dicts[rank]}"
        )


def test_resumability_round_trip_is_deterministic(lance_table):
    """Loading the same checkpoint twice produces identical post-resume sequences."""
    world_size = 2
    checkpoint_at_step = 3

    _, checkpoint = _advance_and_checkpoint(lance_table, world_size, checkpoint_at_step)
    remaining_a = _resume_and_collect(lance_table, world_size, checkpoint)
    remaining_b = _resume_and_collect(lance_table, world_size, checkpoint)

    assert remaining_a == remaining_b, (
        "Same checkpoint produced different sequences on two separate resumes."
    )


def test_resumability_at_epoch_start(lance_table):
    """A checkpoint taken before any samples are consumed resumes as a full epoch."""
    world_size = 2
    reference = _collect_global_batches(lance_table, world_size)

    seen_before, checkpoint = _advance_and_checkpoint(lance_table, world_size, steps=0)
    seen_after = _resume_and_collect(lance_table, world_size, checkpoint)

    assert seen_before == []
    assert seen_after == reference


def test_resumability_at_epoch_end(lance_table):
    """A checkpoint taken after all steps are consumed yields an empty resume."""
    world_size = 2

    _, checkpoint = _advance_and_checkpoint(lance_table, world_size, STEPS_PER_EPOCH)
    remaining = _resume_and_collect(lance_table, world_size, checkpoint)

    assert remaining == [], (
        f"Expected no remaining samples after epoch-end checkpoint, "
        f"but got {len(remaining)} batches: {remaining}"
    )


def test_resumability_state_dict_contains_required_keys(lance_table):
    """state_dict() must contain the keys the design doc mandates.

    From the design doc:
      - shuffle_seed    (must match on resume)
      - num_splits      (must match on resume)
      - epoch           (current epoch)
      - samples_consumed_per_split  (per-split counter, topology-independent)
    """
    _, checkpoint = _advance_and_checkpoint(lance_table, world_size=1, steps=3)
    required_keys = {
        "shuffle_seed",
        "num_splits",
        "epoch",
        "samples_consumed_per_split",
    }
    missing = required_keys - checkpoint.keys()
    assert not missing, (
        f"state_dict() is missing required keys: {missing}\n"
        f"Got keys: {set(checkpoint.keys())}"
    )


def test_resumability_mismatched_num_splits_raises(lance_table):
    """Loading a checkpoint with a different num_splits must raise an error.

    Changing num_splits invalidates the split partitioning and mid-epoch resume
    is no longer meaningful.
    """
    _, checkpoint = _advance_and_checkpoint(lance_table, world_size=1, steps=3)

    different_splits = NUM_SPLITS * 2
    ds = StreamingDataset(
        lance_table,
        num_splits=different_splits,
        shuffle_seed=SHUFFLE_SEED,
        rank=0,
        world_size=1,
    )
    with pytest.raises((ValueError, RuntimeError)):
        ds.load_state_dict(checkpoint)


def test_resumability_mismatched_shuffle_seed_raises(lance_table):
    """Loading a checkpoint with a different shuffle_seed must raise an error.

    A different seed produces a different sample ordering, so the per-split
    consumption counts in the checkpoint no longer refer to the same samples.
    """
    _, checkpoint = _advance_and_checkpoint(lance_table, world_size=1, steps=3)

    ds = StreamingDataset(
        lance_table,
        num_splits=NUM_SPLITS,
        shuffle_seed=SHUFFLE_SEED + 1,
        rank=0,
        world_size=1,
    )
    with pytest.raises((ValueError, RuntimeError)):
        ds.load_state_dict(checkpoint)


# ---------------------------------------------------------------------------
# Large global batch size tests (global_batch_size is a multiple of num_splits)
# ---------------------------------------------------------------------------
# These tests use LARGE_GLOBAL_BATCH_SIZE = 36 = 3 * NUM_SPLITS, so each split
# contributes 3 samples per global step instead of 1.  The elastic-determinism
# and resumability properties must hold regardless of this multiplier.


@pytest.mark.parametrize("world_size", COMPATIBLE_WORLD_SIZES)
def test_large_batch_elastic_det_full_coverage(lance_table_large, world_size):
    """Every sample appears exactly once per epoch regardless of global_batch_size."""
    batches = _collect_global_batches(
        lance_table_large,
        world_size,
        global_batch_size=LARGE_GLOBAL_BATCH_SIZE,
    )
    all_seen = sorted(sid for b in batches for sid in b)
    assert all_seen == list(range(LARGE_NUM_ROWS))


@pytest.mark.parametrize("world_size", COMPATIBLE_WORLD_SIZES)
def test_large_batch_elastic_det_correct_step_count(lance_table_large, world_size):
    """Steps per epoch = LARGE_NUM_ROWS // LARGE_GLOBAL_BATCH_SIZE."""
    expected_steps = LARGE_NUM_ROWS // LARGE_GLOBAL_BATCH_SIZE
    batches = _collect_global_batches(
        lance_table_large,
        world_size,
        global_batch_size=LARGE_GLOBAL_BATCH_SIZE,
    )
    assert len(batches) == expected_steps, (
        f"world_size={world_size}: expected {expected_steps} steps, got {len(batches)}"
    )


@pytest.mark.parametrize("world_size", COMPATIBLE_WORLD_SIZES)
def test_large_batch_elastic_det_correct_batch_size(lance_table_large, world_size):
    """Each global batch contains exactly LARGE_GLOBAL_BATCH_SIZE distinct samples."""
    batches = _collect_global_batches(
        lance_table_large,
        world_size,
        global_batch_size=LARGE_GLOBAL_BATCH_SIZE,
    )
    for step, batch in enumerate(batches):
        assert len(batch) == LARGE_GLOBAL_BATCH_SIZE, (
            f"world_size={world_size} step {step}: "
            f"expected {LARGE_GLOBAL_BATCH_SIZE} samples, got {len(batch)}"
        )


def test_large_batch_elastic_det_same_across_topologies(lance_table_large):
    """Global batch k is identical for every compatible world_size when
    global_batch_size (36) is a 3× multiple of num_splits (12)."""
    reference = _collect_global_batches(
        lance_table_large,
        world_size=1,
        global_batch_size=LARGE_GLOBAL_BATCH_SIZE,
    )
    for ws in COMPATIBLE_WORLD_SIZES[1:]:
        batches = _collect_global_batches(
            lance_table_large,
            world_size=ws,
            global_batch_size=LARGE_GLOBAL_BATCH_SIZE,
        )
        assert len(batches) == len(reference)
        for step, (ref, got) in enumerate(zip(reference, batches)):
            assert ref == got, (
                f"Global batch at step {step} differs between world_size=1 and "
                f"world_size={ws} (global_batch_size={LARGE_GLOBAL_BATCH_SIZE}).\n"
                f"  world_size=1  → {sorted(ref)}\n"
                f"  world_size={ws} → {sorted(got)}"
            )


@pytest.mark.parametrize(
    "initial_world_size,resume_world_size",
    [(1, 4), (2, 4), (4, 2), (4, 1), (3, 6), (6, 3)],
)
def test_large_batch_resumability_elastic_world_size_change(
    lance_table_large, initial_world_size, resume_world_size
):
    """Elastic resume with global_batch_size=36 (3× num_splits): combined
    samples before and after the checkpoint match the reference epoch exactly."""
    checkpoint_at_step = 4
    reference = _collect_global_batches(
        lance_table_large,
        world_size=initial_world_size,
        global_batch_size=LARGE_GLOBAL_BATCH_SIZE,
    )

    seen_before, checkpoint = _advance_and_checkpoint(
        lance_table_large,
        initial_world_size,
        checkpoint_at_step,
        global_batch_size=LARGE_GLOBAL_BATCH_SIZE,
    )
    seen_after = _resume_and_collect(
        lance_table_large,
        resume_world_size,
        checkpoint,
        global_batch_size=LARGE_GLOBAL_BATCH_SIZE,
    )

    for step, (ref, got) in enumerate(zip(reference, seen_before)):
        assert ref == got, (
            f"Pre-checkpoint batch {step} differs "
            f"(initial_world_size={initial_world_size}, "
            f"global_batch_size={LARGE_GLOBAL_BATCH_SIZE}).\n"
            f"  reference: {sorted(ref)}\n"
            f"  got:       {sorted(got)}"
        )

    for offset, (ref, got) in enumerate(
        zip(reference[checkpoint_at_step:], seen_after)
    ):
        step = checkpoint_at_step + offset
        assert ref == got, (
            f"Post-resume batch {step} differs "
            f"(resume_world_size={resume_world_size}, "
            f"global_batch_size={LARGE_GLOBAL_BATCH_SIZE}).\n"
            f"  reference: {sorted(ref)}\n"
            f"  got:       {sorted(got)}"
        )

    before_ids = {sid for b in seen_before for sid in b}
    after_ids = {sid for b in seen_after for sid in b}
    assert before_ids.isdisjoint(after_ids), (
        f"Samples overlap across checkpoint: {sorted(before_ids & after_ids)}"
    )
    assert before_ids | after_ids == set(range(LARGE_NUM_ROWS))


# ---------------------------------------------------------------------------
# Multi-worker tests (num_workers > 1)
# ---------------------------------------------------------------------------
# PyTorch DataLoader collects a full batch from each worker before moving to the
# next (batch-level, not item-level, round-robin).  For the dataset's batches to
# be consistent across num_workers, the dataset must assign each worker a
# contiguous block of splits.  Worker k owns splits in the range:
#   [k * (num_splits // num_workers), (k+1) * (num_splits // num_workers))
# (within a single rank's split allocation).
#
# Tests in this section use worker_info_override to simulate multiple workers
# without spawning real DataLoader worker processes.


def _collect_global_batches_multi_worker(
    table,
    world_size: int,
    num_workers: int,
    *,
    num_splits: int = NUM_SPLITS,
    global_batch_size: int = GLOBAL_BATCH_SIZE,
    shuffle_seed: int = SHUFFLE_SEED,
    epoch: int = 0,
) -> list[frozenset[int]]:
    """Drain a full epoch using multiple simulated workers per rank.

    Creates one StreamingDataset per (rank, worker_id) pair, each configured
    via worker_info_override.  Samples from all (rank, worker_id) pairs at the
    same global step are combined into one frozenset, mirroring the contiguous
    batch-level collection that PyTorch DataLoader performs.

    Constraint: num_splits % (world_size * num_workers) == 0
    """
    assert num_splits % (world_size * num_workers) == 0, (
        f"num_splits={num_splits} must be divisible by "
        f"world_size * num_workers = {world_size * num_workers}"
    )
    assert global_batch_size % (world_size * num_workers) == 0
    samples_per_worker_per_step = global_batch_size // (world_size * num_workers)

    # Collect all samples from every (rank, worker_id) pair.
    worker_samples: dict[tuple[int, int], list[int]] = {}
    for rank in range(world_size):
        for worker_id in range(num_workers):
            ds = _make_dataset(
                table,
                rank,
                world_size,
                num_splits=num_splits,
                shuffle_seed=shuffle_seed,
                epoch=epoch,
                worker_info_override=FakeWorkerInfo(
                    id=worker_id, num_workers=num_workers
                ),
            )
            worker_samples[(rank, worker_id)] = [item["id"] for item in ds]

    lengths = {len(s) for s in worker_samples.values()}
    assert len(lengths) == 1, (
        f"Workers produced unequal sample counts: "
        f"{dict(zip(worker_samples, (len(s) for s in worker_samples.values())))}"
    )
    n_per_worker = lengths.pop()
    assert n_per_worker % samples_per_worker_per_step == 0
    n_steps = n_per_worker // samples_per_worker_per_step

    global_batches: list[frozenset[int]] = []
    for step in range(n_steps):
        batch: set[int] = set()
        for samples in worker_samples.values():
            start = step * samples_per_worker_per_step
            batch.update(samples[start : start + samples_per_worker_per_step])
        global_batches.append(frozenset(batch))

    return global_batches


def _advance_and_checkpoint_multi_worker(
    table,
    world_size: int,
    num_workers: int,
    steps: int,
    *,
    num_splits: int = NUM_SPLITS,
    global_batch_size: int = GLOBAL_BATCH_SIZE,
    shuffle_seed: int = SHUFFLE_SEED,
    epoch: int = 0,
) -> tuple[list[frozenset[int]], dict]:
    """Run `steps` global steps with multiple workers per rank, then checkpoint."""
    assert num_splits % (world_size * num_workers) == 0
    assert global_batch_size % (world_size * num_workers) == 0
    samples_per_worker_per_step = global_batch_size // (world_size * num_workers)

    datasets: dict[tuple[int, int], StreamingDataset] = {}
    iters: dict[tuple[int, int], object] = {}
    for rank in range(world_size):
        for worker_id in range(num_workers):
            ds = _make_dataset(
                table,
                rank,
                world_size,
                num_splits=num_splits,
                shuffle_seed=shuffle_seed,
                epoch=epoch,
                worker_info_override=FakeWorkerInfo(
                    id=worker_id, num_workers=num_workers
                ),
            )
            datasets[(rank, worker_id)] = ds
            iters[(rank, worker_id)] = iter(ds)

    seen: list[frozenset[int]] = []
    for _ in range(steps):
        batch: set[int] = set()
        for it in iters.values():
            for _ in range(samples_per_worker_per_step):
                batch.add(next(it)["id"])
        seen.append(frozenset(batch))

    # State is per-split → same across all rank/worker combinations.
    checkpoint = datasets[(0, 0)].state_dict()
    return seen, checkpoint


# ── Multi-worker correctness ──────────────────────────────────────────────────


@pytest.mark.parametrize("world_size,num_workers", MULTI_WORKER_TOPOLOGIES)
def test_multi_worker_full_coverage(lance_table, world_size, num_workers):
    """Every sample is seen exactly once across all workers and all steps."""
    batches = _collect_global_batches_multi_worker(lance_table, world_size, num_workers)
    all_seen = sorted(sid for b in batches for sid in b)
    assert all_seen == list(range(NUM_ROWS)), (
        f"world_size={world_size} num_workers={num_workers}: "
        f"expected IDs 0..{NUM_ROWS - 1}"
    )


@pytest.mark.parametrize("world_size,num_workers", MULTI_WORKER_TOPOLOGIES)
def test_multi_worker_correct_step_count(lance_table, world_size, num_workers):
    """Step count equals NUM_ROWS // GLOBAL_BATCH_SIZE regardless of topology."""
    batches = _collect_global_batches_multi_worker(lance_table, world_size, num_workers)
    assert len(batches) == STEPS_PER_EPOCH, (
        f"world_size={world_size} num_workers={num_workers}: "
        f"expected {STEPS_PER_EPOCH} steps, got {len(batches)}"
    )


@pytest.mark.parametrize("world_size,num_workers", MULTI_WORKER_TOPOLOGIES)
def test_multi_worker_no_cross_worker_overlap(lance_table, world_size, num_workers):
    """Workers within the same rank have disjoint sample sets."""
    for rank in range(world_size):
        per_worker: list[set[int]] = []
        for worker_id in range(num_workers):
            ds = _make_dataset(
                lance_table,
                rank,
                world_size,
                worker_info_override=FakeWorkerInfo(
                    id=worker_id, num_workers=num_workers
                ),
            )
            per_worker.append({item["id"] for item in ds})

        for i in range(num_workers):
            for j in range(i + 1, num_workers):
                overlap = per_worker[i] & per_worker[j]
                assert not overlap, (
                    f"rank={rank} workers {i} and {j} share samples: {sorted(overlap)}"
                )


# ── Elastic determinism with num_workers ─────────────────────────────────────


@pytest.mark.parametrize("world_size,num_workers", MULTI_WORKER_TOPOLOGIES)
def test_multi_worker_same_global_batches_as_single_worker(
    lance_table, world_size, num_workers
):
    """Global batches produced with num_workers > 1 match those from num_workers=1.

    PyTorch DataLoader collects a complete batch from each worker before moving
    to the next.  With contiguous split assignment, worker k's batch at step s
    contains the same samples as the corresponding segment of the single-worker
    batch at step s.  The union across all workers at step s is therefore the
    same global batch.
    """
    reference = _collect_global_batches(lance_table, world_size)
    batches = _collect_global_batches_multi_worker(lance_table, world_size, num_workers)

    assert len(batches) == len(reference)
    for step, (ref, got) in enumerate(zip(reference, batches)):
        assert ref == got, (
            f"Global batch at step {step} differs between num_workers=1 and "
            f"num_workers={num_workers} (world_size={world_size}).\n"
            f"  num_workers=1 → {sorted(ref)}\n"
            f"  num_workers={num_workers} → {sorted(got)}"
        )


def test_multi_worker_elastic_det_across_worker_counts(lance_table):
    """Global batches match for every compatible world_size / num_workers pair."""
    reference = _collect_global_batches(lance_table, world_size=1)
    for world_size, num_workers in MULTI_WORKER_TOPOLOGIES:
        batches = _collect_global_batches_multi_worker(
            lance_table, world_size, num_workers
        )
        assert batches == reference, (
            f"Mismatch for world_size={world_size}, num_workers={num_workers}"
        )


# ── Resumability with num_workers ─────────────────────────────────────────────


def test_multi_worker_resumability_same_topology(lance_table):
    """Checkpoint with num_workers=2, resume with num_workers=2: exact continuation."""
    world_size = 1
    num_workers = 2
    checkpoint_at_step = 4

    reference = _collect_global_batches_multi_worker(
        lance_table, world_size, num_workers
    )
    seen_before, checkpoint = _advance_and_checkpoint_multi_worker(
        lance_table, world_size, num_workers, checkpoint_at_step
    )

    # Resume: create new datasets, load checkpoint, collect remaining.
    remaining: list[frozenset[int]] = []
    samples_per_worker_per_step = GLOBAL_BATCH_SIZE // (world_size * num_workers)
    datasets = {}
    iters = {}
    for rank in range(world_size):
        for worker_id in range(num_workers):
            ds = _make_dataset(
                lance_table,
                rank,
                world_size,
                worker_info_override=FakeWorkerInfo(
                    id=worker_id, num_workers=num_workers
                ),
            )
            ds.load_state_dict(checkpoint)
            datasets[(rank, worker_id)] = ds
            iters[(rank, worker_id)] = iter(ds)

    _STOP3 = object()
    while True:
        batch: set[int] = set()
        exhausted = 0
        for it in iters.values():
            for _ in range(samples_per_worker_per_step):
                val = next(it, _STOP3)
                if val is _STOP3:
                    exhausted += 1
                    break
                batch.add(val["id"])
        if exhausted == len(iters):
            break
        assert exhausted == 0
        remaining.append(frozenset(batch))

    for step, (ref, got) in enumerate(zip(reference, seen_before)):
        assert ref == got, f"Pre-checkpoint step {step} doesn't match reference"
    for offset, (ref, got) in enumerate(zip(reference[checkpoint_at_step:], remaining)):
        step = checkpoint_at_step + offset
        assert ref == got, f"Post-resume step {step} doesn't match reference"

    all_seen = sorted(sid for b in seen_before + remaining for sid in b)
    assert all_seen == list(range(NUM_ROWS))


def test_multi_worker_resumability_worker_count_change(lance_table):
    """Checkpoint with num_workers=1, resume with num_workers=2.

    Because state is per-split, it is independent of num_workers just as it is
    independent of world_size.
    """
    world_size = 1
    checkpoint_at_step = 4
    reference = _collect_global_batches(lance_table, world_size=1)

    seen_before, checkpoint = _advance_and_checkpoint(
        lance_table, world_size=1, steps=checkpoint_at_step
    )

    # Resume with num_workers=2 using the checkpoint.
    num_workers = 2
    samples_per_worker_per_step = GLOBAL_BATCH_SIZE // (world_size * num_workers)
    iters = {}
    for worker_id in range(num_workers):
        ds = _make_dataset(
            lance_table,
            rank=0,
            world_size=world_size,
            worker_info_override=FakeWorkerInfo(id=worker_id, num_workers=num_workers),
        )
        ds.load_state_dict(checkpoint)
        iters[worker_id] = iter(ds)

    _STOP4 = object()
    remaining: list[frozenset[int]] = []
    while True:
        batch: set[int] = set()
        exhausted = 0
        for it in iters.values():
            for _ in range(samples_per_worker_per_step):
                val = next(it, _STOP4)
                if val is _STOP4:
                    exhausted += 1
                    break
                batch.add(val["id"])
        if exhausted == len(iters):
            break
        assert exhausted == 0
        remaining.append(frozenset(batch))

    for offset, (ref, got) in enumerate(zip(reference[checkpoint_at_step:], remaining)):
        step = checkpoint_at_step + offset
        assert ref == got, (
            f"Post-resume step {step} doesn't match reference when switching "
            f"from num_workers=1 to num_workers=2.\n"
            f"  reference: {sorted(ref)}\n"
            f"  got:       {sorted(got)}"
        )

    before_ids = {sid for b in seen_before for sid in b}
    after_ids = {sid for b in remaining for sid in b}
    assert before_ids.isdisjoint(after_ids)
    assert before_ids | after_ids == set(range(NUM_ROWS))


# ── worker_info_override warning behaviour ────────────────────────────────────


def test_worker_info_override_logs_warning_when_torch_worker_active(
    lance_table, caplog
):
    """When worker_info_override is set but get_worker_info() also returns a
    value (i.e. the dataset is being iterated inside a real DataLoader worker),
    the dataset must log a warning that the override takes precedence and that
    this may produce incorrect or duplicated data.

    The warning is important because silently ignoring the real worker assignment
    would cause all workers to read the same data.
    """
    override = FakeWorkerInfo(id=0, num_workers=2)
    ds = _make_dataset(lance_table, rank=0, world_size=1, worker_info_override=override)

    fake_torch_info = FakeWorkerInfo(id=0, num_workers=4)
    with patch("lancedb.streaming.get_worker_info", return_value=fake_torch_info):
        with caplog.at_level(logging.WARNING, logger="lancedb.streaming"):
            list(ds)

    warning_messages = [
        r.message for r in caplog.records if r.levelno >= logging.WARNING
    ]
    assert warning_messages, (
        "Expected at least one WARNING when worker_info_override is active inside "
        "a real DataLoader worker, but no warnings were logged."
    )
    combined = " ".join(warning_messages).lower()
    assert "override" in combined or "worker_info_override" in combined, (
        f"Warning message did not mention the override. Messages: {warning_messages}"
    )


def test_worker_info_override_no_warning_in_main_process(lance_table, caplog):
    """When get_worker_info() returns None (main process / num_workers=0),
    using worker_info_override is the normal testing path and must not warn.
    """
    override = FakeWorkerInfo(id=0, num_workers=2)
    ds = _make_dataset(lance_table, rank=0, world_size=1, worker_info_override=override)

    with patch("lancedb.streaming.get_worker_info", return_value=None):
        with caplog.at_level(logging.WARNING, logger="lancedb.streaming"):
            list(ds)

    warning_messages = [
        r.message for r in caplog.records if r.levelno >= logging.WARNING
    ]
    assert not warning_messages, (
        f"Unexpected warnings when override used in main process: {warning_messages}"
    )


# ---------------------------------------------------------------------------
# Prefetch queue depth tests
# ---------------------------------------------------------------------------


def test_concurrent_iteration_raises(lance_table):
    """Starting a second iterator while one is already active must raise."""
    ds = StreamingDataset(lance_table, num_splits=NUM_SPLITS, shuffle_seed=SHUFFLE_SEED)
    it1 = iter(ds)
    next(it1)  # advance it1 so the pipeline is live

    it2 = iter(ds)
    with pytest.raises(RuntimeError, match="concurrent"):
        next(it2)


def test_raw_queue_depth_zero_when_not_iterating(lance_table):
    """raw_queue_depth is 0 before iteration starts and after it ends."""
    ds = StreamingDataset(lance_table, num_splits=NUM_SPLITS, shuffle_seed=SHUFFLE_SEED)
    assert ds.raw_queue_depth == 0

    list(ds)  # drain the whole epoch

    assert ds.raw_queue_depth == 0


def test_prefetch_queue_depth_zero_when_not_iterating(lance_table):
    """prefetch_queue_depth is 0 before iteration starts and after it ends."""
    ds = StreamingDataset(lance_table, num_splits=NUM_SPLITS, shuffle_seed=SHUFFLE_SEED)
    assert ds.prefetch_queue_depth == 0

    list(ds)  # drain the whole epoch

    assert ds.prefetch_queue_depth == 0


def test_prefetch_queue_depth_positive_during_iteration(lance_table):
    """prefetch_queue_depth is > 0 while rows are being yielded."""
    ds = StreamingDataset(lance_table, num_splits=NUM_SPLITS, shuffle_seed=SHUFFLE_SEED)
    it = iter(ds)
    next(it)  # advance past the first yield; pipeline is now primed
    # The other splits still have their initial futures in flight.
    assert ds.prefetch_queue_depth > 0

    list(it)  # exhaust the remaining rows

    assert ds.prefetch_queue_depth == 0


# ---------------------------------------------------------------------------
# Transform tests
# ---------------------------------------------------------------------------


def test_fetch_and_transform_time_zero_before_iteration(lance_table):
    """fetch_time and transform_time start at 0."""
    ds = StreamingDataset(lance_table, num_splits=NUM_SPLITS, shuffle_seed=SHUFFLE_SEED)
    assert ds.fetch_time == 0.0
    assert ds.transform_time == 0.0


def test_fetch_and_transform_time_positive_after_iteration(lance_table):
    """Both timers are positive after a full epoch."""
    ds = StreamingDataset(lance_table, num_splits=NUM_SPLITS, shuffle_seed=SHUFFLE_SEED)
    list(ds)
    assert ds.fetch_time > 0.0
    assert ds.transform_time > 0.0


def test_fetch_time_excludes_transform(lance_table):
    """fetch_time does not include transform time: fetch + transform < total wall time,
    and neither counter bleeds into the other."""
    import pyarrow as pa
    import time

    def slow_transform(batch: pa.RecordBatch) -> list:
        time.sleep(0.01)  # 10 ms of artificial transform work
        return batch.to_pydict()["id"]

    ds = StreamingDataset(
        lance_table,
        num_splits=NUM_SPLITS,
        shuffle_seed=SHUFFLE_SEED,
        transform=slow_transform,
    )
    list(ds)

    # The slow transform should dominate transform_time.
    assert ds.transform_time > ds.fetch_time


def test_bytes_loaded_increases_after_iteration(lance_table):
    """bytes_loaded is 0 before iteration and positive after."""
    ds = StreamingDataset(lance_table, num_splits=NUM_SPLITS, shuffle_seed=SHUFFLE_SEED)
    assert ds.bytes_loaded == 0

    list(ds)

    assert ds.bytes_loaded > 0


def test_bytes_loaded_measured_before_transform(lance_table):
    """bytes_loaded measures raw Arrow size even when transform discards everything."""
    import pyarrow as pa

    # This transform throws away every value.  If bytes_loaded were measured
    # after the transform, it would see no Arrow data and stay at 0.
    def discard_everything(batch: pa.RecordBatch) -> list:
        return [None] * batch.num_rows

    ds = StreamingDataset(
        lance_table,
        num_splits=NUM_SPLITS,
        shuffle_seed=SHUFFLE_SEED,
        transform=discard_everything,
    )
    list(ds)

    assert ds.bytes_loaded > 0


def test_transform_is_applied(lance_table):
    """A custom transform passed to StreamingDataset is forwarded to the
    underlying Permutation and applied to every yielded item."""
    import pyarrow as pa

    def id_only(batch: pa.RecordBatch) -> list[int]:
        return batch.column("id").to_pylist()

    ds = StreamingDataset(
        lance_table,
        num_splits=NUM_SPLITS,
        shuffle_seed=SHUFFLE_SEED,
        transform=id_only,
    )
    items = list(ds)

    assert len(items) == NUM_ROWS
    assert all(isinstance(item, int) for item in items), (
        f"Expected ints from transform, got {type(items[0])}"
    )
    assert sorted(items) == list(range(NUM_ROWS))


def test_transform_none_yields_dicts(lance_table):
    """With no transform (the default), items are plain Python dicts."""
    ds = StreamingDataset(
        lance_table,
        num_splits=NUM_SPLITS,
        shuffle_seed=SHUFFLE_SEED,
    )
    items = list(ds)

    assert len(items) == NUM_ROWS
    assert all(isinstance(item, dict) for item in items)
    assert all("id" in item for item in items)


@pytest.mark.parametrize(
    ("configured", "detected", "expected"),
    [(2, 8, 2), (None, 3, 3), (None, None, 1)],
)
def test_transform_parallelism_configures_executor(
    lance_table, monkeypatch, configured, detected, expected
):
    """Explicit transform parallelism overrides the detected CPU count."""
    real_executor = streaming.ThreadPoolExecutor
    monkeypatch.setattr(streaming.os, "cpu_count", lambda: detected)

    with patch.object(streaming, "ThreadPoolExecutor", wraps=real_executor) as executor:
        list(
            StreamingDataset(
                lance_table,
                num_splits=NUM_SPLITS,
                shuffle_seed=SHUFFLE_SEED,
                transform_parallelism=configured,
            )
        )

    assert executor.call_args_list[-1].kwargs["max_workers"] == expected


@pytest.mark.parametrize("transform_parallelism", [0, -1])
def test_transform_parallelism_must_be_positive(lance_table, transform_parallelism):
    with pytest.raises(
        ValueError, match="transform_parallelism must be greater than 0"
    ):
        StreamingDataset(
            lance_table,
            num_splits=NUM_SPLITS,
            transform_parallelism=transform_parallelism,
        )


def test_filter_limits_rows(tmp_path):
    """A filter expression is applied to the permutation so only matching rows
    are yielded.  IDs 0..59 pass ``id < 60``; the other 60 are excluded."""
    db = lancedb.connect(tmp_path)
    table = db.create_table("data", pa.table({"id": list(range(NUM_ROWS))}))

    ds = StreamingDataset(
        table,
        num_splits=NUM_SPLITS,
        shuffle_seed=SHUFFLE_SEED,
        filter="id < 60",
    )
    items = list(ds)

    ids = [item["id"] for item in items]
    assert sorted(ids) == list(range(60)), f"Expected ids 0-59, got {sorted(ids)}"


def test_filter_too_few_rows_raises(tmp_path):
    """A filter that leaves fewer rows than num_splits raises ValueError at
    construction time because each split must have at least one row."""
    db = lancedb.connect(tmp_path)
    table = db.create_table("data", pa.table({"id": list(range(NUM_ROWS))}))

    with pytest.raises(ValueError, match="at least 1 row per split"):
        StreamingDataset(
            table,
            num_splits=NUM_SPLITS,
            shuffle_seed=SHUFFLE_SEED,
            filter="id < 0",
        )


def test_columns_limits_output_columns(tmp_path):
    """Only the requested columns are present in each yielded row."""
    db = lancedb.connect(tmp_path)
    table = db.create_table(
        "data",
        pa.table({"id": list(range(NUM_ROWS)), "val": list(range(NUM_ROWS))}),
    )

    ds = StreamingDataset(
        table,
        num_splits=NUM_SPLITS,
        shuffle_seed=SHUFFLE_SEED,
        columns=["id"],
    )
    items = list(ds)

    assert len(items) == NUM_ROWS
    assert all(list(item.keys()) == ["id"] for item in items), (
        "Expected only 'id' column in each row"
    )
    assert sorted(item["id"] for item in items) == list(range(NUM_ROWS))


def test_columns_invalid_column_raises(tmp_path):
    """Requesting a column that does not exist raises an error at iteration time."""
    db = lancedb.connect(tmp_path)
    table = db.create_table("data", pa.table({"id": list(range(NUM_ROWS))}))

    ds = StreamingDataset(
        table,
        num_splits=NUM_SPLITS,
        shuffle_seed=SHUFFLE_SEED,
        columns=["nonexistent"],
    )
    with pytest.raises(ValueError):
        list(ds)


def test_shuffle_clump_size_yields_all_rows(lance_table):
    """shuffle_clump_size still produces a complete epoch with no duplicates or
    omissions — clumping affects I/O locality, not correctness."""
    ds = StreamingDataset(
        lance_table,
        num_splits=NUM_SPLITS,
        shuffle_seed=SHUFFLE_SEED,
        shuffle_clump_size=4,
    )
    items = list(ds)

    assert sorted(item["id"] for item in items) == list(range(NUM_ROWS)), (
        "Expected every row exactly once with shuffle_clump_size set"
    )


def test_num_splits_defaults_to_world_size(lance_table):
    """Omitting num_splits gives world_size splits (one per rank)."""
    ds = StreamingDataset(
        lance_table,
        shuffle_seed=SHUFFLE_SEED,
    )
    assert ds._num_splits == 1  # world_size defaults to 1

    ds_ws2 = StreamingDataset(
        lance_table,
        world_size=2,
        shuffle_seed=SHUFFLE_SEED,
    )
    assert ds_ws2._num_splits == 2


def test_shuffle_false_sequential_and_deterministic(lance_table):
    """shuffle=False produces identical ordering across two fresh instances."""
    ds1 = StreamingDataset(lance_table, num_splits=NUM_SPLITS, shuffle=False)
    ds2 = StreamingDataset(lance_table, num_splits=NUM_SPLITS, shuffle=False)
    first = [item["id"] for item in ds1]
    second = [item["id"] for item in ds2]
    assert first == second, "shuffle=False must be deterministic"
    assert sorted(first) == list(range(NUM_ROWS)), "All rows must be present"


def test_shuffle_false_vs_true_differ(lance_table):
    """shuffle=True and shuffle=False produce different orderings."""
    ds_shuf = StreamingDataset(
        lance_table,
        num_splits=NUM_SPLITS,
        shuffle=True,
        shuffle_seed=SHUFFLE_SEED,
    )
    ds_seq = StreamingDataset(
        lance_table,
        num_splits=NUM_SPLITS,
        shuffle=False,
    )
    shuffled = [item["id"] for item in ds_shuf]
    sequential = [item["id"] for item in ds_seq]
    assert shuffled != sequential, "Shuffled and sequential orderings should differ"


def test_shuffle_seed_none_generates_stable_seed(lance_table):
    """shuffle_seed=None resolves to a concrete integer at construction time.

    A second dataset built with the same resolved seed must produce the same ordering.
    """
    ds = StreamingDataset(
        lance_table,
        num_splits=NUM_SPLITS,
        shuffle_seed=None,
    )
    assert isinstance(ds._shuffle_seed, int), (
        "shuffle_seed=None must resolve to an integer"
    )
    first = [item["id"] for item in ds]

    ds2 = StreamingDataset(
        lance_table,
        num_splits=NUM_SPLITS,
        shuffle_seed=ds._shuffle_seed,
    )
    second = [item["id"] for item in ds2]
    assert first == second, "Same resolved seed must produce the same ordering"


# ---------------------------------------------------------------------------
# Doc examples — each test mirrors the code snippet in index.mdx so that
# broken doc examples are caught before they ship.
# ---------------------------------------------------------------------------


def test_doc_example_basic(tmp_path):
    """doc: Basic Data loading — StreamingDataset with default params."""
    db = lancedb.connect(tmp_path)
    table = db.create_table(
        "some_table",
        pa.table({"feature": [float(i) for i in range(24)], "label": ["cat"] * 24}),
    )

    ds = StreamingDataset(table, shuffle_seed=42)
    samples = list(ds)

    assert len(samples) == 24
    assert all(isinstance(s, dict) for s in samples)
    assert all("feature" in s and "label" in s for s in samples)


def test_doc_example_prefetch_params(tmp_path):
    """doc: Prefetching — read_batch_size and prefetch_batches still cover all rows."""
    db = lancedb.connect(tmp_path)
    table = db.create_table("t", pa.table({"id": list(range(NUM_ROWS))}))

    ds = StreamingDataset(
        table,
        num_splits=NUM_SPLITS,
        shuffle_seed=SHUFFLE_SEED,
        read_batch_size=8,
        prefetch_batches=2,
    )
    assert sorted(s["id"] for s in ds) == list(range(NUM_ROWS))


def test_doc_example_transform(tmp_path):
    """doc: Transformation — normalize scales values into [0, 1]."""
    db = lancedb.connect(tmp_path)
    table = db.create_table("t", pa.table({"value": list(range(NUM_ROWS))}))

    def normalize(batch: pa.RecordBatch) -> list[dict]:
        rows = batch.to_pylist()
        for row in rows:
            row["value"] = row["value"] / 255.0
        return rows

    ds = StreamingDataset(
        table, num_splits=NUM_SPLITS, shuffle_seed=SHUFFLE_SEED, transform=normalize
    )
    samples = list(ds)

    assert len(samples) == NUM_ROWS
    assert all(0.0 <= s["value"] <= 1.0 for s in samples)


def test_doc_example_observability(lance_table):
    """doc: Observability — counters are zero before and positive after iteration."""
    ds = StreamingDataset(lance_table, num_splits=NUM_SPLITS, shuffle_seed=SHUFFLE_SEED)

    assert ds.unscanned_rows == 0
    assert ds.raw_queue_depth == 0
    assert ds.prefetch_queue_depth == 0
    assert ds.consumed_rows == 0
    assert ds.bytes_loaded == 0
    assert ds.fetch_time == 0.0
    assert ds.transform_time == 0.0

    list(ds)

    assert ds.bytes_loaded > 0
    assert ds.fetch_time >= 0.0
    assert ds.transform_time >= 0.0


def test_doc_example_columns_and_filter(tmp_path):
    """doc: Filtering data — columns + filter reduce both dimensions independently."""
    db = lancedb.connect(tmp_path)
    table = db.create_table(
        "t",
        pa.table(
            {
                "id": list(range(NUM_ROWS)),
                "value": list(range(NUM_ROWS)),
                "category": ["train"] * 60 + ["val"] * 60,
            }
        ),
    )

    ds = StreamingDataset(
        table,
        num_splits=NUM_SPLITS,
        shuffle_seed=SHUFFLE_SEED,
        columns=["id"],
        filter="category = 'train'",
    )
    samples = list(ds)

    assert all(list(s.keys()) == ["id"] for s in samples), "Only 'id' column expected"
    assert len(samples) == 60, "Only train rows expected"
    assert all(s["id"] < 60 for s in samples)


def test_doc_example_epoch_shuffle(lance_table):
    """doc: Shuffling rows — different epochs produce different orderings."""
    ids_e0 = [
        s["id"]
        for s in StreamingDataset(
            lance_table, num_splits=NUM_SPLITS, shuffle_seed=SHUFFLE_SEED, epoch=0
        )
    ]
    ids_e1 = [
        s["id"]
        for s in StreamingDataset(
            lance_table, num_splits=NUM_SPLITS, shuffle_seed=SHUFFLE_SEED, epoch=1
        )
    ]

    assert sorted(ids_e0) == list(range(NUM_ROWS))
    assert sorted(ids_e1) == list(range(NUM_ROWS))
    assert ids_e0 != ids_e1, "Different epochs must produce different orderings"


def test_doc_example_shuffle_false_eval(lance_table):
    """doc: Shuffling rows — shuffle=False gives deterministic sequential order."""
    ids_a = [
        s["id"]
        for s in StreamingDataset(lance_table, num_splits=NUM_SPLITS, shuffle=False)
    ]
    ids_b = [
        s["id"]
        for s in StreamingDataset(lance_table, num_splits=NUM_SPLITS, shuffle=False)
    ]

    assert ids_a == ids_b, "shuffle=False must produce identical orderings"
    assert sorted(ids_a) == list(range(NUM_ROWS))


def test_doc_example_shuffle_clump_size(lance_table):
    """doc: Shuffling rows (Note) — shuffle_clump_size still covers every row."""
    ds = StreamingDataset(
        lance_table,
        num_splits=NUM_SPLITS,
        shuffle_seed=SHUFFLE_SEED,
        shuffle_clump_size=16,
    )
    assert sorted(s["id"] for s in ds) == list(range(NUM_ROWS))


def test_doc_example_elastic_ddp(lance_table):
    """doc: Data splits and elasticity — rank/world_size/num_splits covers all rows."""
    # Use a highly composite num_splits that works across many world sizes
    ELASTIC_SPLITS = 12  # works with world_size 1, 2, 3, 4, 6, 12

    all_ids: set[int] = set()
    for rank in range(4):
        ds = StreamingDataset(
            lance_table,
            num_splits=ELASTIC_SPLITS,
            shuffle_seed=SHUFFLE_SEED,
            rank=rank,
            world_size=4,
        )
        all_ids.update(s["id"] for s in ds)

    assert all_ids == set(range(NUM_ROWS)), "All ranks together must cover every row"


def test_doc_example_checkpoint(lance_table):
    """doc: state_dict/load_state_dict checkpointing resumes without gaps or repeats."""
    STEPS_BEFORE_CHECKPOINT = 4  # consume 4 full cycles then checkpoint

    ds = StreamingDataset(lance_table, num_splits=NUM_SPLITS, shuffle_seed=SHUFFLE_SEED)
    it = iter(ds)
    consumed = [next(it)["id"] for _ in range(STEPS_BEFORE_CHECKPOINT * NUM_SPLITS)]
    checkpoint = ds.state_dict()
    remaining_original = [s["id"] for s in it]  # drain the rest

    # Resume from checkpoint on a fresh dataset
    ds_resumed = StreamingDataset(
        lance_table, num_splits=NUM_SPLITS, shuffle_seed=SHUFFLE_SEED
    )
    ds_resumed.load_state_dict(checkpoint)
    remaining_resumed = [s["id"] for s in ds_resumed]

    assert remaining_original == remaining_resumed, (
        "Resumed dataset must continue from exactly the same position"
    )
    assert sorted(consumed + remaining_original) == list(range(NUM_ROWS)), (
        "Consumed + remaining must cover every row exactly once"
    )
