# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Elastic streaming dataloader for PyTorch.

Provides StreamingDataset, a PyTorch IterableDataset that guarantees:

- **Elastic determinism**: for a fixed (num_splits, shuffle_seed, epoch) the set
  of samples that forms each global training step is identical regardless of
  world_size or num_workers.
- **Resumability**: state_dict / load_state_dict capture per-split consumption
  counts so training can resume from an exact mid-epoch position even when the
  distributed topology changes between runs.
"""

import logging
import os
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Iterator, Optional

from torch.utils.data import IterableDataset, get_worker_info

from .permutation import Permutation, Transforms, permutation_builder

logger = logging.getLogger(__name__)

# Multiplier used to combine shuffle_seed and epoch into a single permutation
# seed.  Chosen to be a large prime so different (seed, epoch) pairs produce
# distinct seeds for any practically encountered epoch count.
_EPOCH_PRIME = 100003

DEFAULT_READ_BATCH_SIZE = 64
DEFAULT_PREFETCH_BATCHES = 4


class StreamingDataset(IterableDataset):
    """An elastic, resumable PyTorch IterableDataset backed by a LanceDB table.

    The table is partitioned into ``num_splits`` fixed splits using a
    deterministic random shuffle controlled by ``shuffle_seed`` and ``epoch``.
    Each rank is assigned a contiguous block of splits, and within a rank each
    DataLoader worker is assigned a contiguous sub-block.  Samples are yielded
    by round-robining over the assigned splits, one sample per split per cycle.

    Internally ``__iter__`` runs a two-stage pipeline:

    - **Stage 1 (I/O)**: one thread pool with ``num_splits * prefetch_batches``
      workers fetches raw ``RecordBatch`` objects from LanceDB in parallel
      across all splits and places them in a per-split raw-batch queue.
    - **Stage 2 (transform)**: a second thread pool with ``os.cpu_count()``
      workers picks up raw batches, applies the transform, and places the
      results in a per-split cooked-row queue.

    The main thread round-robins over the cooked queues, yielding one row per
    split per cycle.

    Parameters
    ----------
    table:
        LanceDB table to stream from.
    num_splits:
        Number of fixed splits to partition the table into.  Must be divisible
        by ``world_size``.  When used with DataLoader workers it must also be
        divisible by ``world_size * num_workers``.
    shuffle_seed:
        Base seed for the random permutation.
    epoch:
        Current training epoch.  Combined with ``shuffle_seed`` so that each
        epoch produces a different sample ordering.
    rank:
        This process's rank in the distributed training group.
    world_size:
        Total number of processes in the distributed training group.
    read_batch_size:
        Number of rows fetched from each split in a single ``take_offsets``
        call.  Larger values amortise per-request overhead (critical on object
        storage) at the cost of higher memory usage per split buffer.  Defaults
        to ``DEFAULT_READ_BATCH_SIZE`` (64).
    prefetch_batches:
        Number of I/O batches to keep in flight per split.  Higher values
        overlap storage latency with transform and training compute at the cost
        of more memory and threads.  Defaults to ``DEFAULT_PREFETCH_BATCHES``
        (4).
    transform:
        Optional callable applied to each ``pyarrow.RecordBatch`` before rows
        are yielded.  Receives one batch at a time and must return an iterable
        whose length equals the number of rows in the batch.  When ``None``
        (the default) rows are returned as plain Python dicts.
    worker_info_override:
        If set, used in place of ``torch.utils.data.get_worker_info()`` to
        determine the DataLoader worker assignment.  Intended for unit tests
        that need to simulate multiple workers without spawning real processes.
        If both this and the real worker info are non-None a warning is logged
        and the override takes precedence.
    """

    def __init__(
        self,
        table,
        *,
        num_splits: int,
        shuffle_seed: int = 0,
        epoch: int = 0,
        rank: int = 0,
        world_size: int = 1,
        read_batch_size: int = DEFAULT_READ_BATCH_SIZE,
        prefetch_batches: int = DEFAULT_PREFETCH_BATCHES,
        transform: Optional[Callable] = None,
        worker_info_override=None,
    ):
        super().__init__()
        if num_splits % world_size != 0:
            raise ValueError(
                f"num_splits ({num_splits}) must be divisible by "
                f"world_size ({world_size})"
            )

        self._table = table
        self._num_splits = num_splits
        self._shuffle_seed = shuffle_seed
        self._epoch = epoch
        self._rank = rank
        self._world_size = world_size
        self._read_batch_size = read_batch_size
        self._prefetch_batches = prefetch_batches
        self._transform = transform
        self._worker_info_override = worker_info_override

        # Live references to pipeline state, set only while __iter__ is running.
        # Used by the queue-depth and progress observability properties.
        self._raw_batches_ref: Optional[list[deque]] = None
        self._cooked_ref: Optional[list[deque]] = None
        self._fetch_head_ref: Optional[list[int]] = None
        self._split_sizes_ref: Optional[list[int]] = None
        self._local_consumed_ref: Optional[list[int]] = None

        # Cumulative bytes of Arrow buffer data fetched across all iterations.
        self._bytes_loaded: int = 0
        # Cumulative seconds spent in LanceDB I/O and in transform functions.
        self._fetch_time: float = 0.0
        self._transform_time: float = 0.0

        # Number of samples each split has already been consumed.  At global
        # step boundaries all splits have consumed this many samples, so a
        # single scalar captures the topology-independent checkpoint state.
        self._resume_offset: int = 0

        # Build the permutation table once, deterministically.
        perm_seed = shuffle_seed + epoch * _EPOCH_PRIME
        self._perm_table = (
            permutation_builder(table)
            .split_random(fixed=num_splits, seed=perm_seed)
            .execute()
        )

        # Contiguous block of global split indices assigned to this rank.
        splits_per_rank = num_splits // world_size
        rank_start = rank * splits_per_rank
        self._rank_splits: list[int] = list(
            range(rank_start, rank_start + splits_per_rank)
        )

    def _resolve_my_splits(self) -> list[int]:
        """Return the split indices this instance should read in __iter__."""
        torch_worker_info = get_worker_info()
        if self._worker_info_override is not None:
            if torch_worker_info is not None:
                logger.warning(
                    "worker_info_override is set but get_worker_info() also returned a "
                    "non-None value; ignoring the real torch worker info and using the "
                    "override instead.  This may lead to duplicated or incorrect data "
                    "from the dataset."
                )
            worker_info = self._worker_info_override
        else:
            worker_info = torch_worker_info

        if worker_info is None:
            return self._rank_splits

        num_workers: int = worker_info.num_workers
        worker_id: int = worker_info.id
        n_rank_splits = len(self._rank_splits)
        if n_rank_splits % num_workers != 0:
            raise ValueError(
                f"Number of rank splits ({n_rank_splits}) must be divisible by "
                f"num_workers ({num_workers})"
            )
        splits_per_worker = n_rank_splits // num_workers
        start = worker_id * splits_per_worker
        return self._rank_splits[start : start + splits_per_worker]

    def __iter__(self) -> Iterator[dict[str, Any]]:
        if self._raw_batches_ref is not None:
            raise RuntimeError(
                "StreamingDataset does not support concurrent iteration. "
                "Only one active iterator per dataset instance is allowed."
            )
        my_splits = self._resolve_my_splits()
        if not my_splits:
            return

        # Set identity transform on each Permutation so __getitems__ returns
        # the raw RecordBatch.  Stage 2 applies the real transform.
        permutations: list[Permutation] = []
        for split_idx in my_splits:
            perm = Permutation.from_tables(
                self._table, self._perm_table, split=split_idx
            )
            perm = perm.with_transform(lambda batch: batch)
            if self._resume_offset > 0:
                perm = perm.with_skip(self._resume_offset)
            permutations.append(perm)

        n = len(permutations)
        split_sizes = [perm.num_rows for perm in permutations]
        initial_offset = self._resume_offset
        local_consumed = [0] * n

        batch_size = self._read_batch_size
        max_prefetch = self._prefetch_batches
        cpu_workers = os.cpu_count() or 1
        final_transform = (
            self._transform if self._transform is not None else Transforms.arrow2python
        )

        # Per-split pipeline state.
        fetch_head = [0] * n
        io_pending = [deque() for _ in range(n)]  # Future[RecordBatch]
        raw_batches = [deque() for _ in range(n)]  # RecordBatch — fetched, awaiting tx
        tx_pending = [deque() for _ in range(n)]  # Future[list[Any]]
        cooked = [deque() for _ in range(n)]  # rows ready to yield

        # Limit simultaneous transforms to cpu_workers across all splits.
        tx_semaphore = threading.Semaphore(cpu_workers)

        # ── Stage 1 helpers ───────────────────────────────────────────────────

        def _io_call(perm, indices):
            t0 = time.perf_counter()
            batch = perm.__getitems__(indices)
            self._bytes_loaded += batch.nbytes
            self._fetch_time += time.perf_counter() - t0
            return batch

        def _submit_io(i: int) -> None:
            remaining = split_sizes[i] - fetch_head[i]
            if remaining <= 0:
                return
            fetch = min(batch_size, remaining)
            start = fetch_head[i]
            fetch_head[i] += fetch
            perm_i = permutations[i]
            indices = list(range(start, start + fetch))
            io_pending[i].append(io_pool.submit(_io_call, perm_i, indices))

        def _fill_io(i: int) -> None:
            while len(io_pending[i]) < max_prefetch and fetch_head[i] < split_sizes[i]:
                _submit_io(i)

        def _drain_io(i: int) -> None:
            """Move completed I/O futures into raw_batches non-blockingly."""
            while io_pending[i] and io_pending[i][0].done():
                raw_batches[i].append(io_pending[i].popleft().result())

        # ── Stage 2 helpers ───────────────────────────────────────────────────

        def _tx_call_guarded(batch):
            try:
                t0 = time.perf_counter()
                result = final_transform(batch)
                self._transform_time += time.perf_counter() - t0
                return result
            finally:
                tx_semaphore.release()

        def _try_submit_tx(i: int) -> None:
            """Submit transforms for raw_batches[i] up to available capacity."""
            while raw_batches[i] and tx_semaphore.acquire(blocking=False):
                batch = raw_batches[i].popleft()
                tx_pending[i].append(tx_pool.submit(_tx_call_guarded, batch))

        def _drain_tx(i: int) -> None:
            """Move completed transform futures into cooked non-blockingly."""
            while tx_pending[i] and tx_pending[i][0].done():
                cooked[i].extend(tx_pending[i].popleft().result())

        # ── Combined advance ──────────────────────────────────────────────────

        def _advance(i: int) -> None:
            """Non-blocking pipeline pump for split i."""
            _drain_io(i)
            _drain_tx(i)
            _try_submit_tx(i)
            _fill_io(i)

        def _ensure_cooked(i: int) -> None:
            """Ensure cooked[i] has at least one row, blocking if necessary."""
            _advance(i)
            while not cooked[i]:
                if tx_pending[i]:
                    # Wait for the oldest in-flight transform.
                    cooked[i].extend(tx_pending[i].popleft().result())
                    _advance(i)
                elif raw_batches[i]:
                    # Acquire a transform slot (may block briefly if all
                    # cpu_workers are busy with other splits).
                    tx_semaphore.acquire()
                    batch = raw_batches[i].popleft()
                    tx_pending[i].append(tx_pool.submit(_tx_call_guarded, batch))
                elif io_pending[i]:
                    # Block on the oldest in-flight I/O fetch.
                    raw_batches[i].append(io_pending[i].popleft().result())
                    _advance(i)
                else:
                    break  # split exhausted

        # ── Main loop ─────────────────────────────────────────────────────────

        with ThreadPoolExecutor(max_workers=n * max_prefetch) as io_pool:
            with ThreadPoolExecutor(max_workers=cpu_workers) as tx_pool:
                self._raw_batches_ref = raw_batches
                self._cooked_ref = cooked
                self._fetch_head_ref = fetch_head
                self._split_sizes_ref = split_sizes
                self._local_consumed_ref = local_consumed
                try:
                    for i in range(n):
                        _fill_io(i)

                    while True:
                        # Stop when any split is exhausted (all exhaust
                        # simultaneously: equal split sizes + round-robin).
                        if any(local_consumed[i] >= split_sizes[i] for i in range(n)):
                            break

                        for i in range(n):
                            _ensure_cooked(i)
                            row = cooked[i].popleft()
                            local_consumed[i] += 1
                            _advance(i)

                            # Update the global offset after the last split in
                            # each cycle so state_dict() returns the correct
                            # completed-cycle count.
                            if i == n - 1:
                                self._resume_offset = initial_offset + local_consumed[i]

                            yield row
                finally:
                    self._raw_batches_ref = None
                    self._cooked_ref = None
                    self._fetch_head_ref = None
                    self._split_sizes_ref = None
                    self._local_consumed_ref = None

    @property
    def bytes_loaded(self) -> int:
        """Cumulative bytes of raw Arrow buffer data fetched from storage.

        Measured on the ``RecordBatch`` before any transform is applied, so
        the value reflects actual I/O rather than the size of transformed
        output.  Accumulates across multiple iterations of the same dataset
        instance and is never reset automatically.
        """
        return self._bytes_loaded

    @property
    def fetch_time(self) -> float:
        """Cumulative seconds spent waiting for data from LanceDB.

        Measured per batch in the Stage 1 I/O threads as the total elapsed
        time of the ``take_offsets`` call.  Accumulates across all splits and
        all iterations.
        """
        return self._fetch_time

    @property
    def transform_time(self) -> float:
        """Cumulative seconds spent applying the transform.

        Measured per batch in the Stage 2 transform threads as the elapsed
        time inside the transform callable (or the default ``arrow2python``
        conversion when no transform is set).  Accumulates across all splits
        and all iterations.
        """
        return self._transform_time

    @property
    def raw_queue_depth(self) -> int:
        """Number of raw rows waiting for a transform thread across all splits.

        A persistently non-zero value means Stage 2 (transform) is the
        bottleneck: I/O is completing faster than transforms can consume
        batches.  Returns 0 when not iterating.
        """
        if self._raw_batches_ref is None:
            return 0
        return sum(batch.num_rows for q in self._raw_batches_ref for batch in q)

    @property
    def prefetch_queue_depth(self) -> int:
        """Number of rows transformed and ready to yield across all splits.

        Counts rows whose transform has completed and are sitting in memory
        waiting for the main thread — rows that can be handed off with no
        I/O or CPU wait.  Returns 0 when not iterating.
        """
        if self._cooked_ref is None:
            return 0
        return sum(len(q) for q in self._cooked_ref)

    @property
    def unscanned_rows(self) -> int:
        """Number of rows not yet submitted to the I/O stage across all splits.

        Decreases as the I/O stage submits fetch requests.  When this reaches
        zero all data has been requested from storage (though it may not have
        arrived yet).  Returns 0 when not iterating.
        """
        if self._fetch_head_ref is None:
            return 0
        return sum(
            size - head
            for size, head in zip(self._split_sizes_ref, self._fetch_head_ref)
        )

    @property
    def consumed_rows(self) -> int:
        """Number of rows already yielded to the caller across all splits.

        Monotonically increases throughout iteration.  Returns 0 when not
        iterating.
        """
        if self._local_consumed_ref is None:
            return 0
        return sum(self._local_consumed_ref)

    def state_dict(self) -> dict:
        """Snapshot the dataset's consumption state.

        The returned dict is topology-independent: at global step boundaries
        every split has been consumed the same number of times (by the
        round-robin design), so the per-split count is a single uniform value
        that is identical across all ranks and DataLoader workers.
        """
        return {
            "shuffle_seed": self._shuffle_seed,
            "num_splits": self._num_splits,
            "epoch": self._epoch,
            "samples_consumed_per_split": [self._resume_offset] * self._num_splits,
        }

    def load_state_dict(self, state: dict) -> None:
        """Resume from a previously snapshotted state.

        Raises ``ValueError`` if ``num_splits`` or ``shuffle_seed`` differ
        from the checkpoint, since a different split structure or shuffle order
        makes mid-epoch resumption meaningless.
        """
        if state["num_splits"] != self._num_splits:
            raise ValueError(
                f"num_splits mismatch: checkpoint has {state['num_splits']}, "
                f"current dataset has {self._num_splits}"
            )
        if state["shuffle_seed"] != self._shuffle_seed:
            raise ValueError(
                f"shuffle_seed mismatch: checkpoint has {state['shuffle_seed']}, "
                f"current dataset has {self._shuffle_seed}"
            )
        consumed = state["samples_consumed_per_split"]
        # All entries are equal at step boundaries; use the first.
        if isinstance(consumed, list):
            self._resume_offset = consumed[0] if consumed else 0
        else:
            self._resume_offset = int(consumed)
