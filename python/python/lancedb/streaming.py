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

Transform failures on bad rows (e.g. nulls or NaNs from incomplete data) can
be tolerated with ``on_transform_error="skip"``; see the parameter
documentation on StreamingDataset for how this interacts with the guarantees
above.
"""

import ctypes
import heapq
import logging
import os
import random
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import RawArray
from typing import Any, Callable, Iterator, NamedTuple, Optional, Union

from torch.utils.data import DataLoader, IterableDataset, get_worker_info

from .permutation import (
    Permutation,
    Transforms,
    permutation_builder,
    _table_from_pickle_state,
    _table_to_pickle_state,
)

logger = logging.getLogger(__name__)

# Multiplier used to combine shuffle_seed and epoch into a single permutation
# seed.  Chosen to be a large prime so different (seed, epoch) pairs produce
# distinct seeds for any practically encountered epoch count.
_EPOCH_PRIME = 100003

DEFAULT_READ_BATCH_SIZE = 64
DEFAULT_PREFETCH_BATCHES = 4


class _WorkerSample(NamedTuple):
    data: Any
    dataset: "StreamingDataset"


class _WorkerBatch(NamedTuple):
    data: Any
    state: dict


class _CheckpointCollate:
    """Attach the worker's post-fetch state to a collated batch."""

    def __init__(self, collate_fn: Callable):
        self._collate_fn = collate_fn

    def __call__(self, samples):
        try:
            if isinstance(samples, list):
                if not samples:
                    return _WorkerBatch(self._collate_fn(samples), {})
                worker_samples = samples
                data = self._collate_fn([sample.data for sample in worker_samples])
                dataset = worker_samples[-1].dataset
            else:
                data = self._collate_fn(samples.data)
                dataset = samples.dataset
        except StopIteration as exc:
            raise RuntimeError(
                "collate_fn raised StopIteration before returning a batch"
            ) from exc
        return _WorkerBatch(data, dataset._checkpoint_snapshot())


class _StreamingDatasetAdapter(IterableDataset):
    """Yield private sample wrappers for :class:`StreamingDataLoader`."""

    def __init__(self, dataset: "StreamingDataset"):
        super().__init__()
        self.dataset = dataset

    def __iter__(self):
        for sample in self.dataset._iter(consumer_checkpoint_transport=True):
            yield _WorkerSample(sample, self.dataset)

    def __getattr__(self, name):
        dataset = self.__dict__.get("dataset")
        if dataset is None:
            raise AttributeError(name)
        return getattr(dataset, name)


class _ConsumerCommitIterator:
    def __init__(self, iterator, dataset: "StreamingDataset", *, require_uniform: bool):
        self._iterator = iterator
        self._dataset = dataset
        self._require_uniform = require_uniform

    def __iter__(self):
        return self

    def __next__(self):
        try:
            batch = next(self._iterator)
        except StopIteration:
            raise
        except BaseException as exc:
            self._dataset._invalidate_checkpoint(
                f"a DataLoader batch failed before it was returned: {exc}"
            )
            raise
        try:
            if not isinstance(batch, _WorkerBatch):
                raise RuntimeError(
                    "StreamingDataLoader did not receive worker checkpoint metadata"
                )
            self._dataset._commit_worker_state(
                batch.state, require_uniform=self._require_uniform
            )
            return batch.data
        except BaseException as exc:
            self._dataset._invalidate_checkpoint(
                f"a DataLoader batch failed before it was returned: {exc}"
            )
            raise

    def __getattr__(self, name):
        return getattr(self._iterator, name)


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
    - **Stage 2 (transform)**: a second thread pool with
      ``transform_parallelism`` workers picks up raw batches, applies the
      transform, and places the results in a per-split cooked-row queue.  By
      default, the number of workers is determined by ``os.cpu_count()``.

    The main thread round-robins over the cooked queues, yielding one row per
    split per cycle.

    Parameters
    ----------
    table:
        LanceDB table to stream from.
    num_splits:
        Number of fixed splits to partition the table into.  Must be divisible
        by ``world_size``.  When used with DataLoader workers it must also be
        divisible by ``world_size * num_workers``.  Defaults to ``world_size``.
        If the row count (after any ``filter``) is not evenly divisible by
        ``num_splits``, the surplus rows — at most ``num_splits - 1`` per epoch
        — are silently dropped to keep all splits the same length.
    shuffle:
        Whether to randomly assign rows to splits.  When ``True`` (the
        default) rows are shuffled using ``shuffle_seed`` and ``epoch``.
        When ``False`` rows are divided into splits sequentially in storage
        order, which can be useful for deterministic debugging or evaluation.
    shuffle_seed:
        Base seed for the random permutation.  Combined with ``epoch`` so
        each epoch produces a different ordering.  Pass ``None`` to generate
        a random seed at construction time.
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
    columns:
        Optional list of column names to read.  When set, only those columns
        are fetched from storage; all others are omitted.  ``None`` (the
        default) reads every column.
    shuffle_clump_size:
        When set, rows are shuffled in contiguous groups of this size rather
        than individually.  Larger clumps improve I/O locality (important on
        object storage) at the cost of reduced randomness.  ``None`` (the
        default) shuffles rows individually.
    filter:
        Optional SQL filter expression (e.g. ``"label = 'dog'"``).  Only rows
        that satisfy the predicate are included in the permutation.  The filter
        is applied during permutation construction so split sizes reflect the
        filtered row count.
    transform:
        Optional callable applied to each ``pyarrow.RecordBatch`` before rows
        are yielded.  Receives one batch at a time and must return an iterable
        whose length equals the number of rows in the batch.  When ``None``
        (the default) rows are returned as plain Python dicts.
    transform_parallelism:
        Maximum number of transforms to run concurrently.  Must be greater
        than zero.  When ``None`` (the default), uses ``os.cpu_count()`` or 1
        when the CPU count is unavailable.
    on_transform_error:
        What to do when the transform raises an exception:

        - ``"raise"`` (the default): the exception propagates and iteration
          aborts.
        - ``"skip"``: the failing rows are dropped and iteration continues.
        - ``"warn"``: like ``"skip"``, but a warning is logged for each
          failing batch.
        - a callable ``handler(exc) -> bool``: called with the exception;
          return ``True`` to skip the failing rows or ``False`` to re-raise.
          Useful to skip only expected error types (compatible with
          ``webdataset.handlers`` style handlers).

        When a batch fails, the transform is re-invoked on each single-row
        slice of the batch so that only the rows that actually fail are
        dropped.  Transforms should therefore be deterministic and accept
        batches of any size (including one row).  Skipped rows are counted in
        ``rows_skipped``.

        Skipping weakens the elastic-determinism guarantee at the end of the
        epoch: splits that lose more rows than others run dry earlier, and
        each rank's iterator ends at the last cycle where every split *it
        owns* still has a row.  Because bad rows are not distributed evenly
        across splits, this means one rank's iterator can yield noticeably
        fewer or more steps than another rank's *in the same run* — there is
        no cross-rank coordination that stops every rank at the same global
        step.  This is generally safe for asynchronous or single-rank use,
        but synchronous distributed training (e.g. ranks that call
        ``all_reduce`` every step) can hang or deadlock if one rank's
        iterator is exhausted while others are still stepping; callers doing
        synchronous multi-rank training with ``on_transform_error != "raise"``
        are responsible for their own cross-rank stopping mechanism (e.g.
        broadcasting a stop signal on ``StopIteration``).  The final few
        global steps can also differ across topologies (bounded by the skew
        in bad-row counts across splits).  The sequence of samples yielded
        from each split remains deterministic.  Mid-epoch
        checkpoints remain exact provided the transform fails
        deterministically; in multi-rank training each rank must save its
        own ``state_dict`` and the states must be combined with
        ``merge_state_dicts`` before resuming on a different topology.
        Prefer the ``filter`` parameter when bad rows can be expressed as a
        SQL predicate (e.g. ``"col IS NOT NULL"``) — filtering happens before
        splits are built, so every guarantee is fully preserved.
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
        num_splits: Optional[int] = None,
        shuffle: bool = True,
        shuffle_seed: Optional[int] = 0,
        epoch: int = 0,
        rank: int = 0,
        world_size: int = 1,
        read_batch_size: int = DEFAULT_READ_BATCH_SIZE,
        prefetch_batches: int = DEFAULT_PREFETCH_BATCHES,
        columns: Optional[list[str]] = None,
        shuffle_clump_size: Optional[int] = None,
        filter: Optional[str] = None,
        transform: Optional[Callable] = None,
        transform_parallelism: Optional[int] = None,
        on_transform_error: Union[str, Callable[[Exception], bool]] = "raise",
        connection_factory: Optional[Callable[[str], Any]] = None,
        worker_info_override=None,
    ):
        super().__init__()
        if num_splits is None:
            num_splits = world_size
        if shuffle_seed is None:
            shuffle_seed = random.randrange(2**32)
        if num_splits % world_size != 0:
            raise ValueError(
                f"num_splits ({num_splits}) must be divisible by "
                f"world_size ({world_size})"
            )
        if transform_parallelism is not None and transform_parallelism <= 0:
            raise ValueError("transform_parallelism must be greater than 0")
        if on_transform_error not in ("raise", "skip", "warn") and not callable(
            on_transform_error
        ):
            raise ValueError(
                "on_transform_error must be 'raise', 'skip', 'warn', or a "
                f"callable, got {on_transform_error!r}"
            )

        self._table = table
        self._num_splits = num_splits
        self._shuffle = shuffle
        self._shuffle_seed = shuffle_seed
        self._epoch = epoch
        self._rank = rank
        self._world_size = world_size
        self._read_batch_size = read_batch_size
        self._prefetch_batches = prefetch_batches
        self._columns = columns
        self._shuffle_clump_size = shuffle_clump_size
        self._filter = filter
        self._transform = transform
        self._transform_parallelism = transform_parallelism
        self._on_transform_error = on_transform_error
        self._connection_factory = connection_factory
        self._worker_info_override = worker_info_override

        # Live references to pipeline state, set only while __iter__ is running
        # in the same process.  Used by the observability properties when the
        # DataLoader runs with num_workers=0.
        self._raw_batches_ref: Optional[list[deque]] = None
        self._cooked_ref: Optional[list[deque]] = None
        self._fetch_head_ref: Optional[list[int]] = None
        self._split_sizes_ref: Optional[list[int]] = None
        self._local_consumed_ref: Optional[list[int]] = None

        # Shared-memory counters written by __iter__ (which may run in a
        # DataLoader worker process) and read by the observability properties
        # in the main process.  RawArray is picklable via the forkserver
        # reduction protocol so it survives the dataset pickle round-trip.
        # Layout: [unscanned_rows, raw_rows, cooked_rows, consumed_rows,
        #          bytes_loaded, fetch_time_us, transform_time_us,
        #          rows_skipped]
        self._worker_stats: RawArray = RawArray(ctypes.c_int64, 8)

        # A standard multi-process DataLoader cannot report which prefetched
        # batches were actually returned to its consumer.  Workers set this
        # shared flag so state_dict() can reject a stale parent checkpoint
        # unless StreamingDataLoader installed the consumer-commit transport.
        self._untracked_worker_iteration: RawArray = RawArray(ctypes.c_int64, 1)

        # Parent-side checkpoint lifecycle.  A failed DataLoader task creates
        # a permanent hole in that iterator's delivery stream, while a
        # multi-worker checkpoint is safe to restore only after all splits
        # reach the same logical step boundary.
        self._checkpoint_invalid_reason: Optional[str] = None
        self._consumer_checkpoint_requires_uniform = False

        # Cumulative bytes of Arrow buffer data fetched across all iterations.
        self._bytes_loaded: int = 0
        # Cumulative seconds spent in LanceDB I/O and in transform functions.
        self._fetch_time: float = 0.0
        self._transform_time: float = 0.0
        # Cumulative rows dropped by on_transform_error across all iterations.
        self._rows_skipped: int = 0

        # Number of samples each split has already been consumed.  At global
        # step boundaries all splits have consumed this many samples, so a
        # single scalar captures the topology-independent checkpoint state.
        self._resume_offset: int = 0
        # Exact yielded-sample counts for splits this process has advanced.
        # Missing entries use _resume_offset, which remains the lower-bound
        # checkpoint inherited from an earlier uniform/global state.
        self._resume_samples: dict[int, int] = {}
        # Permutation position each split has consumed through, keyed by
        # global split index.  Equal to _resume_offset for every split unless
        # on_transform_error skipped rows, in which case skipped positions
        # push the watermark of the affected splits further ahead.  Splits
        # this instance has never iterated have no entry.
        self._resume_positions: dict[int, int] = {}

        # Build the permutation table once, deterministically.
        builder = permutation_builder(table)
        if filter is not None:
            builder = builder.filter(filter)
        if shuffle:
            perm_seed = shuffle_seed + epoch * _EPOCH_PRIME
            self._perm_table = builder.split_random(
                fixed=num_splits, seed=perm_seed, clump_size=shuffle_clump_size
            ).execute()
        else:
            self._perm_table = builder.split_sequential(fixed=num_splits).execute()

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
        return self._iter()

    def _iter(
        self, *, consumer_checkpoint_transport: bool = False
    ) -> Iterator[dict[str, Any]]:
        if self._raw_batches_ref is not None:
            raise RuntimeError(
                "StreamingDataset does not support concurrent iteration. "
                "Only one active iterator per dataset instance is allowed."
            )
        real_worker = get_worker_info() is not None
        if real_worker and not consumer_checkpoint_transport:
            self._untracked_worker_iteration[0] = 1

        my_splits = self._resolve_my_splits()
        if not my_splits:
            return

        # Set identity transform on each Permutation so __getitems__ returns
        # the raw RecordBatch.  Stage 2 applies the real transform.
        permutations: list[Permutation] = []
        initial_samples: list[int] = []
        initial_positions: list[int] = []
        for split_idx in my_splits:
            perm = Permutation.from_tables(
                self._table, self._perm_table, split=split_idx
            )
            if self._columns is not None:
                perm = perm.select_columns(self._columns)
            perm = perm.with_transform(lambda batch: batch)
            sample_count = self._resume_samples.get(split_idx, self._resume_offset)
            start_pos = self._resume_positions.get(split_idx, sample_count)
            if start_pos > 0:
                perm = perm.with_skip(start_pos)
            initial_samples.append(sample_count)
            initial_positions.append(start_pos)
            permutations.append(perm)

        n = len(permutations)
        split_sizes = [perm.num_rows for perm in permutations]
        local_consumed = [0] * n
        # Permutation position each split has consumed through (absolute,
        # i.e. counted from the start of the unskipped split).  Runs ahead of
        # initial + local_consumed when rows are skipped.
        pos_consumed = list(initial_positions)

        batch_size = self._read_batch_size
        max_prefetch = self._prefetch_batches
        transform_workers = (
            self._transform_parallelism
            if self._transform_parallelism is not None
            else (os.cpu_count() or 1)
        )
        final_transform = (
            self._transform if self._transform is not None else Transforms.arrow2python
        )

        # Per-split pipeline state.  Batches are paired with the absolute
        # permutation position of their first row so that skipped rows can be
        # accounted for in pos_consumed.
        fetch_head = [0] * n
        io_pending = [deque() for _ in range(n)]  # (abs_start, Future[RecordBatch])
        raw_batches = [deque() for _ in range(n)]  # (abs_start, RecordBatch)
        tx_pending = [deque() for _ in range(n)]  # Future[list[(abs_pos, row)]]
        cooked = [deque() for _ in range(n)]  # (abs_pos, row) ready to yield

        # Limit simultaneous transforms to transform_workers across all splits.
        tx_semaphore = threading.Semaphore(transform_workers)

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
            abs_start = initial_positions[i] + start
            io_pending[i].append((abs_start, io_pool.submit(_io_call, perm_i, indices)))

        def _fill_io(i: int) -> None:
            while len(io_pending[i]) < max_prefetch and fetch_head[i] < split_sizes[i]:
                _submit_io(i)

        def _drain_io(i: int) -> None:
            """Move completed I/O futures into raw_batches non-blockingly."""
            while io_pending[i] and io_pending[i][0][1].done():
                abs_start, fut = io_pending[i].popleft()
                raw_batches[i].append((abs_start, fut.result()))

        # ── Stage 2 helpers ───────────────────────────────────────────────────

        on_error = self._on_transform_error

        def _should_skip(exc: Exception) -> bool:
            if on_error == "raise":
                return False
            if callable(on_error):
                return bool(on_error(exc))
            return True  # "skip" or "warn"

        def _check_row_count(rows: list, num_rows: int) -> None:
            if len(rows) != num_rows:
                raise ValueError(
                    f"transform returned {len(rows)} rows for a batch of "
                    f"{num_rows}; transforms must return exactly one output "
                    "row per input row.  To drop bad rows, raise inside the "
                    "transform and pass on_transform_error='skip'."
                )

        def _transform_isolated(abs_start, batch, batch_exc):
            """Re-run the transform on single-row slices, dropping failures."""
            out = []
            skipped = 0
            first_exc = None
            for j in range(batch.num_rows):
                try:
                    rows = list(final_transform(batch.slice(j, 1)))
                except Exception as exc:
                    if not _should_skip(exc):
                        raise
                    skipped += 1
                    if first_exc is None:
                        first_exc = exc
                    continue
                _check_row_count(rows, 1)
                out.append((abs_start + j, rows[0]))
            self._rows_skipped += skipped
            if skipped and on_error == "warn":
                logger.warning(
                    "Skipped %d of %d rows whose transform failed (first error: %r)",
                    skipped,
                    batch.num_rows,
                    first_exc if first_exc is not None else batch_exc,
                )
            return out

        def _transform_batch(abs_start, batch):
            """Apply the transform, returning [(abs_pos, row), ...]."""
            try:
                rows = list(final_transform(batch))
            except Exception as exc:
                if not _should_skip(exc):
                    raise
                return _transform_isolated(abs_start, batch, exc)
            _check_row_count(rows, batch.num_rows)
            return [(abs_start + j, row) for j, row in enumerate(rows)]

        def _tx_call_guarded(abs_start, batch):
            try:
                t0 = time.perf_counter()
                result = _transform_batch(abs_start, batch)
                self._transform_time += time.perf_counter() - t0
                return result
            finally:
                tx_semaphore.release()

        def _try_submit_tx(i: int) -> None:
            """Submit transforms for raw_batches[i] up to available capacity."""
            while raw_batches[i] and tx_semaphore.acquire(blocking=False):
                abs_start, batch = raw_batches[i].popleft()
                tx_pending[i].append(tx_pool.submit(_tx_call_guarded, abs_start, batch))

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
                    # transform_workers are busy with other splits).
                    tx_semaphore.acquire()
                    abs_start, batch = raw_batches[i].popleft()
                    tx_pending[i].append(
                        tx_pool.submit(_tx_call_guarded, abs_start, batch)
                    )
                elif io_pending[i]:
                    # Block on the oldest in-flight I/O fetch.
                    abs_start, fut = io_pending[i].popleft()
                    raw_batches[i].append((abs_start, fut.result()))
                    _advance(i)
                else:
                    break  # split exhausted

        # ── Main loop ─────────────────────────────────────────────────────────

        with ThreadPoolExecutor(max_workers=n * max_prefetch) as io_pool:
            with ThreadPoolExecutor(max_workers=transform_workers) as tx_pool:
                self._raw_batches_ref = raw_batches
                self._cooked_ref = cooked
                self._fetch_head_ref = fetch_head
                self._split_sizes_ref = split_sizes
                self._local_consumed_ref = local_consumed
                try:
                    for i in range(n):
                        _fill_io(i)

                    def _yield_row(i: int):
                        pos, row = cooked[i].popleft()
                        local_consumed[i] += 1
                        pos_consumed[i] = pos + 1
                        split_idx = my_splits[i]
                        self._resume_samples[split_idx] = (
                            initial_samples[i] + local_consumed[i]
                        )
                        self._resume_positions[split_idx] = pos_consumed[i]
                        _advance(i)
                        return row

                    def _update_progress_stats() -> None:
                        if not real_worker:
                            self._resume_offset = min(
                                initial_samples[j] + local_consumed[j] for j in range(n)
                            )
                        ws = self._worker_stats
                        ws[0] = sum(split_sizes[j] - fetch_head[j] for j in range(n))
                        ws[1] = sum(
                            batch.num_rows for q in raw_batches for _, batch in q
                        )
                        ws[2] = sum(len(q) for q in cooked)
                        ws[3] = sum(local_consumed)
                        ws[4] = self._bytes_loaded
                        ws[5] = int(self._fetch_time * 1_000_000)
                        ws[6] = int(self._transform_time * 1_000_000)
                        ws[7] = self._rows_skipped

                    # A checkpoint taken between round-robin split turns has
                    # non-uniform counts.  Resume lagging splits first so the
                    # exact canonical sequence continues without replaying
                    # already-consumed rows.
                    if len(set(initial_samples)) > 1:
                        catch_up_to = max(initial_samples)
                        pending = [
                            (initial_samples[i], my_splits[i], i)
                            for i in range(n)
                            if initial_samples[i] < catch_up_to
                        ]
                        heapq.heapify(pending)
                        while pending:
                            consumed, _, i = heapq.heappop(pending)
                            _ensure_cooked(i)
                            if not cooked[i]:
                                return
                            row = _yield_row(i)
                            if consumed + 1 < catch_up_to:
                                heapq.heappush(pending, (consumed + 1, my_splits[i], i))
                            _update_progress_stats()
                            yield row

                    while True:
                        # A cycle only runs if every split can still produce a
                        # row.  Without skips all splits exhaust simultaneously
                        # (equal split sizes + round-robin); when
                        # on_transform_error drops rows a split can run dry
                        # early, ending the epoch at the last complete cycle.
                        # This check only sees splits owned by this rank/worker
                        # (my_splits) — there is no cross-rank coordination, so
                        # a different rank with fewer skipped rows keeps going;
                        # see the on_transform_error docstring.
                        exhausted = False
                        for i in range(n):
                            _ensure_cooked(i)
                            if not cooked[i]:
                                exhausted = True
                                break
                        if exhausted:
                            break

                        for i in range(n):
                            row = _yield_row(i)

                            # After the last split in each cycle: update the
                            # global offset and refresh the shared-memory stats
                            # so the main process can observe pipeline depth
                            # even when __iter__ runs in a worker process.
                            if i == n - 1:
                                _update_progress_stats()

                            yield row
                finally:
                    # Final stats flush: the per-cycle write above never runs
                    # when iteration ends mid-cycle (e.g. a split whose rows
                    # were all skipped before completing a single cycle), so
                    # counters like rows_skipped would otherwise be stale.
                    ws = self._worker_stats
                    ws[0] = sum(split_sizes[j] - fetch_head[j] for j in range(n))
                    ws[1] = 0  # queue-depth properties document 0 when idle
                    ws[2] = 0
                    ws[3] = sum(local_consumed)
                    ws[4] = self._bytes_loaded
                    ws[5] = int(self._fetch_time * 1_000_000)
                    ws[6] = int(self._transform_time * 1_000_000)
                    ws[7] = self._rows_skipped
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
        if self._raw_batches_ref is not None:
            return self._bytes_loaded
        return int(self._worker_stats[4])

    @property
    def fetch_time(self) -> float:
        """Cumulative seconds spent waiting for data from LanceDB.

        Measured per batch in the Stage 1 I/O threads as the total elapsed
        time of the ``take_offsets`` call.  Accumulates across all splits and
        all iterations.
        """
        if self._raw_batches_ref is not None:
            return self._fetch_time
        return self._worker_stats[5] / 1_000_000

    @property
    def transform_time(self) -> float:
        """Cumulative seconds spent applying the transform.

        Measured per batch in the Stage 2 transform threads as the elapsed
        time inside the transform callable (or the default ``arrow2python``
        conversion when no transform is set).  Accumulates across all splits
        and all iterations.
        """
        if self._raw_batches_ref is not None:
            return self._transform_time
        return self._worker_stats[6] / 1_000_000

    @property
    def raw_queue_depth(self) -> int:
        """Number of raw rows waiting for a transform thread across all splits.

        A persistently non-zero value means Stage 2 (transform) is the
        bottleneck: I/O is completing faster than transforms can consume
        batches.  Returns 0 when not iterating.
        """
        if self._raw_batches_ref is not None:
            return sum(batch.num_rows for q in self._raw_batches_ref for _, batch in q)
        return int(self._worker_stats[1])

    @property
    def prefetch_queue_depth(self) -> int:
        """Number of rows transformed and ready to yield across all splits.

        Counts rows whose transform has completed and are sitting in memory
        waiting for the main thread — rows that can be handed off with no
        I/O or CPU wait.  Returns 0 when not iterating.
        """
        if self._cooked_ref is not None:
            return sum(len(q) for q in self._cooked_ref)
        return int(self._worker_stats[2])

    @property
    def unscanned_rows(self) -> int:
        """Number of rows not yet submitted to the I/O stage across all splits.

        Decreases as the I/O stage submits fetch requests.  When this reaches
        zero all data has been requested from storage (though it may not have
        arrived yet).  Returns 0 when not iterating.
        """
        if self._fetch_head_ref is not None:
            return sum(
                size - head
                for size, head in zip(self._split_sizes_ref, self._fetch_head_ref)
            )
        return int(self._worker_stats[0])

    @property
    def rows_skipped(self) -> int:
        """Number of rows dropped because their transform raised an exception.

        Only ever non-zero when ``on_transform_error`` is set to ``"skip"``,
        ``"warn"``, or a callable that returned ``True``.  Accumulates across
        multiple iterations of the same dataset instance and is never reset
        automatically.
        """
        if self._raw_batches_ref is not None:
            return self._rows_skipped
        return int(self._worker_stats[7])

    @property
    def consumed_rows(self) -> int:
        """Number of rows already yielded to the caller across all splits.

        Monotonically increases throughout iteration.  Returns 0 when not
        iterating.
        """
        if self._local_consumed_ref is not None:
            return sum(self._local_consumed_ref)
        return int(self._worker_stats[3])

    def __getstate__(self):
        """Support pickling for multi-worker DataLoader (forkserver / spawn).

        The live LanceDB table object contains non-picklable connection state
        (sockets, Rust-backed PyO3 objects).  If a ``connection_factory`` was
        supplied only the table name is serialised; the factory is called in
        the worker to reopen the connection without embedding any credentials.
        Without a factory the table's own picklable reopen state is captured
        via ``_table_to_pickle_state`` (mirrors the ``Permutation`` approach).
        """
        state = self.__dict__.copy()
        # _table: replace with reconnect info (credentials must not be embedded).
        state["_table_name"] = self._table.name
        if self._connection_factory is not None:
            state["_table"] = None
        else:
            state["_table"] = _table_to_pickle_state(self._table)
        # _perm_table: always in-memory; serialise as Arrow data (mirrors
        # how Permutation.__getstate__ handles its permutation_table).
        state["_perm_table"] = (
            self._perm_table.name,
            self._perm_table.to_arrow(),
        )
        for key in (
            "_raw_batches_ref",
            "_cooked_ref",
            "_fetch_head_ref",
            "_split_sizes_ref",
            "_local_consumed_ref",
        ):
            state[key] = None
        return state

    def __setstate__(self, state):
        """Reconnect to LanceDB after unpickling in a worker process."""
        from . import connect as _connect

        table_name = state.pop("_table_name")
        table_state = state.pop("_table")
        perm_name, perm_data = state.pop("_perm_table")
        self.__dict__.update(state)
        if self._connection_factory is not None:
            self._table = self._connection_factory(table_name)
        else:
            self._table = _table_from_pickle_state(table_state)
        self._perm_table = _connect("memory://").create_table(perm_name, perm_data)

    def state_dict(self) -> dict:
        """Snapshot the dataset's consumption state.

        When using DataLoader workers, construct a
        [StreamingDataLoader][lancedb.streaming.StreamingDataLoader].  It
        commits worker state only when a prefetched batch is returned to the
        trainer.  A standard multi-process ``DataLoader`` cannot expose that
        boundary, so calling this method after one has started raises
        ``RuntimeError`` instead of returning stale producer state.

        ``positions_consumed_per_split`` records how far into each split's
        permutation iteration has advanced.  It only differs from
        ``samples_consumed_per_split`` when ``on_transform_error`` skipped
        rows, in which case entries are exact for the splits this instance
        iterated and a lower bound (the sample count) for splits owned by
        other ranks.  ``StreamingDataLoader`` combines worker state in its
        parent process.  Combine the parent state dicts from all ranks with
        [merge_state_dicts][lancedb.streaming.StreamingDataset.merge_state_dicts]
        to recover the exact value for every split before resuming on a
        different topology.
        """
        if self._untracked_worker_iteration[0] and get_worker_info() is None:
            raise RuntimeError(
                "StreamingDataset cannot checkpoint a standard DataLoader with "
                "num_workers > 0 because prefetched worker progress is not "
                "consumer-committed. Use StreamingDataLoader instead."
            )
        if self._checkpoint_invalid_reason is not None:
            raise RuntimeError(
                "StreamingDataset checkpointing is invalid because "
                f"{self._checkpoint_invalid_reason}. Load the last valid "
                "checkpoint into a fresh dataset before continuing."
            )
        state = self._checkpoint_snapshot()
        samples = state["samples_consumed_per_split"]
        rank_samples = [samples[split] for split in self._rank_splits]
        if self._consumer_checkpoint_requires_uniform and len(set(rank_samples)) > 1:
            raise RuntimeError(
                "StreamingDataLoader checkpointing with multiple workers is only "
                "safe at a complete logical step boundary, when every split "
                "assigned to this rank has the same consumed-sample count. "
                "Consume more batches before calling state_dict()."
            )
        return state

    def _checkpoint_snapshot(self) -> dict:
        samples = [
            self._resume_samples.get(split, self._resume_offset)
            for split in range(self._num_splits)
        ]
        positions = [
            self._resume_positions.get(split, samples[split])
            for split in range(self._num_splits)
        ]
        return {
            "shuffle_seed": self._shuffle_seed,
            "num_splits": self._num_splits,
            "epoch": self._epoch,
            "samples_consumed_per_split": samples,
            "positions_consumed_per_split": positions,
        }

    def _invalidate_checkpoint(self, reason: str) -> None:
        if self._checkpoint_invalid_reason is None:
            self._checkpoint_invalid_reason = reason

    def _commit_worker_state(self, state: dict, *, require_uniform: bool) -> None:
        """Merge one trainer-consumed worker batch into parent state."""
        for key, expected in (
            ("shuffle_seed", self._shuffle_seed),
            ("num_splits", self._num_splits),
            ("epoch", self._epoch),
        ):
            if state.get(key) != expected:
                raise ValueError(
                    f"{key} mismatch in worker checkpoint: "
                    f"{state.get(key)} != {expected}"
                )
        samples = state["samples_consumed_per_split"]
        positions = state.get("positions_consumed_per_split", samples)
        for split, count in enumerate(samples):
            current = self._resume_samples.get(split, self._resume_offset)
            self._resume_samples[split] = max(current, int(count))
        for split, position in enumerate(positions):
            current = self._resume_positions.get(
                split, self._resume_samples.get(split, self._resume_offset)
            )
            self._resume_positions[split] = max(current, int(position))
        self._resume_offset = min(
            self._resume_samples.get(split, self._resume_offset)
            for split in range(self._num_splits)
        )
        self._consumer_checkpoint_requires_uniform |= require_uniform

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
        self._consumer_checkpoint_requires_uniform = False
        consumed = state["samples_consumed_per_split"]
        if isinstance(consumed, list):
            self._resume_offset = min(consumed) if consumed else 0
            self._resume_samples = {
                split: int(count) for split, count in enumerate(consumed)
            }
        else:
            self._resume_offset = int(consumed)
            self._resume_samples = {}
        # Older checkpoints predate positions_consumed_per_split; without
        # skipped rows positions equal sample counts, so falling back to
        # the per-split sample count (the .get default in __iter__) is exact.
        positions = state.get("positions_consumed_per_split")
        if positions is None:
            self._resume_positions = {}
        else:
            self._resume_positions = {
                split: int(pos) for split, pos in enumerate(positions)
            }

    @staticmethod
    def merge_state_dicts(states: list[dict]) -> dict:
        """Merge state dicts saved by different ranks into one exact state.

        Each rank knows exact consumer-committed progress for its own splits
        and records a lower bound for the rest.  Because exactly one rank owns
        each split, the elementwise maximum across all ranks recovers both the
        sample count and permutation position for every split.

        Raises ``ValueError`` if the states are empty or were not produced by
        the same run (mismatched seed, split count, or epoch).

        The merge is always all-to-all and topology-agnostic: collect the
        ``state_dict()`` from every rank of the *previous* run into one list,
        merge that whole list, and hand the identical merged result to every
        rank of the *next* run — regardless of whether the rank count grew,
        shrank, or stayed the same. There is no pairwise or subset merging
        step, because each split's exact position is only known to whichever
        rank owned that split, and the elementwise maximum needs every rank's
        contribution to be correct.

        For example, checkpointing 8 ranks and resuming on 4 (the same
        pattern applies when growing, e.g. 4 ranks resuming on 8)::

            states = [ds.state_dict() for ds in previous_run_datasets]  # 8
            merged = StreamingDataset.merge_state_dicts(states)
            for ds in resumed_datasets:  # now only 4 ranks
                ds.load_state_dict(merged)  # same dict on every rank

        The rank count on either side never affects the merge itself, since
        ``merge_state_dicts`` only cares about the list of states it is
        given.  Each split's position is recovered by elementwise maximum;
        here rank 0 owned split 0 (and skipped two rows there) while rank 1
        owned split 1 (and skipped one row):

        >>> rank0 = {
        ...     "shuffle_seed": 0, "num_splits": 2, "epoch": 0,
        ...     "samples_consumed_per_split": [3, 3],
        ...     "positions_consumed_per_split": [5, 3],
        ... }
        >>> rank1 = {
        ...     "shuffle_seed": 0, "num_splits": 2, "epoch": 0,
        ...     "samples_consumed_per_split": [3, 3],
        ...     "positions_consumed_per_split": [3, 4],
        ... }
        >>> merged = StreamingDataset.merge_state_dicts([rank0, rank1])
        >>> merged["positions_consumed_per_split"]
        [5, 4]
        """
        if not states:
            raise ValueError("merge_state_dicts requires at least one state dict")
        first = states[0]
        for state in states[1:]:
            for key in ("shuffle_seed", "num_splits", "epoch"):
                if state[key] != first[key]:
                    raise ValueError(
                        f"{key} mismatch across state dicts: "
                        f"{state[key]} != {first[key]}"
                    )
        merged = dict(first)
        merged["samples_consumed_per_split"] = [
            max(per_split)
            for per_split in zip(
                *(state["samples_consumed_per_split"] for state in states)
            )
        ]
        all_positions = [
            state.get(
                "positions_consumed_per_split", state["samples_consumed_per_split"]
            )
            for state in states
        ]
        merged["positions_consumed_per_split"] = [
            max(per_split) for per_split in zip(*all_positions)
        ]
        return merged


class StreamingDataLoader(DataLoader):
    """A PyTorch DataLoader with consumer-committed dataset checkpoints.

    PyTorch workers prefetch batches ahead of the trainer, so worker-local
    producer progress is not a safe checkpoint.  This loader carries a state
    snapshot alongside every internal batch and applies it to the parent
    [StreamingDataset][lancedb.streaming.StreamingDataset] only when that batch
    is returned by ``next()``.
    The trainer receives the same collated batch it would receive from a
    standard ``torch.utils.data.DataLoader``.

    With more than one worker, ``state_dict()`` is available only at complete
    logical step boundaries, when every split assigned to the rank has the same
    consumed-sample count.  ``persistent_workers=True`` is not supported because
    prefetched worker copies cannot be restored from parent-committed state.  If
    batch collation raises, checkpointing remains invalid for that dataset
    instance; restore the last valid checkpoint into a fresh dataset before
    continuing.

    Parameters are the same as ``torch.utils.data.DataLoader`` except that
    ``dataset`` must be a
    [StreamingDataset][lancedb.streaming.StreamingDataset].
    Subclasses that override ``StreamingDataset.__iter__`` are not supported
    because the custom iterator cannot provide the exact per-yield checkpoint
    snapshots required by this loader.

    Examples
    --------
    >>> # dataset = StreamingDataset(table, num_splits=2)
    >>> # loader = StreamingDataLoader(dataset, batch_size=8, num_workers=2)
    >>> # batch = next(iter(loader))
    >>> # checkpoint = dataset.state_dict()
    """

    def __init__(self, dataset: StreamingDataset, *args, **kwargs):
        if not isinstance(dataset, StreamingDataset):
            raise TypeError("StreamingDataLoader requires a StreamingDataset")
        if type(dataset).__iter__ is not StreamingDataset.__iter__:
            raise TypeError(
                "StreamingDataLoader does not support StreamingDataset subclasses "
                "that override __iter__ because they cannot provide exact "
                "per-yield checkpoint state"
            )
        if kwargs.get("in_order", True) is False:
            raise ValueError(
                "StreamingDataLoader requires in_order=True for deterministic "
                "consumer checkpoints"
            )
        if kwargs.get("persistent_workers", False):
            raise ValueError(
                "StreamingDataLoader does not support persistent_workers=True "
                "because worker prefetch state cannot be reset from a checkpoint"
            )
        self._streaming_dataset = dataset
        super().__init__(_StreamingDatasetAdapter(dataset), *args, **kwargs)
        self.collate_fn = _CheckpointCollate(self.collate_fn)

    def __iter__(self):
        if self.num_workers > 1:
            samples = self._streaming_dataset._checkpoint_snapshot()[
                "samples_consumed_per_split"
            ]
            rank_samples = [
                samples[split] for split in self._streaming_dataset._rank_splits
            ]
            if len(set(rank_samples)) > 1:
                raise RuntimeError(
                    "StreamingDataLoader cannot start multiple workers from a "
                    "partial logical step; resume from a checkpoint whose splits "
                    "assigned to this rank have equal consumed-sample counts"
                )
        return _ConsumerCommitIterator(
            super().__iter__(),
            self._streaming_dataset,
            require_uniform=self.num_workers > 1,
        )
