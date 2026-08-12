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
import logging
import os
import random
import threading
import time
import warnings
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from multiprocessing import RawArray
from typing import Any, Callable, cast, Iterator, Literal, Optional, Union

import pyarrow as pa
import pyarrow.compute as pc
import torch
from torch.utils.data import IterableDataset, get_worker_info

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
    pack_sequences:
        Sequence-packing mode: token lists from consecutive documents are
        joined with ``eos_id`` and sliced into blocks of this many tokens.
        Each item is then a dict of two ``(pack_sequences,)`` LongTensors —
        ``input_ids`` and ``doc_ids`` (per-position document index within
        the block, for block-diagonal masks or position-id resets).
        * Packing happens independently per owned split and preserves per-split
        resume state.
        * When a split cannot fill a real block for a cycle but an owned sibling
        still can, or when only a short tail remains at epoch end, the short buffer
        is padded to ``pack_sequences`` with ``pad_id`` so every local cycle emits
        one block per owned split.
        * ``eos_id``, ``pad_id``, and ``columns`` naming a single
        list-typed column are required; incompatible with ``transform``.
    eos_id:
        Separator token id between packed documents.  Required with
        ``pack_sequences``, ignored otherwise.
    pad_id:
        Padding token id used to complete blocks when a split runs out of
        real tokens mid-cycle or at epoch end.  Required with
        ``pack_sequences``, ignored otherwise.
    blocks_per_epoch:
        Total number of packed blocks emitted globally per epoch. Required with
        ``pack_sequences``. An integer must be divisible by ``num_splits``.
        Every logical split emits exactly ``blocks_per_epoch / num_splits``
        blocks: exhausted splits emit padding, while tokens beyond the budget
        are left out of the epoch. This fixed per-split budget keeps packed
        iteration and checkpoints independent of rank and worker topology.
        Pass ``"auto"`` to estimate the budget by reading only the token column
        for approximately 1% of the rows, with a target cap of 100,000 rows and
        at least one row from every logical split. Sampling is batched and evenly
        distributed across logical splits. This is a convenience for smaller
        workloads. For large-scale training, materialize a ``token_count`` column,
        calculate an explicit block budget once, and pass that integer instead.
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
        pack_sequences: Optional[int] = None,
        eos_id: Optional[int] = None,
        pad_id: Optional[int] = None,
        blocks_per_epoch: Optional[Union[int, Literal["auto"]]] = None,
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
        if pack_sequences is not None:
            if pack_sequences <= 0:
                raise ValueError("pack_sequences must be greater than 0")
            if eos_id is None:
                raise ValueError("eos_id is required when pack_sequences is set")
            if pad_id is None:
                raise ValueError("pad_id is required when pack_sequences is set")
            if blocks_per_epoch is None:
                raise ValueError(
                    "blocks_per_epoch is required when pack_sequences is set"
                )
            if blocks_per_epoch != "auto":
                if not isinstance(blocks_per_epoch, int) or isinstance(
                    blocks_per_epoch, bool
                ):
                    raise ValueError(
                        "blocks_per_epoch must be a positive integer or 'auto'"
                    )
                if blocks_per_epoch <= 0:
                    raise ValueError("blocks_per_epoch must be greater than 0")
                if blocks_per_epoch % num_splits != 0:
                    raise ValueError(
                        f"blocks_per_epoch ({blocks_per_epoch}) must be divisible by "
                        f"num_splits ({num_splits})"
                    )
            if transform is not None:
                raise ValueError("transform cannot be combined with pack_sequences")
            if columns is None or len(columns) != 1:
                raise ValueError(
                    "pack_sequences requires columns to name exactly one "
                    "list-typed column of token ids"
                )
            field = table.schema.field(columns[0])
            if not (
                pa.types.is_list(field.type)
                or pa.types.is_large_list(field.type)
                or pa.types.is_fixed_size_list(field.type)
            ):
                raise ValueError(
                    f"pack_sequences requires a list-typed token column; "
                    f"{columns[0]} has type {field.type}"
                )
        elif blocks_per_epoch is not None:
            raise ValueError("blocks_per_epoch requires pack_sequences")
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
        self._pack_sequences = pack_sequences
        self._eos_id = eos_id
        self._pad_id = pad_id
        self._blocks_per_epoch = blocks_per_epoch
        self._on_transform_error = on_transform_error
        self._connection_factory = connection_factory
        self._worker_info_override = worker_info_override

        # Packing resume state: documents consumed and partial-block token buffers.
        self._pack_consumed: list[int] = [0] * num_splits
        self._pack_buffers: dict[int, dict[str, list[int]]] = {}
        self._pack_blocks_emitted: list[int] = [0] * num_splits

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

        if self._blocks_per_epoch == "auto":
            self._blocks_per_epoch = self._estimate_blocks_per_epoch()

        # Contiguous block of global split indices assigned to this rank.
        splits_per_rank = num_splits // world_size
        rank_start = rank * splits_per_rank
        self._rank_splits: list[int] = list(
            range(rank_start, rank_start + splits_per_rank)
        )

    def _estimate_blocks_per_epoch(self) -> int:
        """Estimate a fixed packed-block budget from a bounded token sample."""
        if self._pack_sequences is None or not self._columns:
            raise RuntimeError(
                "packing must be configured before estimating its budget"
            )

        pack_len = self._pack_sequences
        token_column = self._columns[0]
        sample_cap_per_split = max(1, 100_000 // self._num_splits)
        sampled_tokens = 0
        total_sampled = 0
        total_rows = 0

        warnings.warn(
            "blocks_per_epoch='auto' is a convenience estimate that samples full "
            "token lists and repeats for every process constructing the dataset. It "
            "is recommended to materialize a token_count column, calculate an "
            "explicit blocks_per_epoch once, and pass that integer instead.",
            # TODO: Link to docs example that shows how to do this
        )

        for split in range(self._num_splits):
            permutation = Permutation.from_tables(
                self._table, self._perm_table, split=split
            )
            permutation = permutation.select_columns([token_column])
            permutation = permutation.with_transform(Transforms.arrow2arrow)
            split_rows = permutation.num_rows
            if split_rows == 0:
                raise ValueError(
                    "blocks_per_epoch='auto' cannot estimate an empty dataset"
                )

            # Roughly 1% per logical split, with at least one row from each
            # split and a global cap of approximately 100,000 rows.
            sample_rows = min(
                split_rows,
                max(1, min((split_rows + 99) // 100, sample_cap_per_split)),
            )
            sample_batch_size = max(1, self._read_batch_size)
            for start in range(0, sample_rows, sample_batch_size):
                stop = min(start + sample_batch_size, sample_rows)
                # approx mid of each bucket
                batch_offsets = [
                    ((2 * index + 1) * split_rows) // (2 * sample_rows)
                    for index in range(start, stop)
                ]
                batch = permutation.__getitems__(batch_offsets)
                lengths = pc.list_value_length(batch.column(0))
                if lengths.null_count:
                    raise ValueError("pack_sequences does not support null token lists")
                sampled_tokens += int(pc.sum(lengths).as_py())

            total_sampled += sample_rows
            total_rows += split_rows

        # Pool all samples into one global average. Each document contributes
        # one EOS token. Round down so estimation
        # favors leaving a short tail unused over emitting all-padding blocks.
        estimated_tokens = (
            (sampled_tokens + total_sampled) * total_rows // total_sampled
        )
        estimated_blocks = estimated_tokens // pack_len
        blocks_per_epoch = max(
            self._num_splits,
            estimated_blocks - estimated_blocks % self._num_splits,
        )
        return blocks_per_epoch

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
        initial_positions: list[int] = []
        for split_idx in my_splits:
            perm = Permutation.from_tables(
                self._table, self._perm_table, split=split_idx
            )
            if self._columns is not None:
                perm = perm.select_columns(self._columns)
            perm = perm.with_transform(Transforms.arrow2arrow)
            # Packing tracks documents consumed per split. Row mode tracks
            # absolute permutation positions so transform failures can skip
            # rows without making resume repeat them.
            start_pos = (
                self._pack_consumed[split_idx]
                if self._pack_sequences is not None
                else self._resume_positions.get(split_idx, self._resume_offset)
            )
            if start_pos > 0:
                perm = perm.with_skip(start_pos)
            initial_positions.append(start_pos)
            permutations.append(perm)

        n = len(permutations)
        split_sizes = [perm.num_rows for perm in permutations]
        initial_offset = self._resume_offset
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
        final_transform: Callable[[pa.RecordBatch], Any]
        if self._pack_sequences is not None:
            # Packing consumes raw token lists, one per document.
            def arrow_tokens(batch: pa.RecordBatch) -> list[list[int]]:
                return cast(list[list[int]], batch.column(0).to_pylist())

            final_transform = arrow_tokens
        else:
            final_transform = (
                self._transform
                if self._transform is not None
                else Transforms.arrow2python
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

        # sequence packing helpers
        pack_len = cast(int, self._pack_sequences)
        eos_id = cast(int, self._eos_id)
        pad_id = cast(int, self._pad_id)
        blocks_per_split = (
            cast(int, self._blocks_per_epoch) // self._num_splits
            if self._pack_sequences is not None
            else 0
        )
        pack_consumed = list(self._pack_consumed)
        pack_buffers = deepcopy(self._pack_buffers)
        pack_blocks_emitted = list(self._pack_blocks_emitted)

        def _pack_buf(i: int) -> dict[str, list[int]]:
            return pack_buffers.setdefault(my_splits[i], {"tokens": [], "starts": []})

        def _fill_block(i: int) -> None:
            """Fill split i's buffer to one block or exhaust the split."""
            buf = _pack_buf(i)
            while len(buf["tokens"]) < pack_len:
                _ensure_cooked(i)
                if not cooked[i]:
                    return
                buf["starts"].append(len(buf["tokens"]))
                pos, tokens = cooked[i].popleft()
                buf["tokens"].extend(tokens)
                buf["tokens"].append(eos_id)
                pack_consumed[my_splits[i]] += 1
                local_consumed[i] += 1
                pos_consumed[i] = pos + 1
                _advance(i)

        def _emit_block(i: int) -> dict[str, Any]:
            buf = _pack_buf(i)
            tokens, starts = buf["tokens"], buf["starts"]
            # doc_ids label document segments within the block; 0 also covers
            # the continuation of a document begun in a prior block.
            doc_ids = torch.zeros(pack_len, dtype=torch.int64)
            doc_starts = [s for s in starts if 0 < s < pack_len]
            doc_ids[doc_starts] = 1
            doc_ids.cumsum_(dim=0)  # cumulative sum marks document boundaries
            block = {
                "input_ids": torch.tensor(tokens[:pack_len], dtype=torch.int64),
                "doc_ids": doc_ids,
            }
            del tokens[:pack_len]
            # Shift start boundaries for the next call.
            buf["starts"] = [s - pack_len for s in starts if s >= pack_len]
            return block

        def _commit_pack_state() -> None:
            self._pack_consumed = list(pack_consumed)
            self._pack_buffers = deepcopy(pack_buffers)
            self._pack_blocks_emitted = list(pack_blocks_emitted)

        # ── Main loop ─────────────────────────────────────────────────────────

        with ThreadPoolExecutor(max_workers=n * max_prefetch) as io_pool:
            with ThreadPoolExecutor(max_workers=transform_workers) as tx_pool:
                self._raw_batches_ref = raw_batches
                self._cooked_ref = cooked
                self._fetch_head_ref = fetch_head
                self._split_sizes_ref = split_sizes
                self._local_consumed_ref = local_consumed

                def _update_stats() -> None:
                    # Refresh the shared-memory stats so the main process can
                    # observe pipeline depth even when __iter__ runs in a
                    # worker process.
                    ws = self._worker_stats
                    ws[0] = sum(split_sizes[j] - fetch_head[j] for j in range(n))
                    ws[1] = sum(batch.num_rows for q in raw_batches for _, batch in q)
                    ws[2] = sum(len(q) for q in cooked)
                    ws[3] = sum(local_consumed)
                    ws[4] = self._bytes_loaded
                    ws[5] = int(self._fetch_time * 1_000_000)
                    ws[6] = int(self._transform_time * 1_000_000)
                    ws[7] = self._rows_skipped

                try:
                    for i in range(n):
                        _fill_io(i)

                    if self._pack_sequences is not None:
                        first_count = pack_blocks_emitted[my_splits[0]]
                        if any(
                            pack_blocks_emitted[split] != first_count
                            for split in my_splits[1:]
                        ):
                            raise ValueError(
                                "Packed checkpoint is not aligned across the splits "
                                "owned by this iterator; merge every rank or worker "
                                "state with merge_state_dicts before resuming on a "
                                "different topology"
                            )

                        while pack_blocks_emitted[my_splits[0]] < blocks_per_split:
                            # Each logical split gets one block per cycle. Exhausted
                            # splits are padded through the fixed global budget.
                            for i in range(n):
                                _fill_block(i)

                            for i in range(n):
                                tokens = _pack_buf(i)["tokens"]
                                tokens.extend([pad_id] * (pack_len - len(tokens)))

                            for i in range(n):
                                block = _emit_block(i)
                                pack_blocks_emitted[my_splits[i]] += 1
                                if i == n - 1:
                                    _commit_pack_state()
                                    _update_stats()
                                yield block
                        return

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
                            pos, row = cooked[i].popleft()
                            local_consumed[i] += 1
                            pos_consumed[i] = pos + 1
                            _advance(i)

                            # After the last split in each cycle: update the
                            # global offset and refresh the shared-memory stats
                            # so the main process can observe pipeline depth
                            # even when __iter__ runs in a worker process.
                            if i == n - 1:
                                self._resume_offset = initial_offset + local_consumed[i]
                                for j, split_idx in enumerate(my_splits):
                                    self._resume_positions[split_idx] = pos_consumed[j]
                                _update_stats()

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

        In row mode, the returned dict is topology-independent at global step
        boundaries. ``positions_consumed_per_split`` records how far each
        split's permutation has advanced, which can differ from the sample
        count when ``on_transform_error`` skips rows. Combine state dicts from
        every rank with
        [merge_state_dicts][lancedb.streaming.StreamingDataset.merge_state_dicts]
        before resuming on a different topology.

        Packed state includes partial token buffers and emitted block counts
        for every logical split. When packing is sharded, merge every rank or
        worker state with ``merge_state_dicts`` before loading it.
        """
        if self._pack_sequences is not None:
            return {
                "shuffle_seed": self._shuffle_seed,
                "num_splits": self._num_splits,
                "epoch": self._epoch,
                "pack_sequences": self._pack_sequences,
                "eos_id": self._eos_id,
                "pad_id": self._pad_id,
                "blocks_per_epoch": self._blocks_per_epoch,
                "samples_consumed_per_split": list(self._pack_consumed),
                "blocks_emitted_per_split": list(self._pack_blocks_emitted),
                "pack_buffers": deepcopy(self._pack_buffers),
            }
        positions = [
            self._resume_positions.get(split, self._resume_offset)
            for split in range(self._num_splits)
        ]
        return {
            "shuffle_seed": self._shuffle_seed,
            "num_splits": self._num_splits,
            "epoch": self._epoch,
            "samples_consumed_per_split": [self._resume_offset] * self._num_splits,
            "positions_consumed_per_split": positions,
        }

    def load_state_dict(self, state: dict) -> None:
        """Resume from a previously snapshotted state.

        Raises ``ValueError`` if ``num_splits`` or ``shuffle_seed`` differ
        from the checkpoint, since a different split structure or shuffle order
        makes mid-epoch resumption meaningless.  Packed checkpoints
        pin ``pack_sequences``, ``eos_id``, ``pad_id``,
        ``blocks_per_epoch``, and ``epoch``.
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

        if "pack_buffers" in state or self._pack_sequences is not None:
            for key in (
                "pack_sequences",
                "eos_id",
                "pad_id",
                "blocks_per_epoch",
                "epoch",
            ):
                ours = getattr(self, f"_{key}")
                if state.get(key) != ours:
                    raise ValueError(
                        f"{key} mismatch: checkpoint has {state.get(key)}, "
                        f"current dataset has {ours}"
                    )
            self._pack_consumed = [int(c) for c in state["samples_consumed_per_split"]]
            self._pack_blocks_emitted = [
                int(c) for c in state["blocks_emitted_per_split"]
            ]
            self._pack_buffers = {
                int(g): {"tokens": list(b["tokens"]), "starts": list(b["starts"])}
                for g, b in state["pack_buffers"].items()
            }
            return

        consumed = state["samples_consumed_per_split"]
        # All entries are equal at step boundaries; use the first.
        if isinstance(consumed, list):
            self._resume_offset = consumed[0] if consumed else 0
        else:
            self._resume_offset = int(consumed)
        # Older checkpoints predate positions_consumed_per_split; without
        # skipped rows positions equal sample counts, so falling back to
        # _resume_offset (the .get default in __iter__) is exact.
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

        For row mode, the elementwise maximum of permutation positions recovers
        splits advanced by different ranks after transform failures. For packed
        mode, the state that emitted the most blocks for each logical split
        supplies that split's document count and partial token buffer. Packed
        states must cover every rank or worker at the same global step.

        Raises ``ValueError`` if the states are empty, were not produced by
        the same run, or do not represent the same global step.

        The merge is always all-to-all and topology-agnostic: collect the
        ``state_dict()`` from every rank or worker of the *previous* run into
        one list, merge that whole list, and hand the identical merged result
        to every rank or worker of the *next* run — regardless of whether the
        topology grew, shrank, or stayed the same. There is no pairwise or
        subset merging step, because each split's exact state is only known to
        whichever iterator owned that split.

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
        packed = "pack_buffers" in first
        for state in states[1:]:
            for key in ("shuffle_seed", "num_splits", "epoch"):
                if state[key] != first[key]:
                    raise ValueError(
                        f"{key} mismatch across state dicts: "
                        f"{state[key]} != {first[key]}"
                    )
            if ("pack_buffers" in state) != packed:
                raise ValueError("cannot merge packed and unpacked state dicts")

        if packed:
            config_keys = (
                "pack_sequences",
                "eos_id",
                "pad_id",
                "blocks_per_epoch",
            )
            for state in states[1:]:
                for key in config_keys:
                    if state[key] != first[key]:
                        raise ValueError(
                            f"{key} mismatch across state dicts: "
                            f"{state[key]} != {first[key]}"
                        )

            num_splits = first["num_splits"]
            for state in states:
                for key in (
                    "samples_consumed_per_split",
                    "blocks_emitted_per_split",
                ):
                    if len(state[key]) != num_splits:
                        raise ValueError(
                            f"{key} must contain one entry per logical split"
                        )

            merged_consumed = []
            merged_emitted = []
            merged_buffers = {}
            for split in range(num_splits):
                owner = states[0]
                owner_progress = (
                    owner["blocks_emitted_per_split"][split],
                    owner["samples_consumed_per_split"][split],
                )
                for state in states[1:]:
                    progress = (
                        state["blocks_emitted_per_split"][split],
                        state["samples_consumed_per_split"][split],
                    )
                    if progress > owner_progress:
                        owner = state
                        owner_progress = progress
                merged_consumed.append(owner["samples_consumed_per_split"][split])
                merged_emitted.append(owner["blocks_emitted_per_split"][split])
                buffer = owner["pack_buffers"].get(
                    split, owner["pack_buffers"].get(str(split))
                )
                if buffer is not None:
                    merged_buffers[split] = deepcopy(buffer)

            if len(set(merged_emitted)) > 1:
                raise ValueError(
                    "packed state dicts were not captured at the same global "
                    "step or do not cover every rank and worker"
                )

            merged = dict(first)
            merged["samples_consumed_per_split"] = merged_consumed
            merged["blocks_emitted_per_split"] = merged_emitted
            merged["pack_buffers"] = merged_buffers
            return merged

        for state in states[1:]:
            if (
                state["samples_consumed_per_split"]
                != first["samples_consumed_per_split"]
            ):
                raise ValueError(
                    "samples_consumed_per_split mismatch across state dicts; "
                    "state_dict() must be called at the same global step "
                    "boundary on every rank"
                )
        merged = dict(first)
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
