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
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Iterator

from torch.utils.data import IterableDataset, get_worker_info

from .permutation import Permutation, permutation_builder

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
        Number of batches to prefetch in parallel per split using background
        threads.  Each batch is ``read_batch_size`` rows.  Higher values
        overlap I/O with training compute at the cost of more memory and
        threads.  Defaults to ``DEFAULT_PREFETCH_BATCHES`` (4).
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
        self._worker_info_override = worker_info_override

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
        my_splits = self._resolve_my_splits()
        if not my_splits:
            return

        # Build one Permutation per assigned split, starting at the resume
        # offset so that already-consumed rows are skipped automatically.
        permutations: list[Permutation] = []
        for split_idx in my_splits:
            perm = Permutation.from_tables(
                self._table, self._perm_table, split=split_idx
            )
            if self._resume_offset > 0:
                perm = perm.with_skip(self._resume_offset)
            permutations.append(perm)

        n = len(permutations)
        split_sizes = [perm.num_rows for perm in permutations]
        initial_offset = self._resume_offset
        local_consumed = [0] * n

        batch_size = self._read_batch_size
        max_prefetch = self._prefetch_batches

        # Per-split: how many rows have been submitted to the executor so far.
        fetch_head = [0] * n
        # Per-split queues of in-flight Future objects (each resolves to a list of
        # rows).
        pending: list[deque] = [deque() for _ in range(n)]
        # Per-split deques of rows already fetched and ready to yield.
        ready: list[deque] = [deque() for _ in range(n)]

        def _submit_one(i: int) -> None:
            remaining = split_sizes[i] - fetch_head[i]
            if remaining <= 0:
                return
            fetch = min(batch_size, remaining)
            start = fetch_head[i]
            fetch_head[i] += fetch
            pending[i].append(
                executor.submit(
                    permutations[i].__getitems__, list(range(start, start + fetch))
                )
            )

        def _fill_pipeline(i: int) -> None:
            while len(pending[i]) < max_prefetch and fetch_head[i] < split_sizes[i]:
                _submit_one(i)

        def _ensure_ready(i: int) -> None:
            if not ready[i] and pending[i]:
                ready[i].extend(pending[i].popleft().result())

        with ThreadPoolExecutor(max_workers=n * max_prefetch) as executor:
            # Prime the pipeline: submit up to max_prefetch batches per split.
            for i in range(n):
                _fill_pipeline(i)

            while True:
                # Stop when any split is exhausted (all exhaust simultaneously
                # because splits have equal size and num_rows % num_splits == 0).
                if any(local_consumed[i] >= split_sizes[i] for i in range(n)):
                    break

                # Round-robin: one sample from each split per cycle.
                for i in range(n):
                    # Block until the oldest in-flight batch lands, then keep
                    # the pipeline full for this split.
                    _ensure_ready(i)
                    row = ready[i].popleft()
                    local_consumed[i] += 1
                    _fill_pipeline(i)

                    # Update the global offset after the last split in each
                    # cycle so state_dict() returns the completed-cycle count.
                    if i == n - 1:
                        self._resume_offset = initial_offset + local_consumed[i]

                    yield row

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
