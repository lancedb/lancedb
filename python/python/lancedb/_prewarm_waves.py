# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Wave boundaries and row ranges for StreamingDataset prewarm runs.

A wave is the same span of permutation positions in every split: wave ``w`` of
split ``s`` is positions ``[start_s + w * P, start_s + (w + 1) * P)``, where
``P`` is the run's ``wave_positions`` and ``start_s`` the position the split
resumes from. Waves depend only on the permutation and the start positions,
never on ``world_size`` or worker count, so every rank computes the same ones.
"""

from typing import List, Mapping

import numpy as np
import pyarrow as pa

from .prewarm_run import RowRange

_OFFSET_BITS = 32
_OFFSET_MASK = (1 << _OFFSET_BITS) - 1


def row_ids_to_ranges(row_ids: np.ndarray) -> List[RowRange]:
    """Merge row ids into sorted ``(fragment_id, start, end)`` ranges.

    Row ids are read as row addresses, ``fragment_id << 32 | offset``; tables
    with stable row ids are not supported.
    """
    if len(row_ids) == 0:
        return []
    ids = np.unique(np.asarray(row_ids, dtype=np.uint64))
    fragments = ids >> np.uint64(_OFFSET_BITS)
    offsets = ids & np.uint64(_OFFSET_MASK)
    # A range breaks where the fragment changes or offsets stop being adjacent.
    breaks = np.flatnonzero(
        (fragments[1:] != fragments[:-1]) | (offsets[1:] != offsets[:-1] + 1)
    )
    starts = np.concatenate(([0], breaks + 1))
    ends = np.concatenate((breaks + 1, [len(ids)]))
    return [
        RowRange(int(fragments[s]), int(offsets[s]), int(offsets[e - 1]) + 1)
        for s, e in zip(starts, ends)
    ]


class WavePlan:
    """The waves of one run, for every split of a permutation."""

    def __init__(
        self,
        permutation: pa.Table,
        num_splits: int,
        wave_positions: int,
        start_positions: Mapping[int, int],
    ):
        """
        Parameters
        ----------
        permutation: pa.Table
            The permutation table (``row_id``, ``split_id``) in read order: a
            split's position ``p`` is its ``p``-th row.
        num_splits: int
            Number of splits, ``0 .. num_splits - 1``.
        wave_positions: int
            Positions of each split in one wave, as the run reports it.
        start_positions: Mapping[int, int]
            Position each split starts reading from; missing splits start at 0.
        """
        if wave_positions <= 0:
            raise ValueError(f"wave_positions must be positive, got {wave_positions}")
        row_ids = permutation.column("row_id").to_numpy().astype(np.uint64)
        split_ids = permutation.column("split_id").to_numpy()
        self._row_ids = [row_ids[split_ids == split] for split in range(num_splits)]
        self._starts = [start_positions.get(split, 0) for split in range(num_splits)]
        self._wave_positions = wave_positions
        for split, (ids, start) in enumerate(zip(self._row_ids, self._starts)):
            if not 0 <= start <= len(ids):
                raise ValueError(
                    f"split {split} starts at position {start} but has {len(ids)} rows"
                )

    @property
    def wave_positions(self) -> int:
        return self._wave_positions

    @property
    def num_waves(self) -> int:
        """Waves until the longest split is read to its end."""
        remaining = max(
            (len(ids) - start for ids, start in zip(self._row_ids, self._starts)),
            default=0,
        )
        return -(-remaining // self._wave_positions)

    def wave_of(self, split: int, position: int) -> int:
        """The wave holding ``position`` of ``split``."""
        start = self._starts[split]
        if position < start:
            raise ValueError(
                f"position {position} of split {split} is before its start {start}"
            )
        return (position - start) // self._wave_positions

    def wave_end(self, split: int, wave: int) -> int:
        """The first position of ``split`` after ``wave``: a fetch must stop
        here to stay inside one wave."""
        end = self._starts[split] + (wave + 1) * self._wave_positions
        return min(end, len(self._row_ids[split]))

    def ranges(self, wave: int) -> List[RowRange]:
        """Every split's rows in ``wave``, as merged row ranges."""
        parts = []
        for split, (ids, start) in enumerate(zip(self._row_ids, self._starts)):
            begin = start + wave * self._wave_positions
            parts.append(ids[begin : self.wave_end(split, wave)])
        return row_ids_to_ranges(np.concatenate(parts))

    def fetch_ranges(self, split: int, start: int, end: int) -> List[RowRange]:
        """The rows of one fetch, positions ``[start, end)`` of ``split``."""
        return row_ids_to_ranges(self._row_ids[split][start:end])
