# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Wave boundaries and row ranges for StreamingDataset prewarm runs.

A wave is the same span of permutation positions in every split: wave ``w`` of
split ``s`` is positions ``[w * P, (w + 1) * P)``, where ``P`` is the run's
``wave_positions``. Waves sit on this fixed grid, so they depend only on the
permutation: never on ``world_size``, worker count or where a resumed run
starts reading. Every rank computes the same waves even when ranks disagree
about other ranks' resume positions; such a disagreement only changes which
rows of a wave get warmed, never what the wave holds.
"""

from typing import List, Mapping, Optional

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
        start_positions: Optional[Mapping[int, int]] = None,
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
        start_positions: Mapping[int, int], optional
            Positions of each split already read; missing splits start at 0.
            A lower bound is safe: it only warms rows that are already read.
            Start positions never change wave numbers, only which rows of a
            wave are left to warm.
        """
        if wave_positions <= 0:
            raise ValueError(f"wave_positions must be positive, got {wave_positions}")
        start_positions = start_positions or {}
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
        longest = max((len(ids) for ids in self._row_ids), default=0)
        return -(-longest // self._wave_positions)

    @property
    def first_wave(self) -> int:
        """The first wave with rows left to read in any split."""
        unfinished = [
            start for ids, start in zip(self._row_ids, self._starts) if start < len(ids)
        ]
        if not unfinished:
            return self.num_waves
        return self.wave_of(min(unfinished))

    def split_rows(self, split: int) -> int:
        """How many positions ``split`` has."""
        return len(self._row_ids[split])

    def wave_of(self, position: int) -> int:
        """The wave holding ``position`` of every split."""
        return position // self._wave_positions

    def wave_end(self, split: int, wave: int) -> int:
        """The first position of ``split`` after ``wave``: a fetch must stop
        here to stay inside one wave."""
        end = (wave + 1) * self._wave_positions
        return min(end, len(self._row_ids[split]))

    def ranges(self, wave: int) -> List[RowRange]:
        """Every split's rows in ``wave`` not yet read, as merged row ranges."""
        parts = []
        for split, (ids, start) in enumerate(zip(self._row_ids, self._starts)):
            begin = max(start, wave * self._wave_positions)
            parts.append(ids[begin : self.wave_end(split, wave)])
        return row_ids_to_ranges(np.concatenate(parts))

    def fetch_ranges(self, split: int, start: int, end: int) -> List[RowRange]:
        """The rows of one fetch, positions ``[start, end)`` of ``split``."""
        return row_ids_to_ranges(self._row_ids[split][start:end])
