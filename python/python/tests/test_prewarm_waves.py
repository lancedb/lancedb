# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import lancedb
import numpy as np
import pyarrow as pa
import pytest
from lancedb._prewarm_waves import WavePlan, row_ids_to_ranges
from lancedb.prewarm_run import RowRange


def address(fragment: int, offset: int) -> int:
    return (fragment << 32) | offset


def test_row_ids_merge_into_sorted_ranges():
    ids = [address(1, 5), address(0, 2), address(0, 0), address(0, 1), address(1, 7)]
    assert row_ids_to_ranges(np.array(ids + [address(0, 1)])) == [
        RowRange(0, 0, 3),
        RowRange(1, 5, 6),
        RowRange(1, 7, 8),
    ]
    assert row_ids_to_ranges(np.array([], dtype=np.uint64)) == []


def permutation(splits):
    """A permutation table from each split's row ids, in read order."""
    row_ids = [row_id for ids in splits for row_id in ids]
    split_ids = [split for split, ids in enumerate(splits) for _ in ids]
    return pa.table(
        {
            "row_id": pa.array(row_ids, pa.uint64()),
            "split_id": pa.array(split_ids, pa.uint32()),
        }
    )


def test_waves_take_the_same_positions_of_every_split():
    # Split 0 has 5 rows, split 1 has 3, so split 1 runs out a wave early.
    split0 = [address(0, i) for i in (4, 0, 1, 2, 9)]
    split1 = [address(1, i) for i in (3, 2, 7)]
    plan = WavePlan(permutation([split0, split1]), 2, 2)
    assert plan.num_waves == 3
    assert plan.ranges(0) == [
        RowRange(0, 0, 1),
        RowRange(0, 4, 5),
        RowRange(1, 2, 4),
    ]
    assert plan.ranges(1) == [RowRange(0, 1, 3), RowRange(1, 7, 8)]
    assert plan.ranges(2) == [RowRange(0, 9, 10)]
    assert plan.ranges(3) == []
    assert plan.first_wave == 0
    assert plan.wave_of(3) == 1
    assert plan.wave_end(0, 1) == 4
    assert plan.wave_end(1, 1) == 3
    assert plan.fetch_ranges(0, 1, 4) == [RowRange(0, 0, 3)]


def test_start_positions_trim_waves_but_keep_their_numbers():
    split0 = [address(0, i) for i in range(6)]
    split1 = [address(1, i) for i in range(6)]
    plan = WavePlan(permutation([split0, split1]), 2, 2, {0: 5, 1: 3})
    assert plan.num_waves == 3
    assert plan.first_wave == 1
    # Wave 1 is positions 2..4: split 0 has read past it, split 1 has read 2.
    assert plan.ranges(1) == [RowRange(1, 3, 4)]
    assert plan.ranges(2) == [RowRange(0, 5, 6), RowRange(1, 4, 6)]
    assert plan.wave_of(5) == 2
    assert plan.wave_end(0, 1) == 4

    # A lower bound for another rank's split warms extra rows of the same
    # waves, never different waves.
    lower = WavePlan(permutation([split0, split1]), 2, 2, {0: 4, 1: 3})
    assert lower.first_wave == 1
    assert lower.ranges(2) == [RowRange(0, 4, 6), RowRange(1, 4, 6)]

    done = WavePlan(permutation([split0, split1]), 2, 2, {0: 6, 1: 6})
    assert done.first_wave == done.num_waves == 3
    with pytest.raises(ValueError):
        WavePlan(permutation([split0, split1]), 2, 2, {0: 7})


@pytest.fixture
def table(tmp_path):
    db = lancedb.connect(tmp_path)
    table = db.create_table("data", pa.table({"id": list(range(40))}))
    for start in range(40, 160, 40):
        table.add(pa.table({"id": list(range(start, start + 40))}))
    return table


def test_waves_do_not_depend_on_world_size(table):
    pytest.importorskip("torch")
    from lancedb import streaming

    num_splits = 4
    plans = []
    for world_size in (1, 2, 4):
        for rank in range(world_size):
            dataset = streaming.StreamingDataset(
                table,
                num_splits=num_splits,
                shuffle_seed=7,
                epoch=1,
                rank=rank,
                world_size=world_size,
            )
            perm = dataset._perm_table.to_arrow()
            plans.append(WavePlan(perm, num_splits, 8))
    waves = [[plan.ranges(w) for w in range(plan.num_waves)] for plan in plans]
    assert all(wave == waves[0] for wave in waves[1:])

    # Every row is in exactly one wave.
    rows = [
        (r.fragment_id, offset)
        for wave in waves[0]
        for r in wave
        for offset in range(r.start, r.end)
    ]
    assert len(rows) == len(set(rows)) == 160
