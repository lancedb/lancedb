# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import functools
import pickle

import lancedb
import pyarrow as pa
import pytest
from lancedb.util import tbl_to_tensor
from lancedb.permutation import Permutation, Permutations, permutation_builder

torch = pytest.importorskip("torch")


def _open_native_table(uri: str, table_name: str):
    """Top-level connection factory used by the explicit-factory pickle test.

    Defined at module scope so that pickle can resolve it by name in the
    worker / unpickling process.
    """
    return lancedb.connect(uri).open_table(table_name)


def test_table_dataloader(mem_db):
    table = mem_db.create_table("test_table", pa.table({"a": range(1000)}))
    dataloader = torch.utils.data.DataLoader(
        table, collate_fn=tbl_to_tensor, batch_size=10, shuffle=True
    )
    for batch in dataloader:
        assert batch.size(0) == 1
        assert batch.size(1) == 10


def test_permutation_dataloader(mem_db):
    table = mem_db.create_table("test_table", pa.table({"a": range(1000)}))

    permutation = Permutation.identity(table)
    dataloader = torch.utils.data.DataLoader(permutation, batch_size=10, shuffle=True)
    for batch in dataloader:
        assert batch["a"].size(0) == 10

    permutation = permutation.with_format("torch")
    dataloader = torch.utils.data.DataLoader(permutation, batch_size=10, shuffle=True)
    for batch in dataloader:
        assert batch.size(0) == 10
        assert batch.size(1) == 1

    permutation = permutation.with_format("torch_col")
    dataloader = torch.utils.data.DataLoader(
        permutation, collate_fn=lambda x: x, batch_size=10, shuffle=True
    )
    for batch in dataloader:
        assert batch.size(0) == 1
        assert batch.size(1) == 10


def test_permutation_is_picklable(tmp_db):
    """A Permutation must be picklable so it can be used with PyTorch's
    DataLoader when num_workers > 0 (which uses multiprocessing and pickles
    the dataset to pass it to worker processes)."""
    table = tmp_db.create_table("test_table", pa.table({"a": range(1000)}))
    permutation = Permutation.identity(table)

    pickled = pickle.dumps(permutation)
    restored = pickle.loads(pickled)

    assert len(restored) == 1000
    rows = restored.__getitems__([0, 1, 2])
    assert rows == [{"a": 0}, {"a": 1}, {"a": 2}]


def test_permutation_with_memory_base_is_picklable(mem_db):
    """An in-memory base table is inlined into the pickle as Arrow IPC bytes
    and rebuilt on the other side as an in-memory LanceTable, so the
    Permutation round-trips even though the original database can't be
    reopened across processes."""
    table = mem_db.create_table("test_table", pa.table({"a": range(50)}))
    permutation = Permutation.identity(table)

    restored = pickle.loads(pickle.dumps(permutation))

    assert len(restored) == 50
    assert restored.__getitems__([0, 10, 49]) == [{"a": 0}, {"a": 10}, {"a": 49}]


def test_permutation_dataloader_multiprocessing(tmp_db):
    """Using a Permutation with a PyTorch DataLoader that has num_workers > 0
    must work end-to-end. Each worker process gets a pickled copy of the
    dataset and reads batches from it."""
    table = tmp_db.create_table("test_table", pa.table({"a": range(1000)}))
    permutation = Permutation.identity(table)

    dataloader = torch.utils.data.DataLoader(
        permutation,
        batch_size=10,
        shuffle=True,
        num_workers=2,
        multiprocessing_context="spawn",
    )
    seen = 0
    for batch in dataloader:
        assert batch["a"].size(0) == 10
        seen += batch["a"].size(0)
    assert seen == 1000


def test_permutation_pickle_with_connection_factory(tmp_path):
    """When the user provides a connection_factory, pickling should round-trip
    through that factory rather than introspecting the connection URI. Useful
    for remote / cloud connections where the URI alone isn't reopenable."""
    db = lancedb.connect(tmp_path)
    db.create_table("test_table", pa.table({"a": range(50)}))

    factory = functools.partial(_open_native_table, str(tmp_path))
    permutation = Permutation.identity(factory("test_table")).with_connection_factory(
        factory
    )

    restored = pickle.loads(pickle.dumps(permutation))

    assert len(restored) == 50
    # The factory survives pickling and is what powered base-table reopen.
    assert restored.connection_factory is not None
    assert restored.connection_factory.func is _open_native_table
    assert restored.__getitems__([0, 1, 2]) == [{"a": 0}, {"a": 1}, {"a": 2}]


def test_permutation_with_builder_is_picklable(tmp_db):
    """A Permutation built from a non-identity permutation table must round-trip
    through pickle while preserving the row order defined by the permutation."""
    table = tmp_db.create_table("test_table", pa.table({"a": range(100)}))
    perm_tbl = (
        permutation_builder(table)
        .split_random(ratios=[0.8, 0.2], seed=42, split_names=["train", "test"])
        .shuffle(seed=42)
        .execute()
    )
    permutations = Permutations(table, perm_tbl)
    permutation = permutations["train"]

    indices = list(range(len(permutation)))
    expected = permutation.__getitems__(indices)

    restored = pickle.loads(pickle.dumps(permutation))

    assert len(restored) == len(permutation)
    assert restored.__getitems__(indices) == expected
