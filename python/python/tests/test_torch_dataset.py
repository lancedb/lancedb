# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import pickle

import pyarrow as pa
import pytest

from lancedb import connect
from lancedb.permutation import permutation_builder

torch = pytest.importorskip("torch")
from lancedb.integrations.torch import (  # noqa: E402
    LanceIterableTorchDataset,
    LanceTorchDataset,
)


@pytest.fixture
def db_path(tmp_path):
    """LanceTorchDataset needs a real, on-disk DB so workers can re-open it."""
    return tmp_path


def _make_table(db_path, name="imgs", n=20):
    db = connect(db_path)
    return db.create_table(
        name,
        pa.table({"x": [float(i) for i in range(n)], "y": list(range(n))}),
    )


def test_basic_len_and_getitem(db_path):
    tbl = _make_table(db_path)
    ds = LanceTorchDataset(tbl)
    assert len(ds) == 20
    row = ds[0]
    # Default ("python") format = list of dicts; __getitem__ wraps a single index.
    assert isinstance(row, list)
    assert row[0] == {"x": 0.0, "y": 0}


def test_getitems_uses_fetch(db_path):
    tbl = _make_table(db_path)
    ds = LanceTorchDataset(tbl)
    rows = ds.__getitems__([0, 2, 4])
    assert rows == [
        {"x": 0.0, "y": 0},
        {"x": 2.0, "y": 2},
        {"x": 4.0, "y": 4},
    ]


def test_dataloader_default_collate(db_path):
    tbl = _make_table(db_path, n=40)
    ds = LanceTorchDataset(tbl)
    loader = torch.utils.data.DataLoader(ds, batch_size=8, shuffle=False)
    batch = next(iter(loader))
    # default collate stacks list-of-dicts into dict-of-tensors
    assert isinstance(batch, dict)
    assert batch["x"].size() == (8,)
    assert batch["y"].size() == (8,)


def test_picklable(db_path):
    tbl = _make_table(db_path)
    ds = LanceTorchDataset(tbl, columns=["x"])

    # Force open then ensure pickle drops the Rust handle.
    _ = len(ds)
    blob = pickle.dumps(ds)
    restored: LanceTorchDataset = pickle.loads(blob)
    # Rust state should not survive pickling.
    assert restored._perm is None
    # …but the dataset must work after re-opening transparently.
    assert len(restored) == 20
    assert restored[0] == [{"x": 0.0}]


def test_dataloader_with_workers(db_path):
    tbl = _make_table(db_path, n=32)
    ds = LanceTorchDataset(tbl)
    loader = torch.utils.data.DataLoader(
        ds, batch_size=4, num_workers=2, shuffle=False
    )
    batches = list(loader)
    seen = []
    for b in batches:
        seen.extend(b["x"].tolist())
    assert sorted(seen) == [float(i) for i in range(32)]


def test_with_permutation_table(db_path):
    tbl = _make_table(db_path, n=30)
    db = connect(db_path)
    perm_tbl = (
        permutation_builder(tbl)
        .split_random(ratios=[0.5, 0.5], seed=1, split_names=["train", "test"])
        .persist(db, "imgs_perm")
        .execute()
    )
    ds = LanceTorchDataset(tbl, permutation_table=perm_tbl, split="train")
    # Should pickle/restore the permutation table reference too.
    blob = pickle.dumps(ds)
    restored = pickle.loads(blob)
    assert len(restored) == 15


def test_format_passthrough_dataloader(db_path):
    """Custom `format` is forwarded to the underlying Permutation."""
    tbl = _make_table(db_path, n=20)
    ds = LanceTorchDataset(tbl, format="arrow")
    # Arrow batches don't go through default_collate, so use a no-op collate.
    loader = torch.utils.data.DataLoader(
        ds, batch_size=5, shuffle=False, collate_fn=lambda x: x
    )
    batch = next(iter(loader))
    assert isinstance(batch, pa.RecordBatch)
    assert batch.num_rows == 5


def test_iterable_dataset(db_path):
    tbl = _make_table(db_path, n=20)
    ds = LanceIterableTorchDataset(tbl, batch_size=5)
    batches = list(ds)
    # default batch size + skip_last_batch=True yields full-size batches only
    assert len(batches) == 4
    assert all(len(b) == 5 for b in batches)


def test_uri_table_name_constructor(db_path):
    _make_table(db_path)
    ds = LanceTorchDataset(uri=str(db_path), table_name="imgs")
    assert len(ds) == 20
    assert ds[0] == [{"x": 0.0, "y": 0}]


def test_constructor_validates_args():
    with pytest.raises(ValueError, match="table"):
        LanceTorchDataset()
