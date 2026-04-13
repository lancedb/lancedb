# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import pyarrow as pa
import pytest
from lancedb.util import tbl_to_tensor
from lancedb.permutation import Permutation

torch = pytest.importorskip("torch")


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
