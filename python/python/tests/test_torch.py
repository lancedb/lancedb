# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import pyarrow as pa
import pytest

torch = pytest.importorskip("torch")

from lancedb._lancedb import async_permutation_builder


def tbl_to_tensor(tbl):
    def to_tensor(col: pa.ChunkedArray):
        if col.num_chunks > 1:
            raise Exception("Single batch was too large to fit into a one-chunk table")
        return torch.from_dlpack(col.chunk(0))

    return torch.stack([to_tensor(tbl.column(i)) for i in range(tbl.num_columns)])


def test_table_dataloader(mem_db):
    table = mem_db.create_table("test_table", pa.table({"a": range(1000)}))
    dataloader = torch.utils.data.DataLoader(
        table, collate_fn=tbl_to_tensor, batch_size=10, shuffle=True
    )
    for batch in dataloader:
        assert batch.size(0) == 1
        assert batch.size(1) == 10
