# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import pyarrow as pa
import pyarrow.compute as pc

import lancedb
from lancedb.integrations.pyarrow import PyarrowDatasetAdapter


def test_dataset_adapter(tmp_path):
    data = pa.table({"x": [1, 2, 3, 4], "y": [5, 6, 7, 8]})
    conn = lancedb.connect(tmp_path)
    tbl = conn.create_table("test", data)

    adapter = PyarrowDatasetAdapter(tbl)

    assert adapter.count_rows() == 4
    assert adapter.count_rows("x > 2") == 2
    assert adapter.schema == data.schema
    assert adapter.head(2) == data.slice(0, 2)
    assert adapter.to_table() == data
    assert adapter.to_batches().read_all() == data
    assert adapter.scanner().to_table() == data
    assert adapter.scanner().to_batches().read_all() == data

    assert adapter.scanner().projected_schema == data.schema
    assert adapter.scanner(columns=["x"]).projected_schema == pa.schema(
        [data.schema.field("x")]
    )
    assert adapter.scanner(columns=["x"]).to_table() == pa.table({"x": [1, 2, 3, 4]})

    # Make sure we bypass the limit
    data = pa.table({"x": range(100)})
    tbl = conn.create_table("test2", data)

    adapter = PyarrowDatasetAdapter(tbl)

    assert adapter.count_rows() == 100
    assert adapter.to_table().num_rows == 100
    assert adapter.head(10).num_rows == 10

    # Empty table
    tbl = conn.create_table("test3", None, schema=pa.schema({"x": pa.int64()}))
    adapter = PyarrowDatasetAdapter(tbl)

    assert adapter.count_rows() == 0
    assert adapter.to_table().num_rows == 0
    assert adapter.head(10).num_rows == 0

    assert adapter.scanner().projected_schema == pa.schema({"x": pa.int64()})


def test_dataset_adapter_expression_filter(tmp_path):
    data = pa.table({"x": [1, 2, 3, 4], "y": [5, 6, 7, 8]})
    conn = lancedb.connect(tmp_path)
    tbl = conn.create_table("test", data)

    adapter = PyarrowDatasetAdapter(tbl)
    filt = pc.field("x") > 2

    assert adapter.count_rows(filt) == 2
    assert adapter.to_table(filter=filt) == data.slice(2, 2)
    assert adapter.head(1, filter=filt) == data.slice(2, 1)
    # The filter can reference a column that is not projected
    assert adapter.to_table(columns=["y"], filter=filt) == pa.table({"y": [7, 8]})
    assert adapter.scanner(filter=filt).count_rows() == 2
