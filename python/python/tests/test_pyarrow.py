import pyarrow as pa

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

    # Make sure we bypass the limit
    data = pa.table({"x": range(100)})
    tbl = conn.create_table("test2", data)

    adapter = PyarrowDatasetAdapter(tbl)

    assert adapter.count_rows() == 100
    assert adapter.to_table().num_rows == 100
    assert adapter.head(10).num_rows == 10
