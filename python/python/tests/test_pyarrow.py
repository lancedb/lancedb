import pyarrow as pa

import lancedb
from lancedb.integrations.pyarrow import PyarrowDatasetAdapter


def test_dataset_adapter(tmp_path):
    data = pa.table({"x": [1, 2, 3, 4], "y": [5, 6, 7, 8]})
    conn = lancedb.connect(tmp_path)
    tbl = conn.create_table("test", data)

    adapter = PyarrowDatasetAdapter(tbl)  # noqa: F841

    assert adapter.count_rows() == 4
    assert adapter.count_rows("x > 2") == 2
    assert adapter.schema == data.schema
    assert adapter.head(2) == data.slice(0, 2)
    assert adapter.to_table() == data
    assert adapter.to_batches().read_all() == data
    assert adapter.scanner().to_table() == data
    assert adapter.scanner().to_batches().read_all() == data
