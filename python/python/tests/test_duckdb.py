import duckdb
import pyarrow as pa

import lancedb
from lancedb.integrations.pyarrow import PyarrowDatasetAdapter


def test_basic_query(tmp_path):
    data = pa.table({"x": [1, 2, 3, 4], "y": [5, 6, 7, 8]})
    conn = lancedb.connect(tmp_path)
    tbl = conn.create_table("test", data)

    adapter = PyarrowDatasetAdapter(tbl)  # noqa: F841

    duck_conn = duckdb.connect()

    results = duck_conn.sql("SELECT SUM(x) FROM adapter").fetchall()
    assert results[0][0] == 10

    results = duck_conn.sql("SELECT SUM(y) FROM adapter").fetchall()
    assert results[0][0] == 26
