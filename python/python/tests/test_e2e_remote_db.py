# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors


import os
from datetime import timedelta
from uuid import uuid4

import lancedb
import numpy as np
import pyarrow as pa
import pytest
from lancedb import LanceDBConnection
from lancedb.index import FTS

# TODO: setup integ test mark and script


@pytest.mark.skipif(
    not os.environ.get("LANCEDB_URI"), reason="Set LANCEDB_URI to a JSON FTS server"
)
def test_json_fts_against_server():
    """Requires a writable database and capacity to run the distributed indexer.

    Configure LANCEDB_URI, LANCEDB_API_KEY, and optionally LANCEDB_REGION and
    LANCEDB_HOST_OVERRIDE before running this test.
    """
    uri = os.environ["LANCEDB_URI"]
    assert uri.startswith("db://"), "LANCEDB_URI must identify a remote database"
    conn = lancedb.connect(
        uri,
        api_key=os.environ.get("LANCEDB_API_KEY"),
        region=os.environ.get("LANCEDB_REGION", "us-east-1"),
        host_override=os.environ.get("LANCEDB_HOST_OVERRIDE"),
    )
    name = f"json_fts_{uuid4().hex}"
    table = conn.create_table(
        name,
        pa.table(
            {
                "id": [0, 1, 2],
                "doc": pa.array(
                    [
                        '{"meta":{"tag":"nature"},"author":"alice"}',
                        '{"meta":{"tag":"city"},"author":"nature"}',
                        '{"meta":{"tag":"nature"},"author":"bob"}',
                    ],
                    type=pa.json_(),
                ),
                "raw": pa.array([b"nature"] * 3, type=pa.large_binary()),
            }
        ),
    )
    try:
        table.create_index("doc", config=FTS(), name="doc_fts")
        table.wait_for_index(["doc_fts"], timeout=timedelta(minutes=5))
        stats = table.index_stats("doc_fts")
        assert stats.num_indexed_rows == 3
        assert stats.num_unindexed_rows == 0
        assert stats.index_type == "FTS"
        for query, expected in [
            ("meta.tag,str,nature", [0, 2]),
            ("meta.tag,str,alice", []),
            ("meta.tag,str,tag", []),
        ]:
            search = table.search(query, query_type="fts", fts_columns="doc")
            assert sorted(row["id"] for row in search.to_list()) == expected
            plan = search.explain_plan(True)
            assert "MatchQuery" in plan, plan
            assert "FlatMatchQuery" not in plan, plan
        with pytest.raises(ValueError, match="FTS.*lance.json.*raw binary"):
            table.create_index("raw", config=FTS())
    finally:
        conn.drop_table(name)


@pytest.mark.skip(reason="Need to set up a local server")
def test_against_local_server():
    conn = LanceDBConnection("lancedb+http://localhost:10024")
    table = conn.open_table("sift1m_ivf1024_pq16")
    df = table.search(np.random.rand(128)).to_pandas()
    assert len(df) == 10
