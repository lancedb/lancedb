# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

from unittest.mock import MagicMock

import lancedb
import pyarrow as pa
from lancedb.remote.client import VectorQuery, VectorQueryResult


class FakeLanceDBClient:
    def close(self):
        pass

    def query(self, table_name: str, query: VectorQuery) -> VectorQueryResult:
        assert table_name == "test"
        t = pa.schema([]).empty_table()
        return VectorQueryResult(t)

    def post(self, path: str):
        pass

    def mount_retry_adapter_for_table(self, table_name: str):
        pass


def test_remote_db():
    conn = lancedb.connect("db://client-will-be-injected", api_key="fake")
    setattr(conn, "_client", FakeLanceDBClient())

    table = conn["test"]
    table.schema = pa.schema([pa.field("vector", pa.list_(pa.float32(), 2))])
    table.search([1.0, 2.0]).to_pandas()


def test_create_empty_table():
    client = MagicMock()
    conn = lancedb.connect("db://client-will-be-injected", api_key="fake")

    conn._client = client

    schema = pa.schema([pa.field("vector", pa.list_(pa.float32(), 2))])

    client.post.return_value = {"status": "ok"}
    table = conn.create_table("test", schema=schema)
    assert table.name == "test"
    assert client.post.call_args[0][0] == "/v1/table/test/create/"

    json_schema = {
        "fields": [
            {
                "name": "vector",
                "nullable": True,
                "type": {
                    "type": "fixed_size_list",
                    "fields": [
                        {"name": "item", "nullable": True, "type": {"type": "float"}}
                    ],
                    "length": 2,
                },
            },
        ]
    }
    client.post.return_value = {"schema": json_schema}
    assert table.schema == schema
    assert client.post.call_args[0][0] == "/v1/table/test/describe/"

    client.post.return_value = 0
    assert table.count_rows(None) == 0
