#  Copyright 2023 LanceDB Developers
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import pyarrow as pa
import os
import uuid
from lancedb.schema import vector

import lancedb
from lancedb.remote.client import VectorQuery, VectorQueryResult


class FakeLanceDBClient:
    async def close(self):
        pass

    async def query(self, table_name: str, query: VectorQuery) -> VectorQueryResult:
        assert table_name == "test"
        t = pa.schema([]).empty_table()
        return VectorQueryResult(t)


def test_remote_db():
    conn = lancedb.connect("db://client-will-be-injected", api_key="fake")
    setattr(conn, "_client", FakeLanceDBClient())

    table = conn["test"]
    table.search([1.0, 2.0]).to_pandas()


def test_list_tables():

    uri = os.environ.get("LANCE_URI")
    api_key = os.environ.get("LANCE_API_KEY")
    region = os.environ.get("LANCE_REGION")

    conn = lancedb.connect(uri=uri, api_key=api_key, region=region)
    table_name = uuid.uuid4().hex
    schema = pa.schema(
        [
            pa.field("id", pa.uint32(), False),
            pa.field("vector", vector(128), False),
            pa.field("s", pa.string(), False),
        ]
    )
    table = conn.create_table(
        table_name,
        schema=schema,
    )
    table.create_index()

    conn.drop_table(table_name)
