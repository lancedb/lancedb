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

import lancedb
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
