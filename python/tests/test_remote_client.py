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

import attr
import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
from aiohttp import web

from lancedb.remote.client import RestfulLanceDBClient, VectorQuery


@attr.define
class MockLanceDBServer:
    runner: web.AppRunner = attr.field(init=False)
    site: web.TCPSite = attr.field(init=False)

    async def query_handler(self, request: web.Request) -> web.Response:
        table_name = request.match_info["table_name"]
        assert table_name == "test_table"

        await request.json()
        # TODO: do some matching

        vecs = pd.Series([np.random.rand(128) for x in range(10)], name="vector")
        ids = pd.Series(range(10), name="id")
        df = pd.DataFrame([vecs, ids]).T

        batch = pa.RecordBatch.from_pandas(
            df,
            schema=pa.schema(
                [
                    pa.field("vector", pa.list_(pa.float32(), 128)),
                    pa.field("id", pa.int64()),
                ]
            ),
        )

        sink = pa.BufferOutputStream()
        with pa.ipc.new_file(sink, batch.schema) as writer:
            writer.write_batch(batch)

        return web.Response(body=sink.getvalue().to_pybytes())

    async def setup(self):
        app = web.Application()
        app.add_routes([web.post("/table/{table_name}", self.query_handler)])
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, "localhost", 8111)

    async def start(self):
        await self.site.start()

    async def stop(self):
        await self.runner.cleanup()


@pytest.mark.skip(reason="flaky somehow, fix later")
@pytest.mark.asyncio
async def test_e2e_with_mock_server():
    mock_server = MockLanceDBServer()
    await mock_server.setup()
    await mock_server.start()

    try:
        client = RestfulLanceDBClient("lancedb+http://localhost:8111")
        df = (
            await client.query(
                "test_table",
                VectorQuery(
                    vector=np.random.rand(128).tolist(),
                    k=10,
                    _metric="L2",
                    columns=["id", "vector"],
                ),
            )
        ).to_df()

        assert "vector" in df.columns
        assert "id" in df.columns
    finally:
        # make sure we don't leak resources
        await mock_server.stop()
