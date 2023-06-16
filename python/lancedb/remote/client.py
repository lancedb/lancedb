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


import functools
import urllib.parse

import aiohttp
import attr
import pyarrow as pa

from lancedb.remote import VectorQuery, VectorQueryResult
from lancedb.remote.errors import LanceDBClientError


def _check_not_closed(f):
    @functools.wraps(f)
    def wrapped(self, *args, **kwargs):
        if self.closed:
            raise ValueError("Connection is closed")
        return f(self, *args, **kwargs)

    return wrapped


@attr.define(slots=False)
class RestfulLanceDBClient:
    url: str
    closed: bool = attr.field(default=False, init=False)

    @functools.cached_property
    def session(self) -> aiohttp.ClientSession:
        parsed = urllib.parse.urlparse(self.url)
        scheme = parsed.scheme
        if not scheme.startswith("lancedb"):
            raise ValueError(
                f"Invalid scheme: {scheme}, must be like lancedb+<flavor>://"
            )
        flavor = scheme.split("+")[1]
        url = f"{flavor}://{parsed.hostname}:{parsed.port}"
        return aiohttp.ClientSession(url)

    async def close(self):
        await self.session.close()
        self.closed = True

    @_check_not_closed
    async def query(self, table_name: str, query: VectorQuery) -> VectorQueryResult:
        async with self.session.post(
            f"/table/{table_name}/", json=query.dict(exclude_none=True)
        ) as resp:
            resp: aiohttp.ClientResponse = resp
            if 400 <= resp.status < 500:
                raise LanceDBClientError(
                    f"Bad Request: {resp.status}, error: {await resp.text()}"
                )
            if 500 <= resp.status < 600:
                raise LanceDBClientError(
                    f"Internal Server Error: {resp.status}, error: {await resp.text()}"
                )
            if resp.status != 200:
                raise LanceDBClientError(
                    f"Unknown Error: {resp.status}, error: {await resp.text()}"
                )

            resp_body = await resp.read()
            with pa.ipc.open_file(pa.BufferReader(resp_body)) as reader:
                tbl = reader.read_all()
        return VectorQueryResult(tbl)
