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
from typing import Any, Callable, Dict, Iterable, List, Optional, Union
from urllib.parse import urljoin

import requests
import attrs
import pyarrow as pa
from pydantic import BaseModel

from lancedb.common import Credential
from lancedb.remote import VectorQuery, VectorQueryResult
from lancedb.remote.errors import LanceDBClientError

ARROW_STREAM_CONTENT_TYPE = "application/vnd.apache.arrow.stream"


def _check_not_closed(f):
    @functools.wraps(f)
    def wrapped(self, *args, **kwargs):
        if self.closed:
            raise ValueError("Connection is closed")
        return f(self, *args, **kwargs)

    return wrapped


def _read_ipc(resp: requests.Response) -> pa.Table:
    resp_body = resp.content
    with pa.ipc.open_file(pa.BufferReader(resp_body)) as reader:
        return reader.read_all()


@attrs.define(slots=False)
class RestfulLanceDBClient:
    db_name: str
    region: str
    api_key: Credential
    host_override: Optional[str] = attrs.field(default=None)

    closed: bool = attrs.field(default=False, init=False)

    @functools.cached_property
    def session(self) -> requests.Session:
        return requests.Session()

    @property
    def url(self) -> str:
        return (
            self.host_override
            or f"https://{self.db_name}.{self.region}.api.lancedb.com"
        )

    def close(self):
        self.session.close()
        self.closed = True

    @functools.cached_property
    def headers(self) -> Dict[str, str]:
        headers = {
            "x-api-key": self.api_key,
        }
        if self.region == "local":  # Local test mode
            headers["Host"] = f"{self.db_name}.{self.region}.api.lancedb.com"
        if self.host_override:
            headers["x-lancedb-database"] = self.db_name
        return headers

    @staticmethod
    def _check_status(resp: requests.Response):
        if resp.status_code == 404:
            raise LanceDBClientError(f"Not found: {resp.text}")
        elif 400 <= resp.status_code < 500:
            raise LanceDBClientError(
                f"Bad Request: {resp.status_code}, error: {resp.text}"
            )
        elif 500 <= resp.status_code < 600:
            raise LanceDBClientError(
                f"Internal Server Error: {resp.status_code}, error: {resp.text}"
            )
        elif resp.status_code != 200:
            raise LanceDBClientError(
                f"Unknown Error: {resp.status_code}, error: {resp.text}"
            )

    @_check_not_closed
    def get(self, uri: str, params: Union[Dict[str, Any], BaseModel] = None):
        """Send a GET request and returns the deserialized response payload."""
        if isinstance(params, BaseModel):
            params: Dict[str, Any] = params.dict(exclude_none=True)
        with self.session.get(
            urljoin(self.url, uri),
            params=params,
            headers=self.headers,
            timeout=(5.0, 30.0),
        ) as resp:
            self._check_status(resp)
            return resp.json()

    @_check_not_closed
    def post(
        self,
        uri: str,
        data: Optional[Union[Dict[str, Any], BaseModel, bytes]] = None,
        params: Optional[Dict[str, Any]] = None,
        content_type: Optional[str] = None,
        deserialize: Callable = lambda resp: resp.json(),
        request_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Send a POST request and returns the deserialized response payload.

        Parameters
        ----------
        uri : str
            The uri to send the POST request to.
        data: Union[Dict[str, Any], BaseModel]
        request_id: Optional[str]
            Optional client side request id to be sent in the request headers.

        """
        if isinstance(data, BaseModel):
            data: Dict[str, Any] = data.dict(exclude_none=True)
        if isinstance(data, bytes):
            req_kwargs = {"data": data}
        else:
            req_kwargs = {"json": data}

        headers = self.headers.copy()
        if content_type is not None:
            headers["content-type"] = content_type
        if request_id is not None:
            headers["x-request-id"] = request_id
        with self.session.post(
            urljoin(self.url, uri),
            headers=headers,
            params=params,
            timeout=(5.0, 30.0),
            **req_kwargs,
        ) as resp:
            self._check_status(resp)
            return deserialize(resp)

    @_check_not_closed
    def list_tables(self, limit: int, page_token: Optional[str] = None) -> List[str]:
        """List all tables in the database."""
        if page_token is None:
            page_token = ""
        json = self.get("/v1/table/", {"limit": limit, "page_token": page_token})
        return json["tables"]

    @_check_not_closed
    def query(self, table_name: str, query: VectorQuery) -> VectorQueryResult:
        """Query a table."""
        tbl = self.post(f"/v1/table/{table_name}/query/", query, deserialize=_read_ipc)
        return VectorQueryResult(tbl)
