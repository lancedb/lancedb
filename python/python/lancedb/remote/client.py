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
import logging
import os
from typing import Any, Callable, Dict, List, Optional, Union
from urllib.parse import urljoin

import attrs
import pyarrow as pa
import requests
from pydantic import BaseModel
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from lancedb.common import Credential
from lancedb.remote import VectorQuery, VectorQueryResult
from lancedb.remote.connection_timeout import LanceDBClientHTTPAdapterFactory
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
        sess = requests.Session()

        retry_adapter_instance = retry_adapter(retry_adapter_options())
        sess.mount(urljoin(self.url, "/v1/table/"), retry_adapter_instance)

        adapter_class = LanceDBClientHTTPAdapterFactory()
        sess.mount("https://", adapter_class())
        return sess

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
            timeout=(120.0, 300.0),
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
            timeout=(120.0, 300.0),
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

    def mount_retry_adapter_for_table(self, table_name: str) -> None:
        """
        Adds an http adapter to session that will retry retryable requests to the table.
        """
        retry_options = retry_adapter_options(methods=["GET", "POST"])
        retry_adapter_instance = retry_adapter(retry_options)
        session = self.session

        session.mount(
            urljoin(self.url, f"/v1/table/{table_name}/query/"), retry_adapter_instance
        )
        session.mount(
            urljoin(self.url, f"/v1/table/{table_name}/describe/"),
            retry_adapter_instance,
        )
        session.mount(
            urljoin(self.url, f"/v1/table/{table_name}/index/list/"),
            retry_adapter_instance,
        )


def retry_adapter_options(methods=["GET"]) -> Dict[str, Any]:
    return {
        "retries": int(os.environ.get("LANCE_CLIENT_MAX_RETRIES", "3")),
        "connect_retries": int(os.environ.get("LANCE_CLIENT_CONNECT_RETRIES", "3")),
        "read_retries": int(os.environ.get("LANCE_CLIENT_READ_RETRIES", "3")),
        "backoff_factor": float(
            os.environ.get("LANCE_CLIENT_RETRY_BACKOFF_FACTOR", "0.25")
        ),
        "backoff_jitter": float(
            os.environ.get("LANCE_CLIENT_RETRY_BACKOFF_JITTER", "0.25")
        ),
        "statuses": [
            int(i.strip())
            for i in os.environ.get(
                "LANCE_CLIENT_RETRY_STATUSES", "429, 500, 502, 503"
            ).split(",")
        ],
        "methods": methods,
    }


def retry_adapter(options: Dict[str, Any]) -> HTTPAdapter:
    total_retries = options["retries"]
    connect_retries = options["connect_retries"]
    read_retries = options["read_retries"]
    backoff_factor = options["backoff_factor"]
    backoff_jitter = options["backoff_jitter"]
    statuses = options["statuses"]
    methods = frozenset(options["methods"])
    logging.debug(
        f"Setting up retry adapter with {total_retries} retries,"  # noqa G003
        + f"connect retries {connect_retries}, read retries {read_retries},"
        + f"backoff factor {backoff_factor}, statuses {statuses}, "
        + f"methods {methods}"
    )

    return HTTPAdapter(
        max_retries=Retry(
            total=total_retries,
            connect=connect_retries,
            read=read_retries,
            backoff_factor=backoff_factor,
            backoff_jitter=backoff_jitter,
            status_forcelist=statuses,
            allowed_methods=methods,
        )
    )
