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

import uuid
from functools import cached_property
from typing import Union

import pyarrow as pa
from lance import json_to_schema

from lancedb.common import DATA, VEC, VECTOR_COLUMN_NAME

from ..query import LanceVectorQueryBuilder
from ..table import Query, Table, _sanitize_data
from .arrow import to_ipc_binary
from .client import ARROW_STREAM_CONTENT_TYPE
from .db import RemoteDBConnection


class RemoteTable(Table):
    def __init__(self, conn: RemoteDBConnection, name: str):
        self._conn = conn
        self._name = name

    def __repr__(self) -> str:
        return f"RemoteTable({self._conn.db_name}.{self._name})"

    @cached_property
    def schema(self) -> pa.Schema:
        """Return the schema of the table."""
        resp = self._conn._loop.run_until_complete(
            self._conn._client.post(f"/v1/table/{self._name}/describe/")
        )
        schema = json_to_schema(resp["schema"])
        return schema

    def to_arrow(self) -> pa.Table:
        """Return the table as an Arrow table."""
        raise NotImplementedError("to_arrow() is not supported on the LanceDB cloud")

    def to_pandas(self):
        """Return the table as a Pandas DataFrame.

        Intercept `to_arrow()` for better error message.
        """
        return NotImplementedError("to_pandas() is not supported on the LanceDB cloud")

    def create_index(
        self,
        metric="L2",
        num_partitions=256,
        num_sub_vectors=96,
        vector_column_name: str = VECTOR_COLUMN_NAME,
        replace: bool = True,
    ):
        raise NotImplementedError

    def add(
        self,
        data: DATA,
        mode: str = "append",
        on_bad_vectors: str = "error",
        fill_value: float = 0.0,
    ) -> int:
        data = _sanitize_data(
            data,
            self.schema,
            metadata=None,
            on_bad_vectors=on_bad_vectors,
            fill_value=fill_value,
        )
        payload = to_ipc_binary(data)

        request_id = uuid.uuid4().hex

        self._conn._loop.run_until_complete(
            self._conn._client.post(
                f"/v1/table/{self._name}/insert/",
                data=payload,
                params={"request_id": request_id, "mode": mode},
                content_type=ARROW_STREAM_CONTENT_TYPE,
            )
        )

    def search(
        self, query: Union[VEC, str], vector_column_name: str = VECTOR_COLUMN_NAME
    ) -> LanceVectorQueryBuilder:
        return LanceVectorQueryBuilder(self, query, vector_column_name)

    def _execute_query(self, query: Query) -> pa.Table:
        result = self._conn._client.query(self._name, query)
        return self._conn._loop.run_until_complete(result).to_arrow()

    def delete(self, predicate: str):
        raise NotImplementedError
