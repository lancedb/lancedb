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

import asyncio
from typing import Union

import pyarrow as pa

from lancedb.common import DATA, VEC, VECTOR_COLUMN_NAME

from ..query import LanceQueryBuilder, Query
from ..table import Query, Table
from .db import RemoteDBConnection


class RemoteTable(Table):
    def __init__(self, conn: RemoteDBConnection, name: str):
        self._conn = conn
        self._name = name

    def __repr__(self) -> str:
        return f"RemoteTable({self._conn.db_name}.{self.name})"

    def schema(self) -> pa.Schema:
        raise NotImplementedError

    def to_arrow(self) -> pa.Table:
        raise NotImplementedError

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
        raise NotImplementedError

    def search(
        self, query: Union[VEC, str], vector_column: str = VECTOR_COLUMN_NAME
    ) -> LanceQueryBuilder:
        return LanceQueryBuilder(self, query, vector_column)

    def _execute_query(self, query: Query) -> pa.Table:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.get_event_loop()
        result = self._conn._client.query(self._name, query)
        return loop.run_until_complete(result).to_arrow()
