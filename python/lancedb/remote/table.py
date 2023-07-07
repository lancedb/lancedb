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

from typing import Union
import asyncio

import pyarrow as paw

from lancedb.common import VEC, VECTOR_COLUMN_NAME

from ..table import Table, Query
from ..query import LanceQueryBuilder, Query
from .db import RemoteDBConnection


class RemoteTable(Table):
    def __init__(self, conn: RemoteDBConnection, name: str):
        self._conn = conn
        self._name = name

    def __repr__(self) -> str:
        return f"RemoteTable({self._conn.db_name}.{self.name})"

    def search(
        self, query: VEC | str, vector_column: str = VECTOR_COLUMN_NAME
    ) -> LanceQueryBuilder:
        return LanceQueryBuilder(self, query, vector_column)

    def _execute_query(self, query: Query) -> pa.Table:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.get_event_loop()
        result = self._table._conn._client.query(self._table.name, query)
