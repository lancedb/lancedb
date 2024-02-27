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

import abc
from typing import List, Optional

import attrs
import pyarrow as pa
from pydantic import BaseModel

from lancedb.common import VECTOR_COLUMN_NAME

__all__ = ["LanceDBClient", "VectorQuery", "VectorQueryResult"]


class VectorQuery(BaseModel):
    # vector to search for
    vector: List[float]

    # sql filter to refine the query with
    filter: Optional[str] = None

    # top k results to return
    k: int

    # # metrics
    _metric: str = "L2"

    # which columns to return in the results
    columns: Optional[List[str]] = None

    # optional query parameters for tuning the results,
    # e.g. `{"nprobes": "10", "refine_factor": "10"}`
    nprobes: int = 10

    refine_factor: Optional[int] = None

    vector_column: str = VECTOR_COLUMN_NAME


@attrs.define
class VectorQueryResult:
    # for now the response is directly seralized into a pandas dataframe
    tbl: pa.Table

    def to_arrow(self) -> pa.Table:
        return self.tbl


class LanceDBClient(abc.ABC):
    @abc.abstractmethod
    def query(self, table_name: str, query: VectorQuery) -> VectorQueryResult:
        """Query the LanceDB server for the given table and query."""
        pass
