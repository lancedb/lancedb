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
from __future__ import annotations

import numpy as np
import pandas as pd

from .common import VECTOR_COLUMN_NAME


class LanceQueryBuilder:
    """
    A builder for nearest neighbor queries for LanceDB.
    """

    def __init__(self, table: "lancedb.table.LanceTable", query: np.ndarray):
        self._table = table
        self._query = query
        self._limit = 10
        self._columns = None
        self._where = None

    def limit(self, limit: int) -> LanceQueryBuilder:
        """Set the maximum number of results to return.

        Parameters
        ----------
        limit: int
            The maximum number of results to return.

        Returns
        -------
        The LanceQueryBuilder object.
        """
        self._limit = limit
        return self

    def select(self, columns: list) -> LanceQueryBuilder:
        """Set the columns to return.

        Parameters
        ----------
        columns: list
            The columns to return.

        Returns
        -------
        The LanceQueryBuilder object.
        """
        self._columns = columns
        return self

    def where(self, where: str) -> LanceQueryBuilder:
        """Set the where clause.

        Parameters
        ----------
        where: str
            The where clause.

        Returns
        -------
        The LanceQueryBuilder object.
        """
        self._where = where
        return self

    def to_df(self) -> pd.DataFrame:
        """Execute the query and return the results as a pandas DataFrame."""
        ds = self._table.to_lance()
        # TODO indexed search
        tbl = ds.to_table(
            columns=self._columns,
            filter=self._where,
            nearest={"column": VECTOR_COLUMN_NAME, "q": self._query, "k": self._limit},
        )
        return tbl.to_pandas()
