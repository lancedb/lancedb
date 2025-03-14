# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import logging
from typing import Any, List, Optional, Tuple, Union, Literal

import pyarrow as pa
import pyarrow.dataset

from ..table import Table

Filter = Union[str, pa.compute.Expression]
Keys = Union[str, List[str]]
JoinType = Literal[
    "left semi",
    "right semi",
    "left anti",
    "right anti",
    "inner",
    "left outer",
    "right outer",
    "full outer",
]


class PyarrowScannerAdapter(pa.dataset.Scanner):
    def __init__(
        self,
        table: Table,
        columns: Optional[List[str]] = None,
        filter: Optional[Filter] = None,
        batch_size: Optional[int] = None,
        batch_readahead: Optional[int] = None,
        fragment_readahead: Optional[int] = None,
        fragment_scan_options: Optional[Any] = None,
        use_threads: bool = True,
        memory_pool: Optional[Any] = None,
    ):
        self.table = table
        self.columns = columns
        self.filter = filter
        self.batch_size = batch_size
        if batch_readahead is not None:
            logging.debug("ignoring batch_readahead which has no lance equivalent")
        if fragment_readahead is not None:
            logging.debug("ignoring fragment_readahead which has no lance equivalent")
        if fragment_scan_options is not None:
            raise NotImplementedError("fragment_scan_options not supported")
        if use_threads is False:
            raise NotImplementedError("use_threads=False not supported")
        if memory_pool is not None:
            raise NotImplementedError("memory_pool not supported")

    def count_rows(self):
        return self.table.count_rows(self.filter)

    def from_batches(self, **kwargs):
        raise NotImplementedError

    def from_dataset(self, **kwargs):
        raise NotImplementedError

    def from_fragment(self, **kwargs):
        raise NotImplementedError

    def head(self, num_rows: int):
        return self.to_reader(limit=num_rows).read_all()

    @property
    def projected_schema(self):
        return self.head(1).schema

    def scan_batches(self):
        return self.to_reader()

    def take(self, indices: List[int]):
        raise NotImplementedError

    def to_batches(self):
        return self.to_reader()

    def to_table(self):
        return self.to_reader().read_all()

    def to_reader(self, *, limit: Optional[int] = None):
        query = self.table.search()
        # Disable the builtin limit
        if limit is None:
            num_rows = self.count_rows()
            query.limit(num_rows)
        elif limit <= 0:
            raise ValueError("limit must be positive")
        else:
            query.limit(limit)
        if self.columns is not None:
            query = query.select(self.columns)
        if self.filter is not None:
            query = query.where(self.filter, prefilter=True)
        return query.to_batches(batch_size=self.batch_size)


class PyarrowDatasetAdapter(pa.dataset.Dataset):
    def __init__(self, table: Table):
        self.table = table

    def count_rows(self, filter: Optional[Filter] = None):
        return self.table.count_rows(filter)

    def get_fragments(self, filter: Optional[Filter] = None):
        raise NotImplementedError

    def head(
        self,
        num_rows: int,
        columns: Optional[List[str]] = None,
        filter: Optional[Filter] = None,
        batch_size: Optional[int] = None,
        batch_readahead: Optional[int] = None,
        fragment_readahead: Optional[int] = None,
        fragment_scan_options: Optional[Any] = None,
        use_threads: bool = True,
        memory_pool: Optional[Any] = None,
    ):
        return self.scanner(
            columns,
            filter,
            batch_size,
            batch_readahead,
            fragment_readahead,
            fragment_scan_options,
            use_threads,
            memory_pool,
        ).head(num_rows)

    def join(
        self,
        right_dataset: Any,
        keys: Keys,
        right_keys: Optional[Keys] = None,
        join_type: Optional[JoinType] = None,
        left_suffix: Optional[str] = None,
        right_suffix: Optional[str] = None,
        coalesce_keys: bool = True,
        use_threads: bool = True,
    ):
        raise NotImplementedError

    def join_asof(
        self,
        right_dataset: Any,
        on: str,
        by: Keys,
        tolerance: int,
        right_on: Optional[str] = None,
        right_by: Optional[Keys] = None,
    ):
        raise NotImplementedError

    @property
    def partition_expression(self):
        raise NotImplementedError

    def replace_schema(self, schema: pa.Schema):
        raise NotImplementedError

    def scanner(
        self,
        columns: Optional[List[str]] = None,
        filter: Optional[Filter] = None,
        batch_size: Optional[int] = None,
        batch_readahead: Optional[int] = None,
        fragment_readahead: Optional[int] = None,
        fragment_scan_options: Optional[Any] = None,
        use_threads: bool = True,
        memory_pool: Optional[Any] = None,
    ):
        return PyarrowScannerAdapter(
            self.table,
            columns,
            filter,
            batch_size,
            batch_readahead,
            fragment_readahead,
            fragment_scan_options,
            use_threads,
            memory_pool,
        )

    @property
    def schema(self):
        return self.table.schema

    def sort_by(self, sorting: Union[str, List[Tuple[str, bool]]]):
        raise NotImplementedError

    def take(
        self,
        indices: List[int],
        columns: Optional[List[str]] = None,
        filter: Optional[Filter] = None,
        batch_size: Optional[int] = None,
        batch_readahead: Optional[int] = None,
        fragment_readahead: Optional[int] = None,
        fragment_scan_options: Optional[Any] = None,
        use_threads: bool = True,
        memory_pool: Optional[Any] = None,
    ):
        raise NotImplementedError

    def to_batches(
        self,
        columns: Optional[List[str]] = None,
        filter: Optional[Filter] = None,
        batch_size: Optional[int] = None,
        batch_readahead: Optional[int] = None,
        fragment_readahead: Optional[int] = None,
        fragment_scan_options: Optional[Any] = None,
        use_threads: bool = True,
        memory_pool: Optional[Any] = None,
    ):
        return self.scanner(
            columns,
            filter,
            batch_size,
            batch_readahead,
            fragment_readahead,
            fragment_scan_options,
            use_threads,
            memory_pool,
        ).to_batches()

    def to_table(
        self,
        columns: Optional[List[str]] = None,
        filter: Optional[Filter] = None,
        batch_size: Optional[int] = None,
        batch_readahead: Optional[int] = None,
        fragment_readahead: Optional[int] = None,
        fragment_scan_options: Optional[Any] = None,
        use_threads: bool = True,
        memory_pool: Optional[Any] = None,
    ):
        return self.scanner(
            columns,
            filter,
            batch_size,
            batch_readahead,
            fragment_readahead,
            fragment_scan_options,
            use_threads,
            memory_pool,
        ).to_table()
