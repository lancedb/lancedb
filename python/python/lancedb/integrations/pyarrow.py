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
        if isinstance(self.filter, pa.compute.Expression):
            # Expression filters are applied to the scanned batches rather
            # than pushed into the query, so the count is computed the
            # same way. Summing batch row counts avoids retaining the
            # payloads just to return a scalar.
            return sum(batch.num_rows for batch in self.to_reader())
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
        expression = (
            self.filter if isinstance(self.filter, pa.compute.Expression) else None
        )
        sql_filter = self.filter if isinstance(self.filter, str) else None

        query = self.table.search()
        # Disable the builtin limit
        if limit is None:
            num_rows = self.table.count_rows(sql_filter)
            query.limit(num_rows)
        elif limit <= 0:
            raise ValueError("limit must be positive")
        elif expression is None:
            query.limit(limit)
        # With an expression filter the limit applies after filtering, so it
        # cannot be pushed into the query.
        if expression is None:
            if self.columns is not None:
                query = query.select(self.columns)
            if sql_filter is not None:
                query = query.where(sql_filter, prefilter=True)
            return query.to_batches(batch_size=self.batch_size)
        batches = query.to_batches(batch_size=self.batch_size)
        return self._filter_batches(batches, expression, limit)

    def _filter_batches(
        self, batches, expression: pa.compute.Expression, limit: Optional[int]
    ):
        # The expression can reference columns outside the projection, so
        # the scan keeps every column and the projection is applied to each
        # batch after filtering. The emitted schema comes from the scanned
        # batches rather than table.schema - storage layouts like blob-v2
        # emit descriptor structs and a _rowid column instead of the table
        # schema's extension types.
        emitted_schema = batches.schema
        schema = (
            pa.schema([emitted_schema.field(column) for column in self.columns])
            if self.columns is not None
            else emitted_schema
        )

        def gen():
            emitted = 0
            for batch in batches:
                # RecordBatch.filter only accepts an Expression on
                # pyarrow>=17; on older versions evaluate the predicate
                # through a dataset scanner so pyarrow>=16 works.
                try:
                    filtered = batch.filter(expression)
                except TypeError:
                    filtered_table = (
                        pa.dataset.dataset([batch])
                        .scanner(filter=expression)
                        .to_table()
                    )
                    filtered_batches = filtered_table.to_batches()
                else:
                    filtered_batches = [filtered]
                for filtered_batch in filtered_batches:
                    if self.columns is not None:
                        filtered_batch = filtered_batch.select(self.columns)
                    if limit is not None:
                        remaining = limit - emitted
                        if remaining <= 0:
                            return
                        emitted += filtered_batch.num_rows
                        filtered_batch = filtered_batch.slice(0, remaining)
                    yield filtered_batch

        return pa.RecordBatchReader.from_batches(schema, gen())


class PyarrowDatasetAdapter(pa.dataset.Dataset):
    def __init__(self, table: Table):
        self.table = table

    def count_rows(self, filter: Optional[Filter] = None):
        if isinstance(filter, pa.compute.Expression):
            return self.scanner(filter=filter).count_rows()
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
