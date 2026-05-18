# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import copy
import json

from deprecation import deprecated
import pyarrow as pa

from ._lancedb import async_permutation_builder, PermutationReader
from .table import LanceTable
from .background_loop import LOOP
from .util import batch_to_tensor, batch_to_tensor_dict, batch_to_tensor_rows
from typing import Any, Callable, Iterator, Literal, Optional, TYPE_CHECKING, Union

if TYPE_CHECKING:
    from lancedb.dependencies import pandas as pd, numpy as np, polars as pl


class PermutationBuilder:
    """
    A utility for creating a "permutation table" which is a table that defines an
    ordering on a base table.

    The permutation table does not store the actual data.  It only stores row
    ids and split ids to define the ordering.  The [Permutation] class can be used to
    read the data from the base table in the order defined by the permutation table.

    Permutations can split, shuffle, and filter the data in the base table.

    A filter limits the rows that are included in the permutation.
    Splits divide the data into subsets (for example, a test/train split, or K
    different splits for cross-validation).
    Shuffling randomizes the order of the rows in the permutation.

    Splits can optionally be named.  If names are provided it will enable them to
    be referenced by name in the future.  If names are not provided then they can only
    be referenced by their ordinal index.  There is no requirement to name every split.

    The permutation is stored in memory and will be lost when the program exits.
    """

    def __init__(self, table: LanceTable):
        """
        Creates a new permutation builder for the given table.

        By default, the permutation builder will create a single split that contains all
        rows in the same order as the base table.
        """
        self._async = async_permutation_builder(table)

    def split_random(
        self,
        *,
        ratios: Optional[list[float]] = None,
        counts: Optional[list[int]] = None,
        fixed: Optional[int] = None,
        seed: Optional[int] = None,
        split_names: Optional[list[str]] = None,
    ) -> "PermutationBuilder":
        """
        Configure random splits for the permutation.

        One of ratios, counts, or fixed must be provided.

        If ratios are provided, they will be used to determine the relative size of each
        split. For example, if ratios are [0.3, 0.7] then the first split will contain
        30% of the rows and the second split will contain 70% of the rows.

        If counts are provided, they will be used to determine the absolute number of
        rows in each split. For example, if counts are [100, 200] then the first split
        will contain 100 rows and the second split will contain 200 rows.

        If fixed is provided, it will be used to determine the number of splits.
        For example, if fixed is 3 then the permutation will be split evenly into 3
        splits.

        Rows will be randomly assigned to splits.  The optional seed can be provided to
        make the assignment deterministic.

        The optional split_names can be provided to name the splits.  If not provided,
        the splits can only be referenced by their index.
        """
        self._async.split_random(
            ratios=ratios,
            counts=counts,
            fixed=fixed,
            seed=seed,
            split_names=split_names,
        )
        return self

    def split_hash(
        self,
        columns: list[str],
        split_weights: list[int],
        *,
        discard_weight: Optional[int] = None,
        split_names: Optional[list[str]] = None,
    ) -> "PermutationBuilder":
        """
        Configure hash-based splits for the permutation.

        First, a hash will be calculated over the specified columns.  The splits weights
        are then used to determine how many rows to assign to each split.  For example,
        if split weights are [1, 2] then the first split will contain 1/3 of the rows
        and the second split will contain 2/3 of the rows.

        The optional discard weight can be provided to determine what percentage of rows
        should be discarded.  For example, if split weights are [1, 2] and discard
        weight is 1 then 25% of the rows will be discarded.

        Hash-based splits are useful if you want the split to be more or less random but
        you don't want the split assignments to change if rows are added or removed
        from the table.

        The optional split_names can be provided to name the splits.  If not provided,
        the splits can only be referenced by their index.
        """
        self._async.split_hash(
            columns,
            split_weights,
            discard_weight=discard_weight,
            split_names=split_names,
        )
        return self

    def split_sequential(
        self,
        *,
        ratios: Optional[list[float]] = None,
        counts: Optional[list[int]] = None,
        fixed: Optional[int] = None,
        split_names: Optional[list[str]] = None,
    ) -> "PermutationBuilder":
        """
        Configure sequential splits for the permutation.

        One of ratios, counts, or fixed must be provided.

        If ratios are provided, they will be used to determine the relative size of each
        split. For example, if ratios are [0.3, 0.7] then the first split will contain
        30% of the rows and the second split will contain 70% of the rows.

        If counts are provided, they will be used to determine the absolute number of
        rows in each split. For example, if counts are [100, 200] then the first split
        will contain 100 rows and the second split will contain 200 rows.

        If fixed is provided, it will be used to determine the number of splits.
        For example, if fixed is 3 then the permutation will be split evenly into 3
        splits.

        Rows will be assigned to splits sequentially.  The first N1 rows are assigned to
        split 1, the next N2 rows are assigned to split 2, etc.

        The optional split_names can be provided to name the splits.  If not provided,
        the splits can only be referenced by their index.
        """
        self._async.split_sequential(
            ratios=ratios, counts=counts, fixed=fixed, split_names=split_names
        )
        return self

    def split_calculated(
        self, calculation: str, split_names: Optional[list[str]] = None
    ) -> "PermutationBuilder":
        """
        Use pre-calculated splits for the permutation.

        The calculation should be an SQL statement that returns an integer value between
        0 and the number of splits - 1.  For example, if you have 3 splits then the
        calculation should return 0 for the first split, 1 for the second split, and 2
        for the third split.

        This can be used to implement any kind of user-defined split strategy.

        The optional split_names can be provided to name the splits.  If not provided,
        the splits can only be referenced by their index.
        """
        self._async.split_calculated(calculation, split_names=split_names)
        return self

    def shuffle(
        self, *, seed: Optional[int] = None, clump_size: Optional[int] = None
    ) -> "PermutationBuilder":
        """
        Randomly shuffle the rows in the permutation.

        An optional seed can be provided to make the shuffle deterministic.

        If a clump size is provided, then data will be shuffled as small "clumps"
        of contiguous rows.  This allows for a balance between randomization and
        I/O performance.  It can be useful when reading from cloud storage.
        """
        self._async.shuffle(seed=seed, clump_size=clump_size)
        return self

    def filter(self, filter: str) -> "PermutationBuilder":
        """
        Configure a filter for the permutation.

        The filter should be an SQL statement that returns a boolean value for each row.
        Only rows where the filter is true will be included in the permutation.
        """
        self._async.filter(filter)
        return self

    def execute(self) -> LanceTable:
        """
        Execute the configuration and create the permutation table.
        """

        async def do_execute():
            inner_tbl = await self._async.execute()
            return LanceTable.from_inner(inner_tbl)

        return LOOP.run(do_execute())


def permutation_builder(table: LanceTable) -> PermutationBuilder:
    return PermutationBuilder(table)


class Permutations:
    """
    A collection of permutations indexed by name or ordinal index.

    Splits are defined when the permutation is created.  Splits can always be referenced
    by their ordinal index.  If names were provided when the permutation was created
    then they can also be referenced by name.

    Each permutation or "split" is a view of a portion of the base table.  For more
    details see [Permutation].

    Attributes
    ----------
    base_table: LanceTable
        The base table that the permutations are based on.
    permutation_table: LanceTable
        The permutation table that defines the splits.
    split_names: list[str]
        The names of the splits.
    split_dict: dict[str, int]
        A dictionary mapping split names to their ordinal index.

    Examples
    --------
    >>> # Initial data
    >>> import lancedb
    >>> db = lancedb.connect("memory:///")
    >>> tbl = db.create_table("tbl", data=[{"x": x} for x in range(1000)])
    >>> # Create a permutation
    >>> perm_tbl = (
    ...    permutation_builder(tbl)
    ...    .split_random(ratios=[0.95, 0.05], split_names=["train", "test"])
    ...    .shuffle()
    ...    .execute()
    ... )
    >>> # Read the permutations
    >>> permutations = Permutations(tbl, perm_tbl)
    >>> permutations["train"]
    <lancedb.permutation.Permutation ...>
    >>> permutations[0]
    <lancedb.permutation.Permutation ...>
    >>> permutations.split_names
    ['train', 'test']
    >>> permutations.split_dict
    {'train': 0, 'test': 1}
    """

    def __init__(self, base_table: LanceTable, permutation_table: LanceTable):
        self.base_table = base_table
        self.permutation_table = permutation_table

        if permutation_table.schema.metadata is not None:
            raw = permutation_table.schema.metadata.get(b"split_names")
            split_names = raw.decode("utf-8") if raw is not None else None
            if split_names is not None:
                self.split_names = json.loads(split_names)
                self.split_dict = {
                    name: idx for idx, name in enumerate(self.split_names)
                }
            else:
                # No split names are defined in the permutation table
                self.split_names = []
                self.split_dict = {}
        else:
            # No metadata is defined in the permutation table
            self.split_names = []
            self.split_dict = {}

    def get_by_name(self, name: str) -> "Permutation":
        """
        Get a permutation by name.

        If no split named `name` is found then an error will be raised.
        """
        idx = self.split_dict.get(name, None)
        if idx is None:
            raise ValueError(f"No split named `{name}` found")
        return self.get_by_index(idx)

    def get_by_index(self, index: int) -> "Permutation":
        """
        Get a permutation by index.
        """
        return Permutation.from_tables(self.base_table, self.permutation_table, index)

    def __getitem__(self, name: Union[str, int]) -> "Permutation":
        if isinstance(name, str):
            return self.get_by_name(name)
        elif isinstance(name, int):
            return self.get_by_index(name)
        else:
            raise TypeError(f"Invalid split name or index: {name}")


class Transforms:
    """
    Namespace for common transformation functions
    """

    @staticmethod
    def arrow2python(batch: pa.RecordBatch) -> list[dict[str, Any]]:
        return batch.to_pylist()

    @staticmethod
    def arrow2pythoncol(batch: pa.RecordBatch) -> dict[str, list[Any]]:
        return batch.to_pydict()

    @staticmethod
    def arrow2arrow(batch: pa.RecordBatch) -> pa.RecordBatch:
        return batch

    @staticmethod
    def arrow2numpy(batch: pa.RecordBatch) -> "np.ndarray":
        return batch.to_pandas().to_numpy()

    @staticmethod
    def arrow2pandas(batch: pa.RecordBatch) -> "pd.DataFrame":
        return batch.to_pandas()

    @staticmethod
    def arrow2polars() -> "pl.DataFrame":
        import polars as pl

        def impl(batch: pa.RecordBatch) -> pl.DataFrame:
            return pl.from_arrow(batch)

        return impl


# HuggingFace uses 10 which is pretty small
DEFAULT_BATCH_SIZE = 100


class Permutation:
    """
    A Permutation is a view of a dataset that can be used as input to model training
    and evaluation.

    A Permutation fulfills the pytorch Dataset contract and is loosely modeled after the
    huggingface Dataset so it should be easy to use with existing code.

    A permutation is not a "materialized view" or copy of the underlying data.  It is
    calculated on the fly from the base table.  As a result, it is truly "lazy" and does
    not require materializing the entire dataset in memory.
    """

    def __init__(
        self,
        base_table: LanceTable,
        permutation_table: Optional[LanceTable],
        split: int,
        selection: dict[str, str],
        batch_size: int,
        transform_fn: Callable[pa.RecordBatch, Any],
        offset: Optional[int] = None,
        limit: Optional[int] = None,
        connection_factory: Optional[Callable[[str], LanceTable]] = None,
        _reader: Optional[PermutationReader] = None,
    ):
        """
        Internal constructor.  Use [from_tables](#from_tables) instead.
        """
        assert base_table is not None, "base_table is required"
        assert selection is not None, "selection is required"
        self.base_table = base_table
        self.permutation_table = permutation_table
        self.split = split
        self.selection = selection
        self.transform_fn = transform_fn
        self.batch_size = batch_size
        self.offset = offset
        self.limit = limit
        self.connection_factory = connection_factory
        if _reader is None:
            _reader = LOOP.run(self._build_reader())
        self.reader: PermutationReader = _reader

    async def _build_reader(self) -> PermutationReader:
        reader = await PermutationReader.from_tables(
            self.base_table, self.permutation_table, self.split
        )
        if self.offset is not None:
            reader = await reader.with_offset(self.offset)
        if self.limit is not None:
            reader = await reader.with_limit(self.limit)
        return reader

    def _with_selection(self, selection: dict[str, str]) -> "Permutation":
        """
        Creates a new permutation with the given selection

        Does not validation of the selection and it replaces it entirely.  This is not
        intended for public use.
        """
        new = copy.copy(self)
        new.selection = selection
        return new

    def with_batch_size(self, batch_size: int) -> "Permutation":
        """
        Creates a new permutation with the given batch size
        """
        new = copy.copy(self)
        new.batch_size = batch_size
        return new

    def with_connection_factory(
        self, connection_factory: Callable[[str], LanceTable]
    ) -> "Permutation":
        """
        Creates a new permutation that will use ``connection_factory`` to reopen
        the base table when this permutation is unpickled in a worker process.

        The factory is a callable that takes a single argument — the base table
        name — and returns a [LanceTable]. It must be picklable; the worker
        will pickle it via standard ``pickle`` and call it to recover the base
        table. Picklable callables in practice means top-level (module-level)
        functions, ``functools.partial`` of such functions, or instances of
        picklable classes implementing ``__call__``. Lambdas and closures over
        local variables don't pickle with the default protocol.

        Setting a factory is necessary when the URI alone is not enough to
        re-open the connection — most importantly for LanceDB Cloud (``db://``)
        connections, where ``api_key`` and ``region`` aren't recoverable from
        the connection object after construction.

        For local file or cloud-storage paths the factory is optional: if not
        set, ``__getstate__`` falls back to capturing
        ``(uri, storage_options, namespace_path)`` and re-opening via
        ``lancedb.connect(uri, storage_options=...)``.

        Examples
        --------
        Basic native (file-system path), parameterized via ``functools.partial``::

            import functools, lancedb
            from lancedb.permutation import Permutation

            def open_native_table(uri: str, table_name: str):
                return lancedb.connect(uri).open_table(table_name)

            factory = functools.partial(open_native_table, "/data/lance_db")
            permutation = Permutation.identity(
                factory("training")
            ).with_connection_factory(factory)

        Native via :func:`lancedb.connect_namespace` (e.g. a directory- or
        REST-backed namespace client). The factory takes the
        implementation name and properties dict as partial-bound args so
        the worker can rebuild the same namespace connection::

            def open_via_namespace(
                impl: str, properties: dict[str, str], table_name: str,
            ):
                return lancedb.connect_namespace(impl, properties).open_table(
                    table_name,
                )

            factory = functools.partial(
                open_via_namespace,
                "dir",
                {"root": "/data/lance_db"},
            )

        LanceDB Cloud, reading credentials from env vars at worker startup
        so secrets aren't pickled into the dataset::

            import os, lancedb

            def open_remote_table(table_name: str):
                db = lancedb.connect(
                    "db://my-database",
                    api_key=os.environ["LANCEDB_API_KEY"],
                    region=os.environ.get("LANCEDB_REGION", "us-east-1"),
                )
                return db.open_table(table_name)

            permutation = Permutation.identity(
                open_remote_table("training")
            ).with_connection_factory(open_remote_table)
        """
        assert connection_factory is not None, "connection_factory is required"
        new = copy.copy(self)
        new.connection_factory = connection_factory
        return new

    @classmethod
    def identity(cls, table: LanceTable) -> "Permutation":
        """
        Creates an identity permutation for the given table.
        """
        return Permutation.from_tables(table, None, None)

    @classmethod
    def from_tables(
        cls,
        base_table: LanceTable,
        permutation_table: Optional[LanceTable] = None,
        split: Optional[Union[str, int]] = None,
    ) -> "Permutation":
        """
        Creates a permutation from the given base table and permutation table.

        A permutation table identifies which rows, and in what order, the data should
        be read from the base table.  For more details see the [PermutationBuilder]
        class.

        If no permutation table is provided, then the identity permutation will be
        created.  An identity permutation is a permutation that reads all rows in the
        base table in the order they are stored.

        The split parameter identifies which split to use.  If no split is provided
        then the first split will be used.
        """
        assert base_table is not None, "base_table is required"
        if split is not None:
            if permutation_table is None:
                raise ValueError(
                    "Cannot create a permutation on split `{split}`"
                    " because no permutation table is provided"
                )
            if isinstance(split, str):
                if permutation_table.schema.metadata is None:
                    raise ValueError(
                        f"Cannot create a permutation on split `{split}`"
                        " because no split names are defined in the permutation table"
                    )
                raw = permutation_table.schema.metadata.get(b"split_names")
                split_names = raw.decode("utf-8") if raw is not None else None
                if split_names is None:
                    raise ValueError(
                        f"Cannot create a permutation on split `{split}`"
                        " because no split names are defined in the permutation table"
                    )
                split_names = json.loads(split_names)
                try:
                    split = split_names.index(split)
                except ValueError:
                    raise ValueError(
                        f"Cannot create a permutation on split `{split}`"
                        f" because split `{split}` is not defined in the "
                        "permutation table"
                    )
            elif isinstance(split, int):
                split = split
            else:
                raise TypeError(f"Invalid split: {split}")
        else:
            split = 0

        async def do_from_tables():
            reader = await PermutationReader.from_tables(
                base_table, permutation_table, split
            )
            schema = await reader.output_schema(None)
            initial_selection = {name: name for name in schema.names}
            return cls(
                base_table,
                permutation_table,
                split,
                initial_selection,
                DEFAULT_BATCH_SIZE,
                Transforms.arrow2python,
                _reader=reader,
            )

        return LOOP.run(do_from_tables())

    def __getstate__(self) -> dict[str, Any]:
        """Build a picklable state dict for this permutation.

        The base table is captured either via a user-supplied
        ``connection_factory`` (see [with_connection_factory]) or, as a
        fallback, by introspecting ``(uri, storage_options, namespace_path)``
        on the connection. The permutation table — always an in-memory
        LanceDB table — is captured as a pyarrow Table (which pickles via
        Arrow IPC natively). The reader is dropped from the wire format;
        ``__setstate__`` rebuilds it from the restored tables.
        """
        permutation_data: Optional[pa.Table] = None
        if self.permutation_table is not None:
            permutation_data = self.permutation_table.to_arrow()

        common = {
            "base_table_name": self.base_table.name,
            "permutation_data": permutation_data,
            "split": self.split,
            "selection": self.selection,
            "batch_size": self.batch_size,
            "transform_fn": self.transform_fn,
            "offset": self.offset,
            "limit": self.limit,
            "connection_factory": self.connection_factory,
        }

        if self.connection_factory is not None:
            # The factory carries enough state to recover the base table on
            # its own; we don't need to capture the URI / storage options /
            # namespace from the existing connection.
            return common

        # URI-introspection fallback: only viable for native (OSS) connections
        # where (uri, storage_options) is enough to reopen. Remote / cloud
        # connections don't expose recoverable api_key / region — those users
        # must call with_connection_factory().
        try:
            base_uri = self.base_table._conn.uri
            storage_options = self.base_table._conn.storage_options
        except AttributeError as e:
            raise ValueError(
                "Cannot pickle this Permutation: the base table's connection "
                "does not expose a uri/storage_options, which usually means it "
                "is a remote (LanceDB Cloud) connection. Call "
                "Permutation.with_connection_factory(...) first to provide a "
                "picklable callable that re-opens the base table from a worker "
                "process."
            ) from e

        if base_uri.startswith("memory://"):
            # In-memory base tables don't exist in any worker process by
            # default, so dump the entire base table into the pickle. This
            # can be expensive for large datasets — users with large
            # in-memory base tables should either persist them or set a
            # connection_factory.
            return {
                **common,
                "base_table_data": self.base_table.to_arrow(),
            }

        return {
            **common,
            "base_table_uri": base_uri,
            "base_table_namespace": self.base_table._namespace_path,
            "base_table_storage_options": storage_options,
        }

    def __setstate__(self, state: dict[str, Any]) -> None:
        from . import connect

        connection_factory = state["connection_factory"]
        if connection_factory is not None:
            base_table = connection_factory(state["base_table_name"])
        elif "base_table_data" in state:
            # In-memory base table inlined into the pickle; rebuild the same
            # way we rebuild the in-memory permutation table.
            mem_db = connect("memory://")
            base_table = mem_db.create_table(
                state["base_table_name"], state["base_table_data"]
            )
        else:
            base_db = connect(
                state["base_table_uri"],
                storage_options=state["base_table_storage_options"],
            )
            base_table = base_db.open_table(
                state["base_table_name"],
                namespace_path=state["base_table_namespace"] or None,
            )

        permutation_table: Optional[LanceTable] = None
        if state["permutation_data"] is not None:
            mem_db = connect("memory://")
            permutation_table = mem_db.create_table(
                "permutation", state["permutation_data"]
            )

        self.base_table = base_table
        self.permutation_table = permutation_table
        self.split = state["split"]
        self.selection = state["selection"]
        self.batch_size = state["batch_size"]
        self.transform_fn = state["transform_fn"]
        self.offset = state["offset"]
        self.limit = state["limit"]
        self.connection_factory = connection_factory
        self.reader = LOOP.run(self._build_reader())

    @property
    def schema(self) -> pa.Schema:
        async def do_output_schema():
            return await self.reader.output_schema(self.selection)

        return LOOP.run(do_output_schema())

    @property
    def num_columns(self) -> int:
        """
        The number of columns in the permutation
        """
        return len(self.schema)

    @property
    def num_rows(self) -> int:
        """
        The number of rows in the permutation
        """
        return self.reader.count_rows()

    @property
    def column_names(self) -> list[str]:
        """
        The names of the columns in the permutation
        """
        return self.schema.names

    @property
    def shape(self) -> tuple[int, int]:
        """
        The shape of the permutation

        This will return self.num_rows, self.num_columns
        """
        return self.num_rows, self.num_columns

    def __len__(self) -> int:
        """
        The number of rows in the permutation

        This is an alias for [num_rows][lancedb.permutation.Permutation.num_rows]
        """
        return self.num_rows

    def unique(self, _column: str) -> list[Any]:
        """
        Get the unique values in the given column
        """
        raise Exception("unique is not yet implemented")

    def flatten(self) -> "Permutation":
        """
        Flatten the permutation

        Each column with a struct type will be flattened into multiple columns.

        This flattening operation happens at read time as a post-processing step
        so this call is cheap and no data is copied or modified in the underlying
        dataset.
        """
        raise Exception("flatten is not yet implemented")

    def remove_columns(self, columns: list[str]) -> "Permutation":
        """
        Remove the given columns from the permutation

        Note: this does not actually modify the underlying dataset.  It only changes
        which columns are visible from this permutation.  Also, this does not introduce
        a post-processing step.  Instead, we simply do not read those columns in the
        first place.

        If any of the provided columns does not exist in the current permutation then it
        will be ignored (no error is raised for missing columns)

        Returns a new permutation with the given columns removed.  This does not modify
        self.
        """
        assert columns is not None, "columns is required"

        new_selection = {
            name: value for name, value in self.selection.items() if name not in columns
        }

        if len(new_selection) == 0:
            raise ValueError("Cannot remove all columns")

        return self._with_selection(new_selection)

    def rename_column(self, old_name: str, new_name: str) -> "Permutation":
        """
        Rename a column in the permutation

        If there is no column named old_name then an error will be raised
        If there is already a column named new_name then an error will be raised

        Note: this does not actually modify the underlying dataset.  It only changes
        the name of the column that is visible from this permutation.  This is a
        post-processing step but done at the batch level and so it is very cheap.
        No data will be copied.
        """
        assert old_name is not None, "old_name is required"
        assert new_name is not None, "new_name is required"
        if old_name not in self.selection:
            raise ValueError(
                f"Cannot rename column `{old_name}` because it does not exist"
            )
        if new_name in self.selection:
            raise ValueError(
                f"Cannot rename column `{old_name}` to `{new_name}` because a column "
                "with that name already exists"
            )
        new_selection = self.selection.copy()
        new_selection[new_name] = new_selection[old_name]
        del new_selection[old_name]
        return self._with_selection(new_selection)

    def rename_columns(self, column_map: dict[str, str]) -> "Permutation":
        """
        Rename the given columns in the permutation

        If any of the columns do not exist then an error will be raised
        If any of the new names already exist then an error will be raised

        Note: this does not actually modify the underlying dataset.  It only changes
        the name of the column that is visible from this permutation.  This is a
        post-processing step but done at the batch level and so it is very cheap.
        No data will be copied.
        """
        assert column_map is not None, "column_map is required"

        new_permutation = self
        for old_name, new_name in column_map.items():
            new_permutation = new_permutation.rename_column(old_name, new_name)
        return new_permutation

    def select_columns(self, columns: list[str]) -> "Permutation":
        """
        Select the given columns from the permutation

        This method refines the current selection, potentially removing columns.  It
        will not add back columns that were previously removed.

        If any of the columns do not exist then an error will be raised

        This does not introduce a post-processing step.  It simply reduces the amount
        of data we read.
        """
        assert columns is not None, "columns is required"
        if len(columns) == 0:
            raise ValueError("Must select at least one column")

        new_selection = {}
        for name in columns:
            value = self.selection.get(name, None)
            if value is None:
                raise ValueError(
                    f"Cannot select column `{name}` because it does not exist"
                )
            new_selection[name] = value
        return self._with_selection(new_selection)

    def __iter__(self) -> Iterator[dict[str, Any]]:
        """
        Iterate over the permutation
        """
        return self.iter(self.batch_size, skip_last_batch=True)

    def iter(
        self, batch_size: int, skip_last_batch: bool = False
    ) -> Iterator[dict[str, Any]]:
        """
        Iterate over the permutation in batches

        If skip_last_batch is True, the last batch will be skipped if it is not a
        multiple of batch_size.
        """

        async def get_iter():
            return await self.reader.read(self.selection, batch_size=batch_size)

        async_iter = LOOP.run(get_iter())

        async def get_next():
            return await async_iter.__anext__()

        try:
            while True:
                batch = LOOP.run(get_next())
                if batch.num_rows == batch_size or not skip_last_batch:
                    yield self.transform_fn(batch)
        except StopAsyncIteration:
            return

    def with_format(
        self,
        format: Literal[
            "numpy",
            "python",
            "python_col",
            "pandas",
            "arrow",
            "torch",
            "torch_row",
            "torch_col",
            "polars",
        ],
    ) -> "Permutation":
        """
        Set the format for batches

        If this method is not called, the "python" format will be used.

        The format can be one of:
        - "numpy" - the batch will be a dict of numpy arrays (one per column)
        - "python" - the batch will be a list of dicts (one per row)
        - "python_col" - the batch will be a dict of lists (one entry per column)
        - "pandas" - the batch will be a pandas DataFrame
        - "arrow" - the batch will be a pyarrow RecordBatch
        - "torch" - the batch will be a dict of torch tensors keyed by column name
          (one 1-D tensor per column). This matches HuggingFace's
          ``dataset.set_format("torch")`` and works with the default
          ``torch.utils.data.DataLoader`` collate.
        - "torch_row" - the batch will be a list of torch tensors, one per row.
          This was the behavior of "torch" prior to the HuggingFace alignment.
        - "torch_col" - the batch will be a 2D torch tensor (first dim indexes columns)
        - "polars" - the batch will be a polars DataFrame

        Conversion may or may not involve a data copy.  Lance uses Arrow internally
        and so it is able to zero-copy to the arrow and polars formats.

        Conversion to torch and torch_col will be zero-copy but will only support a
        subset of data types (numeric types).

        Conversion to numpy and/or pandas will typically be zero-copy for numeric
        types.  Conversion of strings, lists, and structs will require creating python
        objects and this is not zero-copy.

        For custom formatting, use [with_transform](#with_transform) which overrides
        this method.
        """
        assert format is not None, "format is required"
        if format == "python":
            return self.with_transform(Transforms.arrow2python)
        if format == "python_col":
            return self.with_transform(Transforms.arrow2pythoncol)
        elif format == "numpy":
            return self.with_transform(Transforms.arrow2numpy)
        elif format == "pandas":
            return self.with_transform(Transforms.arrow2pandas)
        elif format == "arrow":
            return self.with_transform(Transforms.arrow2arrow)
        elif format == "torch":
            new = self.with_transform(batch_to_tensor_dict)
            new._torch_dict_format = True
            return new
        elif format == "torch_row":
            return self.with_transform(batch_to_tensor_rows)
        elif format == "torch_col":
            return self.with_transform(batch_to_tensor)
        elif format == "polars":
            return self.with_transform(Transforms.arrow2polars())
        else:
            raise ValueError(f"Invalid format: {format}")

    def with_transform(self, transform: Callable[pa.RecordBatch, Any]) -> "Permutation":
        """
        Set a custom transform for the permutation

        The transform is a callable that will be invoked with each record batch.  The
        return value will be used as the batch for iteration.

        Note: transforms are not invoked in parallel.  This method is not a good place
        for expensive operations such as image decoding.
        """
        assert transform is not None, "transform is required"
        new = copy.copy(self)
        new.transform_fn = transform
        new._torch_dict_format = False
        return new

    def __getitem__(self, index: int) -> Any:
        """
        Returns a single row from the permutation by offset
        """
        return self.__getitems__([index])

    def __getitems__(self, indices: list[int]) -> Any:
        """
        Returns rows from the permutation by offset
        """

        async def do_getitems():
            return await self.reader.take_offsets(indices, selection=self.selection)

        batch = LOOP.run(do_getitems())
        result = self.transform_fn(batch)
        # For with_format("torch"), the transform produces a dict of batched
        # tensors. Unbatch into a list of per-row dicts so PyTorch's default
        # DataLoader collate (which expects a list of samples) can stack them
        # back into a batched dict. iter() still yields the batched dict
        # directly.
        if getattr(self, "_torch_dict_format", False):
            return [{k: v[i] for k, v in result.items()} for i in range(len(indices))]
        return result

    @deprecated(details="Use with_skip instead")
    def skip(self, skip: int) -> "Permutation":
        """
        Skip the first `skip` rows of the permutation

        Note: this method returns a new permutation and does not modify `self`
        It is provided for compatibility with the huggingface Dataset API.

        Use [with_skip](#with_skip) instead to avoid confusion.
        """
        return self.with_skip(skip)

    def with_skip(self, skip: int) -> "Permutation":
        """
        Skip the first `skip` rows of the permutation
        """
        new = copy.copy(self)
        new.offset = skip
        new.reader = LOOP.run(new._build_reader())
        return new

    @deprecated(details="Use with_take instead")
    def take(self, limit: int) -> "Permutation":
        """
        Limit the permutation to `limit` rows (following any `skip`)

        Note: this method returns a new permutation and does not modify `self`
        It is provided for compatibility with the huggingface Dataset API.

        Use [with_take](#with_take) instead to avoid confusion.
        """
        return self.with_take(limit)

    def with_take(self, limit: int) -> "Permutation":
        """
        Limit the permutation to `limit` rows (following any `skip`)
        """
        new = copy.copy(self)
        new.limit = limit
        new.reader = LOOP.run(new._build_reader())
        return new

    @deprecated(details="Use with_repeat instead")
    def repeat(self, times: int) -> "Permutation":
        """
        Repeat the permutation `times` times

        Note: this method returns a new permutation and does not modify `self`
        It is provided for compatibility with the huggingface Dataset API.

        Use [with_repeat](#with_repeat) instead to avoid confusion.
        """
        return self.with_repeat(times)

    def with_repeat(self, times: int) -> "Permutation":
        """
        Repeat the permutation `times` times
        """
        raise Exception("with_repeat is not yet implemented")
