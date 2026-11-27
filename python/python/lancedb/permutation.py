# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import copy
import json
import operator
import os

from deprecation import deprecated
import pyarrow as pa

from ._lancedb import Connection as NativeConnection
from ._lancedb import async_permutation_builder, PermutationReader
from .table import LanceTable, Table
from .background_loop import LOOP
from .util import (
    batch_to_tensor,
    batch_to_tensor_columns,
    batch_to_tensor_dict,
    batch_to_tensor_rows,
    get_uri_scheme,
)
from typing import Any, Callable, Iterator, Literal, Optional, TYPE_CHECKING, Union

if TYPE_CHECKING:
    from lancedb.dependencies import pandas as pd, numpy as np, polars as pl
    from .db import DBConnection


def _native_connection(db: Any) -> Any:
    """Return the native connection wrapped by a sync or async connection."""
    async_connection = getattr(db, "_conn", None)
    if async_connection is not None:
        return _native_connection(async_connection)
    inner = getattr(db, "_inner", None)
    if inner is not None:
        return _native_connection(inner)
    if isinstance(db, NativeConnection):
        return db
    raise TypeError("db must be a LanceDB connection")


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

    The permutation is stored in memory and will be lost when the program exits,
    unless [persist][lancedb.permutation.PermutationBuilder.persist] is called before
    [execute][lancedb.permutation.PermutationBuilder.execute].
    """

    def __init__(self, table: Table):
        """
        Creates a new permutation builder for the given table.

        By default, the permutation builder will create a single split that contains all
        rows in the same order as the base table.

        Tables with an RTE write spec are rejected: unflushed rows have no row id.
        """
        self._async = async_permutation_builder(table)
        self._persist_connection: Optional[Any] = None
        self._persist_table_name: Optional[str] = None
        self._persist_namespace_path: list[str] = []

    def split_random(
        self,
        *,
        ratios: Optional[list[float]] = None,
        counts: Optional[list[int]] = None,
        fixed: Optional[int] = None,
        seed: Optional[int] = None,
        clump_size: Optional[int] = None,
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

        If clump_size is provided, rows are shuffled as contiguous groups of that size,
        preserving I/O locality while still randomising the split assignment.

        The optional split_names can be provided to name the splits.  If not provided,
        the splits can only be referenced by their index.
        """
        self._async.split_random(
            ratios=ratios,
            counts=counts,
            fixed=fixed,
            seed=seed,
            clump_size=clump_size,
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

    def bootstrap_sample(
        self,
        num_samples: int,
        *,
        ratios: Optional[list[float]] = None,
        counts: Optional[list[int]] = None,
        fixed: Optional[int] = None,
        seed: Optional[int] = None,
        split_names: Optional[list[str]] = None,
    ) -> "PermutationBuilder":
        """
        Sample rows uniformly **with replacement**, independently per split.

        Unlike the other split strategies, the number of output rows is
        ``num_samples``, not the number of (optionally filtered) input rows: the
        same row can appear zero, one, or many times, and ``num_samples`` may be
        larger or smaller than the input row count. Useful for bootstrap
        resampling, oversampling rare classes, or any training workload that
        wants a fixed-size stream of random draws rather than a single
        deterministic pass over every row. Combine with :meth:`filter` to
        restrict the eligible population.

        One of ratios, counts, or fixed must be provided, with the same meaning
        as in :meth:`split_random`, except they divide ``num_samples`` across
        splits rather than the input row count: if fixed is 4, each split gets
        ``num_samples // 4`` samples.

        Each split draws its own sample using a seed derived independently from
        ``(seed, split_index)``, so a split's content does not change if the samples are
        later redistributed across a different number of splits.

        The optional split_names can be provided to name the splits.  If not provided,
        the splits can only be referenced by their index.
        """
        self._async.bootstrap_sample(
            num_samples,
            ratios=ratios,
            counts=counts,
            fixed=fixed,
            seed=seed,
            split_names=split_names,
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

    def persist(
        self,
        db: "DBConnection",
        table_name: str,
        *,
        namespace_path: Optional[list[str]] = None,
        mode: Literal["create", "overwrite"] = "create",
    ) -> "PermutationBuilder":
        """
        Store the permutation table in an embedded database.

        The base table can be Embedded or Enterprise; this connection is only the
        plan destination. Persisting a plan makes the same split available to
        another process or another machine with access to the destination storage.
        The plan is an ordinary LanceDB table containing row ids and split ids; it
        can be opened with [open_table][lancedb.db.DBConnection.open_table] and
        passed to
        [Permutation.from_tables][lancedb.permutation.Permutation.from_tables].
        The table returned by ``execute`` keeps the destination connection and
        namespace context, so it can be pickled and reopened directly.

        Parameters
        ----------
        db: DBConnection
            An embedded LanceDB connection for the plan destination. Local files
            and object storage are supported. The base table may be Embedded or
            Enterprise; persisting the plan itself to Enterprise is not supported
            yet.
        table_name: str
            The name of the permutation table to create.
        namespace_path: list[str], optional
            The namespace to create the table in. The default is the root
            namespace.
        mode: {"create", "overwrite"}, default "create"
            ``create`` fails if the destination table already exists.
            ``overwrite`` writes a new version of the table. Readers that already
            opened the previous version keep reading that snapshot.

        Returns
        -------
        PermutationBuilder
            This builder, so ``persist`` can be chained with the split, filter,
            and execute methods.
        """
        if mode not in ("create", "overwrite"):
            raise ValueError(f"Invalid mode {mode!r}; expected 'create' or 'overwrite'")

        uri = getattr(db, "uri", None)
        if uri is None:
            raise TypeError("db must be a LanceDB connection")
        if get_uri_scheme(str(uri)) == "db":
            raise NotImplementedError(
                "Permutation persistence is not supported for LanceDB Enterprise "
                "connections yet; reproducible Enterprise plan ordering requires "
                "deterministic remote row ordering."
            )

        if namespace_path is None:
            namespace_path = []
        elif not isinstance(namespace_path, (list, tuple)):
            raise TypeError("namespace_path must be a list of strings or None")

        namespace_path = list(namespace_path)
        self._async.persist(
            _native_connection(db),
            table_name,
            namespace_path=namespace_path,
            mode=mode,
        )
        self._persist_connection = db
        self._persist_table_name = table_name
        self._persist_namespace_path = namespace_path
        return self

    def execute(self) -> LanceTable:
        """
        Execute the configuration and create the permutation table.
        """

        async def do_execute():
            inner_tbl = await self._async.execute()
            if self._persist_connection is None:
                return await LanceTable.from_inner(inner_tbl)
            return await LanceTable.from_inner(
                inner_tbl,
                connection=self._persist_connection,
                name=self._persist_table_name,
                namespace_path=self._persist_namespace_path,
            )

        return LOOP.run(do_execute())


def permutation_builder(table: Table) -> PermutationBuilder:
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
    base_table: Table
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

    def __init__(self, base_table: Table, permutation_table: LanceTable):
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

_MAP_FORMAT_ERROR = (
    "map cannot be combined with with_format or with_transform; convert the "
    "output inside the map callback or in the DataLoader collate_fn instead"
)


def _map_rows(data: Any, num_rows: int) -> list[dict[str, Any]]:
    """Return the data flowing through a map chain as a list of row dicts."""
    if isinstance(data, pa.RecordBatch):
        return data.to_pylist()
    if isinstance(data, dict):
        return [
            {name: values[i] for name, values in data.items()} for i in range(num_rows)
        ]
    return data


def _map_columns(data: Any) -> dict[str, Any]:
    """Return the data flowing through a map chain as a dict of columns."""
    if isinstance(data, pa.RecordBatch):
        return data.to_pydict()
    if isinstance(data, list):
        names = data[0].keys()
        if any(row.keys() != names for row in data):
            raise ValueError(
                "Cannot pass rows to a batched map callback because the previous "
                "callback did not return the same keys for every row"
            )
        return {name: [row[name] for row in data] for name in names}
    return data


class _MapTransform:
    """
    The transform installed by [map][lancedb.permutation.Permutation.map]

    Each step is a ``(fn, batched)`` pair.  This is a module-level class, and not a
    closure, so that a mapped permutation pickles whenever its callbacks do.
    """

    def __init__(self, steps: tuple[tuple[Callable, bool], ...]):
        self.steps = steps

    def __call__(self, batch: pa.RecordBatch) -> list[dict[str, Any]]:
        num_rows = batch.num_rows
        if num_rows == 0:
            return []

        # The layout is only converted when a callback needs the other one
        data = batch
        for fn, batched in self.steps:
            if batched:
                data = fn(_map_columns(data))
                if not isinstance(data, dict):
                    raise TypeError(
                        "A batched map callback must return a dict of columns, "
                        f"got {type(data).__name__}"
                    )
                for name, values in data.items():
                    # A str or dict can be indexed but is not a column of values
                    if isinstance(values, (str, bytes, dict)) or not hasattr(
                        values, "__getitem__"
                    ):
                        raise TypeError(
                            "A batched map callback must return a list of values "
                            f"for column `{name}`, got {type(values).__name__}"
                        )
                    if len(values) != num_rows:
                        raise ValueError(
                            f"A batched map callback returned {len(values)} values "
                            f"for column `{name}` but the batch has {num_rows} rows"
                        )
            else:
                data = [fn(row) for row in _map_rows(data, num_rows)]
                for row in data:
                    if not isinstance(row, dict):
                        raise TypeError(
                            "A map callback must return a dict for each row, "
                            f"got {type(row).__name__}"
                        )
        return _map_rows(data, num_rows)


def _is_persisted_plan(table: Table) -> bool:
    """Whether a permutation table lives in reopenable storage rather than memory."""
    return isinstance(table, LanceTable) and not table._conn.uri.startswith("memory://")


def _table_to_pickle_state(table: Table) -> dict[str, Any]:
    from .remote.table import RemoteTable

    if isinstance(table, RemoteTable):
        # RemoteTable already owns its reopen state; keep the table object so
        # pickle delegates to its __getstate__ without duplicating the fields.
        return {
            "kind": "remote",
            "table": table,
        }

    if not isinstance(table, LanceTable):
        raise ValueError(f"Cannot pickle table of type {type(table)!r}")

    state = table.__getstate__()
    if state.get("kind") == "memory":
        return {
            "kind": "memory",
            "state": state,
        }
    return {
        "kind": "lance",
        "state": state,
    }


def _drop_base_version(permutation_data: pa.Table) -> pa.Table:
    """Strip the recorded base version so the reader leaves the base table unpinned."""
    metadata = dict(permutation_data.schema.metadata or {})
    if metadata.pop(b"base_version", None) is None:
        return permutation_data
    metadata.pop(b"base_branch", None)
    return permutation_data.replace_schema_metadata(metadata)


def _lance_table_from_pickle_state(state: dict[str, Any]) -> LanceTable:
    table = LanceTable.__new__(LanceTable)
    table.__setstate__(state)
    return table


def _table_from_pickle_state(state: dict[str, Any]) -> Table:
    from . import connect

    kind = state["kind"]
    if kind == "remote":
        return state["table"]
    if kind == "lance":
        return _lance_table_from_pickle_state(state["state"])
    if kind == "memory":
        if "state" in state:
            return _lance_table_from_pickle_state(state["state"])
        # Backward compatibility for pickles written before tables carried
        # their own state.
        return connect("memory://").create_table(state["name"], state["data"])
    if kind == "local":
        db = connect(state["uri"], storage_options=state["storage_options"])
        return db.open_table(state["name"], namespace_path=state["namespace"] or None)
    raise ValueError(f"Unknown table pickle state kind: {kind}")


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
        base_table: Table,
        permutation_table: Optional[Table],
        split: int,
        selection: dict[str, str],
        batch_size: int,
        transform_fn: Callable[pa.RecordBatch, Any],
        offset: Optional[int] = None,
        limit: Optional[int] = None,
        connection_factory: Optional[Callable[[str], Table]] = None,
        _reader: Optional[PermutationReader] = None,
        _plan_metadata: Optional[dict[bytes, bytes]] = None,
        _plan_version: Optional[int] = None,
        _plan_branch: Optional[str] = None,
    ):
        """
        Internal constructor.  Use
        [from_tables][lancedb.permutation.Permutation.from_tables] instead.
        """
        assert base_table is not None, "base_table is required"
        assert selection is not None, "selection is required"
        self.base_table = base_table
        self.permutation_table = permutation_table
        self.split = split
        self.selection = selection
        self.transform_fn = transform_fn
        # Whether the default transform was replaced, by with_format, by
        # with_transform, or by a caller passing its own transform here
        self._explicit_transform = transform_fn is not Transforms.arrow2python
        self.batch_size = batch_size
        self.offset = offset
        self.limit = limit
        self.connection_factory = connection_factory
        self._plan_metadata = _plan_metadata
        self._plan_version = _plan_version
        self._plan_branch = _plan_branch
        if _reader is None:
            _reader = LOOP.run(self._build_reader())
        self.reader: PermutationReader = _reader
        self._pid = os.getpid()

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
        self, connection_factory: Callable[[str], Table]
    ) -> "Permutation":
        """
        Creates a new permutation that will use ``connection_factory`` to reopen
        the base table when this permutation is unpickled in a worker process.

        The factory is a callable that takes a single argument, the base table
        name, and returns a LanceDB table. It must be picklable; the worker
        will pickle it via standard ``pickle`` and call it to recover the base
        table. Picklable callables in practice means top-level (module-level)
        functions, ``functools.partial`` of such functions, or instances of
        picklable classes implementing ``__call__``. Lambdas and closures over
        local variables don't pickle with the default protocol.

        A factory is optional for normal LanceDB Embedded and Enterprise connections:
        if not set, ``__getstate__`` captures the table's own picklable reopen
        state. Use a factory when that default state is not enough, for example
        when credentials should be loaded from the worker environment instead
        of being embedded in the pickle.

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

        LanceDB Enterprise, reading credentials from env vars at worker startup
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
    def identity(cls, table: Table) -> "Permutation":
        """
        Creates an identity permutation for the given table.
        """
        return Permutation.from_tables(table, None, None)

    @classmethod
    def from_tables(
        cls,
        base_table: Table,
        permutation_table: Optional[Table] = None,
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
        persisted_plan = permutation_table is not None and _is_persisted_plan(
            permutation_table
        )
        if persisted_plan:
            # Pin before reading metadata or constructing the reader. The caller's
            # handle may keep tracking latest, but the live reader and pickle must
            # share one immutable plan snapshot.
            permutation_table = permutation_table._pin_current_snapshot()
        plan_metadata = None if permutation_table is not None else {}
        if permutation_table is not None and (isinstance(split, str) or persisted_plan):
            plan_metadata = dict(permutation_table.schema.metadata or {})
        if split is not None:
            if permutation_table is None:
                raise ValueError(
                    f"Cannot create a permutation on split `{split}`"
                    " because no permutation table is provided"
                )
            if isinstance(split, str):
                raw = (plan_metadata or {}).get(b"split_names")
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

        plan_version = None
        plan_branch = None
        if persisted_plan:
            # Pin the pickled plan to the snapshot this object was created from,
            # not what the destination name points at after an overwrite.
            plan_version = permutation_table.version
            plan_branch = permutation_table.current_branch()

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
                _plan_metadata=plan_metadata,
                _plan_version=plan_version,
                _plan_branch=plan_branch,
            )

        return LOOP.run(do_from_tables())

    def __getstate__(self) -> dict[str, Any]:
        """Build a picklable state dict for this permutation.

        The base table is captured either via a user-supplied
        ``connection_factory`` (see [with_connection_factory]) or, as a
        fallback, by the table's own picklable reopen state. A permutation
        table in memory is captured as a pyarrow Table (which pickles via Arrow
        IPC natively). A persisted plan is reopened by reference and pinned to
        the plan snapshot this object was built from. The reader is dropped
        from the wire format and rebuilt lazily on first use.
        """
        base_table_state = (
            None
            if self.connection_factory is not None
            else _table_to_pickle_state(self.base_table)
        )
        base_is_memory = (
            base_table_state is not None and base_table_state["kind"] == "memory"
        )

        permutation_data: Optional[pa.Table] = None
        permutation_table_state: Optional[dict[str, Any]] = None
        if (
            self.permutation_table is not None
            and _is_persisted_plan(self.permutation_table)
            and not base_is_memory
        ):
            plan_state = _table_to_pickle_state(self.permutation_table)
            # Keep the plan's own branch/version coordinate. A later overwrite
            # of the destination must not change what this pickle reads.
            table_state = plan_state["state"]
            if self._plan_version is not None:
                table_state["checkout_version"] = self._plan_version
                # None is the main branch. Restore it explicitly because the
                # live handle may have moved to another branch since capture.
                table_state["branch"] = self._plan_branch
            permutation_table_state = plan_state
        elif self.permutation_table is not None:
            permutation_data = self.permutation_table.to_arrow()

        common = {
            "base_table_name": self.base_table.name,
            "split": self.split,
            "selection": self.selection,
            "batch_size": self.batch_size,
            "transform_fn": self.transform_fn,
            "explicit_transform": self._explicit_transform,
            "offset": self.offset,
            "limit": self.limit,
            "connection_factory": self.connection_factory,
        }
        if permutation_table_state is not None:
            common["permutation_table_state"] = permutation_table_state
        else:
            common["permutation_data"] = permutation_data
        if base_table_state is not None:
            common["base_table_state"] = base_table_state
        return common

    def __setstate__(self, state: dict[str, Any]) -> None:
        from . import connect

        connection_factory = state["connection_factory"]
        rebuilt_base = False
        if connection_factory is not None:
            base_table = connection_factory(state["base_table_name"])
        elif "base_table_state" in state:
            base_state = state["base_table_state"]
            rebuilt_base = base_state["kind"] == "memory"
            base_table = _table_from_pickle_state(base_state)
        elif "base_table_data" in state:
            rebuilt_base = True
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

        permutation_table: Optional[Table] = None
        plan_metadata: dict[bytes, bytes] = {}
        plan_version = None
        plan_branch = None
        if "permutation_table_state" in state:
            plan_state = state["permutation_table_state"]
            permutation_table = _table_from_pickle_state(plan_state)
            nested_state = plan_state.get("state", {})
            plan_version = nested_state.get("checkout_version")
            plan_branch = nested_state.get("branch")
            plan_metadata = dict(permutation_table.schema.metadata or {})
        else:
            permutation_data = state.get("permutation_data")
            if permutation_data is not None:
                if rebuilt_base:
                    # The base table was materialized from Arrow, so it is a fresh
                    # single-version dataset and the recorded pin cannot resolve on it.
                    permutation_data = _drop_base_version(permutation_data)
                mem_db = connect("memory://")
                permutation_table = mem_db.create_table("permutation", permutation_data)
                plan_metadata = dict(permutation_table.schema.metadata or {})

        self.base_table = base_table
        self.permutation_table = permutation_table
        self._plan_metadata = plan_metadata
        self._plan_version = plan_version
        self._plan_branch = plan_branch
        self.split = state["split"]
        self.selection = state["selection"]
        self.batch_size = state["batch_size"]
        self.transform_fn = state["transform_fn"]
        # Pickles written before this flag existed still carry their transform.
        # Anything but the default must have been set explicitly.
        self._explicit_transform = state.get(
            "explicit_transform", self.transform_fn is not Transforms.arrow2python
        )
        self.offset = state["offset"]
        self.limit = state["limit"]
        self.connection_factory = connection_factory
        self.reader = None
        self._pid = None

    def __copy__(self) -> "Permutation":
        """Return a shallow copy without going through the pickle path.

        The ``with_*`` builders use ``copy.copy`` to derive a new permutation.
        The default copy protocol would use ``__getstate__``/``__setstate__``,
        which materializes the permutation table with ``to_arrow()`` (not
        supported for remote tables) and rebuilds the base table and reader.
        A shallow copy instead keeps the existing table handles and reader;
        the caller replaces only the attribute it is changing.
        """
        new = self.__class__.__new__(self.__class__)
        new.__dict__ = self.__dict__.copy()
        return new

    def _loaded_plan_metadata(self) -> dict[bytes, bytes]:
        if self._plan_metadata is None:
            self._plan_metadata = (
                dict(self.permutation_table.schema.metadata or {})
                if self.permutation_table is not None
                else {}
            )
        return self._plan_metadata

    @property
    def base_version(self) -> Optional[int]:
        """The base table version recorded in the plan, or ``None`` for legacy plans.

        This value comes from the plan's schema metadata, so reading it does not
        query the base table. Version numbers are per-branch; see
        [base_branch][lancedb.permutation.Permutation.base_branch].
        """
        raw = self._loaded_plan_metadata().get(b"base_version")
        if raw is None:
            return None
        try:
            return int(raw)
        except ValueError as error:
            raise ValueError(
                f"Permutation table has an unreadable base_version of {raw!r}"
            ) from error

    @property
    def base_branch(self) -> Optional[str]:
        """The base table branch recorded in the plan, or ``None`` for main.

        Legacy plans recorded no branch and therefore also return ``None``.
        """
        raw = self._loaded_plan_metadata().get(b"base_branch")
        if raw is None:
            return None
        return raw.decode("utf-8")

    def _ensure_open(self) -> None:
        pid = os.getpid()
        if self.reader is not None and getattr(self, "_pid", None) == pid:
            return
        # The reader owns Rust-side table handles. Rebuild it after unpickle or
        # fork even though the Python table wrappers reopen themselves.
        if hasattr(self.base_table, "_ensure_open"):
            self.base_table._ensure_open()
        if self.permutation_table is not None and hasattr(
            self.permutation_table, "_ensure_open"
        ):
            self.permutation_table._ensure_open()
        self.reader = LOOP.run(self._build_reader())
        self._pid = pid

    @property
    def schema(self) -> pa.Schema:
        self._ensure_open()

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
        self._ensure_open()
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
        # Rebuild the mapping so the renamed column keeps its position
        new_selection = {
            (new_name if name == old_name else name): value
            for name, value in self.selection.items()
        }
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

        If any of the columns do not exist then an error will be raised.

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
                if name == "_rowid":
                    # _rowid is a system column not in the default schema
                    # but can be explicitly selected
                    value = "_rowid"
                else:
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
        self._ensure_open()

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
            "torch_dict",
            "torch_row",
            "torch_col",
            "polars",
        ],
    ) -> "Permutation":
        """
        Set the format for batches

        If this method is not called, the "python" format will be used.

        The format can be one of:
        - "numpy" - a single NumPy ndarray built with
          ``batch.to_pandas().to_numpy()``. This conversion may copy data and
          may promote mixed numeric columns to a common numeric dtype, for
          example int64 and float32 to float64. Nested or incompatible columns
          produce an object array.
        - "python" - the batch will be a list of dicts (one per row)
        - "python_col" - the batch will be a dict of lists (one entry per column)
        - "pandas" - the batch will be a pandas DataFrame
        - "arrow" - the batch will be a pyarrow RecordBatch
        - "torch" - a list of per-row dicts mapping column names to tensors with
          shape ``(*S)``. The default DataLoader collate stacks these into a
          dict of ``(B, *S)`` tensors.
        - "torch_dict" - a dict mapping each column name to one tensor with
          shape ``(B, *S)``. Columns may have different sample shapes ``S``.
          Use an identity collate to consume the batch directly. Tensors share
          Arrow storage where the layout allows.
        - "torch_row" - a list of per-row tensors with shape ``(C, *S)``. All
          columns must have the same sample shape ``S``. A single scalar column
          produces ``(1,)`` tensors. Numeric dtypes are combined with
          ``torch.promote_types``.
        - "torch_col" - one tensor with shape ``(C, B, *S)``. All columns must
          have the same sample shape ``S``. For one column this is an
          ``unsqueeze(0)`` view of the column tensor.
        - "polars" - the batch will be a polars DataFrame

        For torch formats, ``S`` is the shape of one sample. Scalar columns have
        ``S = ()``. A fixed-size-list column of width ``D`` contributes a
        dimension ``D``, and nested fixed-size lists contribute one dimension
        per level. Fixed-shape tensors use their logical shape, including any
        axis permutation. "torch_dict", "torch", and "torch_col" reject nulls;
        use the "python" or "arrow" format for nullable columns. "torch_row"
        preserves the previous NaN behavior for nullable scalar integer and
        floating columns, but rejects nulls in nested fixed-size-list and tensor
        columns.

        ``lance.bfloat16`` values become ``torch.bfloat16`` tensors when the
        leaf field is ``fixed_size_binary[2]`` and carries the
        ``ARROW:extension:name`` metadata value ``lance.bfloat16``. Unmarked
        binary columns are rejected.

        Conversion may or may not involve a data copy. Lance uses Arrow
        internally and can zero-copy to the arrow and polars formats.
        "torch_dict", "torch", and a single-column "torch_col" produce
        numeric and fixed-size-list tensors without copying where the Arrow
        layout allows. Multi-column "torch_col" and "torch_row" stack columns
        and therefore copy. Conversion to numpy and/or pandas can copy even
        homogeneous numeric columns. Conversion of strings, lists, and structs
        will require creating Python objects and this is not zero-copy.

        For custom formatting, use
        [with_transform][lancedb.permutation.Permutation.with_transform] which
        overrides this method.

        A format cannot be combined with
        [map][lancedb.permutation.Permutation.map], which always yields a list
        of dicts.  Doing so raises a ValueError.
        """
        assert format is not None, "format is required"
        # Checked before with_transform does, so that a format with an optional
        # dependency cannot fail on its import first
        if isinstance(self.transform_fn, _MapTransform):
            raise ValueError(_MAP_FORMAT_ERROR)
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
            return self.with_transform(batch_to_tensor_dict)
        elif format == "torch_dict":
            return self.with_transform(batch_to_tensor_columns)
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

        For a higher-level API that works with Python dicts, see
        [map][lancedb.permutation.Permutation.map].  A transform cannot be combined
        with ``map``.  Doing so raises a ValueError.
        """
        assert transform is not None, "transform is required"
        if isinstance(self.transform_fn, _MapTransform):
            raise ValueError(_MAP_FORMAT_ERROR)
        new = copy.copy(self)
        new.transform_fn = transform
        new._explicit_transform = True
        return new

    def map(
        self,
        fn: Callable[[dict[str, Any]], dict[str, Any]],
        *,
        batched: bool = False,
    ) -> "Permutation":
        """
        Apply a function to the rows of the permutation

        This is a dict-based alternative to
        [with_transform][lancedb.permutation.Permutation.with_transform], loosely
        modeled after the huggingface ``Dataset.map``.  It is lazy: ``fn`` is not
        called until data is read.

        Calls to ``map`` can be chained.  The callbacks run in order and the dict
        returned by a callback replaces the row entirely, so the next callback only
        sees what the previous one returned.  Unlike the huggingface method, the
        returned dict is not merged into the row: return every column to keep.

        A mapped permutation always yields a list of dicts (one per row) for each
        batch, which is the same shape as the default "python" format.  An integer
        index unwraps that list and returns the one dict.  ``take_offsets`` and
        ``__getitems__`` still return the list.  ``batched`` only controls what
        ``fn`` receives.  Rows keep their position, so a callback cannot add, drop,
        or reorder rows.

        Since ``map`` decides the output format it cannot be combined with
        [with_format][lancedb.permutation.Permutation.with_format] or
        [with_transform][lancedb.permutation.Permutation.with_transform].  Calling
        one after the other, in either order, raises a ValueError.  Convert the
        mapped rows to tensors in the DataLoader ``collate_fn`` instead.

        [select_columns][lancedb.permutation.Permutation.select_columns],
        [remove_columns][lancedb.permutation.Permutation.remove_columns],
        [rename_column][lancedb.permutation.Permutation.rename_column] and
        [schema][lancedb.permutation.Permutation.schema] describe the columns read
        from the base table, which are the input of the first callback.  They do
        not describe the output of ``map``, whether they are used before or after it.

        Parameters
        ----------
        fn: Callable
            The function to apply.  It receives and returns a single row as a dict
            mapping column names to values.  If ``batched`` is True it instead
            receives and returns a whole batch as a dict mapping column names to
            lists of values, and every returned column must have one value per
            input row.

            DataLoader workers started with ``spawn`` or ``forkserver`` receive a
            pickled copy of the permutation, so ``fn`` must then be picklable.
            Use a top-level function; lambdas and local functions do not pickle.
        batched: bool, default False
            Whether ``fn`` is called once per batch instead of once per row.

        Examples
        --------
        >>> import lancedb
        >>> db = lancedb.connect("memory:///")
        >>> tbl = db.create_table("tbl", data=[{"x": x} for x in range(3)])
        >>> def add_y(row):
        ...     return {**row, "y": row["x"] * 2}
        >>> def add_z(batch):
        ...     return {**batch, "z": [x + y for x, y in zip(batch["x"], batch["y"])]}
        >>> permutation = Permutation.identity(tbl).map(add_y).map(add_z, batched=True)
        >>> permutation.take_offsets([0, 1])
        [{'x': 0, 'y': 0, 'z': 0}, {'x': 1, 'y': 2, 'z': 3}]

        With a PyTorch DataLoader, a ``collate_fn`` turns the mapped rows of each
        batch into tensors::

            import torch

            def collate(rows):
                return {
                    "x": torch.tensor([row["x"] for row in rows]),
                    "z": torch.tensor([row["z"] for row in rows]),
                }

            dataloader = torch.utils.data.DataLoader(
                permutation, batch_size=32, shuffle=True, collate_fn=collate
            )
        """
        if not callable(fn):
            raise TypeError("fn must be callable")
        if self._explicit_transform:
            raise ValueError(_MAP_FORMAT_ERROR)

        steps = ((fn, batched),)
        if isinstance(self.transform_fn, _MapTransform):
            steps = self.transform_fn.steps + steps
        new = copy.copy(self)
        new.transform_fn = _MapTransform(steps)
        return new

    def take_offsets(self, offsets: list[int]) -> Any:
        """
        Take rows from the permutation by offset

        The returned value is passed through the permutation's current transform,
        so `with_format`, `with_transform`, and `map` affect this method in the
        same way they affect iteration. Offsets are positions in the current
        permutation, including any `with_skip` or `with_take` window, for both an
        identity permutation and one backed by a permutation table. An offset
        outside that window raises an error. The batch is returned as-is,
        including a one-element list. For a single row, use
        [__getitem__][lancedb.permutation.Permutation.__getitem__].
        """
        self._ensure_open()

        async def do_take_offsets():
            return await self.reader.take_offsets(offsets, selection=self.selection)

        batch = LOOP.run(do_take_offsets())
        return self.transform_fn(batch)

    def __getitem__(self, index: int) -> Any:
        """
        Return the row at `index`.

        `index` follows Python sequence indexing. `0` is the first row and a
        negative value counts from the end. An index past either end raises
        `IndexError`. A non-integer, such as a slice or a list, raises
        `TypeError`.

        The row is read with
        [take_offsets][lancedb.permutation.Permutation.take_offsets] and passed
        through the current transform
        ([with_format][lancedb.permutation.Permutation.with_format],
        [with_transform][lancedb.permutation.Permutation.with_transform], or
        [map][lancedb.permutation.Permutation.map]).

        A transform that returns a `list` is treated as one item per row, and
        this method returns that single item. The default `"python"` format and
        [map][lancedb.permutation.Permutation.map] therefore return one `dict`.
        `"torch"` returns one per-row dict of tensors and `"torch_row"` returns
        one tensor. If the list length is not one, the list is returned
        unchanged. That keeps a custom transform which does not emit one value
        per row intact.

        Every other return type is returned as the transform produced it, for
        the one requested row. `"arrow"` stays a one-row RecordBatch.
        `"pandas"` and `"polars"` stay one-row frames. `"numpy"` stays a
        one-row ndarray. `"python_col"`, `"torch_dict"`, and `"torch_col"`
        stay a dict or tensor covering that single row.

        [__getitems__][lancedb.permutation.Permutation.__getitems__] and
        `take_offsets` still return the transformed batch. A list-producing
        transform stays a list there, including when only one offset is
        requested, and negative offsets are not accepted.
        """
        resolved = operator.index(index)
        length = len(self)
        if resolved < 0:
            resolved += length
        if resolved < 0 or resolved >= length:
            raise IndexError(f"permutation index {index} is out of range")

        result = self.take_offsets([resolved])
        if isinstance(result, list) and len(result) == 1:
            return result[0]
        return result

    def __getitems__(self, indices: list[int]) -> Any:
        """
        Return rows for `indices` as one transformed batch.

        Unlike [__getitem__][lancedb.permutation.Permutation.__getitem__], this
        does not unwrap a one-element list into a single row.
        """
        return self.take_offsets(indices)

    @deprecated(details="Use with_skip instead")
    def skip(self, skip: int) -> "Permutation":
        """
        Skip the first `skip` rows of the permutation

        Note: this method returns a new permutation and does not modify `self`
        It is provided for compatibility with the huggingface Dataset API.

        Use [with_skip][lancedb.permutation.Permutation.with_skip] instead to
        avoid confusion.
        """
        return self.with_skip(skip)

    def with_skip(self, skip: int) -> "Permutation":
        """
        Skip the first `skip` rows of the permutation
        """
        self._ensure_open()
        new = copy.copy(self)
        new.offset = skip
        new.reader = LOOP.run(new._build_reader())
        new._pid = os.getpid()
        return new

    @deprecated(details="Use with_take instead")
    def take(self, limit: int) -> "Permutation":
        """
        Limit the permutation to `limit` rows (following any `skip`)

        Note: this method returns a new permutation and does not modify `self`
        It is provided for compatibility with the huggingface Dataset API.

        Use [with_take][lancedb.permutation.Permutation.with_take] instead to
        avoid confusion.
        """
        return self.with_take(limit)

    def with_take(self, limit: int) -> "Permutation":
        """
        Limit the permutation to `limit` rows (following any `skip`)
        """
        self._ensure_open()
        new = copy.copy(self)
        new.limit = limit
        new.reader = LOOP.run(new._build_reader())
        new._pid = os.getpid()
        return new

    @deprecated(details="Use with_repeat instead")
    def repeat(self, times: int) -> "Permutation":
        """
        Repeat the permutation `times` times

        Note: this method returns a new permutation and does not modify `self`
        It is provided for compatibility with the huggingface Dataset API.

        Use [with_repeat][lancedb.permutation.Permutation.with_repeat] instead
        to avoid confusion.
        """
        return self.with_repeat(times)

    def with_repeat(self, times: int) -> "Permutation":
        """
        Repeat the permutation `times` times
        """
        raise Exception("with_repeat is not yet implemented")
