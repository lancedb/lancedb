# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

from deprecation import deprecated
import pyarrow as pa

from ._lancedb import async_permutation_builder, PermutationReader
from .table import LanceTable
from .background_loop import LOOP
from typing import Any, Callable, Iterator, Literal, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from lancedb.dependencies import pandas as pd, numpy as np, torch, polars as pl


class PermutationBuilder:
    def __init__(self, table: LanceTable):
        self._async = async_permutation_builder(table)

    def select(self, projections: dict[str, str]) -> "PermutationBuilder":
        self._async.select(projections)
        return self

    def split_random(
        self,
        *,
        ratios: Optional[list[float]] = None,
        counts: Optional[list[int]] = None,
        fixed: Optional[int] = None,
        seed: Optional[int] = None,
    ) -> "PermutationBuilder":
        self._async.split_random(ratios=ratios, counts=counts, fixed=fixed, seed=seed)
        return self

    def split_hash(
        self,
        columns: list[str],
        split_weights: list[int],
        *,
        discard_weight: Optional[int] = None,
    ) -> "PermutationBuilder":
        self._async.split_hash(columns, split_weights, discard_weight=discard_weight)
        return self

    def split_sequential(
        self,
        *,
        ratios: Optional[list[float]] = None,
        counts: Optional[list[int]] = None,
        fixed: Optional[int] = None,
    ) -> "PermutationBuilder":
        self._async.split_sequential(ratios=ratios, counts=counts, fixed=fixed)
        return self

    def split_calculated(self, calculation: str) -> "PermutationBuilder":
        self._async.split_calculated(calculation)
        return self

    def shuffle(
        self, *, seed: Optional[int] = None, clump_size: Optional[int] = None
    ) -> "PermutationBuilder":
        self._async.shuffle(seed=seed, clump_size=clump_size)
        return self

    def filter(self, filter: str) -> "PermutationBuilder":
        self._async.filter(filter)
        return self

    def execute(self) -> LanceTable:
        async def do_execute():
            inner_tbl = await self._async.execute()
            return LanceTable.from_inner(inner_tbl)

        return LOOP.run(do_execute())


def permutation_builder(table: LanceTable) -> PermutationBuilder:
    return PermutationBuilder(table)


class Permutations:
    """
    A collection of permutations indexed by name.

    Each permutation or "split" is a view of a portion of the base table.  For more
    details see [Permutation](#permutation).
    """

    pass


class Transforms:
    """
    Namespace for common transformation functions
    """

    @staticmethod
    def arrow2python(batch: pa.RecordBatch) -> dict[str, list[Any]]:
        return batch.to_pydict()

    @staticmethod
    def arrow2arrow(batch: pa.RecordBatch) -> pa.RecordBatch:
        return batch

    @staticmethod
    def arrow2numpy(batch: pa.RecordBatch) -> dict[str, "np.ndarray"]:
        return batch.to_pandas().to_numpy()

    @staticmethod
    def arrow2pandas(batch: pa.RecordBatch) -> "pd.DataFrame":
        return batch.to_pandas()

    @staticmethod
    def arrow2torch() -> Callable[pa.RecordBatch, "torch.Tensor"]:
        import torch

        def impl(batch: pa.RecordBatch) -> torch.Tensor:
            return torch.from_dlpack(batch.to_dlpack())

        return impl

    @staticmethod
    def arrow2polars(batch: pa.RecordBatch) -> "pl.DataFrame":
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
        reader: PermutationReader,
        selection: dict[str, str],
        batch_size: int,
        transform_fn: Callable[pa.RecordBatch, Any],
    ):
        """
        Internal constructor.  Use [from_tables](#from_tables) instead.
        """
        assert reader is not None, "reader is required"
        assert selection is not None, "selection is required"
        self.reader = reader
        self.selection = selection
        self.transform_fn = transform_fn
        self.batch_size = batch_size

    def _with_selection(self, selection: dict[str, str]) -> "Permutation":
        """
        Creates a new permutation with the given selection

        Does not validation of the selection and it replaces it entirely.  This is not
        intended for public use.
        """
        return Permutation(self.reader, selection, self.batch_size, self.transform_fn)

    def _with_reader(self, reader: PermutationReader) -> "Permutation":
        """
        Creates a new permutation with the given reader

        This is an internal method and should not be used directly.
        """
        return Permutation(reader, self.selection, self.batch_size, self.transform_fn)

    def with_batch_size(self, batch_size: int) -> "Permutation":
        """
        Creates a new permutation with the given batch size
        """
        return Permutation(self.reader, self.selection, batch_size, self.transform_fn)

    @classmethod
    def from_tables(
        cls,
        base_table: LanceTable,
        permutation_table: LanceTable,
        split: Optional[str] = None,
    ) -> "Permutation":
        """
        Creates a permutation from the given base table and permutation table.
        """
        assert base_table is not None, "base_table is required"
        assert permutation_table is not None, "permutation_table is required"
        # TODO: Add str->int mapping for splits in permutation builder
        split = int(split) if split is not None else 0

        async def do_from_tables():
            reader = await PermutationReader.from_tables(
                base_table, permutation_table, split
            )
            schema = await reader.output_schema(None)
            initial_selection = {name: name for name in schema.names}
            return cls(
                reader, initial_selection, DEFAULT_BATCH_SIZE, Transforms.arrow2python
            )

        return LOOP.run(do_from_tables())

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

        async def do_iter():
            async_iter = await self.reader.read(self.selection, batch_size=batch_size)
            batches = []
            try:
                while True:
                    batch = await async_iter.__anext__()
                    if batch.num_rows == batch_size or not skip_last_batch:
                        batches.append(batch)
                    # else skip the partial last batch
            except StopAsyncIteration:
                # Reached the end of the stream
                pass
            return batches

        batches = LOOP.run(do_iter())
        for batch in batches:
            yield self.transform_fn(batch)

    def with_format(
        self, format: Literal["numpy", "python", "pandas", "arrow", "torch", "polars"]
    ) -> "Permutation":
        """
        Set the format for batches

        If this method is not called, the "python" format will be used.

        The format can be one of:
        - "numpy" - the batch will be a dict of numpy arrays (one per column)
        - "python" - the batch will be a dict of lists (one per column)
        - "pandas" - the batch will be a pandas DataFrame
        - "arrow" - the batch will be a pyarrow RecordBatch
        - "torch" - the batch will be a two dimensional torch tensor
        - "polars" - the batch will be a polars DataFrame

        Conversion may or may not involve a data copy.  Lance uses Arrow internally
        and so it is able to zero-copy to the arrow and polars.

        Conversion to torch will be zero-copy but will only support a subset of data
        types (numeric types).

        Conversion to numpy and/or pandas will typically be zero-copy for numeric
        types.  Conversion of strings, lists, and structs will require creating python
        objects and this is not zero-copy.

        For custom formatting, use [with_transform](#with_transform) which overrides
        this method.
        """
        assert format is not None, "format is required"
        if format == "python":
            return self.with_transform(Transforms.arrow2python)
        elif format == "numpy":
            return self.with_transform(Transforms.arrow2numpy)
        elif format == "pandas":
            return self.with_transform(Transforms.arrow2pandas)
        elif format == "arrow":
            return self.with_transform(Transforms.arrow2arrow)
        elif format == "torch":
            return self.with_transform(Transforms.arrow2torch())
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
        return Permutation(self.reader, self.selection, self.batch_size, transform)

    def __getitem__(self, index: int) -> Any:
        """
        Return a single row from the permutation

        The output will always be a python dictionary regardless of the format.

        This method is mostly useful for debugging and exploration.  For actual
        processing use [iter](#iter) or a torch data loader to perform batched
        processing.
        """
        pass

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

        async def do_with_skip():
            reader = await self.reader.with_offset(skip)
            return self._with_reader(reader)

        return LOOP.run(do_with_skip())

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

        async def do_with_take():
            reader = await self.reader.with_limit(limit)
            return self._with_reader(reader)

        return LOOP.run(do_with_take())

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
