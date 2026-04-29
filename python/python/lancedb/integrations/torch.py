# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""
PyTorch integration for LanceDB.

Exposes ``LanceTorchDataset`` (map-style) and ``LanceIterableTorchDataset``
(iterable-style) wrappers that adapt a LanceDB table or permutation to the
PyTorch ``torch.utils.data`` API, while transparently handling the bits
that make a hand-rolled subclass tricky:

* The underlying Lance reader holds Rust state that is not picklable, but
  ``DataLoader(num_workers > 0)`` needs to fork the dataset to its workers.
  These classes strip the reader on pickle and re-open it in the worker on
  first read.
* Constructing a permutation from a table involves several steps
  (``permutation_builder``/``Permutation.from_tables``/``select_columns``
  /``with_format``/...). The wrapper takes those as constructor arguments
  and applies them once the dataset is opened in the worker.

Example
-------
>>> import lancedb, torch                                   # doctest: +SKIP
>>> from lancedb.integrations.torch import LanceTorchDataset
>>> db = lancedb.connect(uri)                               # doctest: +SKIP
>>> tbl = db.open_table("images_224")                       # doctest: +SKIP
>>> ds = LanceTorchDataset(                                 # doctest: +SKIP
...     tbl, columns=["image_bytes", "label"], format="torch"
... )
>>> loader = torch.utils.data.DataLoader(                   # doctest: +SKIP
...     ds, batch_size=64, num_workers=4, shuffle=True,
... )
"""

from typing import Any, Callable, Dict, List, Optional, Union

import torch.utils.data as _torch_data

from ..permutation import Permutation
from ..table import LanceTable


def _capture_table_state(table: LanceTable) -> Dict[str, Any]:
    """Pull just enough state out of a LanceTable so we can re-open the same
    table in a forked worker process where the Rust handle isn't valid."""
    conn = table._conn
    connect_kwargs: Dict[str, Any] = {}
    storage_options = getattr(conn, "storage_options", None)
    if storage_options is not None:
        connect_kwargs["storage_options"] = storage_options
    return {
        "uri": conn.uri,
        "table_name": table.name,
        "connect_kwargs": connect_kwargs,
    }


def _open_permutation(state: Dict[str, Any]) -> Permutation:
    """Reconstruct a Permutation from a captured state dict."""
    import lancedb

    db = lancedb.connect(state["uri"], **state["connect_kwargs"])
    base = db.open_table(state["table_name"])

    perm_table_name = state.get("perm_table_name")
    if perm_table_name is not None:
        perm_tbl = db.open_table(perm_table_name)
        perm = Permutation.from_tables(base, perm_tbl, state.get("split"))
    else:
        perm = Permutation.identity(base)

    columns = state.get("columns")
    fmt = state.get("format")
    transform = state.get("transform")
    batch_size = state.get("batch_size")

    if columns is not None:
        perm = perm.select_columns(columns)
    if fmt is not None:
        perm = perm.with_format(fmt)
    if transform is not None:
        perm = perm.with_transform(transform)
    if batch_size is not None:
        perm = perm.with_batch_size(batch_size)
    return perm


class LanceTorchDataset(_torch_data.Dataset):
    """
    A PyTorch map-style ``Dataset`` backed by a LanceDB table or permutation.

    Pass the same ``LanceTable`` you already opened (and, optionally, a
    permutation table / split / column selection / output format) and use
    the result anywhere a ``torch.utils.data.Dataset`` is expected.

    The wrapper:

    * Stores the URI / table name / storage options needed to re-open the
      table, not the Rust reader handle. Pickling keeps only the rebuild
      recipe, so ``DataLoader(num_workers > 0)`` works out of the box.
    * Implements both ``__getitem__`` and PyTorch's ``__getitems__`` dunder
      so the underlying batched ``Permutation.fetch`` is used when the
      DataLoader fetches a batch of indices.

    Parameters
    ----------
    table : LanceTable, optional
        The base table to read from. Either ``table`` or both ``uri`` and
        ``table_name`` must be provided.
    uri : str, optional
        Database URI to reconnect to. Required if ``table`` is not given.
    table_name : str, optional
        Name of the base table within ``uri``.
    connect_kwargs : dict, optional
        Extra keyword arguments forwarded to ``lancedb.connect`` when
        re-opening the database in a worker.
    permutation_table : LanceTable, optional
        A pre-built permutation table (see ``permutation_builder``) used to
        define the row ordering. If omitted, the identity permutation is
        used (rows in physical order).
    split : str or int, optional
        Split selector when ``permutation_table`` defines splits.
    columns : list[str], optional
        Subset of columns to read.
    format : str, optional
        Output format, forwarded to ``Permutation.with_format`` (e.g.
        ``"torch"`` for HuggingFace-style ``dict[str, Tensor]`` batches).
    transform : Callable, optional
        Custom batch transform, forwarded to ``Permutation.with_transform``.
        Must be picklable to work with ``num_workers > 0``.
    batch_size : int, optional
        Forwarded to ``Permutation.with_batch_size`` for direct iteration.
        DataLoader controls its own batching, so this only matters if the
        dataset is iterated directly.
    """

    def __init__(
        self,
        table: Optional[LanceTable] = None,
        *,
        uri: Optional[str] = None,
        table_name: Optional[str] = None,
        connect_kwargs: Optional[Dict[str, Any]] = None,
        permutation_table: Optional[LanceTable] = None,
        split: Optional[Union[str, int]] = None,
        columns: Optional[List[str]] = None,
        format: Optional[str] = None,
        transform: Optional[Callable] = None,
        batch_size: Optional[int] = None,
    ):
        if table is None and (uri is None or table_name is None):
            raise ValueError(
                "Provide either `table` or both `uri` and `table_name`."
            )

        if table is not None:
            state = _capture_table_state(table)
            if connect_kwargs is not None:
                state["connect_kwargs"] = connect_kwargs
        else:
            state = {
                "uri": uri,
                "table_name": table_name,
                "connect_kwargs": connect_kwargs or {},
            }

        state["perm_table_name"] = (
            permutation_table.name if permutation_table is not None else None
        )
        state["split"] = split
        state["columns"] = columns
        state["format"] = format
        state["transform"] = transform
        state["batch_size"] = batch_size

        self._state: Dict[str, Any] = state
        self._perm: Optional[Permutation] = None

    def __getstate__(self) -> Dict[str, Any]:
        # Strip the Rust-backed reader so the dataset is picklable. Workers
        # rebuild it on first read via _ensure_open().
        d = self.__dict__.copy()
        d["_perm"] = None
        return d

    def __setstate__(self, d: Dict[str, Any]) -> None:
        self.__dict__.update(d)

    def _ensure_open(self) -> None:
        if self._perm is None:
            self._perm = _open_permutation(self._state)

    def __len__(self) -> int:
        self._ensure_open()
        return len(self._perm)

    def __getitem__(self, index: int) -> Any:
        self._ensure_open()
        return self._perm[index]

    def __getitems__(self, indices: List[int]) -> Any:
        self._ensure_open()
        return self._perm.fetch(indices)


class LanceIterableTorchDataset(_torch_data.IterableDataset):
    """
    PyTorch iterable-style ``IterableDataset`` over a LanceDB permutation.

    Yields batches in the order defined by the underlying ``Permutation``.
    With ``num_workers > 1`` each worker iterates the permutation
    independently — for sharded iteration use the map-style
    ``LanceTorchDataset`` together with a sampler.

    Constructor arguments mirror ``LanceTorchDataset``.
    """

    def __init__(self, *args, **kwargs):
        self._inner = LanceTorchDataset(*args, **kwargs)

    def __getstate__(self) -> Dict[str, Any]:
        return {"_inner": self._inner.__getstate__()}

    def __setstate__(self, d: Dict[str, Any]) -> None:
        self._inner = LanceTorchDataset.__new__(LanceTorchDataset)
        self._inner.__setstate__(d["_inner"])

    def __iter__(self):
        self._inner._ensure_open()
        return iter(self._inner._perm)
