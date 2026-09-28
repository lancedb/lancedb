# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""WAL-PK-FUSION: hybrid search's primary-key fallback for MemWAL tables.

Hybrid search joins its vector and full-text legs on `_rowid`, which a MemWAL
table cannot supply: the fresh tier has no stable row id, so the server rejects
`with_row_id` before it plans. When that happens the legs are joined on a
surrogate `_rowid` built from the table's primary key instead.

This is a stopgap until MemWAL supports `_rowid`, and everything it needs lives
here so it can be removed in one pass:

1. Delete this file and `tests/test_wal_hybrid.py`.
2. Run `grep -rn WAL-PK-FUSION python/ rust/` and follow the note at each
   marker; the Rust side has its own copy in
   `rust/lancedb/src/query/wal_fusion.rs`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Awaitable, Callable, List, Optional, Tuple, TypeVar

import pyarrow as pa

from ._blob import blob_v2_projection_sources

if TYPE_CHECKING:
    from .types import QueryProjection

T = TypeVar("T")


# Lance records the unenforced primary key as field metadata, which survives the
# `describe` round trip — so a remote table can resolve the key without opening
# the dataset. Keys are bytes on a pyarrow field.
_PK_MARKER = b"lance-schema:unenforced-primary-key"
_PK_POSITION_MARKER = b"lance-schema:unenforced-primary-key:position"
_TRUTHY = {"1", "true", "on", "yes", "y"}


def _pk_columns(schema: pa.Schema) -> List[str]:
    """The unenforced primary key columns of `schema`, in key order.

    Mirrors Lance's ordering: fields with an explicit 1-based position first, in
    position order, then fields carrying only the legacy boolean marker, in
    declaration order.
    """
    keyed = []
    for index, field in enumerate(schema):
        metadata = field.metadata or {}
        position = metadata.get(_PK_POSITION_MARKER)
        if position is not None:
            try:
                position = int(position)
            except ValueError:
                position = None
        if position is None:
            legacy = metadata.get(_PK_MARKER)
            if legacy is None or legacy.decode().lower() not in _TRUTHY:
                continue
            position = 0
        keyed.append((position == 0, position, index, field.name))
    return [name for *_, name in sorted(keyed)]


# Both layers raise this same sentence verbatim -- sophon's
# `LSM_WITH_ROW_ID_UNSUPPORTED` says so explicitly, and the OSS LSM scanner
# builds it from the same words -- so this substring survives either path and
# the wrapping each adds. It is a coupling to a message rather than a code,
# because the refusal arrives as a generic 400; if the server ever rewords it
# the fallback stops firing and hybrid on a MemWAL table errors visibly again,
# which is loud rather than silently wrong.
_WAL_ROW_ID_REFUSAL = "does not support with_row_id"

WAL_ROW_ID_UNSUPPORTED = (
    "hybrid search on a MemWAL table cannot return _rowid: the fresh tier has "
    "no stable row id, and the ids the fusion joins on are synthesized from "
    "the primary key. Set use_lsm(False) to read the base table only (results "
    "will exclude un-compacted MemWAL data)"
)


def _is_wal_row_id_refusal(error: BaseException) -> bool:
    """Whether `error` is the server declining `_rowid` because the table is
    MemWAL-backed, as opposed to any other failure the query might hit."""
    return _WAL_ROW_ID_REFUSAL in str(error)


def _with_pk_columns(columns, pk_columns: List[str]):
    """Add the key columns a projection is missing.

    Returns the projection to use and the columns added, which the caller drops
    from the output again so an injected key never reaches a caller who did not
    select it.
    """
    if not columns:
        # Every non-system column, the key among them.
        return columns, []
    # `PyQueryRequest.select` reports a dynamic projection as (name, sql) pairs.
    if isinstance(columns, (list, tuple)) and isinstance(columns[0], (list, tuple)):
        columns = dict(columns)
    missing = [c for c in pk_columns if c not in columns]
    if isinstance(columns, dict):
        return {**columns, **{c: c for c in missing}}, missing
    return list(columns) + missing, missing


def _stamp_surrogate_row_ids(
    tables: List[pa.Table], pk_columns: List[str]
) -> List[pa.Table]:
    """Give each table a `_rowid` column derived from its primary key.

    The fusion only ever compares that column for equality — to deduplicate, to
    group by in a reranker, and to restore the original scores — so on a MemWAL
    table, where the server has no stable row id to hand out, any injective
    mapping of the primary key does the same job. Ids are assigned in first-seen
    order across `tables`, so passing them in fusion order (vector, then FTS)
    preserves "first occurrence wins" and its tie break.

    Writing it under the `_rowid` name is what lets every reranker, third-party
    implementations included, run unmodified. The column never reaches the
    caller.
    """
    if not pk_columns:
        raise ValueError(
            "hybrid search on a MemWAL table needs an unenforced primary key to "
            "deduplicate on, and this table declares none"
        )

    ids = {}
    stamped = []
    for table in tables:
        if table.num_rows == 0:
            # Both legs empty: the combined schema carries no key column, and no
            # row that would need one.
            row_ids = pa.array([], type=pa.uint64())
        else:
            missing = [c for c in pk_columns if c not in table.column_names]
            if missing:
                raise ValueError(
                    "hybrid search could not deduplicate: primary key "
                    f"column(s) {missing} missing from a result set with "
                    f"{table.num_rows} rows"
                )
            keys = zip(*(table.column(c).to_pylist() for c in pk_columns))
            row_ids = pa.array(
                [ids.setdefault(key, len(ids)) for key in keys], type=pa.uint64()
            )
        stamped.append(table.append_column("_rowid", row_ids))
    return stamped


def pk_fusion_learned(table, use_lsm: Optional[bool]) -> bool:
    """Whether a previous hybrid query on `table` was refused `_rowid`, so this
    one should go straight to the primary key.

    Learned from a refusal, never probed, so asking is free.
    """
    if table is None or use_lsm is False:
        return False
    # Only an unambiguous True forks to the key: a table type that answers the
    # question loosely keeps the `_rowid` path.
    return table._hybrid_pk_fusion_learned() is True


class PkFusion:
    """How the legs of a hybrid query on a MemWAL table are joined: on a
    surrogate `_rowid` built from the primary key."""

    def __init__(self, pk_columns: List[str]):
        self.pk_columns = pk_columns
        # Key columns the caller did not select, added to the projection so the
        # surrogate can be built and dropped again before returning.
        self.injected: List[str] = []

    @classmethod
    def for_query(
        cls,
        schema: pa.Schema,
        projection: QueryProjection,
        *,
        caller_requested_row_id: bool,
    ) -> PkFusion:
        """Resolve the key for a hybrid query, refusing the shapes that need a
        real `_rowid`."""
        if caller_requested_row_id:
            raise NotImplementedError(WAL_ROW_ID_UNSUPPORTED)
        # A projected blob needs a real row id to fetch through, whatever else
        # has turned `_with_row_id` on — including `return_score="all"`.
        if blob_v2_projection_sources(schema, projection):
            raise NotImplementedError(
                "hybrid search cannot project a blob column on a MemWAL table: "
                "fetching blobs needs a real _rowid, and the fresh tier has no "
                "stable one. Set use_lsm(False) to read the base table only "
                "(results will exclude un-compacted MemWAL data)"
            )
        return cls(_pk_columns(schema))

    def inject(self, columns):
        """`columns` with the key columns it is missing, to project in place of
        the `_rowid` a MemWAL table would refuse."""
        columns, self.injected = _with_pk_columns(columns, self.pk_columns)
        return columns

    def stamp(
        self, vector_results: pa.Table, fts_results: pa.Table
    ) -> Tuple[pa.Table, pa.Table]:
        """Give both legs a surrogate `_rowid` built from the key."""
        # Vector first: `merge_results` concatenates in that order and keeps
        # the first occurrence, so first-seen ids preserve its tie break.
        vector_results, fts_results = _stamp_surrogate_row_ids(
            [vector_results, fts_results], self.pk_columns
        )
        return vector_results, fts_results

    def strip(self, results: pa.Table) -> pa.Table:
        """Drop the surrogate and any injected key columns from the reranked
        results.

        The surrogate is an internal join key, never an answer, and a caller
        who asked for `_rowid` was refused in `for_query`.
        """
        drop = [c for c in ["_rowid", *self.injected] if c in results.column_names]
        return results.drop(drop) if drop else results


def _check_refusal(error: Exception, *, fused: bool, caller_requested_row_id: bool):
    """Re-raise `error` unless it is a MemWAL `_rowid` refusal worth retrying on
    the primary key."""
    if fused or not _is_wal_row_id_refusal(error):
        raise error
    # `return_score="all"` turns row ids on for the reranker, not the caller, so
    # it still falls back; only a caller who asked for them is refused, and with
    # the reason rather than a bare 400.
    if caller_requested_row_id:
        raise NotImplementedError(WAL_ROW_ID_UNSUPPORTED) from error


def with_pk_fallback(
    run: Callable[[], T],
    table,
    *,
    fused: Callable[[], bool],
    caller_requested_row_id: bool,
) -> T:
    """Run a hybrid query, retrying on the primary key if the table refuses
    `_rowid`.

    `_rowid` is asked for first rather than paying a round trip up front to
    find out: only a MemWAL table refuses, and the refusal has to be handled
    anyway -- a write spec installed elsewhere can arrive between any two
    queries. It costs nothing server-side: it is raised before the query is
    planned. `fused` reports whether the attempt already used the key.
    """
    try:
        return run()
    except Exception as e:
        _check_refusal(
            e, fused=fused(), caller_requested_row_id=caller_requested_row_id
        )
        # Learned, so this retry and later queries go straight to the key.
        table._note_hybrid_pk_fusion()
        return run()


async def with_pk_fallback_async(
    run: Callable[[], Awaitable[T]],
    table,
    *,
    fused: Callable[[], bool],
    caller_requested_row_id: bool,
) -> T:
    """The async form of `with_pk_fallback`."""
    try:
        return await run()
    except Exception as e:
        _check_refusal(
            e, fused=fused(), caller_requested_row_id=caller_requested_row_id
        )
        table._note_hybrid_pk_fusion()
        return await run()
