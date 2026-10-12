# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""WAL-PK-FUSION: delete this file with `lancedb/_wal_hybrid.py`."""

from unittest import mock

import lancedb
import pyarrow as pa
import pytest
from lancedb._wal_hybrid import WAL_ROW_ID_UNSUPPORTED, with_pk_fallback
from lancedb.query import LanceHybridQueryBuilder
from lancedb.rerankers import RRFReranker


def _wal_table(schema: pa.Schema) -> mock.Mock:
    """A MemWAL-backed table a previous query has already been refused
    `_rowid` on, so the fusion goes straight to the primary key."""
    table = mock.Mock()
    table._hybrid_pk_fusion_learned.return_value = True
    table.schema = schema
    return table


_PK_SCHEMA = pa.schema(
    [
        pa.field(
            "id",
            pa.string(),
            metadata={b"lance-schema:unenforced-primary-key:position": b"1"},
        ),
        pa.field("text", pa.string()),
    ]
)


def _wal_hybrid(**select):
    builder = (
        LanceHybridQueryBuilder(_wal_table(_PK_SCHEMA)).vector([0.1, 0.2]).text("puppy")
    )
    if "columns" in select:
        builder = builder.select(select["columns"])
    return builder


def test_wal_hybrid_projects_the_key_instead_of_asking_for_row_ids():
    builder = _wal_hybrid(columns=["text"])
    builder._create_query_builders()

    assert builder._pk_fusion.pk_columns == ["id"]
    assert builder._pk_fusion.injected == ["id"]
    for leg in (builder._vector_query, builder._fts_query):
        assert leg._columns == ["text", "id"]
        # A MemWAL table rejects with_row_id before it plans.
        assert leg._with_row_id is not True


def test_wal_hybrid_selecting_the_key_injects_nothing():
    builder = _wal_hybrid(columns=["text", "id"])
    builder._create_query_builders()
    assert builder._pk_fusion.injected == []


def test_wal_hybrid_without_a_projection_leaves_it_alone():
    builder = _wal_hybrid()
    builder._create_query_builders()
    # Select-all already returns the key.
    assert builder._pk_fusion.injected == []
    assert builder._vector_query._columns is None


def test_base_table_hybrid_still_joins_on_row_ids():
    table = mock.Mock()
    table._hybrid_pk_fusion_learned.return_value = False
    builder = LanceHybridQueryBuilder(table).vector([0.1, 0.2]).text("puppy")
    builder._create_query_builders()

    # `to_arrow` is what turns row ids on for this path; all that matters here
    # is that no key was resolved and no projection was touched.
    assert builder._pk_fusion is None
    assert builder._vector_query._columns is None


def test_wal_hybrid_with_use_lsm_false_joins_on_row_ids():
    builder = _wal_hybrid().use_lsm(False)
    builder._create_query_builders()
    assert builder._pk_fusion is None


def test_wal_hybrid_refuses_an_explicit_with_row_id():
    builder = _wal_hybrid().with_row_id(True)
    with pytest.raises(NotImplementedError, match="use_lsm"):
        builder._create_query_builders()


def test_wal_hybrid_return_score_all_is_not_refused():
    """`return_score="all"` turns row ids on for the reranker, not the caller."""
    builder = _wal_hybrid().rerank(RRFReranker(return_score="all"))
    builder._create_query_builders()
    assert builder._pk_fusion.pk_columns == ["id"]


def test_wal_hybrid_fuses_on_the_key_and_drops_it_again():
    builder = _wal_hybrid(columns=["text"])
    builder._create_query_builders()

    vector_results = pa.table(
        {"text": ["a", "b"], "id": ["a", "b"], "_distance": [0.1, 0.2]}
    )
    fts_results = pa.table({"text": ["b", "c"], "id": ["b", "c"], "_score": [1.0, 2.0]})
    vector_results, fts_results = builder._pk_fusion.stamp(vector_results, fts_results)

    combined = LanceHybridQueryBuilder._combine_hybrid_results(
        fts_results=fts_results,
        vector_results=vector_results,
        norm="score",
        fts_query="puppy",
        reranker=RRFReranker(),
        limit=10,
        with_row_ids=True,
    )
    results = builder._finish_hybrid_results(combined)

    assert "_rowid" not in results.column_names, "the surrogate is internal"
    assert "id" not in results.column_names, "the caller selected only `text`"
    assert sorted(results.column("text").to_pylist()) == ["a", "b", "c"]


@pytest.mark.parametrize(
    "row_id_first", [True, False], ids=["row_id_first", "rerank_first"]
)
def test_wal_hybrid_refuses_row_id_even_with_return_score_all(row_id_first: bool):
    """`return_score="all"` sets the same flag the caller's request does, so
    asking for both must still be refused — in either call order."""
    builder = _wal_hybrid()
    reranker = RRFReranker(return_score="all")
    if row_id_first:
        builder = builder.with_row_id(True).rerank(reranker)
    else:
        builder = builder.rerank(reranker).with_row_id(True)

    with pytest.raises(NotImplementedError, match="use_lsm"):
        builder._create_query_builders()


def test_wal_hybrid_refuses_a_blob_projection_with_return_score_all():
    """A projected blob needs a real row id to fetch through, which the
    reranker turning `_with_row_id` on does not provide."""
    schema = pa.schema(
        [
            pa.field(
                "id",
                pa.string(),
                metadata={b"lance-schema:unenforced-primary-key:position": b"1"},
            ),
            pa.field("text", pa.string()),
            lancedb.blob("blob"),
        ]
    )
    builder = (
        LanceHybridQueryBuilder(_wal_table(schema))
        .vector([0.1, 0.2])
        .text("puppy")
        .select(["blob"])
        .rerank(RRFReranker(return_score="all"))
    )

    with pytest.raises(NotImplementedError, match="blob"):
        builder._create_query_builders()


_REFUSAL = RuntimeError(
    "Bad request: the MemWAL LSM scanner does not support with_row_id"
)


def test_fallback_retries_on_the_key_and_remembers_it():
    table = mock.Mock()
    attempts = []

    def run():
        attempts.append(len(attempts))
        if len(attempts) == 1:
            raise _REFUSAL
        return "fused"

    result = with_pk_fallback(
        run, table, fused=lambda: False, caller_requested_row_id=False
    )

    assert result == "fused"
    assert len(attempts) == 2
    table._note_hybrid_pk_fusion.assert_called_once()


def test_fallback_explains_the_refusal_to_a_caller_who_asked_for_row_ids():
    def run():
        raise _REFUSAL

    with pytest.raises(NotImplementedError, match="use_lsm") as info:
        with_pk_fallback(
            run, mock.Mock(), fused=lambda: False, caller_requested_row_id=True
        )
    assert str(info.value) == WAL_ROW_ID_UNSUPPORTED


@pytest.mark.parametrize(
    "error, fused",
    [(ValueError("unrelated"), False), (_REFUSAL, True)],
    ids=["other_error", "already_fused"],
)
def test_fallback_reraises_what_it_cannot_retry(error, fused):
    def run():
        raise error

    table = mock.Mock()
    with pytest.raises(type(error)):
        with_pk_fallback(run, table, fused=lambda: fused, caller_requested_row_id=False)
    table._note_hybrid_pk_fusion.assert_not_called()
