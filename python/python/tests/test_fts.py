# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

# Copyright 2023 LanceDB Developers
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
import os
import random
from unittest import mock

import lancedb as ldb
from lancedb.db import DBConnection
from lancedb.index import FTS
from lancedb.query import BoostQuery, MatchQuery, MultiMatchQuery, PhraseQuery
import numpy as np
import pyarrow as pa
import pandas as pd
import pytest
from utils import exception_output

pytest.importorskip("lancedb.fts")
tantivy = pytest.importorskip("tantivy")


@pytest.fixture
def table(tmp_path) -> ldb.table.LanceTable:
    db = ldb.connect(tmp_path)
    vectors = [np.random.randn(128) for _ in range(100)]

    text_nouns = ("puppy", "car")
    text2_nouns = ("rabbit", "girl", "monkey")
    verbs = ("runs", "hits", "jumps", "drives", "barfs")
    adv = ("crazily.", "dutifully.", "foolishly.", "merrily.", "occasionally.")
    adj = ("adorable", "clueless", "dirty", "odd", "stupid")
    text = [
        " ".join(
            [
                text_nouns[random.randrange(0, len(text_nouns))],
                verbs[random.randrange(0, 5)],
                adv[random.randrange(0, 5)],
                adj[random.randrange(0, 5)],
            ]
        )
        for _ in range(100)
    ]
    text2 = [
        " ".join(
            [
                text2_nouns[random.randrange(0, len(text2_nouns))],
                verbs[random.randrange(0, 5)],
                adv[random.randrange(0, 5)],
                adj[random.randrange(0, 5)],
            ]
        )
        for _ in range(100)
    ]
    count = [random.randint(1, 10000) for _ in range(100)]
    table = db.create_table(
        "test",
        data=pd.DataFrame(
            {
                "vector": vectors,
                "id": [i % 2 for i in range(100)],
                "text": text,
                "text2": text2,
                "nested": [{"text": t} for t in text],
                "count": count,
            }
        ),
    )
    return table


@pytest.fixture
async def async_table(tmp_path) -> ldb.table.AsyncTable:
    db = await ldb.connect_async(tmp_path)
    vectors = [np.random.randn(128) for _ in range(100)]

    text_nouns = ("puppy", "car")
    text2_nouns = ("rabbit", "girl", "monkey")
    verbs = ("runs", "hits", "jumps", "drives", "barfs")
    adv = ("crazily.", "dutifully.", "foolishly.", "merrily.", "occasionally.")
    adj = ("adorable", "clueless", "dirty", "odd", "stupid")
    text = [
        " ".join(
            [
                text_nouns[random.randrange(0, len(text_nouns))],
                verbs[random.randrange(0, 5)],
                adv[random.randrange(0, 5)],
                adj[random.randrange(0, 5)],
            ]
        )
        for _ in range(100)
    ]
    text2 = [
        " ".join(
            [
                text2_nouns[random.randrange(0, len(text2_nouns))],
                verbs[random.randrange(0, 5)],
                adv[random.randrange(0, 5)],
                adj[random.randrange(0, 5)],
            ]
        )
        for _ in range(100)
    ]
    count = [random.randint(1, 10000) for _ in range(100)]
    table = await db.create_table(
        "test",
        data=pd.DataFrame(
            {
                "vector": vectors,
                "id": [i % 2 for i in range(100)],
                "text": text,
                "text2": text2,
                "nested": [{"text": t} for t in text],
                "count": count,
            }
        ),
    )
    return table


def test_create_index(tmp_path):
    index = ldb.fts.create_index(str(tmp_path / "index"), ["text"])
    assert isinstance(index, tantivy.Index)
    assert os.path.exists(str(tmp_path / "index"))


def test_create_index_with_stemming(tmp_path, table):
    index = ldb.fts.create_index(
        str(tmp_path / "index"), ["text"], tokenizer_name="en_stem"
    )
    assert isinstance(index, tantivy.Index)
    assert os.path.exists(str(tmp_path / "index"))

    # Check stemming by running tokenizer on non empty table
    table.create_fts_index("text", tokenizer_name="en_stem", use_tantivy=True)


@pytest.mark.parametrize("use_tantivy", [True, False])
@pytest.mark.parametrize("with_position", [True, False])
def test_create_inverted_index(table, use_tantivy, with_position):
    if use_tantivy and not with_position:
        pytest.skip("we don't support building a tantivy index without position")
    table.create_fts_index("text", use_tantivy=use_tantivy, with_position=with_position)


def test_populate_index(tmp_path, table):
    index = ldb.fts.create_index(str(tmp_path / "index"), ["text"])
    assert ldb.fts.populate_index(index, table, ["text"]) == len(table)


def test_search_index(tmp_path, table):
    index = ldb.fts.create_index(str(tmp_path / "index"), ["text"])
    ldb.fts.populate_index(index, table, ["text"])
    index.reload()
    results = ldb.fts.search_index(index, query="puppy", limit=5)
    assert len(results) == 2
    assert len(results[0]) == 5  # row_ids
    assert len(results[1]) == 5  # _score


@pytest.mark.parametrize("use_tantivy", [True, False])
def test_search_fts(table, use_tantivy):
    table.create_fts_index("text", use_tantivy=use_tantivy)
    results = table.search("puppy").select(["id", "text"]).limit(5).to_list()
    assert len(results) == 5
    assert len(results[0]) == 3  # id, text, _score

    # Default limit of 10
    results = table.search("puppy").select(["id", "text"]).to_list()
    assert len(results) == 10

    if not use_tantivy:
        # Test with a query
        results = (
            table.search(MatchQuery("puppy", "text"))
            .select(["id", "text"])
            .limit(5)
            .to_list()
        )
        assert len(results) == 5

        # Test boost query
        results = (
            table.search(
                BoostQuery(
                    MatchQuery("puppy", "text"),
                    MatchQuery("runs", "text"),
                )
            )
            .select(["id", "text"])
            .limit(5)
            .to_list()
        )
        assert len(results) == 5

        # Test multi match query
        table.create_fts_index("text2", use_tantivy=use_tantivy)
        results = (
            table.search(MultiMatchQuery("puppy", ["text", "text2"]))
            .select(["id", "text"])
            .limit(5)
            .to_list()
        )
        assert len(results) == 5
        assert len(results[0]) == 3  # id, text, _score

        # Test boolean query
        results = (
            table.search(MatchQuery("puppy", "text") & MatchQuery("runs", "text"))
            .select(["id", "text"])
            .limit(5)
            .to_list()
        )
        assert len(results) == 5
        assert len(results[0]) == 3  # id, text, _score
        for r in results:
            assert "puppy" in r["text"]
            assert "runs" in r["text"]


@pytest.mark.asyncio
async def test_fts_select_async(async_table):
    tbl = await async_table
    await tbl.create_index("text", config=FTS())
    await tbl.create_index("text2", config=FTS())
    results = (
        await tbl.query()
        .nearest_to_text("puppy")
        .select(["id", "text"])
        .limit(5)
        .to_list()
    )
    assert len(results) == 5
    assert len(results[0]) == 3  # id, text, _score

    # Test with FullTextQuery
    results = (
        await tbl.query()
        .nearest_to_text(MatchQuery("puppy", "text"))
        .select(["id", "text"])
        .limit(5)
        .to_list()
    )
    assert len(results) == 5
    assert len(results[0]) == 3  # id, text, _score

    # Test with BoostQuery
    results = (
        await tbl.query()
        .nearest_to_text(
            BoostQuery(
                MatchQuery("puppy", "text"),
                MatchQuery("runs", "text"),
            )
        )
        .select(["id", "text"])
        .limit(5)
        .to_list()
    )
    assert len(results) == 5
    assert len(results[0]) == 3  # id, text, _score

    # Test with MultiMatchQuery
    results = (
        await tbl.query()
        .nearest_to_text(MultiMatchQuery("puppy", ["text", "text2"]))
        .select(["id", "text"])
        .limit(5)
        .to_list()
    )
    assert len(results) == 5
    assert len(results[0]) == 3  # id, text, _score

    # Test with search() API
    results = (
        await (await tbl.search(MatchQuery("puppy", "text")))
        .select(["id", "text"])
        .limit(5)
        .to_list()
    )
    assert len(results) == 5
    assert len(results[0]) == 3  # id, text, _score


def test_search_fts_phrase_query(table):
    table.create_fts_index("text", use_tantivy=False, with_position=False)
    try:
        phrase_results = table.search('"puppy runs"').limit(100).to_list()
        assert False
    except Exception:
        pass
    table.create_fts_index("text", use_tantivy=False, with_position=True, replace=True)
    results = table.search("puppy").limit(100).to_list()
    phrase_results = table.search('"puppy runs"').limit(100).to_list()
    assert len(results) > len(phrase_results)
    assert len(phrase_results) > 0

    # Test with a query
    phrase_results = (
        table.search(PhraseQuery("puppy runs", "text")).limit(100).to_list()
    )
    assert len(results) > len(phrase_results)
    assert len(phrase_results) > 0


@pytest.mark.asyncio
async def test_search_fts_phrase_query_async(async_table):
    async_table = await async_table
    await async_table.create_index("text", config=FTS(with_position=False))
    try:
        phrase_results = (
            await async_table.query().nearest_to_text("puppy runs").limit(100).to_list()
        )
        assert False
    except Exception:
        pass
    await async_table.create_index("text", config=FTS(with_position=True))
    results = await async_table.query().nearest_to_text("puppy").limit(100).to_list()
    phrase_results = (
        await async_table.query().nearest_to_text('"puppy runs"').limit(100).to_list()
    )
    assert len(results) > len(phrase_results)
    assert len(phrase_results) > 0

    # Test with a query
    phrase_results = (
        await async_table.query()
        .nearest_to_text(PhraseQuery("puppy runs", "text"))
        .limit(100)
        .to_list()
    )
    assert len(results) > len(phrase_results)
    assert len(phrase_results) > 0


def test_search_fts_specify_column(table):
    table.create_fts_index("text", use_tantivy=False)
    table.create_fts_index("text2", use_tantivy=False)

    results = table.search("puppy", fts_columns="text").limit(5).to_list()
    assert len(results) == 5

    results = table.search("rabbit", fts_columns="text2").limit(5).to_list()
    assert len(results) == 5

    try:
        # we can only specify one column for now
        table.search("puppy", fts_columns=["text", "text2"]).limit(5).to_list()
        assert False
    except Exception:
        pass

    try:
        # have to specify a column because we have two fts indices
        table.search("puppy").limit(5).to_list()
        assert False
    except Exception:
        pass


@pytest.mark.asyncio
async def test_search_fts_async(async_table):
    async_table = await async_table
    await async_table.create_index("text", config=FTS())
    results = await async_table.query().nearest_to_text("puppy").limit(5).to_list()
    assert len(results) == 5

    expected_count = await async_table.count_rows(
        "count > 5000 and contains(text, 'puppy')"
    )
    expected_count = min(expected_count, 10)

    limited_results_pre_filter = await (
        async_table.query()
        .nearest_to_text("puppy")
        .where("count > 5000")
        .limit(10)
        .to_list()
    )
    assert len(limited_results_pre_filter) == expected_count
    limited_results_post_filter = await (
        async_table.query()
        .nearest_to_text("puppy")
        .where("count > 5000")
        .limit(10)
        .postfilter()
        .to_list()
    )
    assert len(limited_results_post_filter) <= expected_count


@pytest.mark.asyncio
async def test_search_fts_specify_column_async(async_table):
    async_table = await async_table
    await async_table.create_index("text", config=FTS())
    await async_table.create_index("text2", config=FTS())

    results = (
        await async_table.query()
        .nearest_to_text("puppy", columns="text")
        .limit(5)
        .to_list()
    )
    assert len(results) == 5

    results = (
        await async_table.query()
        .nearest_to_text("rabbit", columns="text2")
        .limit(5)
        .to_list()
    )
    assert len(results) == 5

    try:
        # we can only specify one column for now
        await (
            async_table.query()
            .nearest_to_text("rabbit", columns="text2")
            .limit(5)
            .to_list()
        )
        assert False
    except Exception:
        pass

    try:
        # have to specify a column because we have two fts indices
        await async_table.query().nearest_to_text("puppy").limit(5).to_list()
        assert False
    except Exception:
        pass


def test_search_ordering_field_index_table(tmp_path, table):
    table.create_fts_index("text", ordering_field_names=["count"], use_tantivy=True)
    rows = (
        table.search("puppy", ordering_field_name="count")
        .limit(20)
        .select(["text", "count"])
        .to_list()
    )
    for r in rows:
        assert "puppy" in r["text"]
    assert sorted(rows, key=lambda x: x["count"], reverse=True) == rows


def test_search_ordering_field_index(tmp_path, table):
    index = ldb.fts.create_index(
        str(tmp_path / "index"), ["text"], ordering_fields=["count"]
    )

    ldb.fts.populate_index(index, table, ["text"], ordering_fields=["count"])
    index.reload()
    results = ldb.fts.search_index(
        index, query="puppy", limit=5, ordering_field="count"
    )
    assert len(results) == 2
    assert len(results[0]) == 5  # row_ids
    assert len(results[1]) == 5  # _distance
    rows = table.to_lance().take(results[0]).to_pylist()

    for r in rows:
        assert "puppy" in r["text"]
    assert sorted(rows, key=lambda x: x["count"], reverse=True) == rows


@pytest.mark.parametrize("use_tantivy", [True, False])
def test_create_index_from_table(tmp_path, table, use_tantivy):
    table.create_fts_index("text", use_tantivy=use_tantivy)
    df = table.search("puppy").limit(5).select(["text"]).to_pandas()
    assert len(df) <= 5
    assert "text" in df.columns

    # Check whether it can be updated
    table.add(
        [
            {
                "vector": np.random.randn(128),
                "id": 101,
                "text": "gorilla",
                "text2": "gorilla",
                "nested": {"text": "gorilla"},
                "count": 10,
            }
        ]
    )

    with pytest.raises(Exception, match="already exists"):
        table.create_fts_index("text", use_tantivy=use_tantivy)

    table.create_fts_index("text", replace=True, use_tantivy=use_tantivy)
    assert len(table.search("gorilla").limit(1).to_pandas()) == 1


def test_create_index_multiple_columns(tmp_path, table):
    table.create_fts_index(["text", "text2"], use_tantivy=True)
    df = table.search("puppy").limit(5).to_pandas()
    assert len(df) == 5
    assert "text" in df.columns
    assert "text2" in df.columns


def test_empty_rs(tmp_path, table, mocker):
    table.create_fts_index(["text", "text2"], use_tantivy=True)
    mocker.patch("lancedb.fts.search_index", return_value=([], []))
    df = table.search("puppy").limit(5).to_pandas()
    assert len(df) == 0


def test_nested_schema(tmp_path, table):
    table.create_fts_index("nested.text", use_tantivy=True)
    rs = table.search("puppy").limit(5).to_list()
    assert len(rs) == 5


@pytest.mark.parametrize("use_tantivy", [True, False])
def test_search_index_with_filter(table, use_tantivy):
    table.create_fts_index("text", use_tantivy=use_tantivy)
    orig_import = __import__

    def import_mock(name, *args):
        if name == "duckdb":
            raise ImportError
        return orig_import(name, *args)

    # no duckdb
    with mock.patch("builtins.__import__", side_effect=import_mock):
        rs = table.search("puppy").where("id=1").limit(10)
        # test schema
        assert rs.to_arrow().drop("_score").schema.equals(table.schema)

        rs = rs.to_list()
        for r in rs:
            assert r["id"] == 1

    # yes duckdb
    rs2 = table.search("puppy").where("id=1").limit(10).to_list()
    for r in rs2:
        assert r["id"] == 1

    assert rs == rs2
    rs = table.search("puppy").where("id=1").with_row_id(True).limit(10).to_list()
    for r in rs:
        assert r["id"] == 1
        assert r["_rowid"] is not None


@pytest.mark.parametrize("use_tantivy", [True, False])
def test_null_input(table, use_tantivy):
    table.add(
        [
            {
                "vector": np.random.randn(128),
                "id": 101,
                "text": None,
                "text2": None,
                "nested": {"text": None},
                "count": 7,
            }
        ]
    )
    table.create_fts_index("text", use_tantivy=use_tantivy)


def test_syntax(table):
    # https://github.com/lancedb/lancedb/issues/769
    table.create_fts_index("text", use_tantivy=True)
    with pytest.raises(ValueError, match="Syntax Error"):
        table.search("they could have been dogs OR").limit(10).to_list()

    # these should work

    # terms queries
    table.search('"they could have been dogs" OR cats').limit(10).to_list()
    table.search("(they AND could) OR (have AND been AND dogs) OR cats").limit(
        10
    ).to_list()

    # phrase queries
    table.search("they could have been dogs OR cats").phrase_query().limit(10).to_list()
    table.search('"they could have been dogs OR cats"').limit(10).to_list()
    table.search('''"the cats OR dogs were not really 'pets' at all"''').limit(
        10
    ).to_list()
    table.search('the cats OR dogs were not really "pets" at all').phrase_query().limit(
        10
    ).to_list()
    table.search('the cats OR dogs were not really "pets" at all').phrase_query().limit(
        10
    ).to_list()


def test_language(mem_db: DBConnection):
    sentences = [
        "Il n'y a que trois routes qui traversent la ville.",
        "Je veux prendre la route vers l'est.",
        "Je te retrouve au café au bout de la route.",
    ]
    data = [{"text": s} for s in sentences]
    table = mem_db.create_table("test", data=data)

    with pytest.raises(ValueError) as e:
        table.create_fts_index("text", use_tantivy=False, language="klingon")

    assert exception_output(e) == (
        "ValueError: LanceDB does not support the requested language: 'klingon'\n"
        "Supported languages: Arabic, Danish, Dutch, English, Finnish, French, "
        "German, Greek, Hungarian, Italian, Norwegian, Portuguese, Romanian, "
        "Russian, Spanish, Swedish, Tamil, Turkish"
    )

    table.create_fts_index(
        "text",
        use_tantivy=False,
        language="French",
        stem=True,
        ascii_folding=True,
        remove_stop_words=True,
    )

    # Can get "routes" and "route" from the same root
    results = table.search("route", query_type="fts").limit(5).to_list()
    assert len(results) == 3

    # Can find "café", without needing to provide accent
    results = table.search("cafe", query_type="fts").limit(5).to_list()
    assert len(results) == 1

    # Stop words -> no results
    results = table.search("la", query_type="fts").limit(5).to_list()
    assert len(results) == 0


def test_fts_on_list(mem_db: DBConnection):
    data = pa.table(
        {
            "text": [
                ["lance database", "the", "search"],
                ["lance database"],
                ["lance", "search"],
                ["database", "search"],
                ["unrelated", "doc"],
            ],
            "vector": [
                [1.0, 2.0, 3.0],
                [4.0, 5.0, 6.0],
                [7.0, 8.0, 9.0],
                [10.0, 11.0, 12.0],
                [13.0, 14.0, 15.0],
            ],
        }
    )
    table = mem_db.create_table("test", data=data)
    table.create_fts_index("text", use_tantivy=False, with_position=True)

    res = table.search("lance").limit(5).to_list()
    assert len(res) == 3

    res = table.search(PhraseQuery("lance database", "text")).limit(5).to_list()
    assert len(res) == 2


def test_fts_ngram(mem_db: DBConnection):
    data = pa.table({"text": ["hello world", "lance database", "lance is cool"]})
    table = mem_db.create_table("test", data=data)
    table.create_fts_index("text", use_tantivy=False, tokenizer_name="ngram")

    results = table.search("lan").limit(10).to_list()
    assert len(results) == 2
    assert set(r["text"] for r in results) == {"lance database", "lance is cool"}

    results = table.search("nce").limit(10).to_list()  # spellchecker:disable-line
    assert len(results) == 2
    assert set(r["text"] for r in results) == {"lance database", "lance is cool"}

    # the default min_ngram_length is 3, so "la" should not match
    results = table.search("la").limit(10).to_list()
    assert len(results) == 0

    # test setting min_ngram_length and prefix_only
    table.create_fts_index(
        "text",
        use_tantivy=False,
        tokenizer_name="ngram",
        replace=True,
        min_ngram_length=2,
        prefix_only=True,
    )

    results = table.search("lan").limit(10).to_list()
    assert len(results) == 2
    assert set(r["text"] for r in results) == {"lance database", "lance is cool"}

    results = table.search("nce").limit(10).to_list()  # spellchecker:disable-line
    assert len(results) == 0

    results = table.search("la").limit(10).to_list()
    assert len(results) == 2
    assert set(r["text"] for r in results) == {"lance database", "lance is cool"}
