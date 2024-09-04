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
from lancedb.index import FTS
import numpy as np
import pandas as pd
import pytest

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
def test_create_inverted_index(table, use_tantivy):
    table.create_fts_index("text", use_tantivy=use_tantivy)


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
    results = table.search("puppy").limit(5).to_list()
    assert len(results) == 5


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
