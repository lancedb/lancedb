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
import shutil
from unittest import mock
from pathlib import Path
import zipfile

import lancedb as ldb
from lancedb.db import DBConnection
from lancedb.index import FTS
from lancedb.query import (
    BoostQuery,
    MatchQuery,
    MultiMatchQuery,
    PhraseQuery,
    BooleanQuery,
    ColumnOrdering,
    Occur,
    LanceFtsQueryBuilder,
)
import numpy as np
import pyarrow as pa
import pandas as pd
import pytest
import pytest_asyncio
from utils import exception_output

TEST_LANGUAGE_MODEL_HOME = Path(__file__).parent / "models"


@pytest.fixture
def table(tmp_path) -> ldb.table.LanceTable:
    # Use local random state to avoid affecting other tests
    rng = np.random.RandomState(42)
    local_random = random.Random(42)
    db = ldb.connect(tmp_path)
    vectors = [rng.randn(128) for _ in range(100)]

    text_nouns = ("puppy", "car")
    text2_nouns = ("rabbit", "girl", "monkey")
    verbs = ("runs", "hits", "jumps", "drives", "barfs")
    adv = ("crazily.", "dutifully.", "foolishly.", "merrily.", "occasionally.")
    adj = ("adorable", "clueless", "dirty", "odd", "stupid")
    text = [
        " ".join(
            [
                text_nouns[local_random.randrange(0, len(text_nouns))],
                verbs[local_random.randrange(0, 5)],
                adv[local_random.randrange(0, 5)],
                adj[local_random.randrange(0, 5)],
            ]
        )
        for _ in range(100)
    ]
    text2 = [
        " ".join(
            [
                text2_nouns[local_random.randrange(0, len(text2_nouns))],
                verbs[local_random.randrange(0, 5)],
                adv[local_random.randrange(0, 5)],
                adj[local_random.randrange(0, 5)],
            ]
        )
        for _ in range(100)
    ]
    count = [local_random.randint(1, 10000) for _ in range(100)]
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
def language_model_home(monkeypatch, tmp_path):
    model_home = tmp_path / "language-models"
    shutil.copytree(TEST_LANGUAGE_MODEL_HOME, model_home)
    monkeypatch.setenv("LANCE_LANGUAGE_MODEL_HOME", str(model_home))
    return model_home


@pytest.fixture
def lindera_ipadic(language_model_home):
    model_path = language_model_home / "lindera" / "ipadic"
    extracted_model = model_path / "main"
    config_path = model_path / "config.yml"

    if extracted_model.exists():
        shutil.rmtree(extracted_model)

    with zipfile.ZipFile(model_path / "main.zip", "r") as zip_ref:
        zip_ref.extractall(model_path)
    config_path.write_text(
        "segmenter:\n"
        '  mode: "normal"\n'
        f'  dictionary: "{extracted_model.resolve().as_posix()}"\n',
        encoding="utf-8",
    )

    try:
        yield
    finally:
        if extracted_model.exists():
            shutil.rmtree(extracted_model)


@pytest_asyncio.fixture
async def async_table(tmp_path) -> ldb.table.AsyncTable:
    # Use local random state to avoid affecting other tests
    rng = np.random.RandomState(42)
    local_random = random.Random(42)
    db = await ldb.connect_async(tmp_path)
    vectors = [rng.randn(128) for _ in range(100)]

    text_nouns = ("puppy", "car")
    text2_nouns = ("rabbit", "girl", "monkey")
    verbs = ("runs", "hits", "jumps", "drives", "barfs")
    adv = ("crazily.", "dutifully.", "foolishly.", "merrily.", "occasionally.")
    adj = ("adorable", "clueless", "dirty", "odd", "stupid")
    text = [
        " ".join(
            [
                text_nouns[local_random.randrange(0, len(text_nouns))],
                verbs[local_random.randrange(0, 5)],
                adv[local_random.randrange(0, 5)],
                adj[local_random.randrange(0, 5)],
            ]
        )
        for _ in range(100)
    ]
    text2 = [
        " ".join(
            [
                text2_nouns[local_random.randrange(0, len(text2_nouns))],
                verbs[local_random.randrange(0, 5)],
                adv[local_random.randrange(0, 5)],
                adj[local_random.randrange(0, 5)],
            ]
        )
        for _ in range(100)
    ]
    count = [local_random.randint(1, 10000) for _ in range(100)]
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


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        (
            {"use_tantivy": True},
            "Tantivy-based FTS has been removed",
        ),
        (
            {"ordering_field_names": ["count"]},
            "ordering_field_names was only supported",
        ),
        (
            {"writer_heap_size": 128},
            "writer_heap_size was only supported",
        ),
    ],
)
def test_reject_removed_tantivy_parameters(table, kwargs, match):
    with pytest.raises(ValueError, match=match):
        table.create_fts_index("text", **kwargs)


def test_reject_legacy_tantivy_index(table):
    path, _, _ = table._get_fts_index_path()
    os.makedirs(path, exist_ok=True)

    with pytest.raises(ValueError, match="Legacy Tantivy FTS index detected"):
        table.search("puppy").limit(5).to_list()

    with pytest.raises(ValueError, match="Legacy Tantivy FTS index detected"):
        table.create_fts_index("text")


@pytest.mark.parametrize("with_position", [True, False])
def test_create_inverted_index(table, with_position):
    with pytest.warns(DeprecationWarning, match="create_fts_index"):
        table.create_fts_index(
            "text",
            with_position=with_position,
            name="custom_fts_index",
        )
    indices = table.list_indices()
    fts_indices = [i for i in indices if i.index_type == "FTS"]
    assert any(i.name == "custom_fts_index" for i in fts_indices)


def test_search_fts(table):
    table.create_fts_index("text")
    results = table.search("puppy").select(["id", "text"]).limit(5).to_list()
    assert len(results) == 5
    assert len(results[0]) == 3  # id, text, _score

    # Default limit of 10
    results = table.search("puppy").select(["id", "text"]).to_list()
    assert len(results) == 10

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
    table.create_fts_index("text2")
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
    tbl = async_table
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
    table.create_fts_index("text", with_position=False)
    try:
        phrase_results = table.search('"puppy runs"').limit(100).to_list()
        assert False
    except Exception:
        pass
    table.create_fts_index("text", with_position=True, replace=True)
    results = table.search("puppy").limit(100).to_list()

    # Test with quotation marks
    phrase_results = table.search('"puppy runs"').limit(100).to_list()
    assert len(results) > len(phrase_results)
    assert len(phrase_results) > 0

    # Test with .phrase_query()
    phrase_results = table.search("puppy runs").phrase_query().limit(100).to_list()
    assert len(results) > len(phrase_results)
    assert len(phrase_results) > 0

    # Test with PhraseQuery()
    phrase_results = (
        table.search(PhraseQuery("puppy runs", "text")).limit(100).to_list()
    )
    assert len(results) > len(phrase_results)
    assert len(phrase_results) > 0


@pytest.mark.asyncio
async def test_search_fts_phrase_query_async(async_table):
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
    table.create_fts_index("text")
    table.create_fts_index("text2")

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


def test_search_order_by_descending(table):
    table.create_fts_index("text")
    rows = (
        table.search("puppy")
        .order_by([ColumnOrdering(column_name="count", ascending=False)])
        .limit(20)
        .select(["text", "count"])
        .to_list()
    )

    for r in rows:
        assert "puppy" in r["text"]
    assert sorted(rows, key=lambda x: x["count"], reverse=True) == rows


def test_search_order_by_ascending(table):
    table.create_fts_index("text")
    rows = (
        table.search("puppy")
        .order_by([ColumnOrdering(column_name="count", ascending=True)])
        .limit(20)
        .select(["text", "count"])
        .to_list()
    )

    for r in rows:
        assert "puppy" in r["text"]
    assert sorted(rows, key=lambda x: x["count"]) == rows


def test_create_index_from_table(tmp_path, table):
    table.create_fts_index("text")
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
        table.create_fts_index("text")

    table.create_fts_index("text", replace=True)
    assert len(table.search("gorilla").limit(1).to_pandas()) == 1


def test_create_index_multiple_columns(tmp_path, table):
    with pytest.raises(ValueError, match="Native FTS indexes can only be created"):
        table.create_fts_index(["text", "text2"])


def test_nested_schema(tmp_path, table):
    table.create_fts_index("nested.text", with_position=True)
    indices = table.list_indices()
    assert len(indices) == 1
    assert indices[0].index_type == "FTS"
    assert indices[0].columns == ["nested.text"]

    results = (
        table.search("puppy", query_type="fts", fts_columns="nested.text")
        .limit(5)
        .to_list()
    )
    assert len(results) > 0
    assert all("puppy" in row["nested"]["text"] for row in results)

    results = table.search(MatchQuery("puppy", "nested.text")).limit(5).to_list()
    assert len(results) > 0
    assert all("puppy" in row["nested"]["text"] for row in results)

    phrase_results = (
        table.search(PhraseQuery("puppy runs", "nested.text")).limit(5).to_list()
    )
    assert len(phrase_results) > 0
    assert all("puppy runs" in row["nested"]["text"] for row in phrase_results)

    hybrid_results = (
        table.search(query_type="hybrid", fts_columns="nested.text")
        .vector([0 for _ in range(128)])
        .text("puppy")
        .limit(5)
        .to_list()
    )
    assert len(hybrid_results) > 0


@pytest.mark.asyncio
async def test_nested_schema_async(async_table):
    await async_table.create_index("nested.text", config=FTS(with_position=True))
    indices = await async_table.list_indices()
    assert len(indices) == 1
    assert indices[0].index_type == "FTS"
    assert indices[0].columns == ["nested.text"]

    results = await (
        async_table.query()
        .nearest_to_text("puppy", columns="nested.text")
        .limit(5)
        .to_list()
    )
    assert len(results) > 0
    assert all("puppy" in row["nested"]["text"] for row in results)

    results = await (
        async_table.query()
        .nearest_to_text(MatchQuery("puppy", "nested.text"))
        .limit(5)
        .to_list()
    )
    assert len(results) > 0
    assert all("puppy" in row["nested"]["text"] for row in results)

    phrase_results = await (
        async_table.query()
        .nearest_to_text(PhraseQuery("puppy runs", "nested.text"))
        .limit(5)
        .to_list()
    )
    assert len(phrase_results) > 0
    assert all("puppy runs" in row["nested"]["text"] for row in phrase_results)

    hybrid_results = await (
        async_table.query()
        .nearest_to([0 for _ in range(128)])
        .nearest_to_text("puppy", columns="nested.text")
        .limit(5)
        .to_list()
    )
    assert len(hybrid_results) > 0


def test_nested_schema_rejects_invalid_fts_fields(tmp_path):
    db = ldb.connect(tmp_path)
    data = pa.table(
        {
            "payload": pa.array(
                [
                    {"text": "puppy runs", "count": 1},
                    {"text": "car drives", "count": 2},
                ]
            ),
            "vector": pa.array(
                [[0.1, 0.1], [0.2, 0.2]],
                type=pa.list_(pa.float32(), list_size=2),
            ),
        }
    )
    table = db.create_table("test", data=data)

    with pytest.raises(ValueError, match="FTS index cannot be created.*payload"):
        table.create_fts_index("payload")

    with pytest.raises(ValueError, match="FTS index cannot be created.*count"):
        table.create_fts_index("payload.count")

    with pytest.raises(ValueError, match="Field path `payload.missing` not found"):
        table.create_fts_index("payload.missing")


def test_search_index_with_filter(table):
    table.create_fts_index("text")
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


def test_null_input(table):
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
    table.create_fts_index("text")


def test_syntax(table):
    # https://github.com/lancedb/lancedb/issues/769
    table.create_fts_index("text")
    table.search("they could have been dogs OR").limit(10).to_list()

    # these should work

    # terms queries
    table.search('"they could have been dogs" OR cats').limit(10).to_list()
    table.search("(they AND could) OR (have AND been AND dogs) OR cats").limit(
        10
    ).to_list()

    # phrase queries
    table.create_fts_index("text", with_position=True, replace=True)
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
        table.create_fts_index("text", language="klingon")

    assert exception_output(e) == (
        "ValueError: LanceDB does not support the requested language: 'klingon'\n"
        "Supported languages: Arabic, Danish, Dutch, English, Finnish, French, "
        "German, Greek, Hungarian, Italian, Norwegian, Portuguese, Romanian, "
        "Russian, Spanish, Swedish, Tamil, Turkish"
    )

    table.create_fts_index(
        "text",
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
    table.create_fts_index("text", with_position=True)

    res = table.search("lance").limit(5).to_list()
    assert len(res) == 3

    res = table.search(PhraseQuery("lance database", "text")).limit(5).to_list()
    assert len(res) == 2


def test_fts_ngram(mem_db: DBConnection):
    data = pa.table({"text": ["hello world", "lance database", "lance is cool"]})
    table = mem_db.create_table("test", data=data)
    table.create_fts_index("text", base_tokenizer="ngram")

    results = table.search("lan", query_type="fts").limit(10).to_list()
    assert len(results) == 2
    assert set(r["text"] for r in results) == {"lance database", "lance is cool"}

    results = (
        table.search("nce", query_type="fts").limit(10).to_list()
    )  # spellchecker:disable-line
    assert len(results) == 2
    assert set(r["text"] for r in results) == {"lance database", "lance is cool"}

    # the default min_ngram_length is 3, so "la" should not match
    results = table.search("la", query_type="fts").limit(10).to_list()
    assert len(results) == 0

    # test setting min_ngram_length and prefix_only
    table.create_fts_index(
        "text",
        base_tokenizer="ngram",
        replace=True,
        ngram_min_length=2,
        prefix_only=True,
    )

    results = table.search("lan", query_type="fts").limit(10).to_list()
    assert len(results) == 2
    assert set(r["text"] for r in results) == {"lance database", "lance is cool"}

    results = (
        table.search("nce", query_type="fts").limit(10).to_list()
    )  # spellchecker:disable-line
    assert len(results) == 0

    results = table.search("la", query_type="fts").limit(10).to_list()
    assert len(results) == 2
    assert set(r["text"] for r in results) == {"lance database", "lance is cool"}


def test_fts_jieba_tokenizer(mem_db: DBConnection, language_model_home):
    data = pa.table({"text": ["我们都有光明的前途", "光明的前途"]})
    table = mem_db.create_table("test_jieba", data=data)
    table.create_fts_index(
        "text",
        base_tokenizer="jieba/default",
        stem=False,
        remove_stop_words=False,
        ascii_folding=False,
    )

    results = table.search("我们", query_type="fts").limit(10).to_list()
    assert [row["text"] for row in results] == ["我们都有光明的前途"]


def test_fts_jieba_missing_language_model_note(
    mem_db: DBConnection, monkeypatch, tmp_path
):
    missing_root = tmp_path / "missing-language-models"
    monkeypatch.setenv("LANCE_LANGUAGE_MODEL_HOME", str(missing_root))
    table = mem_db.create_table(
        "test_missing_jieba_model",
        data=pa.table({"text": ["我们都有光明的前途"]}),
    )

    with pytest.raises((ValueError, RuntimeError)) as e:
        table.create_fts_index(
            "text",
            base_tokenizer="jieba/default",
            stem=False,
            remove_stop_words=False,
            ascii_folding=False,
        )

    output = exception_output(e)
    assert "Invalid directory path:" in output
    assert "LANCE_LANGUAGE_MODEL_HOME" in output
    assert "jieba/default" in output


@pytest.mark.asyncio
async def test_fts_jieba_missing_language_model_note_async(monkeypatch, tmp_path):
    missing_root = tmp_path / "missing-language-models"
    monkeypatch.setenv("LANCE_LANGUAGE_MODEL_HOME", str(missing_root))
    db = await ldb.connect_async(tmp_path / "async-db")
    table = await db.create_table(
        "test_missing_jieba_model_async",
        data=pa.table({"text": ["我们都有光明的前途"]}),
    )

    with pytest.raises((ValueError, RuntimeError)) as e:
        await table.create_index(
            "text",
            config=FTS(
                base_tokenizer="jieba/default",
                stem=False,
                remove_stop_words=False,
                ascii_folding=False,
            ),
        )

    output = exception_output(e)
    assert "Invalid directory path:" in output
    assert "LANCE_LANGUAGE_MODEL_HOME" in output
    assert "jieba/default" in output


def test_fts_lindera_tokenizer(
    mem_db: DBConnection, language_model_home, lindera_ipadic
):
    data = pa.table({"text": ["成田国際空港", "東京国際空港", "羽田空港"]})
    table = mem_db.create_table("test_lindera", data=data)
    table.create_fts_index(
        "text",
        base_tokenizer="lindera/ipadic",
        stem=False,
        remove_stop_words=False,
        ascii_folding=False,
    )

    results = table.search("成田", query_type="fts").limit(10).to_list()
    assert [row["text"] for row in results] == ["成田国際空港"]


def test_fts_query_to_json():
    """Test that FTS query to_json() produces valid JSON strings with exact format."""

    # Test MatchQuery - basic
    match_query = MatchQuery("hello world", "text")
    json_str = match_query.to_json()
    expected = (
        '{"match":{"column":"text","terms":"hello world","boost":1.0,'
        '"fuzziness":0,"max_expansions":50,"operator":"Or","prefix_length":0}}'
    )
    assert json_str == expected

    # Test MatchQuery with options
    match_query = MatchQuery("puppy", "text", fuzziness=2, boost=1.5, prefix_length=3)
    json_str = match_query.to_json()
    expected = (
        '{"match":{"column":"text","terms":"puppy","boost":1.5,"fuzziness":2,'
        '"max_expansions":50,"operator":"Or","prefix_length":3}}'
    )
    assert json_str == expected

    # Test PhraseQuery
    phrase_query = PhraseQuery("quick brown fox", "title")
    json_str = phrase_query.to_json()
    expected = '{"phrase":{"column":"title","terms":"quick brown fox","slop":0}}'
    assert json_str == expected

    # Test PhraseQuery with slop
    phrase_query = PhraseQuery("quick brown", "title", slop=2)
    json_str = phrase_query.to_json()
    expected = '{"phrase":{"column":"title","terms":"quick brown","slop":2}}'
    assert json_str == expected

    # Test BooleanQuery with MUST
    must_query = BooleanQuery(
        [
            (Occur.MUST, MatchQuery("puppy", "text")),
            (Occur.MUST, MatchQuery("runs", "text")),
        ]
    )
    json_str = must_query.to_json()
    expected = (
        '{"boolean":{"should":[],"must":[{"match":{"column":"text","terms":"puppy",'
        '"boost":1.0,"fuzziness":0,"max_expansions":50,"operator":"Or",'
        '"prefix_length":0}},{"match":{"column":"text","terms":"runs","boost":1.0,'
        '"fuzziness":0,"max_expansions":50,"operator":"Or","prefix_length":0}}],'
        '"must_not":[]}}'
    )
    assert json_str == expected

    # Test BooleanQuery with SHOULD
    should_query = BooleanQuery(
        [
            (Occur.SHOULD, MatchQuery("cat", "text")),
            (Occur.SHOULD, MatchQuery("dog", "text")),
        ]
    )
    json_str = should_query.to_json()
    expected = (
        '{"boolean":{"should":[{"match":{"column":"text","terms":"cat","boost":1.0,'
        '"fuzziness":0,"max_expansions":50,"operator":"Or","prefix_length":0}},'
        '{"match":{"column":"text","terms":"dog","boost":1.0,"fuzziness":0,'
        '"max_expansions":50,"operator":"Or","prefix_length":0}}],"must":[],'
        '"must_not":[]}}'
    )
    assert json_str == expected

    # Test BooleanQuery with MUST_NOT
    must_not_query = BooleanQuery(
        [
            (Occur.MUST, MatchQuery("puppy", "text")),
            (Occur.MUST_NOT, MatchQuery("training", "text")),
        ]
    )
    json_str = must_not_query.to_json()
    expected = (
        '{"boolean":{"should":[],"must":[{"match":{"column":"text","terms":"puppy",'
        '"boost":1.0,"fuzziness":0,"max_expansions":50,"operator":"Or",'
        '"prefix_length":0}}],"must_not":[{"match":{"column":"text",'
        '"terms":"training","boost":1.0,"fuzziness":0,"max_expansions":50,'
        '"operator":"Or","prefix_length":0}}]}}'
    )
    assert json_str == expected

    # Test BoostQuery
    positive = MatchQuery("puppy", "text")
    negative = MatchQuery("training", "text")
    boost_query = BoostQuery(positive, negative, negative_boost=0.3)
    json_str = boost_query.to_json()
    expected = (
        '{"boost":{"positive":{"match":{"column":"text","terms":"puppy",'
        '"boost":1.0,"fuzziness":0,"max_expansions":50,"operator":"Or",'
        '"prefix_length":0}},"negative":{"match":{"column":"text",'
        '"terms":"training","boost":1.0,"fuzziness":0,"max_expansions":50,'
        '"operator":"Or","prefix_length":0}},"negative_boost":0.3}}'
    )
    assert json_str == expected

    # Test MultiMatchQuery
    multi_match = MultiMatchQuery("python", ["tags", "title"])
    json_str = multi_match.to_json()
    expected = (
        '{"multi_match":{"query":"python","columns":["tags","title"],'
        '"boost":[1.0,1.0]}}'
    )
    assert json_str == expected

    # Test complex nested BooleanQuery
    inner1 = BooleanQuery(
        [
            (Occur.MUST, MatchQuery("python", "tags")),
            (Occur.MUST, MatchQuery("tutorial", "title")),
        ]
    )
    inner2 = BooleanQuery(
        [
            (Occur.MUST, MatchQuery("rust", "tags")),
            (Occur.MUST, MatchQuery("guide", "title")),
        ]
    )
    complex_query = BooleanQuery(
        [
            (Occur.SHOULD, inner1),
            (Occur.SHOULD, inner2),
        ]
    )
    json_str = complex_query.to_json()
    expected = (
        '{"boolean":{"should":[{"boolean":{"should":[],"must":[{"match":'
        '{"column":"tags","terms":"python","boost":1.0,"fuzziness":0,'
        '"max_expansions":50,"operator":"Or","prefix_length":0}},{"match":'
        '{"column":"title","terms":"tutorial","boost":1.0,"fuzziness":0,'
        '"max_expansions":50,"operator":"Or","prefix_length":0}}],"must_not":[]}}'
        ',{"boolean":{"should":[],"must":[{"match":{"column":"tags",'
        '"terms":"rust","boost":1.0,"fuzziness":0,"max_expansions":50,'
        '"operator":"Or","prefix_length":0}},{"match":{"column":"title",'
        '"terms":"guide","boost":1.0,"fuzziness":0,"max_expansions":50,'
        '"operator":"Or","prefix_length":0}}],"must_not":[]}}],"must":[],'
        '"must_not":[]}}'
    )
    assert json_str == expected


def test_fts_fast_search(table):
    table.create_fts_index("text")

    # Insert some unindexed data
    table.add(
        [
            {
                "text": "xyz",
                "vector": [0 for _ in range(128)],
                "id": 101,
                "text2": "xyz",
                "nested": {"text": "xyz"},
                "count": 10,
            }
        ]
    )

    # Without fast_search, the query object should not have fast_search set
    builder = table.search("xyz", query_type="fts").limit(10)
    query = builder.to_query_object()
    assert query.fast_search is None

    # With fast_search, the query object should have fast_search=True
    builder = table.search("xyz", query_type="fts").fast_search().limit(10)
    query = builder.to_query_object()
    assert query.fast_search is True

    # fast_search should be chainable with other methods
    builder = (
        table.search("xyz", query_type="fts").fast_search().select(["text"]).limit(5)
    )
    query = builder.to_query_object()
    assert query.fast_search is True
    assert query.limit == 5
    assert query.columns == ["text"]

    # fast_search should be enabled by keyword argument too
    query = LanceFtsQueryBuilder(table, "xyz", fast_search=True).to_query_object()
    assert query.fast_search is True

    # Verify it executes without error and skips unindexed data
    results = table.search("xyz", query_type="fts").fast_search().limit(5).to_list()
    assert len(results) == 0

    # Update index and verify it returns results
    table.optimize()
    results = table.search("xyz", query_type="fts").fast_search().limit(5).to_list()
    assert len(results) > 0


@pytest.mark.asyncio
async def test_fts_fast_search_async(async_table):
    await async_table.create_index("text", config=FTS())

    # Insert some unindexed data
    await async_table.add(
        [
            {
                "text": "xyz",
                "vector": [0 for _ in range(128)],
                "id": 101,
                "text2": "xyz",
                "nested": {"text": "xyz"},
                "count": 10,
            }
        ]
    )

    # Without fast_search, should return results
    results = await async_table.query().nearest_to_text("xyz").limit(5).to_list()
    assert len(results) > 0

    # With fast_search, should return no results data unindexed
    fast_results = (
        await async_table.query()
        .nearest_to_text("xyz")
        .fast_search()
        .limit(5)
        .to_list()
    )
    assert len(fast_results) == 0

    # Update index and verify it returns results
    await async_table.optimize()

    fast_results = (
        await async_table.query()
        .nearest_to_text("xyz")
        .fast_search()
        .limit(5)
        .to_list()
    )
    assert len(fast_results) > 0

    # fast_search should be chainable with other methods
    results = (
        await async_table.query()
        .nearest_to_text("xyz")
        .fast_search()
        .select(["text"])
        .limit(5)
        .to_list()
    )
    assert len(results) > 0
