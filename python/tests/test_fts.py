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

import numpy as np
import pandas as pd
import pytest
import tantivy

import lancedb as ldb
import lancedb.fts


@pytest.fixture
def table(tmp_path) -> ldb.table.LanceTable:
    db = ldb.connect(tmp_path)
    vectors = [np.random.randn(128) for _ in range(100)]

    nouns = ("puppy", "car", "rabbit", "girl", "monkey")
    verbs = ("runs", "hits", "jumps", "drives", "barfs")
    adv = ("crazily.", "dutifully.", "foolishly.", "merrily.", "occasionally.")
    adj = ("adorable", "clueless", "dirty", "odd", "stupid")
    text = [
        " ".join(
            [
                nouns[random.randrange(0, 5)],
                verbs[random.randrange(0, 5)],
                adv[random.randrange(0, 5)],
                adj[random.randrange(0, 5)],
            ]
        )
        for _ in range(100)
    ]
    table = db.create_table(
        "test", data=pd.DataFrame({"vector": vectors, "text": text, "text2": text})
    )
    return table


def test_create_index(tmp_path):
    index = ldb.fts.create_index(str(tmp_path / "index"), ["text"])
    assert isinstance(index, tantivy.Index)
    assert os.path.exists(str(tmp_path / "index"))


def test_populate_index(tmp_path, table):
    index = ldb.fts.create_index(str(tmp_path / "index"), ["text"])
    assert ldb.fts.populate_index(index, table, ["text"]) == len(table)


def test_search_index(tmp_path, table):
    index = ldb.fts.create_index(str(tmp_path / "index"), ["text"])
    ldb.fts.populate_index(index, table, ["text"])
    index.reload()
    results = ldb.fts.search_index(index, query="puppy", limit=10)
    assert len(results) == 2
    assert len(results[0]) == 10  # row_ids
    assert len(results[1]) == 10  # _distance


def test_create_index_from_table(tmp_path, table):
    table.create_fts_index("text")
    df = table.search("puppy").limit(10).select(["text"]).to_df()
    assert len(df) == 10
    assert "text" in df.columns


def test_create_index_multiple_columns(tmp_path, table):
    table.create_fts_index(["text", "text2"])
    df = table.search("puppy").limit(10).to_df()
    assert len(df) == 10
    assert "text" in df.columns
    assert "text2" in df.columns


def test_empty_rs(tmp_path, table, mocker):
    table.create_fts_index(["text", "text2"])
    mocker.patch("lancedb.fts.search_index", return_value=([], []))
    df = table.search("puppy").limit(10).to_df()
    assert len(df) == 0
