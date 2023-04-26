#  Copyright 2023 LanceDB Developers
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

import lance
from lancedb.query import LanceQueryBuilder

import numpy as np
import pandas as pd
import pandas.testing as tm
import pyarrow as pa

import pytest


class MockTable:
    def __init__(self, tmp_path):
        self.uri = tmp_path

    def to_lance(self):
        return lance.dataset(self.uri)


@pytest.fixture
def table(tmp_path) -> MockTable:
    df = pd.DataFrame(
        {
            "vector": [[1, 2], [3, 4]],
            "id": [1, 2],
            "str_field": ["a", "b"],
            "float_field": [1.0, 2.0],
        }
    )
    schema = pa.schema(
        [
            pa.field("vector", pa.list_(pa.float32(), list_size=2)),
            pa.field("id", pa.int32()),
            pa.field("str_field", pa.string()),
            pa.field("float_field", pa.float64()),
        ]
    )
    lance.write_dataset(df, tmp_path, schema)
    return MockTable(tmp_path)


def test_query_builder(table):
    df = LanceQueryBuilder(table, [0, 0]).limit(1).select(["id"]).to_df()
    assert df["id"].values[0] == 1
    assert all(df["vector"].values[0] == [1, 2])


def test_query_builder_with_filter(table):
    df = LanceQueryBuilder(table, [0, 0]).where("id = 2").to_df()
    assert df["id"].values[0] == 2
    assert all(df["vector"].values[0] == [3, 4])


def test_query_builder_with_metric(table):
    query = [4, 8]
    df_default = LanceQueryBuilder(table, query).to_df()
    df_l2 = LanceQueryBuilder(table, query).metric("l2").to_df()
    tm.assert_frame_equal(df_default, df_l2)

    df_cosine = LanceQueryBuilder(table, query).metric("cosine").limit(1).to_df()
    assert df_cosine.score[0] == pytest.approx(
        cosine_distance(query, df_cosine.vector[0]),
        abs=1e-6,
    )
    assert 0 <= df_cosine.score[0] <= 1


def cosine_distance(vec1, vec2):
    return 1 - np.dot(vec1, vec2) / (np.linalg.norm(vec1) * np.linalg.norm(vec2))
