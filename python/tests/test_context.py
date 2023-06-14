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

import pandas as pd
import pytest

from lancedb.context import contextualize


@pytest.fixture
def raw_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "token": [
                "The",
                "quick",
                "brown",
                "fox",
                "jumped",
                "over",
                "the",
                "lazy",
                "dog",
                "I",
                "love",
                "sandwiches",
            ],
            "document_id": [1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2],
        }
    )


def test_contextualizer(raw_df: pd.DataFrame):
    result = (
        contextualize(raw_df)
        .window(6)
        .stride(3)
        .text_col("token")
        .groupby("document_id")
        .to_df()["token"]
        .to_list()
    )

    assert result == [
        "The quick brown fox jumped over",
        "fox jumped over the lazy dog",
        "the lazy dog",
        "I love sandwiches",
    ]


def test_contextualizer_with_threshold(raw_df: pd.DataFrame):
    result = (
        contextualize(raw_df)
        .window(6)
        .stride(3)
        .text_col("token")
        .groupby("document_id")
        .min_window_size(4)
        .to_df()["token"]
        .to_list()
    )

    assert result == [
        "The quick brown fox jumped over",
        "fox jumped over the lazy dog",
    ]
