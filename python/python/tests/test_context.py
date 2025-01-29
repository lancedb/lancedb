# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors


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
        .to_pandas()["token"]
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
        .to_pandas()["token"]
        .to_list()
    )

    assert result == [
        "The quick brown fox jumped over",
        "fox jumped over the lazy dog",
    ]
