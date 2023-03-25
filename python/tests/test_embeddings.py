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
import sys

import numpy as np
import pyarrow as pa

from lancedb.embeddings import with_embeddings


def mock_embed_func(input_data):
    return [np.random.randn(128).tolist() for _ in range(len(input_data))]


def test_with_embeddings():
    for wrap_api in [True, False]:
        if wrap_api and sys.version_info.minor >= 11:
            # ratelimiter package doesn't work on 3.11
            continue
        data = pa.Table.from_arrays(
            [
                pa.array(["foo", "bar"]),
                pa.array([10.0, 20.0]),
            ],
            names=["text", "price"],
        )
        data = with_embeddings(mock_embed_func, data, wrap_api=wrap_api)
        assert data.num_columns == 3
        assert data.num_rows == 2
        assert data.column_names == ["text", "price", "vector"]
        assert data.column("text").to_pylist() == ["foo", "bar"]
        assert data.column("price").to_pylist() == [10.0, 20.0]
