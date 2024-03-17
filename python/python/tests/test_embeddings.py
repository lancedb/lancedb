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
from typing import List, Union

import lance
import lancedb
import numpy as np
import pyarrow as pa
import pytest
from lancedb.conftest import MockTextEmbeddingFunction
from lancedb.embeddings import (
    EmbeddingFunctionConfig,
    EmbeddingFunctionRegistry,
    with_embeddings,
)
from lancedb.embeddings.base import TextEmbeddingFunction
from lancedb.embeddings.registry import get_registry, register
from lancedb.pydantic import LanceModel, Vector


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


def test_embedding_function(tmp_path):
    registry = EmbeddingFunctionRegistry.get_instance()

    # let's create a table
    table = pa.table(
        {
            "text": pa.array(["hello world", "goodbye world"]),
            "vector": [np.random.randn(10), np.random.randn(10)],
        }
    )
    conf = EmbeddingFunctionConfig(
        source_column="text",
        vector_column="vector",
        function=MockTextEmbeddingFunction(),
    )
    metadata = registry.get_table_metadata([conf])
    table = table.replace_schema_metadata(metadata)

    # Write it to disk
    lance.write_dataset(table, tmp_path / "test.lance")

    # Load this back
    ds = lance.dataset(tmp_path / "test.lance")

    # can we get the serialized version back out?
    configs = registry.parse_functions(ds.schema.metadata)

    conf = configs["vector"]
    func = conf.function
    actual = func.compute_query_embeddings("hello world")

    # And we make sure we can call it
    expected = func.compute_query_embeddings("hello world")

    assert np.allclose(actual, expected)


@pytest.mark.slow
def test_embedding_function_rate_limit(tmp_path):
    def _get_schema_from_model(model):
        class Schema(LanceModel):
            text: str = model.SourceField()
            vector: Vector(model.ndims()) = model.VectorField()

        return Schema

    db = lancedb.connect(tmp_path)
    registry = EmbeddingFunctionRegistry.get_instance()
    model = registry.get("test-rate-limited").create(max_retries=0)
    schema = _get_schema_from_model(model)
    table = db.create_table("test", schema=schema, mode="overwrite")
    table.add([{"text": "hello world"}])
    with pytest.raises(Exception):
        table.add([{"text": "hello world"}])
    assert len(table) == 1

    model = registry.get("test-rate-limited").create()
    schema = _get_schema_from_model(model)
    table = db.create_table("test", schema=schema, mode="overwrite")
    table.add([{"text": "hello world"}])
    table.add([{"text": "hello world"}])
    assert len(table) == 2


def test_add_optional_vector(tmp_path):
    @register("mock-embedding")
    class MockEmbeddingFunction(TextEmbeddingFunction):
        def ndims(self):
            return 128

        def generate_embeddings(
            self, texts: Union[List[str], np.ndarray]
        ) -> List[np.array]:
            """
            Generate the embeddings for the given texts
            """
            return [np.random.randn(self.ndims()).tolist() for _ in range(len(texts))]

    registry = get_registry()
    model = registry.get("mock-embedding").create()

    class LanceSchema(LanceModel):
        id: str
        vector: Vector(model.ndims()) = model.VectorField(default=None)
        text: str = model.SourceField()

    db = lancedb.connect(tmp_path)
    tbl = db.create_table("optional_vector", schema=LanceSchema)

    # add works
    expected = LanceSchema(id="id", text="text")
    tbl.add([expected])
    assert not (np.abs(tbl.to_pandas()["vector"][0]) < 1e-6).all()
