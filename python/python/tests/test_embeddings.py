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


def test_embedding_with_bad_results(tmp_path):
    @register("mock-embedding")
    class MockEmbeddingFunction(TextEmbeddingFunction):
        def ndims(self):
            return 128

        def generate_embeddings(
            self, texts: Union[List[str], np.ndarray]
        ) -> list[Union[np.array, None]]:
            return [
                None if i % 2 == 0 else np.random.randn(self.ndims())
                for i in range(len(texts))
            ]

    db = lancedb.connect(tmp_path)
    registry = EmbeddingFunctionRegistry.get_instance()
    model = registry.get("mock-embedding").create()

    class Schema(LanceModel):
        text: str = model.SourceField()
        vector: Vector(model.ndims()) = model.VectorField()

    table = db.create_table("test", schema=Schema, mode="overwrite")
    table.add(
        [{"text": "hello world"}, {"text": "bar"}],
        on_bad_vectors="drop",
    )

    df = table.to_pandas()
    assert len(table) == 1
    assert df.iloc[0]["text"] == "bar"

    # table = db.create_table("test2", schema=Schema, mode="overwrite")
    # table.add(
    #     [{"text": "hello world"}, {"text": "bar"}],
    # )
    # assert len(table) == 2
    # tbl = table.to_arrow()
    # assert tbl["vector"].null_count == 1


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


@pytest.mark.parametrize(
    "embedding_type",
    [
        "openai",
        "sentence-transformers",
        "huggingface",
        "ollama",
        "cohere",
        "instructor",
    ],
)
def test_embedding_function_safe_model_dump(embedding_type):
    registry = get_registry()

    # Note: Some embedding types might require specific parameters
    try:
        model = registry.get(embedding_type).create()
    except Exception as e:
        pytest.skip(f"Skipping {embedding_type} due to error: {str(e)}")

    dumped_model = model.safe_model_dump()

    assert all(
        not k.startswith("_") for k in dumped_model.keys()
    ), f"{embedding_type}: Dumped model contains keys starting with underscore"

    assert (
        "max_retries" in dumped_model
    ), f"{embedding_type}: Essential field 'max_retries' is missing from dumped model"

    assert isinstance(
        dumped_model, dict
    ), f"{embedding_type}: Dumped model is not a dictionary"

    for key in model.__dict__:
        if key.startswith("_"):
            assert key not in dumped_model, (
                f"{embedding_type}: Private attribute '{key}' "
                f"is present in dumped model"
            )
