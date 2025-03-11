# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import os
from typing import List, Optional, Union
from unittest.mock import MagicMock, patch

import lance
import lancedb
import numpy as np
import pyarrow as pa
import pytest
import pandas as pd
from lancedb.conftest import MockTextEmbeddingFunction
from lancedb.embeddings import (
    EmbeddingFunctionConfig,
    EmbeddingFunctionRegistry,
)
from lancedb.embeddings.base import TextEmbeddingFunction
from lancedb.embeddings.registry import get_registry, register
from lancedb.embeddings.utils import retry
from lancedb.pydantic import LanceModel, Vector


def mock_embed_func(input_data):
    return [np.random.randn(128).tolist() for _ in range(len(input_data))]


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
        function=MockTextEmbeddingFunction.create(),
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


def test_embedding_function_variables():
    @register("variable-testing")
    class VariableTestingFunction(TextEmbeddingFunction):
        key1: str
        secret_key: Optional[str] = None

        @staticmethod
        def sensitive_keys():
            return ["secret_key"]

        def ndims():
            pass

        def generate_embeddings(self, _texts):
            pass

    registry = EmbeddingFunctionRegistry.get_instance()

    # Should error if variable is not set
    with pytest.raises(ValueError, match="Variable 'test' not found"):
        registry.get("variable-testing").create(
            key1="$var:test",
        )

    # Should use default values if not set
    func = registry.get("variable-testing").create(key1="$var:test:some_value")
    assert func.key1 == "some_value"

    # Should set a variable that the embedding function understands
    registry.set_var("test", "some_value")
    func = registry.get("variable-testing").create(key1="$var:test")
    assert func.key1 == "some_value"

    # Should reject secrets that aren't passed in as variables
    with pytest.raises(
        ValueError,
        match="Sensitive key 'secret_key' cannot be set to a hardcoded value",
    ):
        registry.get("variable-testing").create(
            key1="whatever", secret_key="some_value"
        )

    # Should not serialize secrets.
    registry.set_var("secret", "secret_value")
    func = registry.get("variable-testing").create(
        key1="whatever", secret_key="$var:secret"
    )
    assert func.secret_key == "secret_value"
    assert func.safe_model_dump()["secret_key"] == "$var:secret"


def test_embedding_with_bad_results(tmp_path):
    @register("null-embedding")
    class NullEmbeddingFunction(TextEmbeddingFunction):
        def ndims(self):
            return 128

        def generate_embeddings(
            self, texts: Union[List[str], np.ndarray]
        ) -> list[Union[np.array, None]]:
            # Return None, which is bad if field is non-nullable
            a = [
                (
                    np.full(self.ndims(), np.nan)
                    if i % 2 == 0
                    else np.random.randn(self.ndims())
                )
                for i in range(len(texts))
            ]
            return a

    db = lancedb.connect(tmp_path)
    registry = EmbeddingFunctionRegistry.get_instance()
    model = registry.get("null-embedding").create()

    class Schema(LanceModel):
        text: str = model.SourceField()
        vector: Vector(model.ndims()) = model.VectorField()

    table = db.create_table("test", schema=Schema, mode="overwrite")
    with pytest.raises(RuntimeError):
        # Default on_bad_vectors is "error"
        table.add([{"text": "hello world"}])

    table.add(
        [{"text": "hello world"}, {"text": "bar"}],
        on_bad_vectors="drop",
    )

    df = table.to_pandas()
    assert len(table) == 1
    assert df.iloc[0]["text"] == "bar"

    @register("nan-embedding")
    class NanEmbeddingFunction(TextEmbeddingFunction):
        def ndims(self):
            return 128

        def generate_embeddings(
            self, texts: Union[List[str], np.ndarray]
        ) -> list[Union[np.array, None]]:
            # Return NaN to produce bad vectors
            return [
                [np.NAN] * 128 if i % 2 == 0 else np.random.randn(self.ndims())
                for i in range(len(texts))
            ]

    db = lancedb.connect(tmp_path)
    registry = EmbeddingFunctionRegistry.get_instance()
    model = registry.get("nan-embedding").create()

    table = db.create_table("test2", schema=Schema, mode="overwrite")
    table.alter_columns(dict(path="vector", nullable=True))
    table.add(
        [{"text": "hello world"}, {"text": "bar"}],
        on_bad_vectors="null",
    )
    assert len(table) == 2
    tbl = table.to_arrow()
    assert tbl["vector"].null_count == 1


def test_with_existing_vectors(tmp_path):
    @register("mock-embedding")
    class MockEmbeddingFunction(TextEmbeddingFunction):
        def ndims(self):
            return 128

        def generate_embeddings(
            self, texts: Union[List[str], np.ndarray]
        ) -> List[np.array]:
            return [np.random.randn(self.ndims()).tolist() for _ in range(len(texts))]

    registry = get_registry()
    model = registry.get("mock-embedding").create()

    class Schema(LanceModel):
        text: str = model.SourceField()
        vector: Vector(model.ndims()) = model.VectorField()

    db = lancedb.connect(tmp_path)
    tbl = db.create_table("test", schema=Schema, mode="overwrite")
    tbl.add([{"text": "hello world", "vector": np.zeros(128).tolist()}])

    embeddings = tbl.to_arrow()["vector"].to_pylist()
    assert not np.any(embeddings), "all zeros"


def test_embedding_function_with_pandas(tmp_path):
    @register("mock-embedding")
    class _MockEmbeddingFunction(TextEmbeddingFunction):
        def ndims(self):
            return 128

        def generate_embeddings(
            self, texts: Union[List[str], np.ndarray]
        ) -> List[np.array]:
            return [np.random.randn(self.ndims()).tolist() for _ in range(len(texts))]

    registery = get_registry()
    func = registery.get("mock-embedding").create()

    class TestSchema(LanceModel):
        text: str = func.SourceField()
        val: int
        vector: Vector(func.ndims()) = func.VectorField()

    df = pd.DataFrame(
        {
            "text": ["hello world", "goodbye world"],
            "val": [1, 2],
        }
    )
    db = lancedb.connect(tmp_path)
    tbl = db.create_table("test", schema=TestSchema, mode="overwrite", data=df)
    schema = tbl.schema
    assert schema.field("text").type == pa.string()
    assert schema.field("val").type == pa.int64()
    assert schema.field("vector").type == pa.list_(pa.float32(), 128)

    df = pd.DataFrame(
        {
            "text": ["extra", "more"],
            "val": [4, 5],
        }
    )
    tbl.add(df)

    assert tbl.count_rows() == 4
    embeddings = tbl.to_arrow()["vector"]
    assert embeddings.null_count == 0

    df = pd.DataFrame(
        {
            "text": ["with", "embeddings"],
            "val": [6, 7],
            "vector": [np.zeros(128).tolist(), np.zeros(128).tolist()],
        }
    )
    tbl.add(df)

    embeddings = tbl.search().where("val > 5").to_arrow()["vector"].to_pylist()
    assert not np.any(embeddings), "all zeros"


def test_multiple_embeddings_for_pandas(tmp_path):
    @register("mock-embedding")
    class MockFunc1(TextEmbeddingFunction):
        def ndims(self):
            return 128

        def generate_embeddings(
            self, texts: Union[List[str], np.ndarray]
        ) -> List[np.array]:
            return [np.random.randn(self.ndims()).tolist() for _ in range(len(texts))]

    @register("mock-embedding2")
    class MockFunc2(TextEmbeddingFunction):
        def ndims(self):
            return 512

        def generate_embeddings(
            self, texts: Union[List[str], np.ndarray]
        ) -> List[np.array]:
            return [np.random.randn(self.ndims()).tolist() for _ in range(len(texts))]

    registery = get_registry()
    func1 = registery.get("mock-embedding").create()
    func2 = registery.get("mock-embedding2").create()

    class TestSchema(LanceModel):
        text: str = func1.SourceField()
        val: int
        vec1: Vector(func1.ndims()) = func1.VectorField()
        prompt: str = func2.SourceField()
        vec2: Vector(func2.ndims()) = func2.VectorField()

    df = pd.DataFrame(
        {
            "text": ["hello world", "goodbye world"],
            "val": [1, 2],
            "prompt": ["hello", "goodbye"],
        }
    )
    db = lancedb.connect(tmp_path)
    tbl = db.create_table("test", schema=TestSchema, mode="overwrite", data=df)

    schema = tbl.schema
    assert schema.field("text").type == pa.string()
    assert schema.field("val").type == pa.int64()
    assert schema.field("vec1").type == pa.list_(pa.float32(), 128)
    assert schema.field("prompt").type == pa.string()
    assert schema.field("vec2").type == pa.list_(pa.float32(), 512)
    assert tbl.count_rows() == 2


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


@pytest.mark.slow
@pytest.mark.parametrize(
    "embedding_type",
    [
        "openai",
        "sentence-transformers",
        "huggingface",
        "ollama",
        "cohere",
        "instructor",
        "voyageai",
    ],
)
def test_embedding_function_safe_model_dump(embedding_type):
    registry = get_registry()

    # Note: Some embedding types might require specific parameters
    try:
        model = registry.get(embedding_type).create({"max_retries": 1})
    except Exception as e:
        pytest.skip(f"Skipping {embedding_type} due to error: {str(e)}")

    dumped_model = model.safe_model_dump()

    assert all(not k.startswith("_") for k in dumped_model.keys()), (
        f"{embedding_type}: Dumped model contains keys starting with underscore"
    )

    assert "max_retries" in dumped_model, (
        f"{embedding_type}: Essential field 'max_retries' is missing from dumped model"
    )

    assert isinstance(dumped_model, dict), (
        f"{embedding_type}: Dumped model is not a dictionary"
    )

    for key in model.__dict__:
        if key.startswith("_"):
            assert key not in dumped_model, (
                f"{embedding_type}: Private attribute '{key}' "
                f"is present in dumped model"
            )


@patch("time.sleep")
def test_retry(mock_sleep):
    test_function = MagicMock(side_effect=[Exception] * 9 + ["result"])
    test_function = retry()(test_function)
    result = test_function()
    assert mock_sleep.call_count == 9
    assert result == "result"


@pytest.mark.skipif(
    os.environ.get("OPENAI_API_KEY") is None, reason="OpenAI API key not set"
)
def test_openai_propagates_api_key(monkeypatch):
    # Make sure that if we set it as a variable, the API key is propagated
    api_key = os.environ["OPENAI_API_KEY"]
    monkeypatch.delenv("OPENAI_API_KEY")

    uri = "memory://"
    registry = get_registry()
    registry.set_var("open_api_key", api_key)
    func = registry.get("openai").create(
        name="text-embedding-ada-002",
        max_retries=0,
        api_key="$var:open_api_key",
    )

    class Words(LanceModel):
        text: str = func.SourceField()
        vector: Vector(func.ndims()) = func.VectorField()

    db = lancedb.connect(uri)
    table = db.create_table("words", schema=Words, mode="overwrite")
    table.add([{"text": "hello world"}, {"text": "goodbye world"}])

    query = "greetings"
    actual = table.search(query).limit(1).to_pydantic(Words)[0]
    assert len(actual.text) > 0
