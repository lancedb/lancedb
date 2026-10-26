# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import os
import pickle
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


@pytest.fixture(autouse=True)
def isolated_embedding_registry(monkeypatch):
    registry = get_registry()
    monkeypatch.setattr(registry, "_functions", registry._functions.copy())


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


def test_embedding_function_registered_without_alias(tmp_path):
    registry = EmbeddingFunctionRegistry.get_instance()

    @registry.register()
    class NoAliasEmbeddingFunction(TextEmbeddingFunction):
        def ndims(self):
            return 2

        def generate_embeddings(self, texts):
            return [np.array([1.0, 2.0]) for _ in texts]

    assert registry.get("NoAliasEmbeddingFunction") is NoAliasEmbeddingFunction

    func = NoAliasEmbeddingFunction.create()

    class Schema(LanceModel):
        text: str = func.SourceField()
        vector: Vector(2) = func.VectorField()

    db = lancedb.connect(tmp_path)
    db.create_table("test", schema=Schema)

    # The class name is written to the table metadata, so the function can be
    # resolved again when the table is reopened.
    table = lancedb.connect(tmp_path).open_table("test")
    table.add([{"text": "hello"}])
    conf = table.embedding_functions["vector"]
    assert isinstance(conf.function, NoAliasEmbeddingFunction)
    assert table.to_arrow()["vector"].to_pylist() == [[1.0, 2.0]]


def test_instructor_ndims_uses_instruction():
    instructor = get_registry().get("instructor").create()
    model = MagicMock()
    model.encode.return_value = np.zeros((1, 384))

    with patch.object(type(instructor), "get_model", return_value=model):
        assert instructor.ndims() == 384

    model.encode.assert_called_once_with(
        [[instructor.source_instruction, "foo"]],
        batch_size=instructor.batch_size,
        show_progress_bar=instructor.show_progress_bar,
        normalize_embeddings=instructor.normalize_embeddings,
        device=instructor.device,
    )


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


def test_openai_variables_survive_metadata_round_trip():
    registry = EmbeddingFunctionRegistry.get_instance()

    registry.set_var("test_api_key", "sk-test-key-12345")

    conf = EmbeddingFunctionConfig(
        source_column="text",
        vector_column="vector",
        function=registry.get("openai").create(
            api_key="$var:test_api_key", base_url="https://api.example.com"
        ),
    )

    metadata = registry.get_table_metadata([conf])

    # Create a mock arrow table with the metadata
    schema = pa.schema(
        [
            pa.field("text", pa.string()),
            pa.field("vector", pa.list_(pa.float32(), 1536)),
        ]
    )
    table = pa.table({"text": [], "vector": []}, schema=schema)
    table = table.replace_schema_metadata(metadata)

    ds = lance.write_dataset(table, "memory://")

    configs = registry.parse_functions(ds.schema.metadata)

    assert "vector" in configs
    parsed_func = configs["vector"].function

    assert parsed_func.api_key == "sk-test-key-12345"
    assert parsed_func.base_url == "https://api.example.com"
    assert parsed_func.safe_model_dump()["api_key"] == "$var:test_api_key"

    with patch("lancedb.embeddings.openai.attempt_import_or_raise") as import_openai:
        parsed_func._openai_client

    import_openai.return_value.OpenAI.assert_called_once_with(
        api_key="sk-test-key-12345", base_url="https://api.example.com"
    )


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


def test_embedding_with_empty_output_vectors(tmp_path):
    """Regression test for issue #1672.

    When an embedding function returns an empty list (e.g. for empty-string
    inputs), _append_vector_columns used to crash because PyArrow cannot cast
    [] into a fixed-size list element.  The fix replaces wrong-length vectors
    with None before building the Arrow array so that _handle_bad_vectors can
    process them normally.
    """

    @register("empty-vec-embedding")
    class EmptyVecEmbeddingFunction(TextEmbeddingFunction):
        def ndims(self):
            return 128

        def generate_embeddings(self, texts: Union[List[str], np.ndarray]) -> list:
            # Simulate a model that returns an empty list for blank inputs
            return [
                [] if text.strip() == "" else np.random.randn(self.ndims()).tolist()
                for text in texts
            ]

    db = lancedb.connect(tmp_path)
    registry = EmbeddingFunctionRegistry.get_instance()
    model = registry.get("empty-vec-embedding").create()

    class Schema(LanceModel):
        text: str = model.SourceField()
        vector: Vector(model.ndims()) = model.VectorField()

    table = db.create_table("test_empty_vec", schema=Schema, mode="overwrite")

    # Should not crash; the row with the empty string should be dropped
    table.add(
        [{"text": "hello world"}, {"text": ""}, {"text": "foo"}],
        on_bad_vectors="drop",
    )

    assert len(table) == 2
    texts = table.to_arrow()["text"].to_pylist()
    assert "" not in texts


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

    registry = get_registry()
    func = registry.get("mock-embedding").create()

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

    registry = get_registry()
    func1 = registry.get("mock-embedding").create()
    func2 = registry.get("mock-embedding2").create()

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


@patch("time.sleep")
def test_openai_no_retry_on_401(mock_sleep):
    """
    Test that OpenAI embedding function does not retry on 401 authentication
    errors.
    """
    from lancedb.embeddings.utils import retry_with_exponential_backoff

    # Create a mock that raises an AuthenticationError
    class MockAuthenticationError(Exception):
        """Mock OpenAI AuthenticationError"""

        pass

    MockAuthenticationError.__name__ = "AuthenticationError"

    mock_func = MagicMock(side_effect=MockAuthenticationError("Invalid API key"))

    # Wrap the function with retry logic
    wrapped_func = retry_with_exponential_backoff(mock_func, max_retries=3)

    # Should raise without retrying
    with pytest.raises(MockAuthenticationError):
        wrapped_func()

    # Verify that the function was only called once (no retries)
    assert mock_func.call_count == 1
    # Verify that sleep was never called (no retries)
    assert mock_sleep.call_count == 0


class _HttpStatusError(Exception):
    """Stand-in for provider SDK errors (openai, cohere, ...) with a status code."""

    def __init__(self, status_code, message="error"):
        super().__init__(message)
        self.status_code = status_code


class _ResponseStatusError(Exception):
    """Stand-in for requests/httpx errors, which expose `response.status_code`."""

    def __init__(self, status_code):
        super().__init__(f"HTTP {status_code}")
        self.response = MagicMock(status_code=status_code)


def _run_with_retry(side_effect, **kwargs):
    """Run a function through the retry helper with a mocked `time.sleep`.

    Returns (mock_func, sleeps, error).
    """
    from lancedb.embeddings.utils import retry_with_exponential_backoff

    mock_func = MagicMock(side_effect=side_effect)
    sleeps = []
    error = None
    with patch("time.sleep", side_effect=sleeps.append):
        try:
            retry_with_exponential_backoff(mock_func, **kwargs)()
        except Exception as e:
            error = e
    return mock_func, sleeps, error


@pytest.mark.parametrize("status", [400, 401, 403, 404, 405, 410, 413, 422])
@pytest.mark.parametrize("make_error", [_HttpStatusError, _ResponseStatusError])
def test_retry_does_not_retry_permanent_http_errors(status, make_error):
    err = make_error(status)
    mock_func, sleeps, error = _run_with_retry(err, max_retries=7)

    assert error is err
    assert mock_func.call_count == 1
    assert sleeps == []


@pytest.mark.parametrize("status", [408, 409, 429, 500, 502, 503, 504])
@pytest.mark.parametrize("make_error", [_HttpStatusError, _ResponseStatusError])
def test_retry_retries_transient_http_errors(status, make_error):
    mock_func, sleeps, error = _run_with_retry(
        [make_error(status), make_error(status), "result"], max_retries=7
    )

    assert error is None
    assert mock_func.call_count == 3
    assert len(sleeps) == 2


@pytest.mark.parametrize(
    "exc",
    [
        ConnectionError("connection reset"),
        TimeoutError("timed out"),
        OSError("network is unreachable"),
        ValueError("custom embedding function failure"),
        RuntimeError("boom"),
        Exception("plain exception"),
    ],
)
def test_retry_retries_errors_without_http_status(exc):
    mock_func, sleeps, error = _run_with_retry([exc, exc, "result"], max_retries=7)

    assert error is None
    assert mock_func.call_count == 3
    assert len(sleeps) == 2


def test_retry_error_status_attributes_of_other_types_are_ignored():
    # `code` is a string for openai errors ("model_not_found"), and arbitrary
    # exceptions may carry unrelated numeric attributes. Neither is an HTTP status.
    class WeirdError(Exception):
        code = "model_not_found"
        status_code = None

    class ExitCodeError(Exception):
        code = 2

    for exc in (WeirdError("a"), ExitCodeError("b")):
        mock_func, sleeps, error = _run_with_retry(exc, max_retries=2)
        assert mock_func.call_count == 3
        assert len(sleeps) == 2


def test_retry_botocore_style_errors():
    # botocore ClientError exposes `response["ResponseMetadata"]["HTTPStatusCode"]`.
    # Bedrock reports throttling with a 400, which must stay retryable.
    class ClientError(Exception):
        def __init__(self, status, code):
            super().__init__(code)
            self.response = {
                "ResponseMetadata": {"HTTPStatusCode": status},
                "Error": {"Code": code},
            }

    throttled = ClientError(400, "ThrottlingException")
    mock_func, sleeps, error = _run_with_retry([throttled, "result"], max_retries=3)
    assert error is None
    assert mock_func.call_count == 2

    invalid = ClientError(400, "ValidationException")
    mock_func, sleeps, error = _run_with_retry(invalid, max_retries=3)
    assert error is invalid
    assert mock_func.call_count == 1
    assert sleeps == []


def test_retry_authentication_error_by_name_not_retried():
    class AuthenticationError(Exception):
        pass

    mock_func, sleeps, error = _run_with_retry(AuthenticationError("bad key"))
    assert isinstance(error, AuthenticationError)
    assert mock_func.call_count == 1
    assert sleeps == []


def test_retry_gives_up_after_max_retries_with_original_error_attached():
    original = _HttpStatusError(503, "overloaded")
    mock_func, sleeps, error = _run_with_retry(original, max_retries=3)

    assert mock_func.call_count == 4
    assert len(sleeps) == 3
    assert "Maximum number of retries (3) exceeded" in str(error)
    assert error.args[1] is original
    assert error.__cause__ is original


def test_retry_delay_is_capped_by_max_delay():
    # Default settings (7 retries, 60s cap): the old implementation could sleep for
    # up to ~47 minutes in a single retry and hours in total.
    exc = _HttpStatusError(503)
    for seed in range(50):
        import random

        random.seed(seed)
        _, sleeps, _ = _run_with_retry(exc)
        assert len(sleeps) == 7
        assert max(sleeps) <= 60
        assert sum(sleeps) <= 7 * 60

    # Custom cap
    _, sleeps, _ = _run_with_retry(exc, max_retries=10, max_delay=5)
    assert len(sleeps) == 10
    assert max(sleeps) <= 5


def test_retry_delay_without_jitter_is_exact_exponential_then_capped():
    _, sleeps, _ = _run_with_retry(
        _HttpStatusError(500),
        initial_delay=1,
        exponential_base=2,
        jitter=False,
        max_retries=8,
        max_delay=30,
    )
    assert sleeps == [2, 4, 8, 16, 30, 30, 30, 30]


def test_retry_jitter_is_bounded():
    # Each wait is within [delay / 2, delay], where delay grows by exponential_base
    # per retry (no random factor is compounded across retries).
    exc = _HttpStatusError(500)
    for seed in range(50):
        import random

        random.seed(seed)
        _, sleeps, _ = _run_with_retry(exc, max_retries=8, max_delay=100)
        for i, slept in enumerate(sleeps):
            delay = min(2 ** (i + 1), 100)
            assert delay / 2 <= slept <= delay

    # Extremes of the random source map to the ends of the range
    with patch("random.random", return_value=0.0):
        _, sleeps, _ = _run_with_retry(exc, max_retries=3)
    assert sleeps == [1, 2, 4]
    with patch("random.random", return_value=1.0):
        _, sleeps, _ = _run_with_retry(exc, max_retries=3)
    assert sleeps == [2, 4, 8]


def test_retry_large_max_retries_does_not_overflow():
    _, sleeps, _ = _run_with_retry(_HttpStatusError(503), max_retries=1500)
    assert len(sleeps) == 1500
    assert max(sleeps) <= 60


def _register_failing_embedding(name, error):
    """Register an embedding function that always raises `error`.

    Returns the class and a list that records every call. The function is
    re-created from table metadata, so the call count has to live outside of it.
    """
    calls = []

    @register(name)
    class FailingEmbeddingFunction(TextEmbeddingFunction):
        def ndims(self):
            return 4

        def generate_embeddings(self, texts):
            calls.append(len(texts))
            raise error

    return FailingEmbeddingFunction, calls


@pytest.mark.parametrize("status", [400, 404, 422])
def test_permanent_embedding_error_is_not_retried(tmp_path, status):
    """A permanent provider error (e.g. a misspelled model name) must surface
    immediately from both `table.add` and `table.search` instead of being retried
    with minutes of backoff."""
    error = _HttpStatusError(status, "The model `typo-embed` does not exist")
    cls, calls = _register_failing_embedding(f"failing-embedding-{status}", error)
    func = cls.create()  # default max_retries=7

    class Schema(LanceModel):
        text: str = func.SourceField()
        vector: Vector(func.ndims()) = func.VectorField()

    db = lancedb.connect(tmp_path)
    table = db.create_table("t", schema=Schema)

    with patch("time.sleep") as mock_sleep:
        with pytest.raises(Exception, match="The model `typo-embed` does not exist"):
            table.add([{"text": "hello"}])
        assert len(calls) == 1

        with pytest.raises(_HttpStatusError, match="does not exist"):
            table.search("hello").limit(1).to_list()
        assert len(calls) == 2

    mock_sleep.assert_not_called()


def test_transient_embedding_error_is_retried_in_table_add(tmp_path):
    cls, calls = _register_failing_embedding(
        "failing-embedding-503", _HttpStatusError(503)
    )
    func = cls.create(max_retries=2)

    class Schema(LanceModel):
        text: str = func.SourceField()
        vector: Vector(func.ndims()) = func.VectorField()

    db = lancedb.connect(tmp_path)
    table = db.create_table("t", schema=Schema)

    with patch("time.sleep") as mock_sleep:
        with pytest.raises(Exception, match="Maximum number of retries"):
            table.add([{"text": "hello"}])

    assert len(calls) == 3
    assert mock_sleep.call_count == 2
    assert all(call.args[0] <= 60 for call in mock_sleep.call_args_list)


@patch("time.sleep")
@pytest.mark.parametrize(
    "error",
    [
        pytest.param("missing_key", id="missing-api-key"),
        pytest.param(ImportError("No module named 'voyageai'"), id="missing-package"),
    ],
)
def test_no_retry_on_deterministic_errors(mock_sleep, error):
    from lancedb.embeddings.utils import (
        api_key_not_found_help,
        retry_with_exponential_backoff,
    )

    def func():
        if error == "missing_key":
            api_key_not_found_help("voyageai")
        raise error

    mock_func = MagicMock(side_effect=func)
    wrapped_func = retry_with_exponential_backoff(mock_func, max_retries=3)

    with pytest.raises((ValueError, ImportError)):
        wrapped_func()

    assert mock_func.call_count == 1
    assert mock_sleep.call_count == 0


@patch("time.sleep")
def test_retry_on_wrapped_provider_error(mock_sleep):
    """Providers such as Bedrock wrap throttling in a plain ValueError."""
    from lancedb.embeddings.utils import retry_with_exponential_backoff

    mock_func = MagicMock(
        side_effect=[
            ValueError("Error raised by boto3 client: ThrottlingException"),
            [0.1],
        ]
    )
    wrapped_func = retry_with_exponential_backoff(mock_func, max_retries=3)

    assert wrapped_func() == [0.1]
    assert mock_func.call_count == 2
    assert mock_sleep.call_count == 1


def test_openai_bad_request_partial_batch_recovery():
    # Regression test for issue #1677

    # Fake exception types, avoids requiring the optional openai/httpx deps
    class FakeBadRequestError(Exception):
        pass

    class FakeAuthenticationError(Exception):
        pass

    fake_openai_module = MagicMock()
    fake_openai_module.BadRequestError = FakeBadRequestError
    fake_openai_module.AuthenticationError = FakeAuthenticationError

    registry = get_registry()
    model = registry.get("openai").create(name="text-embedding-ada-002")

    def fake_create(input, model=None, **kwargs):
        # Full batch always rejected
        if len(input) > 1:
            raise FakeBadRequestError("bad request")
        if input[0] == "bad text":
            raise FakeBadRequestError("bad request")
        embedding = MagicMock()
        embedding.embedding = [0.1, 0.2, 0.3]
        result = MagicMock()
        result.data = [embedding]
        return result

    mock_client = MagicMock()
    mock_client.embeddings.create.side_effect = fake_create

    with (
        patch(
            "lancedb.embeddings.openai.attempt_import_or_raise",
            return_value=fake_openai_module,
        ),
        patch.object(type(model), "_openai_client", new_callable=lambda: mock_client),
    ):
        embeddings = model.generate_embeddings(["good text", "bad text"])

    assert embeddings[0] == [0.1, 0.2, 0.3]
    assert embeddings[1] is None


def test_ollama_embeddings_pickle():
    """OllamaEmbeddings must pickle even after the cached client is created."""
    registry = get_registry()
    model = registry.get("ollama").create(name="nomic-embed-text")

    # Simulate accessing the cached client, which stores it on the instance.
    model.__dict__["_ollama_client"] = MagicMock()

    pickled = pickle.dumps(model)
    restored = pickle.loads(pickled)

    assert restored.name == "nomic-embed-text"
    assert restored.host == "http://localhost:11434"
    assert "_ollama_client" not in restored.__dict__


def test_url_retrieve_downloads_image():
    """
    Embedding functions like open-clip, siglip, and jinaai use url_retrieve()
    to download images from HTTP URLs. For example, open_clip._to_pil() calls:

        PIL_Image.open(io.BytesIO(url_retrieve(image)))

    Verify that url_retrieve() can download an image and open it as PIL Image,
    matching the real usage pattern in embedding functions.
    """
    import io

    Image = pytest.importorskip("PIL.Image")
    from lancedb.embeddings.utils import url_retrieve

    image_url = "http://farm1.staticflickr.com/53/167798175_7c7845bbbd_z.jpg"
    image_bytes = url_retrieve(image_url)
    img = Image.open(io.BytesIO(image_bytes))
    assert img.size[0] > 0 and img.size[1] > 0


def test_open_clip_opens_percent_encoded_file_uri(tmp_path):
    """OpenCLIP should decode local file URIs before opening them."""
    Image = pytest.importorskip("PIL.Image")
    from lancedb.embeddings.open_clip import OpenClipEmbeddings

    image_path = tmp_path / "test image.png"
    Image.new("RGB", (4, 4), color="red").save(image_path, format="PNG")

    with OpenClipEmbeddings._to_pil(None, image_path.as_uri()) as image:
        assert image.size == (4, 4)


def test_open_clip_model_in_eval_mode():
    """
    open_clip.create_model_and_transforms returns the model in training mode,
    which leaves dropout, stochastic depth and batch norm in their training
    behaviour. OpenCLIP must switch the model to eval mode before embedding.
    """
    from lancedb.embeddings.open_clip import OpenClipEmbeddings

    class FakeModel:
        training = True

        def to(self, device):
            return self

        def eval(self):
            self.training = False
            return self

    open_clip = MagicMock()
    open_clip.create_model_and_transforms.return_value = (FakeModel(), None, None)

    with patch(
        "lancedb.embeddings.open_clip.attempt_import_or_raise",
        return_value=open_clip,
    ):
        func = OpenClipEmbeddings(name="ViTamin-B", pretrained="datacomp1b")

    open_clip.create_model_and_transforms.assert_called_once_with(
        "ViTamin-B", pretrained="datacomp1b"
    )
    assert func._model.training is False


def test_jina_generate_image_input_dict_local_path(tmp_path):
    """
    JinaEmbeddings._generate_image_input_dict must accept a local image path
    (str or Path), not just bytes. Previously it crashed with
    `AttributeError: 'function' object has no attribute 'urlparse'` on any
    str/Path input because it called `urlparse.urlparse(image)` instead of
    `urlparse(image)` (urlparse was imported as a function, not a module).
    """
    Image = pytest.importorskip("PIL.Image")
    from lancedb.embeddings.jinaai import JinaEmbeddings

    image_path = tmp_path / "test.png"
    Image.new("RGB", (4, 4), color="red").save(image_path, format="PNG")

    for image in (str(image_path), image_path):
        image_dict = JinaEmbeddings._generate_image_input_dict(image)
        assert "image" in image_dict
        assert isinstance(image_dict["image"], str) and len(image_dict["image"]) > 0


def test_jina_embeddings_use_their_own_api_key(monkeypatch):
    """Each JinaEmbeddings instance must send its own API key."""
    pytest.importorskip("requests")
    monkeypatch.delenv("JINA_API_KEY", raising=False)
    registry = get_registry()
    registry.set_var("jina_key_a", "key-a")
    registry.set_var("jina_key_b", "key-b")
    func_a = registry.get("jina").create(api_key="$var:jina_key_a")
    func_b = registry.get("jina").create(api_key="$var:jina_key_b")

    sent = []

    def fake_post(session, url, json=None, **kwargs):
        sent.append(session.headers["Authorization"])
        response = MagicMock()
        response.json.return_value = {"data": [{"index": 0, "embedding": [0.0]}]}
        return response

    with patch("requests.Session.post", fake_post):
        func_a.compute_source_embeddings(["hello"])
        func_b.compute_source_embeddings(["hello"])

    assert sent == ["Bearer key-a", "Bearer key-b"]
