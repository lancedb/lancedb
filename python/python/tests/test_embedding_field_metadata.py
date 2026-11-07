# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import copy

import lancedb
import pytest
from lancedb.embeddings.base import TextEmbeddingFunction
from lancedb.embeddings.registry import get_registry
from lancedb.pydantic import LanceModel, Vector


class LocalEmbedding(TextEmbeddingFunction):
    def ndims(self):
        return 2

    def generate_embeddings(self, texts):
        return [[1.0, 0.0] if text == "alpha" else [0.0, 1.0] for text in texts]


@pytest.mark.parametrize(
    "method,tag",
    [("SourceField", "source_column_for"), ("VectorField", "vector_column_for")],
)
@pytest.mark.parametrize("extra", [None, {}, {"x-purpose": "retrieval"}])
def test_embedding_field_preserves_extra_metadata(method, tag, extra):
    function = LocalEmbedding.create(max_retries=0)
    original = copy.deepcopy(extra)
    field = getattr(function, method)(json_schema_extra=extra)
    assert extra == original
    assert field.json_schema_extra[tag] is function
    for key, value in (extra or {}).items():
        assert field.json_schema_extra[key] == value


@pytest.mark.parametrize(
    "method,tag",
    [("SourceField", "source_column_for"), ("VectorField", "vector_column_for")],
)
def test_extra_metadata_cannot_replace_embedding_identity(method, tag):
    function = LocalEmbedding.create(max_retries=0)
    supplied = {tag: "caller value", "x-purpose": "retrieval"}
    field = getattr(function, method)(json_schema_extra=supplied)
    assert supplied[tag] == "caller value"
    assert field.json_schema_extra[tag] is function
    assert field.json_schema_extra["x-purpose"] == "retrieval"


@pytest.mark.parametrize("method", ["SourceField", "VectorField"])
def test_embedding_field_preserves_other_field_arguments(method):
    function = LocalEmbedding.create(max_retries=0)
    field = getattr(function, method)(title="Custom title", description="Custom field")
    assert field.title == "Custom title"
    assert field.description == "Custom field"


def test_extra_metadata_preserves_schema_and_local_embedding(tmp_path, monkeypatch):
    registry = get_registry()
    monkeypatch.setattr(registry, "_functions", registry._functions.copy())
    registry.register("test-embedding-field-extra")(LocalEmbedding)
    function = LocalEmbedding.create(max_retries=0)

    class Row(LanceModel):
        text: str = function.SourceField(json_schema_extra={"x-purpose": "source"})
        vector: Vector(2) = function.VectorField(
            json_schema_extra={"x-purpose": "embedding"}
        )

    json_schema = Row.model_json_schema()
    assert json_schema["properties"]["text"]["x-purpose"] == "source"
    assert json_schema["properties"]["vector"]["x-purpose"] == "embedding"
    configs = Row.parse_embedding_functions()
    assert len(configs) == 1
    assert configs[0].function is function
    assert configs[0].source_column == "text"
    assert configs[0].vector_column == "vector"
    db = lancedb.connect(tmp_path)
    table = db.create_table(
        "rows", schema=Row, data=[{"text": "alpha"}, {"text": "beta"}]
    )
    assert table.search("alpha").limit(1).to_list()[0]["text"] == "alpha"
    reopened = lancedb.connect(tmp_path).open_table("rows")
    assert isinstance(reopened.embedding_functions["vector"].function, LocalEmbedding)
    assert reopened.search("alpha").limit(1).to_list()[0]["text"] == "alpha"


@pytest.fixture
def local_embedding(monkeypatch):
    registry = get_registry()
    monkeypatch.setattr(registry, "_functions", registry._functions.copy())
    registry.register("test-embedding-field-pairs")(LocalEmbedding)


def test_one_function_pairs_source_and_vector_columns_in_order(
    tmp_path, local_embedding
):
    function = LocalEmbedding.create(max_retries=0)

    class Doc(LanceModel):
        title: str = function.SourceField()
        title_vector: Vector(2) = function.VectorField()
        body: str = function.SourceField()
        body_vector: Vector(2) = function.VectorField()

    db = lancedb.connect(tmp_path)
    db.create_table("docs", schema=Doc, data=[{"title": "alpha", "body": "beta"}])
    table = db.open_table("docs")
    table.add([{"title": "beta", "body": "alpha"}])
    rows = table.to_arrow().to_pylist()
    assert [row["title_vector"] for row in rows] == [[1.0, 0.0], [0.0, 1.0]]
    assert [row["body_vector"] for row in rows] == [[0.0, 1.0], [1.0, 0.0]]


def test_one_function_rejects_unpairable_source_columns(tmp_path, local_embedding):
    function = LocalEmbedding.create(max_retries=0)

    class Doc(LanceModel):
        title: str = function.SourceField()
        body: str = function.SourceField()
        vector: Vector(2) = function.VectorField()

    with pytest.raises(ValueError, match=r"\(title, body\).*\(vector\)"):
        lancedb.connect(tmp_path).create_table("docs", schema=Doc)


def test_separate_functions_embed_their_own_source_columns(tmp_path, local_embedding):
    title_function = LocalEmbedding.create(max_retries=0)
    body_function = LocalEmbedding.create(max_retries=0)

    class Doc(LanceModel):
        title: str = title_function.SourceField()
        title_vector: Vector(2) = title_function.VectorField()
        body: str = body_function.SourceField()
        body_vector: Vector(2) = body_function.VectorField()

    table = lancedb.connect(tmp_path).create_table(
        "docs", schema=Doc, data=[{"title": "alpha", "body": "beta"}]
    )
    row = table.to_arrow().to_pylist()[0]
    assert row["title_vector"] == [1.0, 0.0]
    assert row["body_vector"] == [0.0, 1.0]


def test_one_source_column_can_feed_several_vector_columns(tmp_path, local_embedding):
    function = LocalEmbedding.create(max_retries=0)

    class Doc(LanceModel):
        text: str = function.SourceField()
        vector: Vector(2) = function.VectorField()
        vector_copy: Vector(2) = function.VectorField()

    table = lancedb.connect(tmp_path).create_table(
        "docs", schema=Doc, data=[{"text": "alpha"}]
    )
    row = table.to_arrow().to_pylist()[0]
    assert row["vector"] == row["vector_copy"] == [1.0, 0.0]
