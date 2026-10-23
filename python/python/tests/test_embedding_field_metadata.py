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
