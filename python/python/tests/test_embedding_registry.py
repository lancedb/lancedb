# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import lancedb
import pytest
from lancedb.embeddings.base import TextEmbeddingFunction
from lancedb.embeddings.registry import EmbeddingFunctionRegistry, get_registry
from lancedb.pydantic import LanceModel, Vector


class FirstEmbedding(TextEmbeddingFunction):
    def ndims(self):
        return 2

    def generate_embeddings(self, texts):
        return [[1.0, 0.0] if text == "alpha" else [0.0, 1.0] for text in texts]


class SecondEmbedding(FirstEmbedding):
    def generate_embeddings(self, texts):
        return [[0.0, 1.0] if text == "alpha" else [1.0, 0.0] for text in texts]


@pytest.mark.parametrize(
    "first_alias,second_alias,second_class",
    [
        ("shared", "shared", SecondEmbedding),
        (None, "FirstEmbedding", SecondEmbedding),
        ("SecondEmbedding", None, SecondEmbedding),
        (None, None, FirstEmbedding),
        ("", "", FirstEmbedding),
    ],
)
def test_duplicate_registration_preserves_original(
    first_alias, second_alias, second_class
):
    registry = EmbeddingFunctionRegistry()
    registry.register(first_alias)(FirstEmbedding)
    key = first_alias or "FirstEmbedding"
    original_alias = FirstEmbedding.__embedding_function_registry_alias__
    second_attributes = dict(second_class.__dict__)
    with pytest.raises(KeyError, match=f"{key} was already registered"):
        registry.register(second_alias)(second_class)
    assert registry.get(key) is FirstEmbedding
    assert FirstEmbedding.__embedding_function_registry_alias__ == original_alias
    assert dict(second_class.__dict__) == second_attributes


def test_class_name_does_not_collide_with_distinct_alias():
    registry = EmbeddingFunctionRegistry()
    registry.register()(FirstEmbedding)
    same_name = type("FirstEmbedding", (SecondEmbedding,), {})
    registry.register("separate")(same_name)
    assert registry.get("FirstEmbedding") is FirstEmbedding
    assert registry.get("separate") is same_name


def test_same_class_explicit_alias_registration_is_idempotent():
    registry = EmbeddingFunctionRegistry()
    assert registry.register("shared")(FirstEmbedding) is FirstEmbedding
    attributes = dict(FirstEmbedding.__dict__)
    assert registry.register("shared")(FirstEmbedding) is FirstEmbedding
    assert registry.get("shared") is FirstEmbedding
    assert dict(FirstEmbedding.__dict__) == attributes


def test_alias_collision_does_not_change_persisted_table(tmp_path, monkeypatch):
    registry = get_registry()
    monkeypatch.setattr(registry, "_functions", registry._functions.copy())
    alias = "test-alias-collision-persisted-table"
    registry.register(alias)(FirstEmbedding)
    function = FirstEmbedding.create(max_retries=0)

    class Row(LanceModel):
        text: str = function.SourceField()
        vector: Vector(2) = function.VectorField()

    db = lancedb.connect(tmp_path)
    table = db.create_table(
        "rows", schema=Row, data=[{"text": "alpha"}, {"text": "beta"}]
    )
    assert table.search("alpha").limit(1).to_list()[0]["text"] == "alpha"
    assert registry.register(alias)(FirstEmbedding) is FirstEmbedding
    with pytest.raises(KeyError, match=f"{alias} was already registered"):
        registry.register(alias)(SecondEmbedding)
    reopened = lancedb.connect(tmp_path).open_table("rows")
    assert isinstance(reopened.embedding_functions["vector"].function, FirstEmbedding)
    assert reopened.search("alpha").limit(1).to_list()[0]["text"] == "alpha"
