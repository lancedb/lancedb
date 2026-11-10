# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

from lancedb.embeddings import EmbeddingFunctionRegistry, TextEmbeddingFunction


class ExampleEmbedding(TextEmbeddingFunction):
    def ndims(self):
        return 2

    def generate_embeddings(self, texts):
        return [[1.0, 0.0] for _ in texts]


class OtherEmbedding(ExampleEmbedding):
    pass


def test_list_functions_empty_registry():
    registry = EmbeddingFunctionRegistry()
    assert registry.list_functions() == []


def test_list_functions_names_and_registration_order():
    registry = EmbeddingFunctionRegistry()
    registry.register("custom-embedding")(ExampleEmbedding)
    registry.register()(OtherEmbedding)

    assert registry.list_functions() == ["custom-embedding", "OtherEmbedding"]
    assert [registry.get(name) for name in registry.list_functions()] == [
        ExampleEmbedding,
        OtherEmbedding,
    ]


def test_list_functions_returns_independent_snapshot():
    registry = EmbeddingFunctionRegistry()
    registry.register("first")(ExampleEmbedding)
    names = registry.list_functions()
    names.append("not-registered")
    assert registry.list_functions() == ["first"]

    registry.register("second")(OtherEmbedding)
    assert names == ["first", "not-registered"]
    assert registry.list_functions() == ["first", "second"]


def test_list_functions_after_reset():
    registry = EmbeddingFunctionRegistry()
    registry.register("custom-embedding")(ExampleEmbedding)
    names = registry.list_functions()
    registry.reset()

    assert registry.list_functions() == []
    assert names == ["custom-embedding"]
