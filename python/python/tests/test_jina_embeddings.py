# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import pyarrow as pa
import pytest

import lancedb
from lancedb.embeddings import EmbeddingFunctionConfig, get_registry


@pytest.mark.parametrize(
    "name,dims",
    [
        ("jina-clip-v1", 768),
        ("jina-clip-v2", 1024),
        ("jina-embeddings-v2-small-en", 512),
        ("jina-embeddings-v2-base-en", 768),
        ("jina-embeddings-v3", 1024),
        ("jina-embeddings-v4", 2048),
    ],
)
def test_jina_ndims_per_model(name, dims):
    func = get_registry().get("jina").create(name=name)
    assert func.ndims() == dims


def test_jina_ndims_unknown_model():
    func = get_registry().get("jina").create(name="not-a-jina-model")
    with pytest.raises(ValueError, match="Unknown Jina model"):
        func.ndims()


@pytest.mark.parametrize("width", [512, 768])
def test_jina_append_uses_stored_width_for_unknown_model(tmp_path, monkeypatch, width):
    func = get_registry().get("jina").create(name="jina-embeddings-v5-text-nano")
    returned_width = width
    monkeypatch.setattr(
        type(func),
        "_generate_embeddings",
        lambda self, input, *args, **kwargs: [[0.5] * returned_width for _ in input],
    )
    schema = pa.schema(
        [
            pa.field("text", pa.string()),
            pa.field("vector", pa.list_(pa.float32(), width)),
        ]
    )
    config = EmbeddingFunctionConfig(
        source_column="text", vector_column="vector", function=func
    )
    lancedb.connect(tmp_path).create_table(
        "t", schema=schema, embedding_functions=[config]
    )

    table = lancedb.connect(tmp_path).open_table("t")
    table.add([{"text": "hello"}])
    assert table.to_arrow()["vector"].to_pylist() == [[0.5] * width]

    returned_width = width + 1
    # Mismatched vectors go through on_bad_vectors, which errors by default.
    with pytest.raises(RuntimeError, match="Vector column .vector. has"):
        table.add([{"text": "world"}])
    assert table.count_rows() == 1
