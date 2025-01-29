# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors


from pathlib import Path

import lancedb
import numpy as np
import pyarrow as pa
import pytest
from lancedb.embeddings import get_registry
from lancedb.embeddings.base import TextEmbeddingFunction
from lancedb.embeddings.registry import register
from lancedb.pydantic import LanceModel, Vector

datasets = pytest.importorskip("datasets")


@pytest.fixture(scope="session")
def mock_embedding_function():
    @register("random")
    class MockTextEmbeddingFunction(TextEmbeddingFunction):
        def generate_embeddings(self, texts):
            return [np.random.randn(128).tolist() for _ in range(len(texts))]

        def ndims(self):
            return 128


@pytest.fixture
def mock_hf_dataset():
    # Create pyarrow table with `text` and `label` columns
    train = datasets.Dataset(
        pa.table(
            {
                "text": ["foo", "bar"],
                "label": [0, 1],
            }
        ),
        split="train",
    )

    test = datasets.Dataset(
        pa.table(
            {
                "text": ["fizz", "buzz"],
                "label": [0, 1],
            }
        ),
        split="test",
    )
    return datasets.DatasetDict({"train": train, "test": test})


@pytest.fixture
def hf_dataset_with_split():
    # Create pyarrow table with `text` and `label` columns
    train = datasets.Dataset(
        pa.table(
            {"text": ["foo", "bar"], "label": [0, 1], "split": ["train", "train"]}
        ),
        split="train",
    )

    test = datasets.Dataset(
        pa.table(
            {"text": ["fizz", "buzz"], "label": [0, 1], "split": ["test", "test"]}
        ),
        split="test",
    )
    return datasets.DatasetDict({"train": train, "test": test})


def test_write_hf_dataset(tmp_path: Path, mock_embedding_function, mock_hf_dataset):
    db = lancedb.connect(tmp_path)
    emb = get_registry().get("random").create()

    class Schema(LanceModel):
        text: str = emb.SourceField()
        label: int
        vector: Vector(emb.ndims()) = emb.VectorField()

    train_table = db.create_table("train", schema=Schema)
    train_table.add(mock_hf_dataset["train"])

    class WithSplit(LanceModel):
        text: str = emb.SourceField()
        label: int
        vector: Vector(emb.ndims()) = emb.VectorField()
        split: str

    full_table = db.create_table("full", schema=WithSplit)
    full_table.add(mock_hf_dataset)

    assert len(train_table) == mock_hf_dataset["train"].num_rows
    assert len(full_table) == sum(ds.num_rows for ds in mock_hf_dataset.values())

    rt_train_table = full_table.to_lance().to_table(
        columns=["text", "label"], filter="split='train'"
    )
    assert rt_train_table.to_pylist() == mock_hf_dataset["train"].data.to_pylist()


def test_bad_hf_dataset(tmp_path: Path, mock_embedding_function, hf_dataset_with_split):
    db = lancedb.connect(tmp_path)
    emb = get_registry().get("random").create()

    class Schema(LanceModel):
        text: str = emb.SourceField()
        label: int
        vector: Vector(emb.ndims()) = emb.VectorField()
        split: str

    train_table = db.create_table("train", schema=Schema)
    # this should still work because we don't add the split column
    # if it already exists
    train_table.add(hf_dataset_with_split)


def test_generator(tmp_path: Path):
    db = lancedb.connect(tmp_path)

    def gen():
        yield {"pokemon": "bulbasaur", "type": "grass"}
        yield {"pokemon": "squirtle", "type": "water"}

    ds = datasets.Dataset.from_generator(gen)
    tbl = db.create_table("pokemon", ds)

    assert len(tbl) == 2
    assert tbl.schema == ds.features.arrow_schema
