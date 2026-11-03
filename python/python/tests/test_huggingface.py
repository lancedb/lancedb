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


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("operation", ["create", "add"])
@pytest.mark.parametrize(
    "selection",
    ["normal", "contiguous", "selected", "duplicates", "filtered", "shuffled", "empty"],
)
async def test_write_selected_hf_dataset(tmp_path, asynchronous, operation, selection):
    source = datasets.Dataset.from_dict({"id": list(range(5)), "text": list("abcde")})
    data = {
        "normal": source,
        "contiguous": source.select(range(1, 4)),
        "selected": source.select([4, 1]),
        "duplicates": source.select([2, 2, 0]),
        "filtered": source.filter(lambda row: row["id"] % 2 == 1),
        "shuffled": source.shuffle(seed=42),
        "empty": source.select([]),
    }[selection]
    expected = data.to_list()
    if asynchronous:
        db = await lancedb.connect_async(tmp_path)
        if operation == "create":
            table = await db.create_table("selected", data)
        else:
            table = await db.create_table("selected", schema=data.features.arrow_schema)
            await table.add(data)
        actual = await table.to_arrow()
    else:
        db = lancedb.connect(tmp_path)
        if operation == "create":
            table = db.create_table("selected", data)
        else:
            table = db.create_table("selected", schema=data.features.arrow_schema)
            table.add(data)
        actual = table.to_arrow()
    assert actual.to_pylist() == expected
    assert actual.schema.equals(data.features.arrow_schema, check_metadata=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("operation", ["create", "add"])
async def test_write_selected_hf_splits(tmp_path, asynchronous, operation):
    source = datasets.Dataset.from_dict({"id": list(range(5)), "text": list("abcde")})
    data = datasets.DatasetDict(
        {
            "train": source.select([4, 1]),
            "test": source.select([3, 0, 2]),
            "empty": source.select([]),
        }
    )
    expected = [
        dict(row, split=split)
        for split, dataset in data.items()
        for row in dataset.to_list()
    ]
    schema = source.features.arrow_schema.append(pa.field("split", pa.string()))
    if asynchronous:
        db = await lancedb.connect_async(tmp_path)
        if operation == "create":
            table = await db.create_table("splits", data)
        else:
            table = await db.create_table("splits", schema=schema)
            await table.add(data)
        actual = await table.to_arrow()
    else:
        db = lancedb.connect(tmp_path)
        if operation == "create":
            table = db.create_table("splits", data)
        else:
            table = db.create_table("splits", schema=schema)
            table.add(data)
        actual = table.to_arrow()
    assert actual.to_pylist() == expected
    assert actual.schema.equals(schema, check_metadata=True)


@pytest.mark.parametrize("format_type", [None, "numpy", "pandas", "arrow"])
@pytest.mark.parametrize("empty", [False, True])
def test_selected_hf_reader_is_rescannable(format_type, empty):
    from lancedb.scannable import _register_optional_converters, to_scannable

    source = datasets.Dataset.from_dict(
        {"id": list(range(2105)), "text": [str(i) for i in range(2105)]}
    )
    indices = [] if empty else list(reversed(range(2105))) + [1, 1]
    data = source.select(indices).with_format(format_type, columns=["id"])
    original_format = data.format
    expected = source.select(indices).to_list()
    _register_optional_converters()
    scannable = to_scannable(data)
    assert scannable.rescannable
    assert scannable.num_rows == len(expected)
    for _ in range(2):
        batches = list(scannable.reader())
        if not empty:
            assert all(batch.num_rows <= 1000 for batch in batches)
        actual = pa.Table.from_batches(batches, schema=scannable.schema)
        assert actual.to_pylist() == expected
        assert actual.schema.equals(data.features.arrow_schema, check_metadata=True)
    assert data.format == original_format
