# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import lancedb
import pytest
from pydantic import BaseModel, Field
from lancedb.pydantic import LanceModel


class Details(BaseModel):
    label: str
    transient: str = Field(default="nested default", exclude=True)


class Row(LanceModel):
    id: int
    details: Details
    transient: str = Field(default="local default", exclude=True)
    generated: list[str] = Field(default_factory=lambda: ["local"], exclude=True)
    retained: str = Field(default="persisted", exclude=False)


def rows():
    return [
        Row(
            id=i,
            details=Details(label=str(i), transient="private"),
            transient="private",
            generated=["private"],
        )
        for i in (1, 2)
    ]


def assert_schema(schema):
    assert schema.names == ["id", "details", "retained"]
    assert [field.name for field in schema.field("details").type] == ["label"]
    assert Row.field_names() == ["id", "details", "transient", "generated", "retained"]


def assert_restored(restored):
    assert sorted(row.id for row in restored) == [1, 2]
    for row in restored:
        assert row.transient == "local default"
        assert row.generated == ["local"]
        assert row.details.transient == "nested default"
        assert row.details.label == str(row.id)
        assert row.retained == "persisted"


@pytest.mark.parametrize("explicit_schema", [False, True])
def test_excluded_fields_round_trip(tmp_path, explicit_schema):
    data = rows()
    kwargs = {"schema": Row} if explicit_schema else {}
    db = lancedb.connect(tmp_path)
    table = db.create_table("rows", data=data[:1], **kwargs)
    table.add(data[1:])
    assert_schema(table.schema)
    reopened = lancedb.connect(tmp_path).open_table("rows")
    assert_restored(reopened.search().to_pydantic(Row))
    assert data[0].transient == "private"
    assert data[0].generated == ["private"]


@pytest.mark.asyncio
@pytest.mark.parametrize("explicit_schema", [False, True])
async def test_excluded_fields_round_trip_async(tmp_path, explicit_schema):
    data = rows()
    kwargs = {"schema": Row} if explicit_schema else {}
    db = await lancedb.connect_async(tmp_path)
    table = await db.create_table("rows", data=data[:1], **kwargs)
    await table.add(data[1:])
    assert_schema(await table.schema())
    reopened_db = await lancedb.connect_async(tmp_path)
    reopened = await reopened_db.open_table("rows")
    assert_restored(await reopened.query().to_pydantic(Row))
    assert data[0].details.transient == "private"


@pytest.mark.parametrize("excluded", ["transient", "source", "vector"])
def test_excluded_fields_keep_embedding_metadata_consistent(
    tmp_path, monkeypatch, excluded
):
    from lancedb.embeddings.base import TextEmbeddingFunction
    from lancedb.embeddings.registry import get_registry
    from lancedb.pydantic import Vector

    calls = []

    class LocalEmbedding(TextEmbeddingFunction):
        def ndims(self):
            return 2

        def generate_embeddings(self, texts):
            calls.extend(texts)
            return [[1.0, 0.0] for _ in texts]

    registry = get_registry()
    monkeypatch.setattr(registry, "_functions", registry._functions.copy())
    registry.register("test-excluded-fields")(LocalEmbedding)
    function = LocalEmbedding.create(max_retries=0)

    class EmbeddedRow(LanceModel):
        text: str = function.SourceField(default="alpha", exclude=excluded == "source")
        vector: Vector(2) = function.VectorField(
            default_factory=lambda: [1.0, 0.0], exclude=excluded == "vector"
        )
        transient: str = Field(default="local", exclude=True)

    configs = EmbeddedRow.parse_embedding_functions()
    assert len(configs) == (1 if excluded == "transient" else 0)
    db = lancedb.connect(tmp_path)
    data = [{"text": "alpha"}] if excluded == "transient" else [EmbeddedRow()]
    table = db.create_table("embeddings", schema=EmbeddedRow, data=data)
    assert "transient" not in table.schema.names
    assert ("text" in table.schema.names) == (excluded != "source")
    assert ("vector" in table.schema.names) == (excluded != "vector")
    reopened = lancedb.connect(tmp_path).open_table("embeddings")
    assert len(reopened.embedding_functions) == len(configs)
    restored = reopened.search().to_pydantic(EmbeddedRow)
    assert restored[0].text == "alpha"
    assert restored[0].vector == [1.0, 0.0]
    assert restored[0].transient == "local"
    if excluded == "transient":
        assert calls == ["alpha"]
        assert reopened.search("alpha").limit(1).to_list()[0]["text"] == "alpha"
    else:
        assert calls == []
