# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

# --8<-- [start:camelot_schema]
from lancedb.pydantic import LanceModel
from pydantic import ConfigDict


class Stats(LanceModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    strength: int
    courage: int
    magic: int
    wisdom: int


class Character(LanceModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    id: int
    name: str
    role: str
    description: str
    stats: Stats
    image_filename: str
    image: bytes
# --8<-- [end:camelot_schema]


# --8<-- [start:camelot_batches]
import json
from collections.abc import Iterator
from pathlib import Path


def validated_batches(
    input_path: Path, batch_size: int
) -> Iterator[list[dict]]:
    raw_records = json.loads(input_path.read_text())
    asset_root = input_path.parent.parent
    batch: list[dict] = []

    for raw in raw_records:
        payload = dict(raw)
        image_path = asset_root / payload.pop("img")
        payload["image_filename"] = image_path.name
        payload["image"] = image_path.read_bytes()

        character = Character.model_validate(payload)
        batch.append(character.model_dump(mode="python"))

        if len(batch) == batch_size:
            yield batch
            batch = []

    if batch:
        yield batch
# --8<-- [end:camelot_batches]


# --8<-- [start:camelot_oss_ingestion]
import lancedb


def ingest_oss(
    input_path: Path,
    uri: str = "data/camelot.lancedb",
    table_name: str = "camelot_multimodal",
    batch_size: int = 64,
):
    db = lancedb.connect(uri)
    if table_name in db.list_tables():
        raise ValueError(
            f"Table {table_name!r} already exists. Choose a fresh table name."
        )

    table = db.create_table(table_name, schema=Character)
    for batch in validated_batches(input_path, batch_size):
        table.add(batch)

    table.optimize()
    return table


if __name__ == "__main__":
    ingest_oss(Path("data/camelot.json"))
# --8<-- [end:camelot_oss_ingestion]


def test_camelot_oss_ingestion(tmp_path):
    source = Path(
        "docs/static/assets/tutorials/build-with-ai-agents/camelot/data/camelot.json"
    )
    table = ingest_oss(
        source,
        uri=str(tmp_path / "camelot.lancedb"),
        table_name="camelot",
    )

    rows = (
        table.search()
        .select(["id", "name", "image", "image_filename"])
        .limit(8)
        .to_list()
    )
    assert len(rows) == 8
    assert all(row["image"] for row in rows)
    assert all(row["image_filename"].endswith(".jpg") for row in rows)
