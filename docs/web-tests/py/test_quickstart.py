# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import os
import shutil
from pathlib import Path

import lancedb
import pytest
import pytest_asyncio

# What the quickstart's download step leaves in an application's directory.
IMAGE = (
    Path(__file__).resolve().parents[2]
    / "web/static/assets/images/quickstart/sir-lancelot.jpg"
)
DEPLOYMENT = ("LANCEDB_URI", "LANCEDB_HOST_OVERRIDE", "LANCEDB_API_KEY")


def deployment_settings():
    if not all(os.environ.get(name) for name in DEPLOYMENT):
        pytest.skip("set " + ", ".join(DEPLOYMENT) + " to run against a deployment")
    return dict(
        uri=os.environ["LANCEDB_URI"],
        api_key=os.environ["LANCEDB_API_KEY"],
        region=os.environ.get("LANCEDB_REGION", "us-east-1"),
        host_override=os.environ["LANCEDB_HOST_OVERRIDE"],
    )


@pytest.fixture(params=["local", "enterprise"])
def db(request, db_path_factory):
    if request.param == "local":
        return lancedb.connect(db_path_factory("quickstart_db"))
    return lancedb.connect(**deployment_settings())


@pytest_asyncio.fixture(params=["local", "enterprise"])
async def async_db(request, db_path_factory):
    if request.param == "local":
        return await lancedb.connect_async(db_path_factory("quickstart_async_db"))
    return await lancedb.connect_async(**deployment_settings())


def create_characters(db):
    # --8<-- [start:quickstart_data]
    data = [
        {
            "id": "1",
            "name": "King Arthur",
            "role": "King",
            "description": "Leader of Camelot and wielder of Excalibur.",
            "stats": {"strength": 4, "magic": 1, "leadership": 5, "wisdom": 4},
            "vector": [0.7, 0.1, 0.9, 0.7],
        },
        {
            "id": "2",
            "name": "Merlin",
            "role": "Wizard",
            "description": "Advisor and prophet with deep magical knowledge.",
            "stats": {"strength": 2, "magic": 5, "leadership": 4, "wisdom": 5},
            "vector": [0.2, 0.9, 0.4, 0.9],
        },
        {
            "id": "3",
            "name": "Sir Lancelot",
            "role": "Knight",
            "description": "Legendary knight known for courage and combat skill.",
            "stats": {"strength": 5, "magic": 1, "leadership": 3, "wisdom": 3},
            "vector": [0.9, 0.1, 0.5, 0.4],
        },
    ]
    # --8<-- [end:quickstart_data]

    # --8<-- [start:quickstart_create_table]
    table = db.create_table("characters", data=data, mode="overwrite")
    # --8<-- [end:quickstart_create_table]
    assert table.count_rows() == 3

    # --8<-- [start:quickstart_add_data]
    table.add(
        [
            {
                "id": "4",
                "name": "Morgana",
                "role": "Sorceress",
                "description": "Powerful sorceress of Avalon.",
                "stats": {"strength": 2, "magic": 5, "leadership": 4, "wisdom": 4},
                "vector": [0.3, 0.9, 0.6, 0.8],
            }
        ]
    )
    # --8<-- [end:quickstart_add_data]
    assert table.count_rows() == 4
    return table


def test_quickstart(db):
    table = create_characters(db)

    # --8<-- [start:quickstart_vector_search_1]
    # Search for examples similar to a "wise magical advisor"
    query_vector = [0.2, 0.8, 0.4, 0.9]

    # Ensure you run `pip install polars` beforehand
    result = (
        table.search(query_vector)
        .select(["name", "role", "description", "_distance"])
        .limit(2)
        .to_polars()
    )
    print(result)
    # --8<-- [end:quickstart_vector_search_1]
    assert result["name"].to_list() == ["Merlin", "Morgana"]

    # --8<-- [start:quickstart_output_pandas]
    # Ensure you run `pip install pandas` beforehand
    result = table.search(query_vector).limit(2).to_pandas()
    print(result)
    # --8<-- [end:quickstart_output_pandas]
    assert result["name"].to_list() == ["Merlin", "Morgana"]

    # --8<-- [start:quickstart_curate_with_metadata]
    curated = (
        table.search(query_vector)
        .where("stats.magic >= 4")
        .select(["name", "role", "description", "_distance"])
        .limit(2)
        .to_polars()
    )
    print(curated)
    # --8<-- [end:quickstart_curate_with_metadata]
    assert curated["name"].to_list() == ["Merlin", "Morgana"]

    curated = table.search(query_vector).where("stats.magic < 4").limit(2).to_list()
    assert [row["name"] for row in curated] == ["King Arthur", "Sir Lancelot"]


@pytest.mark.asyncio
async def test_quickstart_async_api(async_db):
    data = [
        {
            "id": "2",
            "name": "Merlin",
            "role": "Wizard",
            "description": "Advisor and prophet with deep magical knowledge.",
            "stats": {"strength": 2, "magic": 5, "leadership": 4, "wisdom": 5},
            "vector": [0.2, 0.9, 0.4, 0.9],
        }
    ]

    # --8<-- [start:quickstart_create_table_async]
    async_table = await async_db.create_table(
        "characters",
        data=data,
        mode="overwrite",
    )
    # --8<-- [end:quickstart_create_table_async]

    # --8<-- [start:quickstart_vector_search_1_async]
    # Search for examples similar to a "wise magical advisor"
    query_vector = [0.2, 0.8, 0.4, 0.9]

    # Ensure you run `pip install polars` beforehand
    async_result = await (
        await async_table.search(query_vector)
    ).select(["name", "role", "description", "_distance"]).limit(2).to_polars()
    print(async_result)
    # --8<-- [end:quickstart_vector_search_1_async]
    assert async_result["name"].to_list() == ["Merlin"]


def test_quickstart_derived_feature(db):
    table = create_characters(db)

    # --8<-- [start:quickstart_add_feature]
    table.add_columns(
        {
            "power_score": "cast(((stats.strength + stats.magic + stats.leadership + stats.wisdom) / 4.0) as float)"
        }
    )
    # --8<-- [end:quickstart_add_feature]

    # --8<-- [start:quickstart_query_feature]
    features = table.search().select(["name", "role", "power_score"]).to_polars()
    print(features)
    # --8<-- [end:quickstart_query_feature]
    assert sorted(zip(features["name"], features["power_score"])) == [
        ("King Arthur", 3.5),
        ("Merlin", 4.0),
        ("Morgana", 3.75),
        ("Sir Lancelot", 3.0),
    ]


def description_lengths(table):
    rows = table.search().select(["description", "description_length"]).to_list()
    return {row["description"]: row["description_length"] for row in rows}


def test_quickstart_computed_column(db_path_factory):
    table = create_characters(lancedb.connect(db_path_factory("computed")))

    # --8<-- [start:quickstart_computed_feature]
    table.add_columns(computed={"description_length": "length(description)"})
    table.refresh_column("description_length")
    # --8<-- [end:quickstart_computed_feature]
    lengths = description_lengths(table)
    assert lengths == {text: len(text) for text in lengths}


def test_quickstart_computed_column_on_a_deployment():
    table = create_characters(lancedb.connect(**deployment_settings()))

    # --8<-- [start:quickstart_computed_feature_enterprise]
    table.add_columns(computed={"description_length": "length(description)"})
    job = table.refresh_column_async("description_length")
    job.wait()
    # --8<-- [end:quickstart_computed_feature_enterprise]
    lengths = description_lengths(table)
    assert len(lengths) == 4
    assert lengths == {text: len(text) for text in lengths}
    with pytest.raises(Exception):
        table.refresh_column("description_length")


def test_quickstart_multimodal(db, tmp_path, monkeypatch):
    app = tmp_path / "app"
    (app / "data").mkdir(parents=True)
    shutil.copy(IMAGE, app / "data/sir-lancelot.jpg")
    monkeypatch.chdir(app)

    # --8<-- [start:quickstart_multimodal_bytes]
    from pathlib import Path

    image_bytes = Path("data/sir-lancelot.jpg").read_bytes()

    multimodal_table = db.create_table(
        "character_images",
        data=[
            {
                "id": "lancelot",
                "description": "Portrait of Sir Lancelot",
                "image": image_bytes,
                "vector": [0.9, 0.1, 0.5, 0.4],
            }
        ],
        mode="overwrite",
    )
    # --8<-- [end:quickstart_multimodal_bytes]
    assert multimodal_table.count_rows() == 1
    stored = multimodal_table.search().select(["image"]).to_list()[0]["image"]
    assert stored == IMAGE.read_bytes()


def test_quickstart_training(db):
    table = create_characters(db)

    # --8<-- [start:quickstart_training]
    import torch
    from lancedb.permutation import Permutation

    table.add_columns(
        {
            "magic": "cast(stats.magic as float)",
            "strength": "cast(stats.strength as float)",
        }
    )
    dataset = Permutation.identity(table).select_columns(["magic", "strength"])
    dataloader = torch.utils.data.DataLoader(dataset, batch_size=1024, shuffle=True)
    # --8<-- [end:quickstart_training]
    batches = list(dataloader)
    assert len(batches) == 1
    assert set(batches[0]) == {"magic", "strength"}
    assert all(isinstance(value, torch.Tensor) for value in batches[0].values())
    assert sorted(batches[0]["magic"].tolist()) == [1.0, 1.0, 5.0, 5.0]
