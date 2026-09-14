# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import lancedb
import pytest


def test_quickstart(db_path_factory):
    uri = db_path_factory("quickstart_db")
    db = lancedb.connect(uri)

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
    assert len(table) == 3
    # Drop the table to test create without overwrite
    db.drop_table("characters")

    # --8<-- [start:quickstart_create_table_no_overwrite]
    table = db.create_table("characters", data=data)
    # --8<-- [end:quickstart_create_table_no_overwrite]
    assert len(table) == 3
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
    assert result.head(1)["name"][0] == "Merlin"

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
    assert curated.head(1)["name"][0] == "Merlin"

    # --8<-- [start:quickstart_output_pandas]
    # Ensure you run `pip install pandas` beforehand
    result = table.search(query_vector).limit(2).to_pandas()
    print(result)
    # --8<-- [end:quickstart_output_pandas]
    assert result.iloc[0]["name"] == "Merlin"

    # --8<-- [start:quickstart_add_feature]
    table.add_columns(
        {
            "power_score": "cast(((stats.strength + stats.magic + stats.leadership + stats.wisdom) / 4.0) as float)"
        }
    )
    # --8<-- [end:quickstart_add_feature]
    assert "power_score" in table.schema.names

    # --8<-- [start:quickstart_query_feature]
    features = table.search().select(["name", "role", "power_score"]).to_polars()
    print(features)
    # --8<-- [end:quickstart_query_feature]
    assert "power_score" in features.columns

    # --8<-- [start:quickstart_multimodal_bytes]
    from pathlib import Path

    image_path = Path("docs/static/assets/images/quickstart/sir-lancelot.jpg")
    image_bytes = image_path.read_bytes()

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
    assert len(multimodal_table) == 1

    # --8<-- [start:quickstart_open_table]
    table = db.open_table("characters")
    # --8<-- [end:quickstart_open_table]

    # --8<-- [start:quickstart_add_data]
    more_data = [
        {
            "id": "4",
            "name": "Morgana",
            "role": "Sorceress",
            "description": "Powerful sorceress of Avalon.",
            "stats": {"strength": 2, "magic": 5, "leadership": 4, "wisdom": 4},
            "vector": [0.3, 0.9, 0.6, 0.8],
            "power_score": 3.75,
        },
    ]

    # Add data to table
    table.add(more_data)
    # --8<-- [end:quickstart_add_data]
    assert len(table) == 4

    # --8<-- [start:quickstart_vector_search_2]
    # Search for examples similar to a "powerful sorceress"
    query_vector = [0.3, 0.9, 0.6, 0.8]

    results = table.search(query_vector).limit(2).to_polars()
    print(results)
    # --8<-- [end:quickstart_vector_search_2]
    assert results.head(1)["name"][0] == "Morgana"


@pytest.mark.asyncio
async def test_quickstart_async_api(db_path_factory):
    db_uri = db_path_factory("quickstart_async_db")
    import lancedb
    async_db = await lancedb.connect_async(db_uri)

    # --8<-- [start:quickstart_data_async]
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
    # --8<-- [end:quickstart_data_async]

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

    assert async_result.head(1)["name"][0] == "Merlin"
