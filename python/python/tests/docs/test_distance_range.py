# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import shutil
import pytest

# --8<-- [start:imports]
import lancedb
import numpy as np
# --8<-- [end:imports]

shutil.rmtree("data/distance_range_demo", ignore_errors=True)


def test_binary_vector():
    # --8<-- [start:sync_distance_range]
    db = lancedb.connect("data/distance_range_demo")
    data = [
        {
            "id": i,
            "vector": np.random.random(256),
        }
        for i in range(1024)
    ]
    tbl = db.create_table("my_table", data=data)
    query = np.random.random(256)

    # Search for the vectors within the range of [0.1, 0.5)
    tbl.search(query).distance_range(0.1, 0.5).to_arrow()

    # Search for the vectors with the distance less than 0.5
    tbl.search(query).distance_range(upper_bound=0.5).to_arrow()

    # Search for the vectors with the distance greater or equal to 0.1
    tbl.search(query).distance_range(lower_bound=0.1).to_arrow()

    # --8<-- [end:sync_distance_range]
    db.drop_table("my_table")


@pytest.mark.asyncio
async def test_binary_vector_async():
    # --8<-- [start:async_distance_range]
    db = await lancedb.connect_async("data/distance_range_demo")
    data = [
        {
            "id": i,
            "vector": np.random.random(256),
        }
        for i in range(1024)
    ]
    tbl = await db.create_table("my_table", data=data)
    query = np.random.random(256)

    # Search for the vectors within the range of [0.1, 0.5)
    await (await tbl.search(query)).distance_range(0.1, 0.5).to_arrow()

    # Search for the vectors with the distance less than 0.5
    await (await tbl.search(query)).distance_range(upper_bound=0.5).to_arrow()

    # Search for the vectors with the distance greater or equal to 0.1
    await (await tbl.search(query)).distance_range(lower_bound=0.1).to_arrow()

    # --8<-- [end:async_distance_range]
    await db.drop_table("my_table")
