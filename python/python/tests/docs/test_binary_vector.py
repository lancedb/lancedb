# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import shutil

# --8<-- [start:imports]
import lancedb
import numpy as np
import pyarrow as pa
import pytest
# --8<-- [end:imports]

shutil.rmtree("data/binary_lancedb", ignore_errors=True)


def test_binary_vector():
    # --8<-- [start:sync_binary_vector]
    db = lancedb.connect("data/binary_lancedb")
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            # for dim=256, lance stores every 8 bits in a byte
            # so the vector field should be a list of 256 / 8 = 32 bytes
            pa.field("vector", pa.list_(pa.uint8(), 32)),
        ]
    )
    tbl = db.create_table("my_binary_vectors", schema=schema)

    data = []
    for i in range(1024):
        vector = np.random.randint(0, 2, size=256)
        # pack the binary vector into bytes to save space
        packed_vector = np.packbits(vector)
        data.append(
            {
                "id": i,
                "vector": packed_vector,
            }
        )
    tbl.add(data)

    query = np.random.randint(0, 2, size=256)
    packed_query = np.packbits(query)
    tbl.search(packed_query).distance_type("hamming").to_arrow()
    # --8<-- [end:sync_binary_vector]
    db.drop_table("my_binary_vectors")


@pytest.mark.asyncio
async def test_binary_vector_async():
    # --8<-- [start:async_binary_vector]
    db = await lancedb.connect_async("data/binary_lancedb")
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            # for dim=256, lance stores every 8 bits in a byte
            # so the vector field should be a list of 256 / 8 = 32 bytes
            pa.field("vector", pa.list_(pa.uint8(), 32)),
        ]
    )
    tbl = await db.create_table("my_binary_vectors", schema=schema)

    data = []
    for i in range(1024):
        vector = np.random.randint(0, 2, size=256)
        # pack the binary vector into bytes to save space
        packed_vector = np.packbits(vector)
        data.append(
            {
                "id": i,
                "vector": packed_vector,
            }
        )
    await tbl.add(data)

    query = np.random.randint(0, 2, size=256)
    packed_query = np.packbits(query)
    await (await tbl.search(packed_query)).distance_type("hamming").to_arrow()
    # --8<-- [end:async_binary_vector]
    await db.drop_table("my_binary_vectors")
