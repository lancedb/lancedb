# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import shutil
from lancedb.index import IvfPq
import pytest

# --8<-- [start:imports]
import lancedb
import numpy as np
import pyarrow as pa
# --8<-- [end:imports]

shutil.rmtree("data/multivector_demo", ignore_errors=True)


def test_multivector():
    # --8<-- [start:sync_multivector]
    db = lancedb.connect("data/multivector_demo")
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            # float16, float32, and float64 are supported
            pa.field("vector", pa.list_(pa.list_(pa.float32(), 256))),
        ]
    )
    data = [
        {
            "id": i,
            "vector": np.random.random(size=(2, 256)).tolist(),
        }
        for i in range(1024)
    ]
    tbl = db.create_table("my_table", data=data, schema=schema)

    # only cosine similarity is supported for multi-vectors
    tbl.create_index(metric="cosine")

    # query with single vector
    query = np.random.random(256).astype(np.float16)
    tbl.search(query).to_arrow()

    # query with multiple vectors
    query = np.random.random(size=(2, 256))
    tbl.search(query).to_arrow()

    # --8<-- [end:sync_multivector]
    db.drop_table("my_table")


@pytest.mark.asyncio
async def test_multivector_async():
    # --8<-- [start:async_multivector]
    db = await lancedb.connect_async("data/multivector_demo")
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            # float16, float32, and float64 are supported
            pa.field("vector", pa.list_(pa.list_(pa.float32(), 256))),
        ]
    )
    data = [
        {
            "id": i,
            "vector": np.random.random(size=(2, 256)).tolist(),
        }
        for i in range(1024)
    ]
    tbl = await db.create_table("my_table", data=data, schema=schema)

    # only cosine similarity is supported for multi-vectors
    await tbl.create_index(column="vector", config=IvfPq(distance_type="cosine"))

    # query with single vector
    query = np.random.random(256)
    await tbl.query().nearest_to(query).to_arrow()

    # query with multiple vectors
    query = np.random.random(size=(2, 256))
    await tbl.query().nearest_to(query).to_arrow()

    # --8<-- [end:async_multivector]
    await db.drop_table("my_table")
