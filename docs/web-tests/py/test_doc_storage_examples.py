# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import lancedb
import numpy as np
import pyarrow as pa
import pytest


def test_rebuild_vector_index(tmp_db):
    rng = np.random.default_rng(7)
    table = tmp_db.create_table(
        "rebuild_index",
        [{"id": i, "vector": v.tolist()} for i, v in enumerate(rng.random((512, 4)))],
    )
    table.create_index(
        "vector", config=lancedb.index.IvfFlat(distance_type="l2", num_partitions=4)
    )
    original_index = table.list_indices()[0]
    table.add(
        [
            {"id": 512 + i, "vector": v.tolist()}
            for i, v in enumerate(rng.random((64, 4)) + 5)
        ]
    )
    assert table.index_stats("vector_idx").num_unindexed_rows == 64

    # --8<-- [start:storage_rebuild_vector_index]
    from lancedb.index import IvfFlat

    table.create_index(
        "vector",
        config=IvfFlat(distance_type="l2", num_partitions=4),
        replace=True,
    )
    # --8<-- [end:storage_rebuild_vector_index]

    indices = table.list_indices()
    assert len(indices) == 1
    assert indices[0].name == original_index.name
    assert indices[0].index_uuid != original_index.index_uuid
    stats = table.index_stats("vector_idx")
    assert stats.num_indexed_rows == 576
    assert stats.num_unindexed_rows == 0


def test_local_shallow_clone_pins_source_version(tmp_path):
    db = lancedb.connect(str(tmp_path))
    source = db.create_table("source", [{"id": 1}])
    clone = db.clone_table("snapshot", source_uri=str(tmp_path / "source.lance"))
    source.add([{"id": 2}])
    assert source.count_rows() == 2
    assert clone.to_arrow().to_pylist() == [{"id": 1}]


def _reader():
    return pa.RecordBatchReader.from_batches(
        pa.schema([("id", pa.int64())]),
        [pa.record_batch([[1, 2]], names=["id"]), pa.record_batch([[3]], names=["id"])],
    )


@pytest.mark.parametrize("parallelism", [1, 2])
def test_streaming_write_parallelism(tmp_db, parallelism):
    table = tmp_db.create_table("streaming", [{"id": 0}])
    progress = []
    table.add(_reader(), write_parallelism=parallelism, progress=progress.append)
    assert sorted(table.to_arrow()["id"].to_pylist()) == [0, 1, 2, 3]
    assert progress[-1]["done"]
    assert progress[-1]["output_rows"] == 3
    assert {p["total_tasks"] for p in progress} == {parallelism}


@pytest.mark.asyncio
@pytest.mark.parametrize("parallelism", [1, 2])
async def test_async_streaming_write_parallelism(mem_db_async, parallelism):
    table = await mem_db_async.create_table("streaming", [{"id": 0}])
    progress = []
    await table.add(_reader(), write_parallelism=parallelism, progress=progress.append)
    data = await table.to_arrow()
    assert sorted(data["id"].to_pylist()) == [0, 1, 2, 3]
    assert progress[-1]["done"]
    assert progress[-1]["output_rows"] == 3
    assert {p["total_tasks"] for p in progress} == {parallelism}
