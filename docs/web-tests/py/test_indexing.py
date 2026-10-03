# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import random
import string
import uuid

import numpy as np
import pyarrow as pa
import pytest


def _make_vector_rows(count: int, dim: int, column: str = "vector"):
    rows = [
        {column: np.random.random(dim).astype(np.float32).tolist(), "id": i}
        for i in range(count)
    ]
    assert len(rows) == count
    assert all(len(row[column]) == dim for row in rows)
    return rows


def test_vector_index_configure_ivf(tmp_db):
    table = tmp_db.create_table(
        "vector_index_configure_ivf",
        _make_vector_rows(512, 4),
        mode="overwrite",
    )

    # --8<-- [start:vector_index_configure_ivf]
    table.create_index(metric="l2", num_partitions=16, num_sub_vectors=4)
    # --8<-- [end:vector_index_configure_ivf]

    assert table.list_indices()


def test_vector_index_setup(tmp_db):
    tmp_db.create_table(
        "vector-index-tbl",
        _make_vector_rows(8, 4),
        mode="overwrite",
    )

    db = tmp_db
    # --8<-- [start:vector_index_setup]
    table_name = "vector-index-tbl"
    table = db.open_table(table_name)
    # --8<-- [end:vector_index_setup]

    assert table.name == table_name


def test_vector_index_build_ivf(tmp_db):
    table = tmp_db.create_table(
        "vector-index-build-ivf",
        _make_vector_rows(512, 4, column="keywords_embeddings"),
        mode="overwrite",
    )
    db = tmp_db
    # --8<-- [start:vector_index_build_ivf]
    table_name = "vector-index-build-ivf"
    table = db.open_table(table_name)
    table.create_index(
        metric="cosine",
        vector_column_name="keywords_embeddings",
    )
    # --8<-- [end:vector_index_build_ivf]

    assert table.list_indices()


def test_vector_index_nested_field(tmp_db):
    dim = 2
    schema = pa.schema(
        [
            pa.field("id", pa.int32()),
            pa.field(
                "image",
                pa.struct([pa.field("embedding", pa.list_(pa.float32(), dim))]),
            ),
        ]
    )
    data = [
        {
            "id": i,
            "image": {"embedding": np.random.random(dim).astype(np.float32).tolist()},
        }
        for i in range(512)
    ]
    table = tmp_db.create_table(
        "vector_index_nested_field", data=data, schema=schema, mode="overwrite"
    )

    # --8<-- [start:vector_index_nested_field]
    # The vector column `embedding` is nested inside the `image` struct.
    # Pass its full dotted path as `vector_column_name`; the same path is used
    # at query time and is what `list_indices()` reports under `columns`.
    table.create_index(
        vector_column_name="image.embedding",
        num_partitions=1,
        num_sub_vectors=1,
        name="image_embedding_idx",
    )

    results = (
        table.search([0.0, 1.0], vector_column_name="image.embedding")
        .limit(1)
        .to_list()
    )
    # --8<-- [end:vector_index_nested_field]

    assert table.index_stats("image_embedding_idx")
    assert len(results) == 1


@pytest.mark.asyncio
async def test_vector_index_async_config(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    # --8<-- [start:vector_index_async_config]
    import lancedb
    import numpy as np
    from lancedb.index import IvfPq

    async def main():
        data = [
            {"id": i, "vector": np.random.random(8).astype(np.float32).tolist()}
            for i in range(512)
        ]

        db = await lancedb.connect_async("ex_lancedb")
        table = await db.create_table(
            "vector_index_async", data=data, mode="overwrite"
        )

        await table.create_index(
            "vector",
            config=IvfPq(
                distance_type="cosine",
                num_partitions=16,
                num_sub_vectors=4,
            ),
        )
        return await table.list_indices()
    # --8<-- [end:vector_index_async_config]

    assert await main()


def test_vector_index_query_ivf(tmp_db):
    dim = 1536
    data = [
        {"id": i, "keywords_embeddings": np.random.random(dim).tolist()}
        for i in range(512)
    ]
    table = tmp_db.create_table("vector_index_query_ivf", data, mode="overwrite")
    table.create_index(
        metric="cosine",
        vector_column_name="keywords_embeddings",
    )

    # --8<-- [start:vector_index_query_ivf]
    tbl = table
    tbl.search(np.random.random((1536))).limit(2).nprobes(20).refine_factor(
        10
    ).to_pandas()
    # --8<-- [end:vector_index_query_ivf]

    df = (
        table.search(np.random.random((1536)))
        .limit(2)
        .nprobes(20)
        .refine_factor(10)
        .to_pandas()
    )
    assert len(df) == 2


def test_vector_index_nprobes(tmp_db):
    dim = 128
    data = [
        {"id": i, "keywords_embeddings": np.random.random(dim).tolist()}
        for i in range(512)
    ]
    table = tmp_db.create_table("vector_index_nprobes", data, mode="overwrite")
    table.create_index(
        metric="cosine",
        vector_column_name="keywords_embeddings",
    )

    # --8<-- [start:vector_index_nprobes]
    # Always scan 10 partitions; scan up to 50 only if the initial pass
    # returns fewer than `limit` results (common with narrow filters).
    (
        table.search(np.random.random(128))
        .minimum_nprobes(10)
        .maximum_nprobes(50)
        .where("id > 100")
        .limit(5)
        .to_pandas()
    )
    # --8<-- [end:vector_index_nprobes]


def test_vector_index_distance_range(tmp_db):
    dim = 128
    data = [
        {"id": i, "keywords_embeddings": np.random.random(dim).tolist()}
        for i in range(256)
    ]
    table = tmp_db.create_table("vector_index_distance_range", data, mode="overwrite")
    table.create_index(
        metric="cosine",
        vector_column_name="keywords_embeddings",
    )

    # --8<-- [start:vector_index_distance_range]
    # Only return results whose distance falls within [0.0, 0.5).
    # Useful for near-duplicate detection or thresholded similarity search.
    (
        table.search(np.random.random(128))
        .distance_range(lower_bound=0.0, upper_bound=0.5)
        .limit(10)
        .to_pandas()
    )
    # --8<-- [end:vector_index_distance_range]


def test_vector_index_bypass_recall(tmp_db):
    dim = 128
    data = [
        {"id": i, "keywords_embeddings": np.random.random(dim).tolist()}
        for i in range(256)
    ]
    table = tmp_db.create_table("vector_index_bypass_recall", data, mode="overwrite")
    table.create_index(
        metric="cosine",
        vector_column_name="keywords_embeddings",
    )

    # --8<-- [start:vector_index_bypass_recall]
    query = np.random.random(128)
    k = 10

    # Ground truth: flat (exhaustive) scan, ignoring the ANN index.
    truth = set(table.search(query).bypass_vector_index().limit(k).to_pandas()["id"])

    # ANN results with the current nprobes setting.
    ann = set(table.search(query).nprobes(20).limit(k).to_pandas()["id"])

    recall_at_k = len(truth & ann) / k
    # --8<-- [end:vector_index_bypass_recall]
    assert 0.0 <= recall_at_k <= 1.0


def test_vector_index_custom_name(tmp_db):
    table = tmp_db.create_table(
        "vector_index_custom_name",
        _make_vector_rows(512, 8, column="keywords_embeddings"),
        mode="overwrite",
    )

    # --8<-- [start:vector_index_custom_name]
    # Override the default `{column}_idx` convention by passing `name=...`.
    table.create_index(
        metric="cosine",
        vector_column_name="keywords_embeddings",
        name="my_custom_index",
    )
    table.wait_for_index(["my_custom_index"])
    print(table.index_stats("my_custom_index"))
    # --8<-- [end:vector_index_custom_name]

    assert table.index_stats("my_custom_index")


def test_vector_index_hnsw(tmp_db):
    table = tmp_db.create_table(
        "vector_index_hnsw",
        _make_vector_rows(64, 16),
        mode="overwrite",
    )

    # --8<-- [start:vector_index_build_hnsw]
    table.create_index(index_type="IVF_HNSW_SQ")
    # --8<-- [end:vector_index_build_hnsw]

    # --8<-- [start:vector_index_query_hnsw]
    tbl = table
    tbl.search(np.random.random((16))).limit(2).to_pandas()
    # --8<-- [end:vector_index_query_hnsw]

    df = table.search(np.random.random((16))).limit(2).to_pandas()
    assert len(df) == 2


def test_vector_index_binary(tmp_db):
    table_name = "hamming-index-tbl"
    ndim = 256
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("vector", pa.list_(pa.uint8(), ndim // 8)),
        ]
    )

    # --8<-- [start:vector_index_binary_schema]
    table = tmp_db.create_table(table_name, schema=schema, mode="overwrite")
    # --8<-- [end:vector_index_binary_schema]

    data = []
    for i in range(64):
        vector = np.random.randint(0, 2, size=ndim)
        vector = np.packbits(vector)
        data.append({"id": i, "vector": vector})

    # --8<-- [start:vector_index_binary_add_data]
    table.add(data)
    # --8<-- [end:vector_index_binary_add_data]

    # --8<-- [start:vector_index_binary_build_index]
    table.create_index(
        metric="hamming",
        vector_column_name="vector",
        index_type="IVF_FLAT",
    )
    # --8<-- [end:vector_index_binary_build_index]

    # --8<-- [start:vector_index_binary_search]
    query = np.random.randint(0, 2, size=ndim)
    query = np.packbits(query)
    df = table.search(query).metric("hamming").limit(10).to_pandas()
    df.vector = df.vector.apply(np.unpackbits)
    # --8<-- [end:vector_index_binary_search]

    assert not df.empty


def test_vector_index_check_status(tmp_db):
    table = tmp_db.create_table(
        "vector_index_check_status",
        _make_vector_rows(512, 8, column="keywords_embeddings"),
        mode="overwrite",
    )
    table.create_index(
        metric="cosine",
        vector_column_name="keywords_embeddings",
    )

    # --8<-- [start:vector_index_check_status]
    index_name = "keywords_embeddings_idx"
    table.wait_for_index([index_name])
    print(table.index_stats(index_name))
    # --8<-- [end:vector_index_check_status]

    assert table.index_stats(index_name)


def test_scalar_index_build(tmp_db):
    table = tmp_db.create_table(
        "scalar_index_build",
        [
            {"book_id": 1, "publisher": "A", "vector": [0.1, 0.2]},
            {"book_id": 2, "publisher": "B", "vector": [0.2, 0.3]},
        ],
        mode="overwrite",
    )
    db = tmp_db
    # --8<-- [start:scalar_index_build]
    tbl = db.open_table("scalar_index_build")
    tbl.create_scalar_index("book_id")
    tbl.create_scalar_index("publisher", index_type="BITMAP")
    # --8<-- [end:scalar_index_build]

    assert tbl.list_indices()


def test_scalar_index_wait(tmp_db):
    table = tmp_db.create_table(
        "scalar_index_wait",
        [{"label": "fiction"}],
        mode="overwrite",
    )
    table.create_scalar_index("label")

    # --8<-- [start:scalar_index_wait]
    index_name = "label_idx"
    table.wait_for_index([index_name])
    # --8<-- [end:scalar_index_wait]

    assert table.list_indices()


def test_scalar_index_optimize(tmp_db):
    table = tmp_db.create_table(
        "scalar_index_optimize",
        [{"vector": [7.0, 8.0], "book_id": 3}],
        mode="overwrite",
    )

    # --8<-- [start:scalar_index_optimize]
    table.add([{"vector": [7, 8], "book_id": 4}])
    table.optimize()
    # --8<-- [end:scalar_index_optimize]

    result = table.search().where("book_id = 4").limit(10).to_pandas()
    assert len(result) == 1


def test_scalar_index_filter(tmp_db):
    table = tmp_db.create_table(
        "books",
        [
            {"vector": [1.1, 1.2], "book_id": 1},
            {"vector": [2.1, 2.2], "book_id": 2},
        ],
        mode="overwrite",
    )
    db = tmp_db
    # --8<-- [start:scalar_index_filter]
    table = db.open_table("books")
    result = table.search().where("book_id = 2").limit(10).to_pandas()
    # --8<-- [end:scalar_index_filter]

    assert len(result) == 1


def test_scalar_index_prefilter(tmp_db):
    table = tmp_db.create_table(
        "book_with_embeddings",
        [
            {"vector": [1.2, 1.3], "book_id": 1},
            {"vector": [4.2, 4.3], "book_id": 2},
        ],
        mode="overwrite",
    )
    db = tmp_db
    # --8<-- [start:scalar_index_prefilter]
    table = db.open_table("book_with_embeddings")
    table.search([1.2] * 2).where("book_id != 3").limit(10).to_pandas()
    # --8<-- [end:scalar_index_prefilter]

    result = table.search([1.2] * 2).where("book_id != 3").limit(10).to_pandas()
    assert len(result) == 2


def test_scalar_index_uuid(tmp_db):
    # --8<-- [start:scalar_index_uuid_type]
    import pyarrow as pa
    # --8<-- [end:scalar_index_uuid_type]

    # --8<-- [start:scalar_index_uuid_data]
    def generate_random_names():
        base_names = ["Alice", "Bob", "Carla", "David", "Eve", "Frank", "Grace"]
        letter = random.choice(string.ascii_uppercase)
        return f"{random.choice(base_names)} {letter}."

    def generate_uuids(num_items):
        return [uuid.uuid4().bytes for _ in range(num_items)]

    # Generate some UUIDs and random names
    n = 7
    uuids = generate_uuids(n)
    names = [generate_random_names() for _ in range(n)]
    # --8<-- [end:scalar_index_uuid_data]

    db = tmp_db
    # --8<-- [start:scalar_index_uuid_table]
    table_name = "index-on-uuid"

    uuid_array = pa.array(uuids, pa.uuid())
    name_array = pa.array(names, pa.string())
    schema = pa.schema(
        [
            pa.field("id", pa.uuid()),
            pa.field("name", pa.string()),
        ]
    )
    data_table = pa.Table.from_arrays([uuid_array, name_array], schema=schema)
    table = db.create_table(table_name, data=data_table, mode="overwrite")
    # --8<-- [end:scalar_index_uuid_table]

    # --8<-- [start:scalar_index_uuid_wait]
    index_name = "id_idx"
    table.create_scalar_index("id")
    table.wait_for_index([index_name])
    # --8<-- [end:scalar_index_uuid_wait]

    # --8<-- [start:scalar_index_uuid_upsert]
    new_users = [
        {"id": uuid.uuid4().bytes, "name": "Hannah D."},
        {"id": uuid.uuid4().bytes, "name": "Ian B."},
    ]
    # Insert or update using the UUID index
    table.merge_insert(
        "id"
    ).when_matched_update_all().when_not_matched_insert_all().execute(new_users)
    # --8<-- [end:scalar_index_uuid_upsert]

    assert table.list_indices()
    result = table.search().limit(100).to_pandas()
    assert len(result) == n + len(new_users)


@pytest.mark.asyncio
async def test_scalar_index_nested_fields(mem_db_async):
    db = mem_db_async

    # --8<-- [start:scalar_index_nested_fields]
    import pyarrow as pa
    from lancedb.index import BTree

    metadata_type = pa.struct(
        [
            pa.field("user_id", pa.int32()),
            pa.field("user.id", pa.int32()),
        ]
    )
    data = pa.Table.from_arrays(
        [
            pa.array([1, 2, 3], type=pa.int32()),
            pa.array(
                [
                    {"user_id": 10, "user.id": 100},
                    {"user_id": 20, "user.id": 200},
                    {"user_id": 30, "user.id": 300},
                ],
                type=metadata_type,
            ),
        ],
        names=["user_id", "metadata"],
    )
    table = await db.create_table("nested_scalar_index", data)

    # Index a nested struct field.
    await table.create_index(
        "metadata.user_id", config=BTree(), name="nested_user_id_idx"
    )

    # Escape literal dots inside a segment with backticks.
    await table.create_index(
        "metadata.`user.id`", config=BTree(), name="escaped_user_id_idx"
    )

    # `columns` is returned as the canonical path you passed in.
    for index in await table.list_indices():
        print(index.name, index.columns)
    # nested_user_id_idx  ['metadata.user_id']
    # escaped_user_id_idx ['metadata.`user.id`']
    # --8<-- [end:scalar_index_nested_fields]

    index_columns = {index.name: index.columns for index in await table.list_indices()}
    assert index_columns["nested_user_id_idx"] == ["metadata.user_id"]
    assert index_columns["escaped_user_id_idx"] == ["metadata.`user.id`"]


def test_fts_index_create(tmp_db):
    table = tmp_db.create_table(
        "fts-index-create",
        [{"text": "hello world", "vector": [0.1, 0.2]}],
        mode="overwrite",
    )

    db = tmp_db
    # --8<-- [start:fts_index_create]
    table_name = "fts-index-create"
    table = db.open_table(table_name)
    table.create_fts_index("text")
    # --8<-- [end:fts_index_create]

    assert table.list_indices()


def test_fts_index_wait(tmp_db):
    table = tmp_db.create_table(
        "fts-index-wait",
        [{"text": "full text search"}],
        mode="overwrite",
    )
    
    db = tmp_db
    # --8<-- [start:fts_index_wait]
    table_name = "fts-index-wait"

    table = db.open_table(table_name)
    table.create_fts_index("text")

    index_name = "text_idx"
    table.wait_for_index([index_name])
    # --8<-- [end:fts_index_wait]

    assert table.list_indices()


def test_fts_index_nested_field(tmp_db):
    nested_schema = pa.struct([
        pa.field("text", pa.string()),
        pa.field("count", pa.int32()),
    ])
    schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field("payload", nested_schema),
    ])
    tmp_db.create_table(
        "fts-index-nested",
        pa.table(
            {
                "id": pa.array([1, 2], pa.int64()),
                "payload": pa.array(
                    [
                        {"text": "Frodo was a happy puppy", "count": 1},
                        {"text": "puppy runs through the meadow", "count": 2},
                    ],
                    type=nested_schema,
                ),
            },
            schema=schema,
        ),
        mode="overwrite",
    )

    db = tmp_db
    # --8<-- [start:fts_index_nested]
    from lancedb.query import MatchQuery, PhraseQuery

    table = db.open_table("fts-index-nested")

    # Index a text leaf inside a struct column using a dotted path.
    table.create_fts_index("payload.text", with_position=True)

    # The same dotted path works in MatchQuery and PhraseQuery.
    matches = (
        table.search(MatchQuery("puppy", "payload.text")).limit(5).to_list()
    )
    phrases = (
        table.search(PhraseQuery("puppy runs", "payload.text"))
        .limit(5)
        .to_list()
    )
    # --8<-- [end:fts_index_nested]

    assert len(matches) > 0
    assert all("puppy" in row["payload"]["text"] for row in matches)
    assert len(phrases) > 0
    assert all("puppy runs" in row["payload"]["text"] for row in phrases)


@pytest.mark.asyncio
async def test_fts_index_async(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    # --8<-- [start:fts_index_async]
    import asyncio

    import lancedb
    import polars as pl
    from lancedb.index import FTS

    data = pl.DataFrame(
        {
            "id": [1, 2],
            "text": [
                "His first language is spanish",
                "Her first language is english",
            ],
        }
    )

    async def main(data: pl.DataFrame):
        uri = "ex_lancedb"
        db = await lancedb.connect_async(uri)
        tbl = await db.create_table("my_text", data=data, mode="overwrite")

        await tbl.create_index("text", config=FTS(language="English"))

        response = await tbl.search("spanish", query_type="fts")
        result = await response.limit(1).to_polars()
        print(result)
        return result

    if __name__ == "__main__":
        asyncio.run(main(data))
    # --8<-- [end:fts_index_async]

    result = await main(data)
    assert result.height == 1


def test_gpu_index_snippets(tmp_db, monkeypatch):
    table = tmp_db.create_table(
        "gpu_index",
        _make_vector_rows(32, 8),
        mode="overwrite",
    )

    calls = []

    def fake_create_index(*args, **kwargs):
        calls.append(kwargs)
        return None

    monkeypatch.setattr(table, "create_index", fake_create_index)

    # --8<-- [start:gpu_index_cuda]
    table.create_index(
        num_partitions=256,
        num_sub_vectors=96,
        accelerator="cuda",
    )
    # --8<-- [end:gpu_index_cuda]

    # --8<-- [start:gpu_index_mps]
    table.create_index(
        num_partitions=256,
        num_sub_vectors=96,
        accelerator="mps",
    )
    # --8<-- [end:gpu_index_mps]

    assert calls[0]["accelerator"] == "cuda"
    assert calls[1]["accelerator"] == "mps"


def test_reindexing_incremental(tmp_db):
    table = tmp_db.create_table(
        "reindexing_incremental",
        [{"vector": [3.1, 4.1], "text": "Frodo was a happy puppy"}],
        mode="overwrite",
    )
    db = tmp_db
    # --8<-- [start:reindexing_incremental]
    table = db.open_table("reindexing_incremental")
    table.add([{"vector": [3.1, 4.1], "text": "Frodo was a happy puppy"}])
    table.optimize()
    # --8<-- [end:reindexing_incremental]

    result = table.search().limit(10).to_pandas()
    assert len(result) == 2
