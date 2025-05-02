# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import pytest


def test_upsert(mem_db):
    db = mem_db
    # --8<-- [start:upsert_basic]
    table = db.create_table(
        "users",
        [
            {"id": 0, "name": "Alice"},
            {"id": 1, "name": "Bob"},
        ],
    )
    new_users = [
        {"id": 1, "name": "Bobby"},
        {"id": 2, "name": "Charlie"},
    ]
    res = (
        table.merge_insert("id")
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute(new_users)
    )
    table.count_rows()  # 3
    res  # {'num_inserted_rows': 1, 'num_updated_rows': 1, 'num_deleted_rows': 0}
    # --8<-- [end:upsert_basic]
    assert table.count_rows() == 3
    assert res.num_inserted_rows == 1
    assert res.num_deleted_rows == 0
    assert res.num_updated_rows == 1


@pytest.mark.asyncio
async def test_upsert_async(mem_db_async):
    db = mem_db_async
    # --8<-- [start:upsert_basic_async]
    table = await db.create_table(
        "users",
        [
            {"id": 0, "name": "Alice"},
            {"id": 1, "name": "Bob"},
        ],
    )
    new_users = [
        {"id": 1, "name": "Bobby"},
        {"id": 2, "name": "Charlie"},
    ]
    res = await (
        table.merge_insert("id")
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute(new_users)
    )
    await table.count_rows()  # 3
    res
    # MergeResult(version=2, num_updated_rows=1,
    # num_inserted_rows=1, num_deleted_rows=0)
    # --8<-- [end:upsert_basic_async]
    assert await table.count_rows() == 3
    assert res.version == 2
    assert res.num_inserted_rows == 1
    assert res.num_deleted_rows == 0
    assert res.num_updated_rows == 1


def test_insert_if_not_exists(mem_db):
    db = mem_db
    # --8<-- [start:insert_if_not_exists]
    table = db.create_table(
        "domains",
        [
            {"domain": "google.com", "name": "Google"},
            {"domain": "github.com", "name": "GitHub"},
        ],
    )
    new_domains = [
        {"domain": "google.com", "name": "Google"},
        {"domain": "facebook.com", "name": "Facebook"},
    ]
    res = (
        table.merge_insert("domain").when_not_matched_insert_all().execute(new_domains)
    )
    table.count_rows()  # 3
    res
    # MergeResult(version=2, num_updated_rows=0,
    # num_inserted_rows=1, num_deleted_rows=0)
    # --8<-- [end:insert_if_not_exists]
    assert table.count_rows() == 3
    assert res.version == 2
    assert res.num_inserted_rows == 1
    assert res.num_deleted_rows == 0
    assert res.num_updated_rows == 0


@pytest.mark.asyncio
async def test_insert_if_not_exists_async(mem_db_async):
    db = mem_db_async
    # --8<-- [start:insert_if_not_exists_async]
    table = await db.create_table(
        "domains",
        [
            {"domain": "google.com", "name": "Google"},
            {"domain": "github.com", "name": "GitHub"},
        ],
    )
    new_domains = [
        {"domain": "google.com", "name": "Google"},
        {"domain": "facebook.com", "name": "Facebook"},
    ]
    res = await (
        table.merge_insert("domain").when_not_matched_insert_all().execute(new_domains)
    )
    await table.count_rows()  # 3
    res
    # MergeResult(version=2, num_updated_rows=0,
    # num_inserted_rows=1, num_deleted_rows=0)
    # --8<-- [end:insert_if_not_exists]
    assert await table.count_rows() == 3
    assert res.version == 2
    assert res.num_inserted_rows == 1
    assert res.num_deleted_rows == 0
    assert res.num_updated_rows == 0


def test_replace_range(mem_db):
    db = mem_db
    # --8<-- [start:replace_range]
    table = db.create_table(
        "chunks",
        [
            {"doc_id": 0, "chunk_id": 0, "text": "Hello"},
            {"doc_id": 0, "chunk_id": 1, "text": "World"},
            {"doc_id": 1, "chunk_id": 0, "text": "Foo"},
            {"doc_id": 1, "chunk_id": 1, "text": "Bar"},
        ],
    )
    new_chunks = [
        {"doc_id": 1, "chunk_id": 0, "text": "Baz"},
    ]
    res = (
        table.merge_insert(["doc_id", "chunk_id"])
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .when_not_matched_by_source_delete("doc_id = 1")
        .execute(new_chunks)
    )
    table.count_rows("doc_id = 1")  # 1
    res
    # MergeResult(version=2, num_updated_rows=1,
    # num_inserted_rows=0, num_deleted_rows=1)
    # --8<-- [end:insert_if_not_exists]
    assert table.count_rows("doc_id = 1") == 1
    assert res.version == 2
    assert res.num_inserted_rows == 0
    assert res.num_deleted_rows == 1
    assert res.num_updated_rows == 1


@pytest.mark.asyncio
async def test_replace_range_async(mem_db_async):
    db = mem_db_async
    # --8<-- [start:replace_range_async]
    table = await db.create_table(
        "chunks",
        [
            {"doc_id": 0, "chunk_id": 0, "text": "Hello"},
            {"doc_id": 0, "chunk_id": 1, "text": "World"},
            {"doc_id": 1, "chunk_id": 0, "text": "Foo"},
            {"doc_id": 1, "chunk_id": 1, "text": "Bar"},
        ],
    )
    new_chunks = [
        {"doc_id": 1, "chunk_id": 0, "text": "Baz"},
    ]
    res = await (
        table.merge_insert(["doc_id", "chunk_id"])
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .when_not_matched_by_source_delete("doc_id = 1")
        .execute(new_chunks)
    )
    await table.count_rows("doc_id = 1")  # 1
    res
    # MergeResult(version=2, num_updated_rows=1,
    # num_inserted_rows=0, num_deleted_rows=1)
    # --8<-- [end:insert_if_not_exists]
    assert await table.count_rows("doc_id = 1") == 1
    assert res.version == 2
    assert res.num_inserted_rows == 0
    assert res.num_deleted_rows == 1
    assert res.num_updated_rows == 1
