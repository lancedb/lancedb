#  Copyright 2024 Lance Developers
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import asyncio
import copy
from datetime import timedelta
import threading

import pytest
import pyarrow as pa
import lancedb


# These are all keys that are accepted by storage_options
CONFIG = {
    "allow_http": "true",
    "aws_access_key_id": "ACCESSKEY",
    "aws_secret_access_key": "SECRETKEY",
    "aws_endpoint": "http://localhost:4566",
    "dynamodb_endpoint": "http://localhost:4566",
    "aws_region": "us-east-1",
}


def get_boto3_client(*args, **kwargs):
    import boto3

    return boto3.client(
        *args,
        region_name=CONFIG["aws_region"],
        aws_access_key_id=CONFIG["aws_access_key_id"],
        aws_secret_access_key=CONFIG["aws_secret_access_key"],
        **kwargs,
    )


@pytest.fixture(scope="module")
def s3_bucket():
    s3 = get_boto3_client("s3", endpoint_url=CONFIG["aws_endpoint"])
    bucket_name = "lance-integtest"
    # if bucket exists, delete it
    try:
        delete_bucket(s3, bucket_name)
    except s3.exceptions.NoSuchBucket:
        pass
    s3.create_bucket(Bucket=bucket_name)
    yield bucket_name

    delete_bucket(s3, bucket_name)


def delete_bucket(s3, bucket_name):
    # Delete all objects first
    for obj in s3.list_objects(Bucket=bucket_name).get("Contents", []):
        s3.delete_object(Bucket=bucket_name, Key=obj["Key"])
    s3.delete_bucket(Bucket=bucket_name)


@pytest.mark.s3_test
def test_s3_lifecycle(s3_bucket: str):
    storage_options = copy.copy(CONFIG)

    uri = f"s3://{s3_bucket}/test_lifecycle"
    data = pa.table({"x": [1, 2, 3]})

    async def test():
        db = await lancedb.connect_async(uri, storage_options=storage_options)

        table = await db.create_table("test", schema=data.schema)
        assert await table.count_rows() == 0

        table = await db.create_table("test", data, mode="overwrite")
        assert await table.count_rows() == 3

        await table.add(data, mode="append")
        assert await table.count_rows() == 6

        table = await db.open_table("test")
        assert await table.count_rows() == 6

        await db.drop_table("test")

        await db.drop_database()

    asyncio.run(test())


@pytest.fixture()
def kms_key():
    kms = get_boto3_client("kms", endpoint_url=CONFIG["aws_endpoint"])
    key_id = kms.create_key()["KeyMetadata"]["KeyId"]
    yield key_id
    kms.schedule_key_deletion(KeyId=key_id, PendingWindowInDays=7)


def validate_objects_encrypted(bucket: str, path: str, kms_key: str):
    s3 = get_boto3_client("s3", endpoint_url=CONFIG["aws_endpoint"])
    objects = s3.list_objects_v2(Bucket=bucket, Prefix=path)["Contents"]
    for obj in objects:
        info = s3.head_object(Bucket=bucket, Key=obj["Key"])
        assert info["ServerSideEncryption"] == "aws:kms", (
            "object %s not encrypted" % obj["Key"]
        )
        assert info["SSEKMSKeyId"].endswith(kms_key), (
            "object %s not encrypted with correct key" % obj["Key"]
        )


@pytest.mark.s3_test
def test_s3_sse(s3_bucket: str, kms_key: str):
    storage_options = copy.copy(CONFIG)

    uri = f"s3://{s3_bucket}/test_lifecycle"
    data = pa.table({"x": [1, 2, 3]})

    async def test():
        # Create a table with SSE
        db = await lancedb.connect_async(uri, storage_options=storage_options)

        table = await db.create_table(
            "table1",
            schema=data.schema,
            storage_options={
                "aws_server_side_encryption": "aws:kms",
                "aws_sse_kms_key_id": kms_key,
            },
        )
        await table.add(data)
        await table.update({"x": "1"})

        path = "test_lifecycle/table1.lance"
        validate_objects_encrypted(s3_bucket, path, kms_key)

        # Test we can set encryption at connection level too.
        db = await lancedb.connect_async(
            uri,
            storage_options=dict(
                aws_server_side_encryption="aws:kms",
                aws_sse_kms_key_id=kms_key,
                **storage_options,
            ),
        )

        table = await db.create_table("table2", schema=data.schema)
        await table.add(data)
        await table.update({"x": "1"})

        path = "test_lifecycle/table2.lance"
        validate_objects_encrypted(s3_bucket, path, kms_key)

    asyncio.run(test())


@pytest.fixture(scope="module")
def commit_table():
    ddb = get_boto3_client("dynamodb", endpoint_url=CONFIG["dynamodb_endpoint"])
    table_name = "lance-integtest"
    try:
        ddb.delete_table(TableName=table_name)
    except ddb.exceptions.ResourceNotFoundException:
        pass
    ddb.create_table(
        TableName=table_name,
        KeySchema=[
            {"AttributeName": "base_uri", "KeyType": "HASH"},
            {"AttributeName": "version", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "base_uri", "AttributeType": "S"},
            {"AttributeName": "version", "AttributeType": "N"},
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
    )
    yield table_name
    ddb.delete_table(TableName=table_name)


@pytest.mark.s3_test
def test_s3_dynamodb(s3_bucket: str, commit_table: str):
    storage_options = copy.copy(CONFIG)

    uri = f"s3+ddb://{s3_bucket}/test?ddbTableName={commit_table}"
    data = pa.table({"x": [1, 2, 3]})

    async def test():
        db = await lancedb.connect_async(
            uri,
            storage_options=storage_options,
            read_consistency_interval=timedelta(0),
        )

        table = await db.create_table("test", data)

        # Five concurrent writers
        async def insert():
            # independent table refs for true concurrent writes.
            table = await db.open_table("test")
            await table.add(data, mode="append")

        tasks = [insert() for _ in range(5)]
        await asyncio.gather(*tasks)

        row_count = await table.count_rows()
        assert row_count == 3 * 6

    asyncio.run(test())


@pytest.mark.s3_test
def test_s3_dynamodb_sync(s3_bucket: str, commit_table: str, monkeypatch):
    # Sync API doesn't support storage_options, so we have to provide as env vars
    for key, value in CONFIG.items():
        monkeypatch.setenv(key.upper(), value)

    uri = f"s3+ddb://{s3_bucket}/test2?ddbTableName={commit_table}"
    data = pa.table({"x": ["a", "b", "c"]})

    db = lancedb.connect(
        uri,
        read_consistency_interval=timedelta(0),
    )

    table = db.create_table("test_ddb_sync", data)

    # Five concurrent writers
    def insert():
        table = db.open_table("test_ddb_sync")
        table.add(data, mode="append")

    threads = []
    for _ in range(5):
        thread = threading.Thread(target=insert)
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    row_count = table.count_rows()
    assert row_count == 3 * 6

    # FTS indices should error since they are not supported yet.
    with pytest.raises(
        NotImplementedError,
        match="Full-text search is only supported on the local filesystem",
    ):
        table.create_fts_index("x")

    # make sure list tables still works
    assert db.table_names() == ["test_ddb_sync"]
    db.drop_table("test_ddb_sync")
    assert db.table_names() == []
    db.drop_database()
