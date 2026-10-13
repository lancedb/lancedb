# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

from pathlib import Path
import shutil


def test_connection():
    # --8<-- [start:connect]
    import lancedb

    uri = "ex_lancedb"
    db = lancedb.connect(uri)
    # --8<-- [end:connect]
    assert db is not None
    shutil.rmtree(uri, ignore_errors=True)
    assert not Path(uri).exists()


async def connect_async_example():
    # --8<-- [start:connect_async]
    import lancedb

    uri = "ex_lancedb"
    async_db = await lancedb.connect_async(uri)
    # --8<-- [end:connect_async]

    return async_db


def connect_enterprise_quickstart_config():
    import lancedb

    # --8<-- [start:connect_enterprise_quickstart]
    uri = "db://your-database-uri"
    api_key = "your-api-key"
    region = "us-east-1"
    host_override = "https://your-enterprise-endpoint.com"

    db = lancedb.connect(
        uri=uri,
        api_key=api_key,
        region=region,
        host_override=host_override,
    )
    # --8<-- [end:connect_enterprise_quickstart]
    return db


def test_connect_enterprise_quickstart(monkeypatch):
    import lancedb

    captured = {}

    def fake_connect(**kwargs):
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(lancedb, "connect", fake_connect)

    db = connect_enterprise_quickstart_config()
    assert db is not None
    assert captured == {
        "uri": "db://your-database-uri",
        "api_key": "your-api-key",
        "region": "us-east-1",
        "host_override": "https://your-enterprise-endpoint.com",
    }


def connect_object_storage_config():
    # --8<-- [start:connect_object_storage]
    import lancedb

    uri = "s3://your-bucket/path"
    # You can also use "gs://your-bucket/path" or "az://your-container/path".
    db = lancedb.connect(uri)
    # --8<-- [end:connect_object_storage]

    return db


def namespace_table_ops_example():
    # --8<-- [start:namespace_table_ops]
    import lancedb

    db = lancedb.connect_namespace("dir", {"root": "./local_lancedb"})

    # Create namespace tree: prod/search
    db.create_namespace(["prod"], mode="exist_ok")
    db.create_namespace(["prod", "search"], mode="exist_ok")
    db.create_namespace(["prod", "recommendations"], mode="exist_ok")

    db.create_table(
        "user",
        data=[{"id": 1, "vector": [0.1, 0.2], "name": "alice"}],
        namespace_path=["prod", "search"],
        mode="create",  # use "overwrite" only if you want to replace existing table
    )

    db.create_table(
        "user",
        data=[{"id": 2, "vector": [0.3, 0.4], "name": "bob"}],
        namespace_path=["prod", "recommendations"],
        mode="create",  # use "overwrite" only if you want to replace existing table
    )

    # Verify
    print(db.list_namespaces())  # ['prod']
    print(db.list_namespaces(namespace_path=["prod"]))  # ['recommendations', 'search']
    print(db.list_tables(namespace_path=["prod", "search"]))  # ['user']
    print(db.list_tables(namespace_path=["prod", "recommendations"]))  # ['user']
    # --8<-- [end:namespace_table_ops]


def namespace_admin_ops_example():
    # --8<-- [start:namespace_admin_ops]
    import lancedb

    db = lancedb.connect_namespace("dir", {"root": "./local_lancedb"})
    namespace = ["prod", "search"]

    db.create_namespace(["prod"])
    db.create_namespace(["prod", "search"])

    child_namespaces = db.list_namespaces(namespace_path=["prod"]).namespaces
    print(f"Child namespaces under {namespace}: {child_namespaces}")
    # Child namespaces under ['prod', 'search']: ['search']

    metadata = db.describe_namespace(["prod", "search"])
    print(f"Metadata for namespace {namespace}: {metadata}")
    # Metadata for namespace ['prod', 'search']: properties=None

    db.drop_namespace(["prod", "search"], mode="skip")
    db.drop_namespace(["prod"], mode="skip")
    # --8<-- [end:namespace_admin_ops]
    return child_namespaces, metadata

async def connect_object_storage_config_async():
    # --8<-- [start:connect_object_storage_async]
    import lancedb

    uri = "s3://your-bucket/path"
    # You can also use "gs://your-bucket/path" or "az://your-container/path".
    async_db = await lancedb.connect_async(uri)
    # --8<-- [end:connect_object_storage_async]

    return async_db
