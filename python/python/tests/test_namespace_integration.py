# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""
Integration tests for LanceDB Namespace with S3 and credential refresh.

This test uses DirectoryNamespace with native ops_metrics and vend_input_storage_options
features to track API calls and test credential refresh mechanisms.

Tests are parameterized to run with both DirectoryNamespace and a CustomNamespace
wrapper to verify Python-Rust binding works correctly for custom implementations.

Tests verify:
- Storage options provider is auto-created and used
- Credentials are properly cached during reads
- Credentials refresh when they expire
- Both create and open operations work with credential rotation
"""

import copy
import time
import uuid
from typing import Dict, Optional

import pyarrow as pa
import pytest
from lance.namespace import (
    DeclareTableRequest,
    DeclareTableResponse,
    DescribeTableRequest,
    DescribeTableResponse,
    DirectoryNamespace,
    LanceNamespace,
)
from lance_namespace import (
    CreateNamespaceRequest,
    CreateNamespaceResponse,
    CreateTableRequest,
    CreateTableResponse,
    CreateTableVersionRequest,
    CreateTableVersionResponse,
    DeregisterTableRequest,
    DeregisterTableResponse,
    DescribeNamespaceRequest,
    DescribeNamespaceResponse,
    DescribeTableVersionRequest,
    DescribeTableVersionResponse,
    DropNamespaceRequest,
    DropNamespaceResponse,
    DropTableRequest,
    DropTableResponse,
    ListNamespacesRequest,
    ListNamespacesResponse,
    ListTablesRequest,
    ListTablesResponse,
    ListTableVersionsRequest,
    ListTableVersionsResponse,
    NamespaceExistsRequest,
    RegisterTableRequest,
    RegisterTableResponse,
    TableExistsRequest,
)
from lancedb.namespace import LanceNamespaceDBConnection


class CustomNamespace(LanceNamespace):
    """A custom namespace wrapper that delegates to DirectoryNamespace.

    This class verifies that the Python-Rust binding works correctly for
    custom namespace implementations that wrap the native DirectoryNamespace.
    All methods simply delegate to the underlying DirectoryNamespace instance.
    """

    def __init__(self, inner: DirectoryNamespace):
        self._inner = inner

    def namespace_id(self) -> str:
        return f"CustomNamespace[{self._inner.namespace_id()}]"

    def create_namespace(
        self, request: CreateNamespaceRequest
    ) -> CreateNamespaceResponse:
        return self._inner.create_namespace(request)

    def describe_namespace(
        self, request: DescribeNamespaceRequest
    ) -> DescribeNamespaceResponse:
        return self._inner.describe_namespace(request)

    def namespace_exists(self, request: NamespaceExistsRequest) -> None:
        return self._inner.namespace_exists(request)

    def drop_namespace(self, request: DropNamespaceRequest) -> DropNamespaceResponse:
        return self._inner.drop_namespace(request)

    def list_namespaces(self, request: ListNamespacesRequest) -> ListNamespacesResponse:
        return self._inner.list_namespaces(request)

    def create_table(
        self, request: CreateTableRequest, data: bytes
    ) -> CreateTableResponse:
        return self._inner.create_table(request, data)

    def declare_table(self, request: DeclareTableRequest) -> DeclareTableResponse:
        return self._inner.declare_table(request)

    def describe_table(self, request: DescribeTableRequest) -> DescribeTableResponse:
        return self._inner.describe_table(request)

    def table_exists(self, request: TableExistsRequest) -> None:
        return self._inner.table_exists(request)

    def drop_table(self, request: DropTableRequest) -> DropTableResponse:
        return self._inner.drop_table(request)

    def list_tables(self, request: ListTablesRequest) -> ListTablesResponse:
        return self._inner.list_tables(request)

    def register_table(self, request: RegisterTableRequest) -> RegisterTableResponse:
        return self._inner.register_table(request)

    def deregister_table(
        self, request: DeregisterTableRequest
    ) -> DeregisterTableResponse:
        return self._inner.deregister_table(request)

    def list_table_versions(
        self, request: ListTableVersionsRequest
    ) -> ListTableVersionsResponse:
        return self._inner.list_table_versions(request)

    def describe_table_version(
        self, request: DescribeTableVersionRequest
    ) -> DescribeTableVersionResponse:
        return self._inner.describe_table_version(request)

    def create_table_version(
        self, request: CreateTableVersionRequest
    ) -> CreateTableVersionResponse:
        return self._inner.create_table_version(request)

    def retrieve_ops_metrics(self) -> Optional[Dict[str, int]]:
        return self._inner.retrieve_ops_metrics()


def _wrap_if_custom(ns_client: DirectoryNamespace, use_custom: bool):
    """Wrap namespace client in CustomNamespace if use_custom is True."""
    if use_custom:
        return CustomNamespace(ns_client)
    return ns_client


# LocalStack S3 configuration
CONFIG = {
    "allow_http": "true",
    "aws_access_key_id": "ACCESSKEY",
    "aws_secret_access_key": "SECRETKEY",
    "aws_endpoint": "http://localhost:4566",
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
    """Create and cleanup S3 bucket for integration tests."""
    s3 = get_boto3_client("s3", endpoint_url=CONFIG["aws_endpoint"])
    bucket_name = "lancedb-namespace-integtest"

    # Clean up existing bucket if it exists
    try:
        delete_bucket(s3, bucket_name)
    except s3.exceptions.NoSuchBucket:
        pass

    s3.create_bucket(Bucket=bucket_name)
    yield bucket_name

    # Cleanup after tests
    delete_bucket(s3, bucket_name)


def delete_bucket(s3, bucket_name):
    """Delete S3 bucket and all its contents."""
    try:
        # Delete all objects first
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket_name):
            if "Contents" in page:
                for obj in page["Contents"]:
                    s3.delete_object(Bucket=bucket_name, Key=obj["Key"])
        s3.delete_bucket(Bucket=bucket_name)
    except Exception:
        pass


def create_tracking_namespace(
    bucket_name: str,
    storage_options: dict,
    credential_expires_in_seconds: int = 60,
    use_custom: bool = False,
):
    """Create a DirectoryNamespace with ops metrics and credential vending enabled.

    Uses native DirectoryNamespace features:
    - ops_metrics_enabled=true: Tracks API call counts via retrieve_ops_metrics()
    - vend_input_storage_options=true: Returns input storage options in responses
    - vend_input_storage_options_refresh_interval_millis: Adds expires_at_millis

    Args:
        bucket_name: S3 bucket name or local path
        storage_options: Storage options to pass through (credentials, endpoint, etc.)
        credential_expires_in_seconds: Interval in seconds for credential expiration
        use_custom: If True, wrap in CustomNamespace for testing custom implementations

    Returns:
        Tuple of (namespace_client, inner_namespace_client) where inner is always
        the DirectoryNamespace (used for metrics retrieval)
    """
    # Add refresh_offset_millis to storage options so that credentials are not
    # considered expired immediately. Set to 1 second (1000ms) so that refresh
    # checks work correctly with short-lived credentials in tests.
    storage_options_with_refresh = dict(storage_options)
    storage_options_with_refresh["refresh_offset_millis"] = "1000"

    dir_props = {f"storage.{k}": v for k, v in storage_options_with_refresh.items()}

    if bucket_name.startswith("/") or bucket_name.startswith("file://"):
        dir_props["root"] = f"{bucket_name}/namespace_root"
    else:
        dir_props["root"] = f"s3://{bucket_name}/namespace_root"

    # Enable ops metrics tracking
    dir_props["ops_metrics_enabled"] = "true"
    # Enable storage options vending
    dir_props["vend_input_storage_options"] = "true"
    # Set refresh interval in milliseconds
    dir_props["vend_input_storage_options_refresh_interval_millis"] = str(
        credential_expires_in_seconds * 1000
    )

    inner_ns_client = DirectoryNamespace(**dir_props)
    ns_client = _wrap_if_custom(inner_ns_client, use_custom)
    return ns_client, inner_ns_client


def get_describe_call_count(namespace_client) -> int:
    """Get the number of describe_table calls made to the namespace client."""
    return namespace_client.retrieve_ops_metrics().get("describe_table", 0)


def get_declare_call_count(namespace_client) -> int:
    """Get the number of declare_table calls made to the namespace client."""
    return namespace_client.retrieve_ops_metrics().get("declare_table", 0)


@pytest.mark.s3_test
@pytest.mark.parametrize("use_custom", [False, True], ids=["DirectoryNS", "CustomNS"])
def test_namespace_create_table_with_provider(s3_bucket: str, use_custom: bool):
    """
    Test creating a table through namespace with storage options provider.

    Verifies:
    - declare_table is called once to reserve location
    - Storage options provider is auto-created
    - Table can be written successfully
    - Credentials are cached during write operations
    """
    storage_options = copy.deepcopy(CONFIG)

    ns_client, inner_ns_client = create_tracking_namespace(
        bucket_name=s3_bucket,
        storage_options=storage_options,
        credential_expires_in_seconds=3600,  # 1 hour
        use_custom=use_custom,
    )

    db = LanceNamespaceDBConnection(ns_client)

    # Create unique namespace for this test
    namespace_name = f"test_ns_{uuid.uuid4().hex[:8]}"
    db.create_namespace([namespace_name])

    table_name = f"test_table_{uuid.uuid4().hex}"
    namespace_path = [namespace_name]

    # Verify initial state
    assert get_declare_call_count(inner_ns_client) == 0
    assert get_describe_call_count(inner_ns_client) == 0

    # Create table with data
    data = pa.table(
        {
            "id": [1, 2, 3],
            "vector": [[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]],
            "text": ["hello", "world", "test"],
        }
    )

    table = db.create_table(table_name, data, namespace_path=namespace_path)

    # Verify declare_table was called exactly once
    assert get_declare_call_count(inner_ns_client) == 1
    # describe_table should NOT be called during create in create mode
    assert get_describe_call_count(inner_ns_client) == 0

    # Verify table was created successfully
    assert table.name == table_name
    result = table.to_pandas()
    assert len(result) == 3
    assert list(result["id"]) == [1, 2, 3]


@pytest.mark.s3_test
@pytest.mark.parametrize("use_custom", [False, True], ids=["DirectoryNS", "CustomNS"])
def test_namespace_open_table_with_provider(s3_bucket: str, use_custom: bool):
    """
    Test opening a table through namespace with storage options provider.

    Verifies:
    - describe_table is called once when opening
    - Storage options provider is auto-created
    - Table can be read successfully
    - Credentials are cached during read operations
    """
    storage_options = copy.deepcopy(CONFIG)

    ns_client, inner_ns_client = create_tracking_namespace(
        bucket_name=s3_bucket,
        storage_options=storage_options,
        credential_expires_in_seconds=3600,
        use_custom=use_custom,
    )

    db = LanceNamespaceDBConnection(ns_client)

    # Create unique namespace for this test
    namespace_name = f"test_ns_{uuid.uuid4().hex[:8]}"
    db.create_namespace([namespace_name])

    table_name = f"test_table_{uuid.uuid4().hex}"
    namespace_path = [namespace_name]

    # Create table first
    data = pa.table(
        {
            "id": [1, 2, 3, 4, 5],
            "vector": [[1.0, 2.0], [3.0, 4.0], [5.0, 6.0], [7.0, 8.0], [9.0, 10.0]],
            "value": [10, 20, 30, 40, 50],
        }
    )

    db.create_table(table_name, data, namespace_path=namespace_path)

    initial_declare_count = get_declare_call_count(inner_ns_client)
    assert initial_declare_count == 1

    # Open the table
    opened_table = db.open_table(table_name, namespace_path=namespace_path)

    # Verify describe_table was called exactly once
    assert get_describe_call_count(inner_ns_client) == 1
    # declare_table should not be called again
    assert get_declare_call_count(inner_ns_client) == initial_declare_count

    # Perform multiple read operations
    describe_count_after_open = get_describe_call_count(inner_ns_client)

    for _ in range(3):
        result = opened_table.to_pandas()
        assert len(result) == 5
        count = opened_table.count_rows()
        assert count == 5

    # Verify credentials were cached (no additional describe_table calls)
    assert get_describe_call_count(inner_ns_client) == describe_count_after_open


@pytest.mark.s3_test
@pytest.mark.parametrize("use_custom", [False, True], ids=["DirectoryNS", "CustomNS"])
def test_namespace_credential_refresh_on_read(s3_bucket: str, use_custom: bool):
    """
    Test credential refresh when credentials expire during read operations.

    Verifies:
    - Credentials are cached initially (no additional describe_table calls)
    - After expiration, credentials are refreshed (describe_table called again)
    - Read operations continue to work with refreshed credentials
    """
    storage_options = copy.deepcopy(CONFIG)

    ns_client, inner_ns_client = create_tracking_namespace(
        bucket_name=s3_bucket,
        storage_options=storage_options,
        credential_expires_in_seconds=3,  # Short expiration for testing
        use_custom=use_custom,
    )

    db = LanceNamespaceDBConnection(ns_client)

    # Create unique namespace for this test
    namespace_name = f"test_ns_{uuid.uuid4().hex[:8]}"
    db.create_namespace([namespace_name])

    table_name = f"test_table_{uuid.uuid4().hex}"
    namespace_path = [namespace_name]

    # Create table
    data = pa.table(
        {
            "id": [1, 2, 3],
            "vector": [[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]],
        }
    )

    db.create_table(table_name, data, namespace_path=namespace_path)

    # Open table (triggers describe_table)
    opened_table = db.open_table(table_name, namespace_path=namespace_path)

    # Perform an immediate read (should use credentials from open)
    result = opened_table.to_pandas()
    assert len(result) == 3

    describe_count_after_first_read = get_describe_call_count(inner_ns_client)

    # Wait for credentials to expire (3 seconds + buffer)
    time.sleep(5)

    # Perform read after expiration (should trigger credential refresh)
    result = opened_table.to_pandas()
    assert len(result) == 3

    describe_count_after_refresh = get_describe_call_count(inner_ns_client)
    # Verify describe_table was called again (credential refresh)
    refresh_delta = describe_count_after_refresh - describe_count_after_first_read

    # Verify the exact count: credential refresh should call describe_table exactly
    # once
    assert refresh_delta == 1, (
        f"Credential refresh should call describe_table exactly once "
        f"(got {refresh_delta})"
    )


@pytest.mark.s3_test
@pytest.mark.parametrize("use_custom", [False, True], ids=["DirectoryNS", "CustomNS"])
def test_namespace_credential_refresh_on_write(s3_bucket: str, use_custom: bool):
    """
    Test credential refresh when credentials expire during write operations.

    Verifies:
    - Credentials are cached during initial writes
    - After expiration, new credentials are fetched before writes
    - Write operations continue to work with refreshed credentials
    """
    storage_options = copy.deepcopy(CONFIG)

    ns_client, inner_ns_client = create_tracking_namespace(
        bucket_name=s3_bucket,
        storage_options=storage_options,
        credential_expires_in_seconds=3,  # Short expiration
        use_custom=use_custom,
    )

    db = LanceNamespaceDBConnection(ns_client)

    # Create unique namespace for this test
    namespace_name = f"test_ns_{uuid.uuid4().hex[:8]}"
    db.create_namespace([namespace_name])

    table_name = f"test_table_{uuid.uuid4().hex}"
    namespace_path = [namespace_name]

    # Create table
    initial_data = pa.table(
        {
            "id": [1, 2],
            "vector": [[1.0, 2.0], [3.0, 4.0]],
        }
    )

    table = db.create_table(table_name, initial_data, namespace_path=namespace_path)

    # Add more data (should use cached credentials)
    new_data = pa.table(
        {
            "id": [3, 4],
            "vector": [[5.0, 6.0], [7.0, 8.0]],
        }
    )
    table.add(new_data)

    # Wait for credentials to expire
    time.sleep(5)

    # Add more data (should trigger credential refresh)
    more_data = pa.table(
        {
            "id": [5, 6],
            "vector": [[9.0, 10.0], [11.0, 12.0]],
        }
    )
    table.add(more_data)

    # Verify final row count
    assert table.count_rows() == 6


@pytest.mark.s3_test
@pytest.mark.parametrize("use_custom", [False, True], ids=["DirectoryNS", "CustomNS"])
def test_namespace_overwrite_mode(s3_bucket: str, use_custom: bool):
    """
    Test creating table in overwrite mode with credential tracking.

    Verifies:
    - First create calls declare_table exactly once
    - Overwrite mode calls describe_table exactly once to check existence
    - Storage options provider works in overwrite mode
    """
    storage_options = copy.deepcopy(CONFIG)

    ns_client, inner_ns_client = create_tracking_namespace(
        bucket_name=s3_bucket,
        storage_options=storage_options,
        credential_expires_in_seconds=3600,
        use_custom=use_custom,
    )

    db = LanceNamespaceDBConnection(ns_client)

    # Create unique namespace for this test
    namespace_name = f"test_ns_{uuid.uuid4().hex[:8]}"
    db.create_namespace([namespace_name])

    table_name = f"test_table_{uuid.uuid4().hex}"
    namespace_path = [namespace_name]

    # Create initial table
    data1 = pa.table(
        {
            "id": [1, 2],
            "vector": [[1.0, 2.0], [3.0, 4.0]],
        }
    )

    table = db.create_table(table_name, data1, namespace_path=namespace_path)
    # Exactly one declare_table call for initial create
    assert get_declare_call_count(inner_ns_client) == 1
    # No describe_table calls in create mode
    assert get_describe_call_count(inner_ns_client) == 0
    assert table.count_rows() == 2

    # Overwrite the table
    data2 = pa.table(
        {
            "id": [10, 20, 30],
            "vector": [[10.0, 20.0], [30.0, 40.0], [50.0, 60.0]],
        }
    )

    table2 = db.create_table(
        table_name, data2, namespace_path=namespace_path, mode="overwrite"
    )

    # Should still have only 1 declare_table call
    # (overwrite reuses location from describe_table)
    assert get_declare_call_count(inner_ns_client) == 1
    # Should have called describe_table exactly once to get existing table location
    assert get_describe_call_count(inner_ns_client) == 1

    # Verify new data
    assert table2.count_rows() == 3
    result = table2.to_pandas()
    assert list(result["id"]) == [10, 20, 30]


@pytest.mark.s3_test
@pytest.mark.parametrize("use_custom", [False, True], ids=["DirectoryNS", "CustomNS"])
def test_namespace_multiple_tables(s3_bucket: str, use_custom: bool):
    """
    Test creating and opening multiple tables in the same namespace.

    Verifies:
    - Each table gets its own storage options provider
    - Credentials are tracked independently per table
    - Multiple tables can coexist in the same namespace
    """
    storage_options = copy.deepcopy(CONFIG)

    ns_client, inner_ns_client = create_tracking_namespace(
        bucket_name=s3_bucket,
        storage_options=storage_options,
        credential_expires_in_seconds=3600,
        use_custom=use_custom,
    )

    db = LanceNamespaceDBConnection(ns_client)

    # Create unique namespace for this test
    namespace_name = f"test_ns_{uuid.uuid4().hex[:8]}"
    db.create_namespace([namespace_name])
    namespace_path = [namespace_name]

    # Create first table
    table1_name = f"table1_{uuid.uuid4().hex}"
    data1 = pa.table({"id": [1, 2], "value": [10, 20]})
    db.create_table(table1_name, data1, namespace_path=namespace_path)

    # Create second table
    table2_name = f"table2_{uuid.uuid4().hex}"
    data2 = pa.table({"id": [3, 4], "value": [30, 40]})
    db.create_table(table2_name, data2, namespace_path=namespace_path)

    # Should have 2 declare calls (one per table)
    assert get_declare_call_count(inner_ns_client) == 2

    # Open both tables
    opened1 = db.open_table(table1_name, namespace_path=namespace_path)
    opened2 = db.open_table(table2_name, namespace_path=namespace_path)

    # Should have 2 describe calls (one per open)
    assert get_describe_call_count(inner_ns_client) == 2

    # Verify both tables work independently
    assert opened1.count_rows() == 2
    assert opened2.count_rows() == 2

    result1 = opened1.to_pandas()
    result2 = opened2.to_pandas()

    assert list(result1["id"]) == [1, 2]
    assert list(result2["id"]) == [3, 4]


@pytest.mark.s3_test
@pytest.mark.parametrize("use_custom", [False, True], ids=["DirectoryNS", "CustomNS"])
def test_namespace_with_schema_only(s3_bucket: str, use_custom: bool):
    """
    Test creating empty table with schema only (no data).

    Verifies:
    - Empty table creation works with storage options provider
    - describe_table is NOT called during create
    - Data can be added later
    """
    storage_options = copy.deepcopy(CONFIG)

    ns_client, inner_ns_client = create_tracking_namespace(
        bucket_name=s3_bucket,
        storage_options=storage_options,
        credential_expires_in_seconds=3600,
        use_custom=use_custom,
    )

    db = LanceNamespaceDBConnection(ns_client)

    # Create unique namespace for this test
    namespace_name = f"test_ns_{uuid.uuid4().hex[:8]}"
    db.create_namespace([namespace_name])

    table_name = f"test_table_{uuid.uuid4().hex}"
    namespace_path = [namespace_name]

    # Create empty table with schema
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("vector", pa.list_(pa.float32(), 2)),
            pa.field("text", pa.utf8()),
        ]
    )

    table = db.create_table(table_name, schema=schema, namespace_path=namespace_path)

    # Should have called declare_table once
    assert get_declare_call_count(inner_ns_client) == 1
    # Should NOT have called describe_table in create mode
    assert get_describe_call_count(inner_ns_client) == 0

    # Verify empty table
    assert table.count_rows() == 0

    # Add data
    data = pa.table(
        {
            "id": [1, 2],
            "vector": [[1.0, 2.0], [3.0, 4.0]],
            "text": ["hello", "world"],
        }
    )
    table.add(data)

    # Verify data was added
    assert table.count_rows() == 2
