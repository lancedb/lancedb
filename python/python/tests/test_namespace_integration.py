# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""
Integration tests for LanceDB Namespace with S3 and credential refresh.

This test simulates a namespace server that returns incrementing credentials
and verifies that the credential refresh mechanism works correctly for both
create_table and open_table operations.

Tests verify:
- Storage options provider is auto-created and used
- Credentials are properly cached during reads
- Credentials refresh when they expire
- Both create and open operations work with credential rotation
"""

import copy
import time
import uuid
from threading import Lock
from typing import Dict

import pyarrow as pa
import pytest
from lance_namespace import (
    CreateEmptyTableRequest,
    CreateEmptyTableResponse,
    DeclareTableRequest,
    DeclareTableResponse,
    DescribeTableRequest,
    DescribeTableResponse,
    LanceNamespace,
)
from lancedb.namespace import LanceNamespaceDBConnection

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


class TrackingNamespace(LanceNamespace):
    """
    Mock namespace that wraps DirectoryNamespace and tracks API calls.

    This namespace returns incrementing credentials with each API call to simulate
    credential rotation. It also tracks the number of times each API is called
    to verify caching behavior.
    """

    def __init__(
        self,
        bucket_name: str,
        storage_options: Dict[str, str],
        credential_expires_in_seconds: int = 60,
    ):
        from lance.namespace import DirectoryNamespace

        self.bucket_name = bucket_name
        self.base_storage_options = storage_options
        self.credential_expires_in_seconds = credential_expires_in_seconds
        self.describe_call_count = 0
        self.create_call_count = 0
        self.lock = Lock()

        # Create underlying DirectoryNamespace with storage options
        dir_props = {f"storage.{k}": v for k, v in storage_options.items()}

        # Use S3 path for bucket name, local path for file paths
        if bucket_name.startswith("/") or bucket_name.startswith("file://"):
            dir_props["root"] = f"{bucket_name}/namespace_root"
        else:
            dir_props["root"] = f"s3://{bucket_name}/namespace_root"

        self.inner = DirectoryNamespace(**dir_props)

    def get_describe_call_count(self) -> int:
        """Thread-safe getter for describe call count."""
        with self.lock:
            return self.describe_call_count

    def get_create_call_count(self) -> int:
        """Thread-safe getter for create call count."""
        with self.lock:
            return self.create_call_count

    def namespace_id(self) -> str:
        """Return namespace identifier."""
        return f"TrackingNamespace {{ inner: {self.inner.namespace_id()} }}"

    def _modify_storage_options(
        self, storage_options: Dict[str, str], count: int
    ) -> Dict[str, str]:
        """
        Add incrementing credentials with expiration timestamp.

        This simulates a credential rotation system where each call returns
        new credentials that expire after credential_expires_in_seconds.
        """
        modified = copy.deepcopy(storage_options) if storage_options else {}

        # Increment credentials to simulate rotation
        modified["aws_access_key_id"] = f"AKID_{count}"
        modified["aws_secret_access_key"] = f"SECRET_{count}"
        modified["aws_session_token"] = f"TOKEN_{count}"

        # Set expiration time
        expires_at_millis = int(
            (time.time() + self.credential_expires_in_seconds) * 1000
        )
        modified["expires_at_millis"] = str(expires_at_millis)

        return modified

    def declare_table(self, request: DeclareTableRequest) -> DeclareTableResponse:
        """Track declare_table calls and inject rotating credentials."""
        with self.lock:
            self.create_call_count += 1
            count = self.create_call_count

        response = self.inner.declare_table(request)
        response.storage_options = self._modify_storage_options(
            response.storage_options, count
        )

        return response

    def create_empty_table(
        self, request: CreateEmptyTableRequest
    ) -> CreateEmptyTableResponse:
        """Track create_empty_table calls and inject rotating credentials."""
        with self.lock:
            self.create_call_count += 1
            count = self.create_call_count

        response = self.inner.create_empty_table(request)
        response.storage_options = self._modify_storage_options(
            response.storage_options, count
        )

        return response

    def describe_table(self, request: DescribeTableRequest) -> DescribeTableResponse:
        """Track describe_table calls and inject rotating credentials."""
        with self.lock:
            self.describe_call_count += 1
            count = self.describe_call_count

        response = self.inner.describe_table(request)
        response.storage_options = self._modify_storage_options(
            response.storage_options, count
        )

        return response

    # Pass through other methods to inner namespace
    def list_tables(self, request):
        return self.inner.list_tables(request)

    def drop_table(self, request):
        return self.inner.drop_table(request)

    def list_namespaces(self, request):
        return self.inner.list_namespaces(request)

    def create_namespace(self, request):
        return self.inner.create_namespace(request)

    def drop_namespace(self, request):
        return self.inner.drop_namespace(request)


@pytest.mark.s3_test
def test_namespace_create_table_with_provider(s3_bucket: str):
    """
    Test creating a table through namespace with storage options provider.

    Verifies:
    - create_empty_table is called once to reserve location
    - Storage options provider is auto-created
    - Table can be written successfully
    - Credentials are cached during write operations
    """
    storage_options = copy.deepcopy(CONFIG)

    namespace = TrackingNamespace(
        bucket_name=s3_bucket,
        storage_options=storage_options,
        credential_expires_in_seconds=3600,  # 1 hour
    )

    db = LanceNamespaceDBConnection(namespace)

    # Create unique namespace for this test
    namespace_name = f"test_ns_{uuid.uuid4().hex[:8]}"
    db.create_namespace([namespace_name])

    table_name = f"test_table_{uuid.uuid4().hex}"
    namespace_path = [namespace_name]

    # Verify initial state
    assert namespace.get_create_call_count() == 0
    assert namespace.get_describe_call_count() == 0

    # Create table with data
    data = pa.table(
        {
            "id": [1, 2, 3],
            "vector": [[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]],
            "text": ["hello", "world", "test"],
        }
    )

    table = db.create_table(table_name, data, namespace=namespace_path)

    # Verify create_empty_table was called exactly once
    assert namespace.get_create_call_count() == 1
    # describe_table should NOT be called during create in create mode
    assert namespace.get_describe_call_count() == 0

    # Verify table was created successfully
    assert table.name == table_name
    result = table.to_pandas()
    assert len(result) == 3
    assert list(result["id"]) == [1, 2, 3]


@pytest.mark.s3_test
def test_namespace_open_table_with_provider(s3_bucket: str):
    """
    Test opening a table through namespace with storage options provider.

    Verifies:
    - describe_table is called once when opening
    - Storage options provider is auto-created
    - Table can be read successfully
    - Credentials are cached during read operations
    """
    storage_options = copy.deepcopy(CONFIG)

    namespace = TrackingNamespace(
        bucket_name=s3_bucket,
        storage_options=storage_options,
        credential_expires_in_seconds=3600,
    )

    db = LanceNamespaceDBConnection(namespace)

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

    db.create_table(table_name, data, namespace=namespace_path)

    initial_create_count = namespace.get_create_call_count()
    assert initial_create_count == 1

    # Open the table
    opened_table = db.open_table(table_name, namespace=namespace_path)

    # Verify describe_table was called exactly once
    assert namespace.get_describe_call_count() == 1
    # create_empty_table should not be called again
    assert namespace.get_create_call_count() == initial_create_count

    # Perform multiple read operations
    describe_count_after_open = namespace.get_describe_call_count()

    for _ in range(3):
        result = opened_table.to_pandas()
        assert len(result) == 5
        count = opened_table.count_rows()
        assert count == 5

    # Verify credentials were cached (no additional describe_table calls)
    assert namespace.get_describe_call_count() == describe_count_after_open


@pytest.mark.s3_test
def test_namespace_credential_refresh_on_read(s3_bucket: str):
    """
    Test credential refresh when credentials expire during read operations.

    Verifies:
    - Credentials are cached initially (no additional describe_table calls)
    - After expiration, credentials are refreshed (describe_table called again)
    - Read operations continue to work with refreshed credentials
    """
    storage_options = copy.deepcopy(CONFIG)

    namespace = TrackingNamespace(
        bucket_name=s3_bucket,
        storage_options=storage_options,
        credential_expires_in_seconds=3,  # Short expiration for testing
    )

    db = LanceNamespaceDBConnection(namespace)

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

    db.create_table(table_name, data, namespace=namespace_path)

    # Open table (triggers describe_table)
    opened_table = db.open_table(table_name, namespace=namespace_path)

    # Perform an immediate read (should use credentials from open)
    result = opened_table.to_pandas()
    assert len(result) == 3

    describe_count_after_first_read = namespace.get_describe_call_count()

    # Wait for credentials to expire (3 seconds + buffer)
    time.sleep(5)

    # Perform read after expiration (should trigger credential refresh)
    result = opened_table.to_pandas()
    assert len(result) == 3

    describe_count_after_refresh = namespace.get_describe_call_count()
    # Verify describe_table was called again (credential refresh)
    refresh_delta = describe_count_after_refresh - describe_count_after_first_read

    # Verify the exact count: credential refresh should call describe_table exactly
    # once
    assert refresh_delta == 1, (
        f"Credential refresh should call describe_table exactly once "
        f"(got {refresh_delta})"
    )


@pytest.mark.s3_test
def test_namespace_credential_refresh_on_write(s3_bucket: str):
    """
    Test credential refresh when credentials expire during write operations.

    Verifies:
    - Credentials are cached during initial writes
    - After expiration, new credentials are fetched before writes
    - Write operations continue to work with refreshed credentials
    """
    storage_options = copy.deepcopy(CONFIG)

    namespace = TrackingNamespace(
        bucket_name=s3_bucket,
        storage_options=storage_options,
        credential_expires_in_seconds=3,  # Short expiration
    )

    db = LanceNamespaceDBConnection(namespace)

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

    table = db.create_table(table_name, initial_data, namespace=namespace_path)

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
def test_namespace_overwrite_mode(s3_bucket: str):
    """
    Test creating table in overwrite mode with credential tracking.

    Verifies:
    - First create calls create_empty_table exactly once
    - Overwrite mode calls describe_table exactly once to check existence
    - Storage options provider works in overwrite mode
    """
    storage_options = copy.deepcopy(CONFIG)

    namespace = TrackingNamespace(
        bucket_name=s3_bucket,
        storage_options=storage_options,
        credential_expires_in_seconds=3600,
    )

    db = LanceNamespaceDBConnection(namespace)

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

    table = db.create_table(table_name, data1, namespace=namespace_path)
    # Exactly one create_empty_table call for initial create
    assert namespace.get_create_call_count() == 1
    # No describe_table calls in create mode
    assert namespace.get_describe_call_count() == 0
    assert table.count_rows() == 2

    # Overwrite the table
    data2 = pa.table(
        {
            "id": [10, 20, 30],
            "vector": [[10.0, 20.0], [30.0, 40.0], [50.0, 60.0]],
        }
    )

    table2 = db.create_table(
        table_name, data2, namespace=namespace_path, mode="overwrite"
    )

    # Should still have only 1 create_empty_table call
    # (overwrite reuses location from describe_table)
    assert namespace.get_create_call_count() == 1
    # Should have called describe_table exactly once to get existing table location
    assert namespace.get_describe_call_count() == 1

    # Verify new data
    assert table2.count_rows() == 3
    result = table2.to_pandas()
    assert list(result["id"]) == [10, 20, 30]


@pytest.mark.s3_test
def test_namespace_multiple_tables(s3_bucket: str):
    """
    Test creating and opening multiple tables in the same namespace.

    Verifies:
    - Each table gets its own storage options provider
    - Credentials are tracked independently per table
    - Multiple tables can coexist in the same namespace
    """
    storage_options = copy.deepcopy(CONFIG)

    namespace = TrackingNamespace(
        bucket_name=s3_bucket,
        storage_options=storage_options,
        credential_expires_in_seconds=3600,
    )

    db = LanceNamespaceDBConnection(namespace)

    # Create unique namespace for this test
    namespace_name = f"test_ns_{uuid.uuid4().hex[:8]}"
    db.create_namespace([namespace_name])
    namespace_path = [namespace_name]

    # Create first table
    table1_name = f"table1_{uuid.uuid4().hex}"
    data1 = pa.table({"id": [1, 2], "value": [10, 20]})
    db.create_table(table1_name, data1, namespace=namespace_path)

    # Create second table
    table2_name = f"table2_{uuid.uuid4().hex}"
    data2 = pa.table({"id": [3, 4], "value": [30, 40]})
    db.create_table(table2_name, data2, namespace=namespace_path)

    # Should have 2 create calls (one per table)
    assert namespace.get_create_call_count() == 2

    # Open both tables
    opened1 = db.open_table(table1_name, namespace=namespace_path)
    opened2 = db.open_table(table2_name, namespace=namespace_path)

    # Should have 2 describe calls (one per open)
    assert namespace.get_describe_call_count() == 2

    # Verify both tables work independently
    assert opened1.count_rows() == 2
    assert opened2.count_rows() == 2

    result1 = opened1.to_pandas()
    result2 = opened2.to_pandas()

    assert list(result1["id"]) == [1, 2]
    assert list(result2["id"]) == [3, 4]


@pytest.mark.s3_test
def test_namespace_with_schema_only(s3_bucket: str):
    """
    Test creating empty table with schema only (no data).

    Verifies:
    - Empty table creation works with storage options provider
    - describe_table is NOT called during create
    - Data can be added later
    """
    storage_options = copy.deepcopy(CONFIG)

    namespace = TrackingNamespace(
        bucket_name=s3_bucket,
        storage_options=storage_options,
        credential_expires_in_seconds=3600,
    )

    db = LanceNamespaceDBConnection(namespace)

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

    table = db.create_table(table_name, schema=schema, namespace=namespace_path)

    # Should have called create_empty_table once
    assert namespace.get_create_call_count() == 1
    # Should NOT have called describe_table in create mode
    assert namespace.get_describe_call_count() == 0

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
