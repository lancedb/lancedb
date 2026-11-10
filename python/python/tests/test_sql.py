# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import threading
from uuid import UUID

import numpy as np
import pytest
import pyarrow as pa

import lancedb
from lancedb import _lancedb
from lancedb.arrow import AsyncRecordBatchReader
from lancedb.db import AsyncConnection
from lancedb.remote.db import RemoteDBConnection
from lancedb.sql import AsyncQuery, Query, to_parameter_batch

NIL_QUERY_ID = UUID(int=0)


class FakeNativeQuery:
    id = UUID("0198f1b2-c3d4-7e5f-8123-456789abcdef")

    async def reader(self):
        return pa.table({"value": [1, 2]})


class FakeNativeConnection:
    def __init__(self):
        self.parameters = []

    async def execute_query_async(
        self, query, *, default_namespace_path=None, parameters=None
    ):
        self.parameters.append(parameters)
        return FakeNativeQuery()


class FakeAsyncConnection:
    async def execute_query_async(
        self, query, *, default_namespace_path=None, parameters=None
    ):
        return AsyncQuery(FakeNativeQuery())


def remote_connection(sql_host_override=None):
    return lancedb.connect(
        "db://analytics",
        api_key="test-key",
        host_override="http://localhost:10024",
        sql_host_override=sql_host_override,
    )


def test_sql_is_connection_scoped():
    assert hasattr(lancedb, "sql")
    assert not callable(lancedb.sql)
    assert not hasattr(_lancedb, "sql")
    assert not hasattr(remote_connection(), "sql")
    assert hasattr(remote_connection(), "execute_query")
    assert hasattr(remote_connection(), "execute_query_async")
    assert hasattr(remote_connection(), "describe_query")


def test_query_id_is_uuid():
    query = AsyncQuery(FakeNativeQuery())
    assert isinstance(query.id, UUID)
    assert Query(query).id == query.id


def test_connection_serializes_sql_host_override():
    endpoint = "grpc+tls://sql.example.com:10026"
    restored = lancedb.deserialize_conn(
        remote_connection(sql_host_override=endpoint).serialize()
    )
    assert restored.sql_host_override == endpoint


@pytest.mark.asyncio
async def test_async_sql_reader_is_record_batch_stream():
    reader = await AsyncQuery(FakeNativeQuery()).reader()
    assert isinstance(reader, AsyncRecordBatchReader)
    assert (await reader.read_all())[0].column(0).to_pylist() == [1, 2]


def test_sync_sql_reader_is_record_batch_reader():
    reader = Query(AsyncQuery(FakeNativeQuery())).reader()
    assert isinstance(reader, pa.RecordBatchReader)
    assert reader.read_all().column(0).to_pylist() == [1, 2]


def test_execute_query_returns_blocking_reader():
    connection = RemoteDBConnection.__new__(RemoteDBConnection)
    connection._conn = FakeAsyncConnection()
    reader = connection.execute_query("SELECT 1")
    assert isinstance(reader, pa.RecordBatchReader)
    assert reader.read_all().column(0).to_pylist() == [1, 2]


@pytest.mark.asyncio
async def test_async_execute_query_returns_async_reader():
    connection = AsyncConnection(FakeNativeConnection())
    reader = await connection.execute_query("SELECT 1")
    assert isinstance(reader, AsyncRecordBatchReader)
    assert (await reader.read_all())[0].column(0).to_pylist() == [1, 2]


def test_local_connection_rejects_sql(tmp_path):
    connection = lancedb.connect(tmp_path)
    with pytest.raises(NotImplementedError, match="SQL"):
        connection.execute_query("SELECT 1")
    with pytest.raises(NotImplementedError, match="SQL"):
        connection.execute_query_async("SELECT 1")
    with pytest.raises(NotImplementedError, match="SQL"):
        connection.describe_query(NIL_QUERY_ID)


@pytest.mark.asyncio
async def test_local_async_connection_rejects_sql(tmp_path):
    connection = await lancedb.connect_async(tmp_path)
    with pytest.raises(NotImplementedError, match="SQL"):
        await connection.execute_query("SELECT 1")
    with pytest.raises(NotImplementedError, match="SQL"):
        await connection.execute_query_async("SELECT 1")
    with pytest.raises(NotImplementedError, match="SQL"):
        await connection.describe_query(NIL_QUERY_ID)


@pytest.mark.asyncio
async def test_async_namespace_connection_rejects_sql(tmp_path):
    connection = lancedb.connect_namespace_async("dir", {"root": str(tmp_path)})
    with pytest.raises(NotImplementedError, match="SQL"):
        await connection.execute_query("SELECT 1")
    with pytest.raises(NotImplementedError, match="SQL"):
        await connection.execute_query_async("SELECT 1")
    with pytest.raises(NotImplementedError, match="SQL"):
        await connection.describe_query(NIL_QUERY_ID)


@pytest.mark.asyncio
@pytest.mark.parametrize("identity", [NIL_QUERY_ID, str(NIL_QUERY_ID)])
async def test_describe_query_accepts_uuid_and_job_id_string(identity):
    calls = []

    class NativeConnection:
        async def describe_query(self, query_id):
            calls.append(query_id)

    connection = AsyncConnection(NativeConnection())
    await connection.describe_query(identity)
    assert calls == [NIL_QUERY_ID]


def test_describe_query_rejects_registry_job_id():
    with pytest.raises(ValueError, match="UUID"):
        remote_connection().describe_query("registry-job-id")


@pytest.mark.parametrize(
    "default_namespace_path",
    ["public", ("public",), [1]],
)
def test_execute_query_async_requires_namespace_path_list(default_namespace_path):
    with pytest.raises(ValueError, match="default_namespace_path"):
        remote_connection().execute_query_async(
            "SELECT 1", default_namespace_path=default_namespace_path
        )


def test_execute_query_async_rejects_invalid_endpoint():
    connection = remote_connection(sql_host_override="invalid://localhost")
    with pytest.raises(ValueError, match="sql_host_override"):
        connection.execute_query_async("SELECT 1")


@pytest.mark.parametrize(
    "default_namespace_path",
    [[""], ["café"], ["pub\tlic"], ["events$raw"]],
)
def test_execute_query_async_rejects_invalid_namespace_components(
    default_namespace_path,
):
    with pytest.raises(ValueError, match="default_namespace_path"):
        remote_connection().execute_query_async(
            "SELECT 1", default_namespace_path=default_namespace_path
        )


def test_positional_parameters_keep_their_types():
    vector = np.array([0.1, 0.2, 0.3], dtype=np.float32)
    batch = to_parameter_batch(
        [42, 1.5, "a", np.float32(0.25), vector, pa.scalar(7, pa.int16())]
    )
    assert batch.num_rows == 1
    assert batch.schema.names == ["1", "2", "3", "4", "5", "6"]
    assert batch.schema.types == [
        pa.int64(),
        pa.float64(),
        pa.string(),
        pa.float32(),
        pa.list_(pa.float32(), 3),
        pa.int16(),
    ]
    sent = batch.column(4).flatten().to_numpy()
    assert sent.dtype == np.float32
    assert sent.tobytes() == vector.tobytes()


def test_named_parameters_and_batches():
    half = pa.array(np.array([1.0, 2.0], dtype=np.float16))
    batch = to_parameter_batch({"vector": half, "k": 3})
    assert batch.schema.names == ["vector", "k"]
    assert batch.schema.field("vector").type == pa.list_(pa.float16(), 2)

    row = pa.record_batch({"id": [1]})
    assert to_parameter_batch(row) is row
    assert to_parameter_batch(pa.table({"id": [1]})).equals(row)
    assert to_parameter_batch(None) is None


@pytest.mark.parametrize(
    "parameters",
    [pa.record_batch({"id": [1, 2]}), pa.table({"id": pa.array([], pa.int64())})],
)
def test_parameters_are_exactly_one_row(parameters):
    with pytest.raises(ValueError, match="exactly one row"):
        to_parameter_batch(parameters)


def test_parameters_must_be_a_collection():
    with pytest.raises(TypeError, match="query parameters"):
        to_parameter_batch("SELECT 1")


@pytest.mark.asyncio
async def test_async_execute_query_sends_parameters_as_a_batch():
    native = FakeNativeConnection()
    connection = AsyncConnection(native)
    await connection.execute_query("SELECT $1", parameters=[1])
    await connection.execute_query_async("SELECT 1")
    sent, unparameterized = native.parameters
    assert sent.equals(pa.record_batch({"1": [1]}))
    assert unparameterized is None


@pytest.mark.asyncio
@pytest.mark.parametrize("raw", [1001, 86399999999999, 1000, None])
@pytest.mark.parametrize("named", [False, True])
async def test_async_query_preserves_arrow_scalar_values(raw, named):
    value = pa.scalar(raw, pa.time64("ns"))
    native = FakeNativeConnection()
    connection = AsyncConnection(native)
    await connection.execute_query_async(
        "SELECT $time" if named else "SELECT $1",
        parameters={"time": value} if named else [value],
    )
    (sent,) = native.parameters
    assert sent.schema.types == [value.type]
    assert sent.column(0).cast(pa.int64()).to_pylist() == [raw]


@pytest.mark.asyncio
@pytest.mark.parametrize("named", [False, True])
@pytest.mark.parametrize(
    "value",
    [
        pa.scalar(None, pa.list_(pa.float32(), 2)),
        pa.scalar(None, pa.list_(pa.int64())),
        pa.scalar(None, pa.map_(pa.string(), pa.int64())),
        pa.scalar([[1, None], None], pa.list_(pa.list_(pa.int64()))),
    ],
    ids=["null-vector", "null-list", "null-map", "nested-null-list"],
)
async def test_async_query_preserves_arrow_scalars_with_nulls(value, named):
    native = FakeNativeConnection()
    connection = AsyncConnection(native)
    await connection.execute_query_async(
        "SELECT $value" if named else "SELECT $1",
        parameters={"value": value} if named else [value],
    )
    (sent,) = native.parameters
    assert sent.num_rows == 1
    assert sent.schema.names == ["value" if named else "1"]
    assert sent.column(0).type == value.type
    assert sent.column(0)[0].equals(value)


@pytest.mark.parametrize("named", [False, True])
def test_parameters_reach_the_server_as_the_values_that_were_sent(named):
    """A round trip through the bindings against a Flight server that answers
    each exchange with the parameter row it received."""
    flight = pytest.importorskip("pyarrow.flight")

    class EchoServer(flight.FlightServerBase):
        def __init__(self):
            super().__init__("grpc://127.0.0.1:0")
            self.commands = []
            self.lock = threading.Lock()

        def do_exchange(self, context, descriptor, reader, writer):
            parameters = reader.read_all()
            with self.lock:
                self.commands.append(descriptor.command)
            writer.begin(parameters.schema)
            writer.write_table(parameters)
            writer.close()

    vector = np.array([0.1, 1e-45, -0.0], dtype=np.float32)
    parameters = {
        "vector": vector,
        "k": 10,
        "time": pa.scalar(1001, pa.time64("ns")),
        "null_vector": pa.scalar(None, pa.list_(pa.float32(), 2)),
        "null_list": pa.scalar(None, pa.list_(pa.int64())),
        "null_map": pa.scalar(None, pa.map_(pa.string(), pa.int64())),
        "nested_list": pa.scalar([[1, None], None], pa.list_(pa.list_(pa.int64()))),
    }
    with EchoServer() as server:
        db = lancedb.connect(
            "db://analytics",
            api_key="test-key",
            host_override="http://localhost:10024",
            sql_host_override=f"grpc://127.0.0.1:{server.port}",
        )
        echoed = db.execute_query(
            "SELECT echo",
            parameters=parameters if named else list(parameters.values()),
        ).read_all()
        (command,) = server.commands

    assert b"CommandStatementQuery" in command
    assert b"SELECT echo" in command
    assert echoed.schema.names == (
        list(parameters) if named else [str(i + 1) for i in range(len(parameters))]
    )
    echoed = echoed.rename_columns(list(parameters))
    assert echoed.schema.field("vector").type == pa.list_(pa.float32(), 3)
    received = echoed.column("vector").combine_chunks().flatten().to_numpy()
    assert received.view(np.uint32).tolist() == vector.view(np.uint32).tolist()
    assert echoed.column("k").to_pylist() == [10]
    assert echoed.schema.field("time").type == pa.time64("ns")
    assert echoed.column("time").cast(pa.int64()).to_pylist() == [1001]
    for name in ["null_vector", "null_list", "null_map", "nested_list"]:
        value = parameters[name]
        assert echoed.schema.field(name).type == value.type
        assert echoed.column(name)[0].equals(value)


@pytest.mark.parametrize("job_id", [None, "sql-job-1"])
def test_sql_input_error_metadata_crosses_native_boundary(job_id):
    flight = pytest.importorskip("pyarrow.flight")
    from lancedb.exceptions import JobFailedError, ColumnNotFoundError
    from lancedb.remote.errors import SqlColumnNotFoundError

    request_ids = []

    class Metadata(flight.ServerMiddleware):
        def sending_headers(self):
            headers = {
                "x-lancedb-error-code": "column_not_found",
                "x-job-request-id": "worker-request",
            }
            if job_id is not None:
                headers["x-lancedb-sql-job-id"] = job_id
            return headers

    class Factory(flight.ServerMiddlewareFactory):
        def start_call(self, info, headers):
            request_ids.append(headers["x-request-id"][0])
            return Metadata()

    class RejectingServer(flight.FlightServerBase):
        def __init__(self):
            super().__init__("grpc://127.0.0.1:0", middleware={"metadata": Factory()})

        def do_get(self, context, ticket):
            context.add_trailer("x-lancedb-error-code", "column_not_found")
            context.add_trailer("x-job-request-id", "worker-request")
            if job_id is not None:
                context.add_trailer("x-lancedb-sql-job-id", job_id)
            raise pa.ArrowInvalid("No column 'missing' in table events")

    with RejectingServer() as server:
        db = lancedb.connect(
            "db://analytics",
            api_key="test-key",
            host_override="http://localhost:10024",
            sql_host_override=f"grpc://127.0.0.1:{server.port}",
        )
        expected = JobFailedError if job_id is not None else SqlColumnNotFoundError
        with pytest.raises(expected) as exc_info:
            db.execute_query("SELECT missing FROM events").read_all()
    error = exc_info.value
    if job_id is not None:
        assert error.job_id == job_id
        assert error.request_id == request_ids[-1]
        assert error.job_request_id == "worker-request"
        error = error.__cause__
    assert isinstance(error, SqlColumnNotFoundError)
    assert isinstance(error, ColumnNotFoundError)
    assert error.request_id == request_ids[-1]
    assert error.job_id == job_id
    assert error.job_request_id == "worker-request"
    assert error.grpc_code == 3
    assert str(error).startswith("No column 'missing' in table events")
    assert not str(error).startswith("SQL error")
