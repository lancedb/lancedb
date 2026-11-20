# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import contextlib
import functools
import http.server
import json
import multiprocessing as mp
import pickle
import re
import struct
import sys

import lancedb
import numpy as np
import pyarrow as pa
import pytest
from lancedb.permutation import Permutation, Permutations, permutation_builder
from lancedb.util import batch_to_tensor, batch_to_tensor_columns, tbl_to_tensor
from utils import start_server_thread

torch = pytest.importorskip("torch")


REMOTE_ROWS = list(range(100))


def _identity_collate(batch):
    return batch


class _LanceBFloat16Type(pa.ExtensionType):
    def __init__(self):
        pa.ExtensionType.__init__(self, pa.binary(2), "lance.bfloat16")

    def __arrow_ext_serialize__(self):
        return b""

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized):
        return cls()


def _bf16_storage(values):
    bits = torch.tensor(values, dtype=torch.bfloat16).view(torch.int16).tolist()
    return [struct.pack("<H", bit & 0xFFFF) for bit in bits]


def _bf16_array(values):
    storage = pa.array(_bf16_storage(values), type=pa.binary(2))
    return pa.ExtensionArray.from_storage(_LanceBFloat16Type(), storage)


def _fixed_shape_tensor_array(physical_values, shape, permutation=None):
    tensor_type = pa.fixed_shape_tensor(
        pa.float32(), shape, permutation=permutation or []
    )
    flat_values = np.asarray(physical_values, dtype=np.float32).reshape(-1)
    storage = pa.FixedSizeListArray.from_arrays(
        pa.array(flat_values), int(np.prod(shape))
    )
    return pa.ExtensionArray.from_storage(tensor_type, storage)


def _make_mock_http_handler(handler):
    class MockLanceDBHandler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            handler(self)

        def do_POST(self):
            handler(self)

    return MockLanceDBHandler


def _remote_schema_payload():
    return {
        "version": 1,
        "schema": {
            "fields": [
                {"name": "a", "type": {"type": "int64"}, "nullable": False},
            ]
        },
    }


def _offsets_from_filter(filter_sql: str | None) -> list[int]:
    if filter_sql is None:
        return REMOTE_ROWS
    match = re.search(r"_rowoffset in \((.*?)\)", filter_sql)
    if match is None:
        return REMOTE_ROWS
    raw_offsets = match.group(1).strip()
    if raw_offsets == "":
        return []
    return [int(offset.strip()) for offset in raw_offsets.split(",")]


def _remote_dataset_handler(request):
    request.close_connection = True
    if request.path == "/v1/table/test/describe/":
        request.send_response(200)
        request.send_header("Content-Type", "application/json")
        request.end_headers()
        request.wfile.write(json.dumps(_remote_schema_payload()).encode())
    elif request.path == "/v1/table/test/count_rows/":
        request.send_response(200)
        request.send_header("Content-Type", "application/json")
        request.end_headers()
        request.wfile.write(str(len(REMOTE_ROWS)).encode())
    elif request.path == "/v1/table/test/query/":
        content_len = int(request.headers.get("Content-Length"))
        body = json.loads(request.rfile.read(content_len))
        offsets = _offsets_from_filter(body.get("filter"))
        requested_columns = body.get("columns") or ["a"]
        # A dynamic projection is sent as {output name: source expression}; take
        # queries use it to alias `_rowoffset` as `__lancedb_take_row_offset`.
        if isinstance(requested_columns, dict):
            projection = requested_columns
        else:
            projection = {column: column for column in requested_columns}

        sources = {
            "a": [REMOTE_ROWS[offset] for offset in offsets],
            "_rowoffset": offsets,
            "_rowid": offsets,
        }
        data = {
            name: sources[source]
            for name, source in projection.items()
            if source in sources
        }

        table = pa.table(data)
        request.send_response(200)
        request.send_header("Content-Type", "application/vnd.apache.arrow.file")
        request.end_headers()
        with pa.ipc.new_file(request.wfile, schema=table.schema) as writer:
            writer.write_table(table)
    else:
        request.send_response(404)
        request.end_headers()


@contextlib.contextmanager
def _remote_dataset_table():
    with http.server.ThreadingHTTPServer(
        ("127.0.0.1", 0), _make_mock_http_handler(_remote_dataset_handler)
    ) as server:
        port = server.server_address[1]
        handle = start_server_thread(server)
        try:
            db = lancedb.connect(
                "db://dev",
                api_key="fake",
                host_override=f"http://127.0.0.1:{port}",
                client_config={
                    "retry_config": {"retries": 0},
                    "timeout_config": {"connect_timeout": 2, "read_timeout": 2},
                },
            )
            yield db.open_table("test")
        finally:
            server.shutdown()
            handle.join()


def _open_native_table(uri: str, table_name: str):
    """Top-level connection factory used by the explicit-factory pickle test.

    Defined at module scope so that pickle can resolve it by name in the
    worker / unpickling process.
    """
    return lancedb.connect(uri).open_table(table_name)


class _DatasetHoldingConnectionAndTable(torch.utils.data.Dataset):
    """User-style Dataset that keeps both the connection and table."""

    def __init__(self, db, table):
        self._db = db
        self._table = table

    def __len__(self):
        return self._table.count_rows()

    def __getitem__(self, index):
        row = self._table.take_offsets([index]).to_arrow().to_pylist()[0]
        # Touch the connection too, so workers prove both objects were restored.
        row["db_uri"] = self._db.uri
        return row


def test_table_dataloader(mem_db):
    table = mem_db.create_table("test_table", pa.table({"a": range(1000)}))
    dataloader = torch.utils.data.DataLoader(
        table, collate_fn=tbl_to_tensor, batch_size=10, shuffle=True
    )
    for batch in dataloader:
        assert batch.size(0) == 1
        assert batch.size(1) == 10


def test_permutation_dataloader(mem_db):
    table = mem_db.create_table("test_table", pa.table({"a": range(1000)}))

    permutation = Permutation.identity(table)
    dataloader = torch.utils.data.DataLoader(permutation, batch_size=10, shuffle=True)
    for batch in dataloader:
        assert batch["a"].size(0) == 10

    # "torch" produces a list of per-row dicts per batch. The default
    # DataLoader collate stacks the per-row dicts back into a batched dict.
    torch_perm = permutation.with_format("torch")
    batch = next(torch_perm.iter(10, skip_last_batch=False))
    assert isinstance(batch, list)
    assert len(batch) == 10
    assert isinstance(batch[0], dict)
    assert isinstance(batch[0]["a"], torch.Tensor)
    rows = torch_perm.__getitems__([0, 1, 2])
    assert isinstance(rows, list)
    assert len(rows) == 3
    assert isinstance(rows[0], dict)
    assert isinstance(rows[0]["a"], torch.Tensor)
    dataloader = torch.utils.data.DataLoader(torch_perm, batch_size=10, shuffle=True)
    for batch in dataloader:
        assert isinstance(batch, dict)
        assert batch["a"].shape == (10,)
    # Spawn-based workers exercise the pickle round-trip path: the new
    # transform-as-list shape must survive pickling so workers produce the
    # same per-row dicts the parent does.
    spawn_loader = torch.utils.data.DataLoader(
        torch_perm,
        batch_size=10,
        num_workers=2,
        multiprocessing_context="spawn",
    )
    for batch in spawn_loader:
        assert isinstance(batch, dict)
        assert batch["a"].shape == (10,)

    # "torch_row" returns a list of row tensors. Works with the default
    # DataLoader collate (stacks rows into 2D).
    row_perm = permutation.with_format("torch_row")
    dataloader = torch.utils.data.DataLoader(row_perm, batch_size=10, shuffle=True)
    for batch in dataloader:
        assert batch.size(0) == 10
        assert batch.size(1) == 1

    col_perm = permutation.with_format("torch_col")
    dataloader = torch.utils.data.DataLoader(
        col_perm, collate_fn=lambda x: x, batch_size=10, shuffle=True
    )
    for batch in dataloader:
        assert batch.size(0) == 1
        assert batch.size(1) == 10


def test_permutation_is_picklable(tmp_db):
    """A Permutation must be picklable so it can be used with PyTorch's
    DataLoader when num_workers > 0 (which uses multiprocessing and pickles
    the dataset to pass it to worker processes)."""
    table = tmp_db.create_table("test_table", pa.table({"a": range(1000)}))
    permutation = Permutation.identity(table)

    pickled = pickle.dumps(permutation)
    restored = pickle.loads(pickled)

    assert len(restored) == 1000
    rows = restored.__getitems__([0, 1, 2])
    assert rows == [{"a": 0}, {"a": 1}, {"a": 2}]


def test_permutation_with_memory_base_is_picklable(mem_db):
    """An in-memory base table is inlined into the pickle as Arrow IPC bytes
    and rebuilt on the other side as an in-memory LanceTable, so the
    Permutation round-trips even though the original database can't be
    reopened across processes."""
    table = mem_db.create_table("test_table", pa.table({"a": range(50)}))
    permutation = Permutation.identity(table)

    restored = pickle.loads(pickle.dumps(permutation))

    assert len(restored) == 50
    assert restored.__getitems__([0, 10, 49]) == [{"a": 0}, {"a": 10}, {"a": 49}]


@pytest.mark.parametrize("start_method", ["spawn", "forkserver"])
def test_dataset_with_connection_and_table_dataloader(tmp_path, start_method):
    if start_method not in mp.get_all_start_methods():
        pytest.skip(f"{start_method} start method is not available")

    db = lancedb.connect(tmp_path)
    table = db.create_table("test_table", pa.table({"a": range(20)}))
    dataset = _DatasetHoldingConnectionAndTable(db, table)

    dataloader = torch.utils.data.DataLoader(
        dataset,
        batch_size=4,
        num_workers=2,
        multiprocessing_context=start_method,
    )

    seen = []
    expected_uri = str(tmp_path)
    for batch in dataloader:
        seen.extend(batch["a"].tolist())
        assert all(uri == expected_uri for uri in batch["db_uri"])
    assert sorted(seen) == list(range(20))


def test_permutation_dataloader_multiprocessing(tmp_db):
    """Using a Permutation with a PyTorch DataLoader that has num_workers > 0
    must work end-to-end. Each worker process gets a pickled copy of the
    dataset and reads batches from it."""
    table = tmp_db.create_table("test_table", pa.table({"a": range(1000)}))
    permutation = Permutation.identity(table)

    dataloader = torch.utils.data.DataLoader(
        permutation,
        batch_size=10,
        shuffle=True,
        num_workers=2,
        multiprocessing_context="spawn",
    )
    seen = 0
    for batch in dataloader:
        assert batch["a"].size(0) == 10
        seen += batch["a"].size(0)
    assert seen == 1000


def test_remote_table_dataloader_multiprocessing():
    with _remote_dataset_table() as table:
        dataloader = torch.utils.data.DataLoader(
            table,
            collate_fn=tbl_to_tensor,
            batch_size=10,
            num_workers=2,
            multiprocessing_context="spawn",
        )
        seen = 0
        for batch in dataloader:
            assert batch.size(0) == 1
            assert batch.size(1) == 10
            seen += batch.size(1)
        assert seen == len(REMOTE_ROWS)


def test_remote_permutation_dataloader_multiprocessing():
    with _remote_dataset_table() as table:
        permutation = Permutation.identity(table)
        dataloader = torch.utils.data.DataLoader(
            permutation,
            batch_size=10,
            num_workers=2,
            multiprocessing_context="spawn",
        )
        seen = 0
        for batch in dataloader:
            assert batch["a"].size(0) == 10
            seen += batch["a"].size(0)
        assert seen == len(REMOTE_ROWS)


def test_permutation_pickle_with_connection_factory(tmp_path):
    """When the user provides a connection_factory, pickling should round-trip
    through that factory rather than introspecting the connection URI. Useful
    for remote / cloud connections where the URI alone isn't reopenable."""
    db = lancedb.connect(tmp_path)
    db.create_table("test_table", pa.table({"a": range(50)}))

    factory = functools.partial(_open_native_table, str(tmp_path))
    permutation = Permutation.identity(factory("test_table")).with_connection_factory(
        factory
    )

    restored = pickle.loads(pickle.dumps(permutation))

    assert len(restored) == 50
    # The factory survives pickling and is what powered base-table reopen.
    assert restored.connection_factory is not None
    assert restored.connection_factory.func is _open_native_table
    assert restored.__getitems__([0, 1, 2]) == [{"a": 0}, {"a": 1}, {"a": 2}]


def test_permutation_with_builder_is_picklable(tmp_db):
    """A Permutation built from a non-identity permutation table must round-trip
    through pickle while preserving the row order defined by the permutation."""
    table = tmp_db.create_table("test_table", pa.table({"a": range(100)}))
    perm_tbl = (
        permutation_builder(table)
        .split_random(ratios=[0.8, 0.2], seed=42, split_names=["train", "test"])
        .shuffle(seed=42)
        .execute()
    )
    permutations = Permutations(table, perm_tbl)
    permutation = permutations["train"]

    indices = list(range(len(permutation)))
    expected = permutation.__getitems__(indices)

    restored = pickle.loads(pickle.dumps(permutation))

    assert len(restored) == len(permutation)
    assert restored.__getitems__(indices) == expected


def _multiworker_dataloader_target(db_uri: str, result_queue):
    import lancedb
    from lancedb.permutation import Permutation

    db = lancedb.connect(db_uri)
    table = db.open_table("test_table")
    permutation = Permutation.identity(table)

    dataloader = torch.utils.data.DataLoader(
        permutation,
        batch_size=10,
        num_workers=2,
        multiprocessing_context="fork",
    )
    count = 0
    for batch in dataloader:
        assert batch["a"].size(0) == 10
        count += 1
    result_queue.put(count)


def _remote_multiworker_dataloader_target(port: int, result_queue):
    import lancedb
    from lancedb.permutation import Permutation

    db = lancedb.connect(
        "db://dev",
        api_key="fake",
        host_override=f"http://127.0.0.1:{port}",
        client_config={
            "retry_config": {"retries": 0},
            "timeout_config": {"connect_timeout": 2, "read_timeout": 2},
        },
    )
    table = db.open_table("test")
    permutation = Permutation.identity(table)

    dataloader = torch.utils.data.DataLoader(
        permutation,
        batch_size=10,
        num_workers=2,
        multiprocessing_context="fork",
    )
    count = 0
    for batch in dataloader:
        assert batch["a"].size(0) == 10
        count += 1
    result_queue.put(count)


@pytest.mark.skipif(
    sys.platform != "linux",
    reason=(
        "fork() is unavailable on Windows and unsafe on macOS "
        "(Apple frameworks/TLS are not fork-safe)"
    ),
)
def test_permutation_dataloader_fork_workers(tmp_path):
    """A Permutation used by a fork-based DataLoader should not hang.

    PyTorch's DataLoader uses fork-based multiprocessing by default on Linux.
    LanceDB drives async work through a background asyncio thread that does
    not survive a fork, so any LOOP.run() in a worker blocks forever.
    """
    import lancedb

    db_uri = str(tmp_path / "db")
    db = lancedb.connect(db_uri)
    db.create_table("test_table", pa.table({"a": list(range(1000))}))

    ctx = mp.get_context("spawn")
    queue = ctx.Queue()
    proc = ctx.Process(target=_multiworker_dataloader_target, args=(db_uri, queue))
    proc.start()
    proc.join(timeout=30)

    if proc.is_alive():
        proc.terminate()
        proc.join(timeout=5)
        if proc.is_alive():
            proc.kill()
            proc.join()
        pytest.fail("Permutation hung when iterated in a fork-based DataLoader worker")

    assert proc.exitcode == 0, f"child exited with code {proc.exitcode}"
    assert not queue.empty(), "child produced no batches"
    assert queue.get() == 100


@pytest.mark.skipif(
    sys.platform != "linux",
    reason=(
        "fork() is unavailable on Windows and unsafe on macOS "
        "(Apple frameworks/TLS are not fork-safe)"
    ),
)
def test_remote_permutation_dataloader_fork_workers():
    with http.server.ThreadingHTTPServer(
        ("127.0.0.1", 0), _make_mock_http_handler(_remote_dataset_handler)
    ) as server:
        port = server.server_address[1]
        handle = start_server_thread(server)
        try:
            ctx = mp.get_context("spawn")
            queue = ctx.Queue()
            proc = ctx.Process(
                target=_remote_multiworker_dataloader_target,
                args=(port, queue),
            )
            proc.start()
            proc.join(timeout=30)

            if proc.is_alive():
                proc.terminate()
                proc.join(timeout=5)
                if proc.is_alive():
                    proc.kill()
                    proc.join()
                pytest.fail(
                    "Remote permutation hung when iterated in a fork-based "
                    "DataLoader worker"
                )

            assert proc.exitcode == 0, f"child exited with code {proc.exitcode}"
            assert not queue.empty(), "child produced no batches"
            assert queue.get() == 10
        finally:
            server.shutdown()
            handle.join()


def test_permutation_torch_dict_mixed_columns(mem_db):
    vectors = [[float(i), float(i) + 0.5, float(i) + 1.0] for i in range(5)]
    grids = [
        [[float(row), float(row) + 1.0], [float(row) + 2.0, float(row) + 3.0]]
        for row in range(5)
    ]
    table = mem_db.create_table(
        "mixed",
        pa.table(
            {
                "id": pa.array(range(5), type=pa.int32()),
                "score": pa.array([i / 2 for i in range(5)], type=pa.float32()),
                "vector": pa.array(vectors, type=pa.list_(pa.float32(), 3)),
                "grid": pa.array(grids, type=pa.list_(pa.list_(pa.float32(), 2), 2)),
            }
        ),
    )
    permutation = Permutation.identity(table).with_format("torch_dict")

    batch = next(permutation.iter(5, skip_last_batch=False))
    assert set(batch) == {"id", "score", "vector", "grid"}
    assert batch["id"].shape == (5,)
    assert batch["id"].dtype == torch.int32
    assert batch["score"].dtype == torch.float32
    assert batch["vector"].shape == (5, 3)
    assert batch["grid"].shape == (5, 2, 2)
    assert torch.equal(batch["vector"], torch.tensor(vectors))
    assert torch.equal(batch["grid"], torch.tensor(grids))

    items = permutation.__getitems__([0, 2, 4])
    assert items["id"].shape == (3,)
    assert items["score"].shape == (3,)
    assert items["vector"].shape == (3, 3)
    assert items["grid"].shape == (3, 2, 2)

    dataloader = torch.utils.data.DataLoader(
        permutation,
        batch_size=2,
        shuffle=False,
        collate_fn=_identity_collate,
    )
    batches = list(dataloader)
    assert [batch["id"].shape[0] for batch in batches] == [2, 2, 1]
    assert batches[0]["vector"].shape == (2, 3)
    assert batches[-1]["grid"].shape == (1, 2, 2)

    empty = permutation.take_offsets([])
    assert empty["id"].shape == (0,)
    assert empty["vector"].shape == (0, 3)
    assert empty["grid"].shape == (0, 2, 2)

    torch_permutation = Permutation.identity(table).with_format("torch")
    per_row = next(torch_permutation.iter(2, skip_last_batch=False))
    assert isinstance(per_row, list)
    assert per_row[0]["vector"].shape == (3,)
    assert per_row[0]["grid"].shape == (2, 2)
    torch_loader = torch.utils.data.DataLoader(
        torch_permutation, batch_size=2, shuffle=False
    )
    collated = next(iter(torch_loader))
    assert collated["vector"].shape == (2, 3)
    assert collated["grid"].shape == (2, 2, 2)


def test_permutation_torch_dict_dataloader_spawn(tmp_db):
    vectors = [[float(i), float(i) + 0.5] for i in range(11)]
    table = tmp_db.create_table(
        "spawn",
        pa.table(
            {
                "id": range(11),
                "vector": pa.array(vectors, type=pa.list_(pa.float32(), 2)),
            }
        ),
    )
    permutation = Permutation.identity(table).with_format("torch_dict")

    dataloader = torch.utils.data.DataLoader(
        permutation,
        batch_size=4,
        shuffle=False,
        num_workers=2,
        multiprocessing_context="spawn",
        collate_fn=_identity_collate,
    )
    seen = []
    for batch in dataloader:
        assert isinstance(batch, dict)
        assert batch["id"].shape == (batch["id"].shape[0],)
        assert batch["vector"].shape == (batch["id"].shape[0], 2)
        seen.extend(batch["id"].tolist())
    assert sorted(seen) == list(range(11))


def test_batch_to_tensor_columns_zero_copy_and_slice_offsets():
    vectors = [[float(i), float(i) + 0.5, float(i) + 1.0] for i in range(5)]
    nested_values = np.arange(5 * 3 * 2, dtype=np.float32).reshape(5, 3, 2)
    batch = pa.record_batch(
        [
            pa.array(range(5), type=pa.int64()),
            pa.array(vectors, type=pa.list_(pa.float32(), 3)),
            pa.array(
                nested_values.tolist(),
                type=pa.list_(pa.list_(pa.float32(), 2), 3),
            ),
            pa.array([True, False, True, False, True]),
        ],
        names=["id", "vector", "nested", "active"],
    ).slice(2, 2)

    tensors = batch_to_tensor_columns(batch)
    assert tensors["id"].tolist() == [2, 3]
    assert torch.equal(tensors["vector"], torch.tensor(vectors[2:4]))
    assert torch.equal(tensors["nested"], torch.from_numpy(nested_values[2:4].copy()))
    assert tensors["active"].dtype == torch.bool
    assert tensors["active"].tolist() == [True, False]

    id_array = batch.column("id")
    assert tensors["id"].data_ptr() == (
        id_array.buffers()[1].address + id_array.offset * 8
    )
    vector_array = batch.column("vector")
    vector_values = vector_array.values
    expected_vector_offset = (vector_array.offset * 3 + vector_values.offset) * 4
    assert tensors["vector"].data_ptr() == (
        vector_values.buffers()[1].address + expected_vector_offset
    )

    single_column = pa.record_batch([batch.column("vector")], names=["vector"])
    column_tensor = batch_to_tensor(single_column)
    assert column_tensor.shape == (1, 2, 3)
    assert column_tensor.data_ptr() == tensors["vector"].data_ptr()


def test_permutation_torch_col_and_row_vector_layouts(mem_db):
    left = [[float(i), float(i) + 0.5] for i in range(4)]
    right = [[float(i) + 1.0, float(i) + 1.5] for i in range(4)]
    table = mem_db.create_table(
        "uniform",
        pa.table(
            {
                "left": pa.array(left, type=pa.list_(pa.float32(), 2)),
                "right": pa.array(right, type=pa.list_(pa.float32(), 2)),
            }
        ),
    )
    permutation = Permutation.identity(table)

    column_batch = next(
        permutation.with_format("torch_col").iter(4, skip_last_batch=False)
    )
    assert column_batch.shape == (2, 4, 2)
    assert column_batch.dtype == torch.float32
    assert torch.equal(column_batch[0], torch.tensor(left))

    row_batch = next(
        permutation.with_format("torch_row").iter(4, skip_last_batch=False)
    )
    assert len(row_batch) == 4
    assert row_batch[0].shape == (2, 2)
    assert torch.equal(
        row_batch[0], torch.stack([torch.tensor(left[0]), torch.tensor(right[0])])
    )

    dataloader = torch.utils.data.DataLoader(
        permutation.with_format("torch_row"), batch_size=4, shuffle=False
    )
    stacked = next(iter(dataloader))
    assert stacked.shape == (4, 2, 2)

    scalar_table = mem_db.create_table(
        "single_scalar", pa.table({"x": pa.array(range(4), type=pa.int64())})
    )
    scalar_rows = next(
        Permutation.identity(scalar_table)
        .with_format("torch_row")
        .iter(4, skip_last_batch=False)
    )
    assert scalar_rows[0].shape == (1,)
    assert scalar_rows[0].dtype == torch.int64

    vector_table = mem_db.create_table(
        "single_vector",
        pa.table({"v": pa.array(left, type=pa.list_(pa.float32(), 2))}),
    )
    vector_rows = next(
        Permutation.identity(vector_table)
        .with_format("torch_row")
        .iter(4, skip_last_batch=False)
    )
    assert vector_rows[0].shape == (1, 2)
    vector_column = next(
        Permutation.identity(vector_table)
        .with_format("torch_col")
        .iter(4, skip_last_batch=False)
    )
    assert vector_column.shape == (1, 4, 2)


def test_torch_col_and_row_reject_heterogeneous_shapes(mem_db):
    table = mem_db.create_table(
        "heterogeneous",
        pa.table(
            {
                "id": pa.array(range(4), type=pa.int64()),
                "vector": pa.array(
                    [[float(i), float(i) + 1.0, float(i) + 2.0] for i in range(4)],
                    type=pa.list_(pa.float32(), 3),
                ),
            }
        ),
    )
    permutation = Permutation.identity(table)
    for format_name in ("torch_col", "torch_row"):
        with pytest.raises(ValueError, match="torch_dict") as exc_info:
            next(permutation.with_format(format_name).iter(2, skip_last_batch=False))
        message = str(exc_info.value)
        assert "`vector`" in message
        assert "(3,)" in message
        assert "`id`" in message
        assert "()" in message

    different_dims = mem_db.create_table(
        "different_dims",
        pa.table(
            {
                "left": pa.array(
                    [[1.0, 2.0] for _ in range(4)],
                    type=pa.list_(pa.float32(), 2),
                ),
                "right": pa.array(
                    [[1.0, 2.0, 3.0] for _ in range(4)],
                    type=pa.list_(pa.float32(), 3),
                ),
            }
        ),
    )
    with pytest.raises(ValueError, match="torch_dict") as exc_info:
        next(
            Permutation.identity(different_dims)
            .with_format("torch_row")
            .iter(2, skip_last_batch=False)
        )
    message = str(exc_info.value)
    assert "`right`" in message and "(3,)" in message
    assert "`left`" in message and "(2,)" in message


def test_torch_row_uses_pytorch_numeric_promotion(mem_db):
    table = mem_db.create_table(
        "promotion",
        pa.table(
            {
                "id": pa.array(range(4), type=pa.int64()),
                "score": pa.array([i / 2 for i in range(4)], type=pa.float32()),
                "count": pa.array(range(4), type=pa.int32()),
            }
        ),
    )
    rows = next(
        Permutation.identity(table)
        .with_format("torch_row")
        .iter(4, skip_last_batch=False)
    )
    assert rows[0].shape == (3,)
    assert rows[0].dtype == torch.float32
    assert rows[0].tolist() == [0.0, 0.0, 0.0]
    assert rows[2].tolist() == [2.0, 1.0, 2.0]


def test_permutation_torch_formats_fixed_shape_tensor_permutation(mem_db):
    physical_values = np.arange(2 * 2 * 3 * 4, dtype=np.float32).reshape(2, 2, 3, 4)
    tensor_array = _fixed_shape_tensor_array(
        physical_values, shape=(2, 3, 4), permutation=[0, 2, 1]
    )
    table = mem_db.create_table(
        "tensor_permutation",
        pa.table({"id": range(2), "tensor": tensor_array}),
    )
    permutation = Permutation.identity(table)
    expected = np.transpose(physical_values, (0, 1, 3, 2))

    batch = next(permutation.with_format("torch_dict").iter(2, skip_last_batch=False))
    assert batch["tensor"].shape == (2, 2, 4, 3)
    assert torch.equal(batch["tensor"], torch.from_numpy(expected.copy()))

    tensor_only = permutation.select_columns(["tensor"])
    rows = next(tensor_only.with_format("torch_row").iter(2, skip_last_batch=False))
    assert rows[0].shape == (1, 2, 4, 3)
    assert torch.equal(rows[0][0], torch.from_numpy(expected[0].copy()))

    dataloader = torch.utils.data.DataLoader(
        tensor_only.with_format("torch"), batch_size=2, shuffle=False
    )
    default_collated = next(iter(dataloader))
    assert default_collated["tensor"].shape == (2, 2, 4, 3)


def test_permutation_torch_formats_bfloat16(mem_db):
    values = [1.0, -1.5, 2.5, 0.0]
    scalar_values = _bf16_array(values)
    vector_values = pa.FixedSizeListArray.from_arrays(
        _bf16_array([value for value in values for _ in range(2)]), 2
    )
    table = mem_db.create_table(
        "bfloat16",
        pa.table(
            {
                "id": range(4),
                "scalar": scalar_values,
                "vector": vector_values,
            }
        ),
    )
    permutation = Permutation.identity(table)

    batch = next(permutation.with_format("torch_dict").iter(2, skip_last_batch=False))
    assert batch["scalar"].dtype == torch.bfloat16
    assert batch["vector"].dtype == torch.bfloat16
    assert torch.equal(batch["scalar"], torch.tensor(values[:2], dtype=torch.bfloat16))
    assert torch.equal(
        batch["vector"],
        torch.tensor([[value, value] for value in values[:2]], dtype=torch.bfloat16),
    )

    arrow_batch = next(permutation.with_format("arrow").iter(2, skip_last_batch=False))
    arrow_tensors = batch_to_tensor_columns(arrow_batch)
    scalar_array = arrow_batch.column("scalar")
    assert arrow_tensors["scalar"].data_ptr() == (
        scalar_array.buffers()[1].address + scalar_array.offset * 2
    )

    sliced = arrow_batch.slice(1, 1)
    sliced_tensors = batch_to_tensor_columns(sliced)
    assert torch.equal(
        sliced_tensors["scalar"],
        torch.tensor([values[1]], dtype=torch.bfloat16),
    )
    assert torch.equal(
        sliced_tensors["vector"],
        torch.tensor([[values[1], values[1]]], dtype=torch.bfloat16),
    )

    skipped = next(
        permutation.with_skip(1)
        .with_format("torch_dict")
        .iter(2, skip_last_batch=False)
    )
    assert torch.equal(
        skipped["scalar"], torch.tensor(values[1:3], dtype=torch.bfloat16)
    )

    empty = permutation.with_format("torch_dict").take_offsets([])
    assert empty["scalar"].shape == (0,)
    assert empty["scalar"].dtype == torch.bfloat16
    assert empty["vector"].shape == (0, 2)
    assert empty["vector"].dtype == torch.bfloat16

    unmarked = mem_db.create_table(
        "unmarked_bfloat16",
        pa.table(
            {
                "vector": pa.FixedSizeListArray.from_arrays(
                    pa.array(_bf16_storage(values), type=pa.binary(2)), 2
                )
            }
        ),
    )
    with pytest.raises(ValueError, match="not marked.*lance.bfloat16"):
        next(
            Permutation.identity(unmarked)
            .with_format("torch_dict")
            .iter(2, skip_last_batch=False)
        )


def test_torch_formats_reject_nulls(mem_db):
    parent_nulls = mem_db.create_table(
        "parent_nulls",
        pa.table(
            {
                "id": range(3),
                "values": pa.array(
                    [[1.0, 2.0], None, [5.0, 6.0]],
                    type=pa.list_(pa.float32(), 2),
                ),
            }
        ),
    )
    with pytest.raises(ValueError, match="contains 1 null"):
        next(
            Permutation.identity(parent_nulls)
            .with_format("torch_dict")
            .iter(2, skip_last_batch=False)
        )

    leaf_nulls = mem_db.create_table(
        "leaf_nulls",
        pa.table(
            {
                "id": range(2),
                "values": pa.FixedSizeListArray.from_arrays(
                    pa.array([1.0, None, 5.0, 6.0], type=pa.float32()), 2
                ),
            }
        ),
    )
    with pytest.raises(ValueError, match="contains 1 null"):
        next(
            Permutation.identity(leaf_nulls)
            .with_format("torch_dict")
            .iter(2, skip_last_batch=False)
        )
