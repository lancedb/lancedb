# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import contextlib
import functools
import http.server
import json
import multiprocessing as mp
import pickle
import re
import sys
import threading

import lancedb
import pyarrow as pa
import pytest
from lancedb.permutation import Permutation, Permutations, permutation_builder
from lancedb.util import tbl_to_tensor

torch = pytest.importorskip("torch")


REMOTE_ROWS = list(range(100))


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
        if isinstance(requested_columns, dict):
            requested_columns = list(requested_columns)

        data = {}
        for column in requested_columns:
            if column == "a":
                data[column] = [REMOTE_ROWS[offset] for offset in offsets]
            elif column == "_rowoffset":
                data[column] = offsets
            elif column == "_rowid":
                data[column] = offsets

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
        ("localhost", 0), _make_mock_http_handler(_remote_dataset_handler)
    ) as server:
        port = server.server_address[1]
        handle = threading.Thread(target=server.serve_forever)
        handle.start()
        try:
            db = lancedb.connect(
                "db://dev",
                api_key="fake",
                host_override=f"http://localhost:{port}",
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

    # "torch" produces dict-of-batched-tensors via iter() and
    # list-of-per-row-dicts via __getitems__ so the default DataLoader
    # collate stacks per-row dicts back into a batched dict.
    torch_perm = permutation.with_format("torch")
    batch = next(torch_perm.iter(10, skip_last_batch=False))
    assert isinstance(batch, dict)
    assert "a" in batch
    assert batch["a"].shape == (10,)
    rows = torch_perm.__getitems__([0, 1, 2])
    assert isinstance(rows, list)
    assert len(rows) == 3
    assert isinstance(rows[0], dict)
    assert isinstance(rows[0]["a"], torch.Tensor)
    dataloader = torch.utils.data.DataLoader(torch_perm, batch_size=10, shuffle=True)
    for batch in dataloader:
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
        host_override=f"http://localhost:{port}",
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
        ("localhost", 0), _make_mock_http_handler(_remote_dataset_handler)
    ) as server:
        port = server.server_address[1]
        handle = threading.Thread(target=server.serve_forever)
        handle.start()
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
