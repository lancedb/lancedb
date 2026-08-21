# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors
import contextlib
import http.server
import json
import re
import threading

import lancedb
import pyarrow as pa
import pytest

ARROW_FILE_CONTENT_TYPE = "application/vnd.apache.arrow.file"


def exception_output(e_info: pytest.ExceptionInfo):
    import traceback

    # skip traceback part, since it's not worth checking in tests
    lines = traceback.format_exception_only(e_info.type, e_info.value)
    return "".join(lines).strip()


def parse_in_list(filter_sql: str) -> list[int]:
    """Pull the integers out of a `<col> IN (a, b, c)` predicate.

    Scoped to the parenthesised list so a cast in the SQL adds no phantom values.
    """
    match = re.search(r"\bIN\s*\(([^)]*)\)", filter_sql, re.IGNORECASE)
    assert match is not None, f"expected an IN list, got: {filter_sql}"
    return [int(m) for m in re.findall(r"-?\d+", match.group(1))]


def is_row_id_take(body) -> bool:
    """True when a query body fetches specific rows by row id."""
    return "_rowid" in (body.get("filter") or "")


def arrow_file_bytes(table: pa.Table) -> bytes:
    """Serialize to the Arrow IPC *file* framing the /query/ route answers with."""
    sink = pa.BufferOutputStream()
    with pa.ipc.new_file(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


class MockPermutationServer:
    """A stand-in LanceDB server hosting one table whose ``id`` equals its ``_rowid``.

    Records every ``/query/`` body so tests can assert on the request shapes sent to
    the server, which is the part that has to stay compatible.
    """

    def __init__(self, name="remote_data", num_rows=8):
        self.name = name
        self.num_rows = num_rows
        self.query_bodies = []

    def __call__(self, request):
        path = request.path
        if path == f"/v1/table/{self.name}/describe/":
            return self._json(
                request,
                {
                    "version": 1,
                    "schema": {
                        "fields": [
                            {"name": "id", "type": {"type": "int64"}, "nullable": False}
                        ]
                    },
                },
            )
        if path == f"/v1/table/{self.name}/get_lsm_write_spec/":
            self._read_body(request)
            # Null spec: this table has no LSM write path.
            return self._json(request, {"lsm_write_spec": None})
        if path == f"/v1/table/{self.name}/count_rows/":
            self._read_body(request)
            return self._json(request, self.num_rows)
        if path == f"/v1/table/{self.name}/query/":
            return self._query(request, self._read_body(request))

        # Drain first, so an unexpected route cannot desync a keep-alive connection.
        self._read_body(request)
        request.send_response(404)
        request.end_headers()

    @property
    def scans(self):
        """Bodies of the permutation build scan: the row id column, nothing else."""
        return [b for b in self.query_bodies if b.get("columns") == ["_rowid"]]

    @property
    def takes(self):
        """Bodies of the row-id takes the loader fetches batches with.

        Keyed on `_rowid`, not "has a filter": the schema probe also has a predicate.
        """
        return [b for b in self.query_bodies if is_row_id_take(b)]

    @staticmethod
    def _read_body(request):
        content_len = int(request.headers.get("Content-Length") or 0)
        return json.loads(request.rfile.read(content_len)) if content_len else {}

    @staticmethod
    def _json(request, payload):
        request.send_response(200)
        request.send_header("Content-Type", "application/json")
        request.end_headers()
        request.wfile.write(json.dumps(payload).encode())

    @staticmethod
    def _arrow(request, table):
        body = arrow_file_bytes(table)
        request.send_response(200)
        request.send_header("Content-Type", ARROW_FILE_CONTENT_TYPE)
        request.send_header("Content-Length", str(len(body)))
        request.end_headers()
        request.wfile.write(body)

    def _query(self, request, body):
        self.query_bodies.append(body)

        if is_row_id_take(body):
            # A row-id take. Answer ascending, so tests prove the client reorders.
            row_ids = sorted(parse_in_list(body["filter"]))
            return self._arrow(
                request,
                pa.table(
                    {
                        "id": pa.array(row_ids, pa.int64()),
                        "_rowid": pa.array(row_ids, pa.uint64()),
                    }
                ),
            )

        if body.get("columns") == ["_rowid"]:
            # The permutation build scan: row ids and nothing else.
            return self._arrow(
                request,
                pa.table({"_rowid": pa.array(range(self.num_rows), pa.uint64())}),
            )

        # The schema probe: filtered to nothing, so it carries schema and no rows.
        return self._arrow(request, pa.table({"id": pa.array([], pa.int64())}))


def _make_handler(serve):
    class MockLanceDBHandler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            serve(self)

        def do_POST(self):
            serve(self)

        def log_message(self, *args):
            pass  # keep pytest output readable

    return MockLanceDBHandler


@contextlib.contextmanager
def mock_remote_table(server):
    """Run ``server`` on a local port and yield an open remote table against it.

    Threading: the loader fans out fetch threads a single-threaded server would
    serialize, hiding the prefetch overlap under test.
    """
    with http.server.ThreadingHTTPServer(
        ("localhost", 0), _make_handler(server)
    ) as srv:
        thread = threading.Thread(target=srv.serve_forever)
        thread.start()
        try:
            db = lancedb.connect(
                "db://dev",
                api_key="fake",
                host_override=f"http://localhost:{srv.server_address[1]}",
                client_config={"timeout_config": {"connect_timeout": 5}},
            )
            yield db.open_table(server.name)
        finally:
            srv.shutdown()
            thread.join()


def assert_server_safe_row_id_requests(server):
    """Assert the loader fetched rows by row id and bounded everything else.

    `.get`, not `[...]`, so a dropped field reads as the assertion, not a KeyError.
    """
    for body in server.takes:
        # The fetch needs the row id back to restore the requested order.
        assert body.get("with_row_id") is True, body
        assert "_rowid" in body["filter"], body

    # Only the one-off permutation scan may scan the whole table; the schema probe is
    # built once per split per epoch. `k == 0` counts as unbounded: lance reads a zero
    # limit as "no limit".
    def is_unbounded(body):
        if is_row_id_take(body):
            return False
        k = body.get("k")
        return k is None or k == 0 or k > server.num_rows

    unbounded = [b for b in server.query_bodies if is_unbounded(b)]
    assert unbounded == server.scans, (
        f"only the permutation scan may be unbounded, got {unbounded}"
    )
