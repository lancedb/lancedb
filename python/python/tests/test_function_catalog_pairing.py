# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path

import lancedb
from lancedb.remote.errors import SqlError
import pyarrow as pa
import pytest
from utils import start_server_thread


DESCRIBE = 'DESCRIBE FUNCTION "embed" FORMAT JSON'
DESCRIBE_VERSION = "DESCRIBE FUNCTION \"embed\" WITH (version = '1') FORMAT JSON"
LIST = 'SHOW FUNCTIONS HISTORY IN "public" LIMIT 1000'
CONTINUE = "SHOW FUNCTIONS HISTORY IN \"public\" LIMIT 1 FROM 'opaque''cursor'"
TRAILING_INPUT = "unexpected trailing input after the statement: "


@pytest.fixture
def flight_catalog():
    flight = pytest.importorskip("pyarrow.flight")

    class Catalog(flight.FlightServerBase):
        def __init__(self):
            super().__init__("grpc://127.0.0.1:0")
            self.replies = deque()
            self.requests = []

        def do_get(self, context, ticket):
            statement, response = self.replies.popleft()
            assert ticket.ticket.endswith(statement.encode()), ticket.ticket
            self.requests.append(statement)
            if isinstance(response, Exception):
                raise response
            return flight.RecordBatchStream(response)

    with Catalog() as server:
        yield server
        assert not server.replies


@pytest.fixture
def discovery_404():
    paths = []

    class Discovery(BaseHTTPRequestHandler):
        def do_GET(self):
            paths.append(self.path)
            self.send_response(404)
            self.end_headers()

    with ThreadingHTTPServer(("127.0.0.1", 0), Discovery) as server:
        thread = start_server_thread(server)
        try:
            yield f"http://127.0.0.1:{server.server_port}", paths
        finally:
            server.shutdown()
            thread.join()


async def connect(server, asynchronous, endpoint="http://127.0.0.1:1"):
    options = {
        "api_key": "test-key",
        "host_override": endpoint,
        "sql_host_override": f"grpc://127.0.0.1:{server.port}",
        "client_config": {"retry_config": {"retries": 0}},
    }
    if asynchronous:
        return await lancedb.connect_async("db://catalog", **options)
    return lancedb.connect("db://catalog", **options)


@pytest.mark.parametrize(
    ("statement", "exception_type", "diagnostic", "code"),
    [
        (DESCRIBE, "ArrowInvalid", TRAILING_INPUT + "LIMIT", 3),
        (LIST, "ArrowInvalid", TRAILING_INPUT + "FORMAT", 3),
        (DESCRIBE, "ArrowInvalid", TRAILING_INPUT + "FORMATTER", 3),
        (DESCRIBE, "ArrowKeyError", "Function 'embed' was not found", 5),
        (DESCRIBE, "FlightUnauthorizedError", TRAILING_INPUT + "FORMAT", 7),
        (LIST, "FlightUnavailableError", TRAILING_INPUT + "LIMIT", 14),
        ("SELECT 1", "ArrowInvalid", TRAILING_INPUT + "FORMAT", 3),
    ],
)
def test_function_catalog_preserves_other_sql_errors(
    flight_catalog, statement, exception_type, diagnostic, code
):
    flight = pytest.importorskip("pyarrow.flight")
    failure_type = getattr(
        pa if exception_type.startswith("Arrow") else flight, exception_type
    )
    flight_catalog.replies.append((statement, failure_type(diagnostic)))
    db = lancedb.connect(
        "db://catalog",
        api_key="test-key",
        host_override="http://127.0.0.1:1",
        sql_host_override=f"grpc://127.0.0.1:{flight_catalog.port}",
        client_config={"retry_config": {"retries": 0}},
    )
    with pytest.raises(SqlError) as exc:
        if statement == DESCRIBE:
            db.get_function("embed")
        elif statement == LIST:
            next(db.list_functions())
        else:
            db.execute_query(statement).read_all()
    assert diagnostic in str(exc.value)
    assert "lancedb==" not in str(exc.value)
    assert exc.value.grpc_code == code
    assert flight_catalog.requests == [statement]


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_function_catalog_reads_and_pages_after_discovery_404(
    flight_catalog, discovery_404, asynchronous
):
    endpoint, discovery_requests = discovery_404
    fixture = (
        Path(__file__).resolve().parents[3]
        / "rust/lancedb/tests/fixtures/first_class_functions/v1"
        / "remote_function_version.canonical.json"
    )
    first = json.loads(fixture.read_text())
    second = {**first, "version": "2"}
    description = pa.table(
        {"description": [json.dumps(first)]}
    ).replace_schema_metadata({"lancedb.describe.format": "json"})
    page = pa.table({"definition": [json.dumps(first)]}).replace_schema_metadata(
        {"lancedb.next_page_token": "opaque'cursor"}
    )
    flight_catalog.replies.extend(
        [
            (DESCRIBE_VERSION, description),
            ('SHOW FUNCTIONS HISTORY IN "public" LIMIT 1', page),
            (CONTINUE, pa.table({"definition": [json.dumps(second)]})),
        ]
    )
    db = await connect(flight_catalog, asynchronous, endpoint)
    assert discovery_requests == []
    with pytest.raises((NotImplementedError, RuntimeError), match="returned 404"):
        if asynchronous:
            await db.server_info()
        else:
            db.server_info()
    assert discovery_requests == ["/v1/server/info"]

    description = db.get_function("embed", version="1")
    if asynchronous:
        description = await description
    assert (description.name, description.version) == ("embed", "1")
    listing = db.list_functions(page_limit=1)
    assert flight_catalog.requests == [DESCRIBE_VERSION]
    original = await anext(listing) if asynchronous else next(listing)
    assert (original.name, original.version) == ("embed", "1")
    assert listing.num_page_results() == 0
    assert listing.page_token() == "opaque'cursor"
    later = await anext(listing) if asynchronous else next(listing)
    assert (later.name, later.version) == ("embed", "2")
    assert listing.page_token() is None
    assert ([item async for item in listing] if asynchronous else list(listing)) == []
    assert discovery_requests == ["/v1/server/info"]


@pytest.mark.asyncio
async def test_function_catalog_keeps_malformed_result_errors(flight_catalog):
    flight_catalog.replies.extend(
        [
            (DESCRIBE, pa.table({"name": ["embed"], "version": ["1"]})),
            (LIST, pa.table({"name": ["embed"]})),
        ]
    )
    db = await connect(flight_catalog, True)
    with pytest.raises(RuntimeError, match="requires one UTF8 description column"):
        await db.get_function("embed")
    with pytest.raises(RuntimeError, match="missing string column 'definition'"):
        await anext(db.list_functions())
