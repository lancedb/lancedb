# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import asyncio
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import lancedb
from lancedb.remote.errors import HttpError
import pytest
from utils import start_server_thread


@pytest.fixture
def listing_server():
    requests, responses = [], []

    class Handler(BaseHTTPRequestHandler):
        def handle_request(self):
            body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            requests.append((self.command, self.path, json.loads(body) if body else {}))
            status, response = responses.pop(0)
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(response).encode())

        do_GET = handle_request
        do_POST = handle_request

    with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
        thread = start_server_thread(server, daemon=True)
        try:
            yield f"http://127.0.0.1:{server.server_port}", requests, responses
        finally:
            server.shutdown()
            thread.join()


def response_page(kind, names, token):
    if kind == "secrets":
        items = [{"name": name} for name in names]
    elif kind == "jobs":
        items = [
            {
                "job_id": name,
                "table": "t",
                "job_type": "create_index",
                "state": "in_progress",
                "created_at_millis": 1,
            }
            for name in names
        ]
    elif kind == "functions":
        fixture = (
            Path(__file__).resolve().parents[3]
            / "rust/lancedb/tests/fixtures/first_class_functions/v1"
            / "remote_function_version.canonical.json"
        )
        definition = json.loads(fixture.read_text())
        items = [
            {"name": "embed", "version": "1", "definition": definition} for _ in names
        ]
    else:
        items = names
    key = kind
    return {key: items, "page_token": token}


async def connection(endpoint, asynchronous):
    kwargs = dict(
        api_key="fake",
        host_override=endpoint,
        client_config={"retry_config": {"retries": 0}},
    )
    if asynchronous:
        return await lancedb.connect_async("db://dev", **kwargs)
    return lancedb.connect("db://dev", **kwargs)


async def advance(items, asynchronous):
    return await items.__anext__() if asynchronous else next(items)


KINDS = ["jobs"]


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["tables", "namespaces"])
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_compatible_name_listing_pages(listing_server, kind, asynchronous):
    endpoint, requests, responses = listing_server
    responses.extend(
        [
            (200, response_page(kind, ["a", "b"], "next")),
            (200, response_page(kind, ["c"], None)),
        ]
    )
    db = await connection(endpoint, asynchronous)
    listing = getattr(db, "list_" + kind)(
        namespace_path=["team"], page_token="start", limit=2
    )
    field = kind
    assert len(requests) == (0 if asynchronous else 1)
    if asynchronous:
        with pytest.raises(RuntimeError, match="Await or iterate"):
            getattr(listing, field)
        assert listing.page_token == "start"
        assert await listing is listing
    assert len(requests) == 1
    page_names = getattr(listing, field)
    assert type(page_names) is list
    assert page_names == ["a", "b"]
    page_names.append("changed")
    assert getattr(listing, field) == ["a", "b"]
    assert listing.page_token == "next"
    assert listing.num_page_results() == 2
    assert await advance(listing, asynchronous) == "a"
    assert getattr(listing, field) == ["a", "b"]
    assert await advance(listing, asynchronous) == "b"
    assert len(requests) == 1
    assert await advance(listing, asynchronous) == "c"
    assert getattr(listing, field) == ["c"]
    assert listing.page_token is None
    assert len(requests) == 2
    assert ([item async for item in listing] if asynchronous else list(listing)) == []
    if asynchronous:
        assert await listing is listing
    assert len(requests) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["tables", "namespaces"])
async def test_name_listing_skips_empty_page_only_during_iteration(
    listing_server, kind
):
    endpoint, requests, responses = listing_server
    responses.extend(
        [
            (200, response_page(kind, [], "next")),
            (200, response_page(kind, ["a"], None)),
        ]
    )
    db = await connection(endpoint, True)
    listing = getattr(db, "list_" + kind)()
    await listing
    assert getattr(listing, kind) == []
    assert listing.page_token == "next"
    assert len(requests) == 1
    assert [name async for name in listing] == ["a"]
    assert len(requests) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["tables", "namespaces"])
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_name_listing_zero_limit_uses_no_request(
    listing_server, kind, asynchronous
):
    endpoint, requests, _ = listing_server
    db = await connection(endpoint, asynchronous)
    listing = getattr(db, "list_" + kind)(limit=0)
    if asynchronous:
        assert await listing is listing
        assert [name async for name in listing] == []
    else:
        assert list(listing) == []
    assert getattr(listing, kind) == []
    assert listing.page_token is None
    assert requests == []


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["tables", "namespaces"])
async def test_name_listing_failure_retains_token(listing_server, kind):
    endpoint, requests, responses = listing_server
    responses.append((401, {}))
    db = await connection(endpoint, True)
    listing = getattr(db, "list_" + kind)(page_token="retry")
    with pytest.raises(HttpError):
        await listing
    assert listing.page_token == "retry"
    with pytest.raises(HttpError):
        await listing
    assert [name async for name in listing] == []
    assert len(requests) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["tables", "namespaces"])
async def test_name_listing_direct_async_iteration(listing_server, kind):
    endpoint, requests, responses = listing_server
    responses.extend(
        [
            (200, response_page(kind, ["a"], "next")),
            (200, response_page(kind, ["b"], None)),
        ]
    )
    db = await connection(endpoint, True)
    listing = getattr(db, "list_" + kind)(limit=1)
    assert requests == []
    assert [name async for name in listing] == ["a", "b"]
    assert getattr(listing, kind) == ["b"]
    assert len(requests) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["tables", "namespaces"])
async def test_name_listing_serializes_concurrent_advances(listing_server, kind):
    endpoint, requests, responses = listing_server
    responses.append((200, response_page(kind, ["a", "b"], None)))
    db = await connection(endpoint, True)
    listing = getattr(db, "list_" + kind)()
    results = await asyncio.gather(
        listing.__anext__(),
        listing.__anext__(),
        listing.__anext__(),
        return_exceptions=True,
    )
    assert results[:2] == ["a", "b"]
    assert isinstance(results[2], StopAsyncIteration)
    assert len(requests) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_listing_lazy_pages_and_resume(listing_server, kind, asynchronous):
    endpoint, requests, responses = listing_server
    responses.extend(
        (200, response_page(kind, names, token))
        for names, token in [(["a", "b"], "empty"), ([], "last"), (["c", "d"], "")]
    )
    db = await connection(endpoint, asynchronous)
    method = getattr(db, "list_" + kind)
    kwargs = {} if kind == "jobs" else {"namespace_path": ["team"]}
    items = method(page_token="start/token", page_limit=2, **kwargs)
    assert requests == []
    assert items.page_token() == "start/token"
    assert items.num_page_results() == 0
    first = await advance(items, asynchronous)
    if kind == "functions":
        assert first.name == "embed"
        assert first.version == "1"
    elif kind == "jobs":
        assert first.job_id == "a"
        assert first.state == "running"
    else:
        assert first == "a"
    assert items.num_page_results() == 1
    assert items.page_token() == "empty"
    await advance(items, asynchronous)
    assert items.num_page_results() == 0
    assert len(requests) == 1
    resumed = method(page_token=items.page_token(), page_limit=2, **kwargs)
    await advance(resumed, asynchronous)
    assert len(requests) == 3
    assert resumed.page_token() is None
    assert resumed.num_page_results() == 1
    remaining = [item async for item in resumed] if asynchronous else list(resumed)
    assert len(remaining) == 1
    assert resumed.num_page_results() == 0
    assert ([item async for item in resumed] if asynchronous else list(resumed)) == []
    for (verb, url, body), token in zip(requests, ["start/token", "empty", "last"]):
        path = urlsplit(url)
        if kind == "jobs":
            assert verb == "POST"
            assert path.path == "/v1/jobs/list"
            assert body == {"database": "dev", "page_token": token, "limit": 2}
        else:
            namespace = "team"
            resource = kind.removesuffix("s")
            assert path.path == f"/v1/namespace/{namespace}/{resource}/list"
            expected = {"page_token": [token], "limit": ["2"]}
            if kind == "functions":
                expected["include_definition"] = ["true"]
            assert parse_qs(path.query) == expected
    assert responses == []


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_listing_error_retains_token(listing_server, kind, asynchronous):
    endpoint, requests, responses = listing_server
    responses.extend([(401, {}), (200, response_page(kind, ["a"], None))])
    db = await connection(endpoint, asynchronous)
    method = getattr(db, "list_" + kind)
    items = method(page_token="retry")
    with pytest.raises(HttpError):
        await advance(items, asynchronous)
    assert items.page_token() == "retry"
    assert items.num_page_results() == 0
    assert ([item async for item in items] if asynchronous else list(items)) == []
    assert len(requests) == 1
    resumed = method(page_token=items.page_token())
    assert len([item async for item in resumed] if asynchronous else list(resumed)) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", KINDS)
async def test_listing_serializes_concurrent_advances(listing_server, kind):
    endpoint, requests, responses = listing_server
    responses.append((200, response_page(kind, ["a", "b"], None)))
    db = await connection(endpoint, True)
    items = getattr(db, "list_" + kind)()
    results = await asyncio.gather(
        items.__anext__(), items.__anext__(), items.__anext__(), return_exceptions=True
    )
    assert (
        len([result for result in results if isinstance(result, StopAsyncIteration)])
        == 1
    )
    assert len(requests) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("page_limit", [0, 2**32 - 1])
async def test_listing_invalid_limit_does_not_fetch(listing_server, kind, page_limit):
    endpoint, requests, _ = listing_server
    db = await connection(endpoint, True)
    items = getattr(db, "list_" + kind)(page_limit=page_limit)
    with pytest.raises(ValueError, match="limit"):
        await items.__anext__()
    assert requests == []


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("namespace", [[], ["team", "project"]])
@pytest.mark.parametrize("multiple_types", [False, True])
async def test_job_filters_persist_across_pages_and_resume(
    listing_server, asynchronous, namespace, multiple_types
):
    endpoint, requests, responses = listing_server
    responses.extend(
        (200, response_page("jobs", names, token))
        for names, token in [(["a"], "empty"), ([], "last"), (["b"], None)]
    )
    db = await connection(endpoint, asynchronous)
    types = ["column_refresh", "materialized_view_refresh"]
    filters = dict(
        table="events",
        namespace_path=namespace,
        state="in_progress",
        created_at_gte_millis=100,
        created_at_lt_millis=200,
        **({"job_types": types} if multiple_types else {"job_type": types[0]}),
    )
    items = db.list_jobs(page_limit=1, **filters)
    assert requests == []
    assert (await advance(items, asynchronous)).job_id == "a"
    assert len(requests) == 1
    resumed = db.list_jobs(page_token=items.page_token(), page_limit=1, **filters)
    assert (await advance(resumed, asynchronous)).job_id == "b"
    assert resumed.page_token() is None
    assert len(requests) == 3
    expected = dict(
        database="dev",
        limit=1,
        table_ref={
            "db_name": "dev",
            "table_id": {"namespace_path": namespace, "table_name": "events"},
        },
        state="in_progress",
        job_types=types if multiple_types else types[:1],
        created_at_gte_millis=100,
        created_at_lt_millis=200,
    )
    for (_, path, body), token in zip(requests, [None, "empty", "last"]):
        assert path == "/v1/jobs/list"
        assert body == {**expected, **({"page_token": token} if token else {})}


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize(
    "filters",
    [
        {},
        {"state": "failed"},
        {"job_types": ["column_refresh"]},
        {"created_at_lt_millis": 200},
    ],
)
async def test_job_listing_without_table_stays_in_connected_database(
    listing_server, asynchronous, filters
):
    endpoint, requests, responses = listing_server
    responses.append((200, response_page("jobs", ["a"], None)))
    db = await connection(endpoint, asynchronous)
    items = db.list_jobs(**filters)
    assert (await advance(items, asynchronous)).job_id == "a"
    assert requests[0][2] == {"database": "dev", **filters}


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize(
    "filters, message",
    [
        ({"namespace_path": ["team"]}, "requires a table"),
        ({"created_at_gte_millis": 200, "created_at_lt_millis": 200}, "less than"),
        ({"created_at_gte_millis": 201, "created_at_lt_millis": 200}, "less than"),
        ({"job_type": "column_refresh", "job_types": []}, "only one"),
        ({"state": "unknown"}, "state must be"),
    ],
)
async def test_invalid_job_filters_do_not_fetch(
    listing_server, asynchronous, filters, message
):
    endpoint, requests, _ = listing_server
    db = await connection(endpoint, asynchronous)
    with pytest.raises(ValueError, match=message):
        items = db.list_jobs(**filters)
        await advance(items, asynchronous)
    assert requests == []


@pytest.mark.asyncio
async def test_listing_slice_and_index_fetch_only_needed_pages(listing_server):
    endpoint, requests, responses = listing_server
    responses.extend(
        (200, response_page("jobs", names, token))
        for names, token in [(["a", "b"], "second"), (["c", "d"], "last")]
    )
    db = await connection(endpoint, False)
    items = db.list_jobs(table="events", page_limit=2)
    assert items[:0] == []
    assert items[5:2] == []
    assert requests == []
    assert [job.job_id for job in items[:2]] == ["a", "b"]
    assert len(requests) == 1
    assert items.num_page_results() == 2
    assert items.page_token() == "second"
    assert items[1].job_id == "b"
    assert next(items).job_id == "a"
    assert [job.job_id for job in items[:2]] == ["b", "c"]
    assert len(requests) == 2
    assert items.num_page_results() == 3
    assert items.page_token() == "last"
    assert [job.job_id for job in items[0:3:2][:1]] == ["b"]
    assert len(requests) == 2


@pytest.mark.asyncio
async def test_listing_length_and_slices_preserve_remaining_items(listing_server):
    endpoint, requests, responses = listing_server
    responses.extend(
        (200, response_page("jobs", names, token))
        for names, token in [(["a", "b"], "empty"), ([], "last"), (["c", "d"], None)]
    )
    db = await connection(endpoint, False)
    items = db.list_jobs(page_limit=2)
    assert bool(items)
    assert len(requests) == 1
    assert next(items).job_id == "a"
    assert len(items) == 3
    assert len(requests) == 3
    assert items.num_page_results() == 3
    assert items.page_token() is None
    assert items[-1].job_id == "d"
    assert [job.job_id for job in items[::-1]] == ["d", "c", "b"]
    assert [job.job_id for job in items[1:]] == ["c", "d"]
    assert [job.job_id for job in items[:100:2]] == ["b", "d"]
    assert [job.job_id for job in items[: 2**100 : 2**100]] == ["b"]
    with pytest.raises(IndexError):
        _ = items[3]
    assert [job.job_id for job in items] == ["b", "c", "d"]
    assert len(items) == 0
    assert not items
    assert len(requests) == 3


@pytest.mark.asyncio
async def test_listing_invalid_slice_does_not_fetch(listing_server):
    endpoint, requests, _ = listing_server
    db = await connection(endpoint, False)
    items = db.list_jobs()
    with pytest.raises(ValueError, match="step"):
        _ = items[::0]
    with pytest.raises(TypeError):
        _ = items["invalid"]
    with pytest.raises(TypeError):
        _ = items[:1.5]
    assert requests == []


@pytest.mark.asyncio
async def test_listing_slice_failure_preserves_cache_and_retry_token(listing_server):
    endpoint, requests, responses = listing_server
    responses.extend([(200, response_page("jobs", ["a", "b"], "retry")), (401, {})])
    db = await connection(endpoint, False)
    items = db.list_jobs(page_limit=2)
    with pytest.raises(HttpError):
        _ = items[:3]
    assert items.page_token() == "retry"
    assert items.num_page_results() == 2
    assert [job.job_id for job in items[:2]] == ["a", "b"]
    assert [job.job_id for job in items] == ["a", "b"]
    assert len(requests) == 2
