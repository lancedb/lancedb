# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import asyncio
from dataclasses import FrozenInstanceError
import json
from unittest.mock import AsyncMock

import pytest

import lancedb
from lancedb.authz import (
    AccessControlEntry,
    AsyncAuthorization,
    Authorization,
    GroupInfo,
    IdentityInfo,
    Object,
    PrincipalInfo,
    Privilege,
    RoleBinding,
    Subject,
)
from lancedb.remote import HeaderProvider
from lancedb.remote.errors import HttpError
from test_catalog import catalog_server as _catalog_server

catalog_server = _catalog_server


class DynamicHeaders(HeaderProvider):
    def get_headers(self):
        return {
            "Authorization": "Bearer rotating-token",
            "X-LanceDB-Database": "wrong-dynamic-db",
            "X-LanceDB-Database-Prefix": "wrong-prefix",
        }


@pytest.mark.parametrize("asynchronous", [False, True])
def test_authz_operations_and_scope(catalog_server, asynchronous):
    endpoint, requests, responses = catalog_server
    responses.extend(
        [
            (200, {}),
            (200, {}),
            (200, {}),
            (200, {}),
            (
                200,
                {
                    "entries": [
                        {
                            "subject_id": "g:team-id",
                            "object": "table:tenant/db:a$b$events",
                            "privilege": "PRIVILEGE20",
                        }
                    ]
                },
            ),
            (
                200,
                {
                    "bindings": [
                        {
                            "subject_id": "p:alice-id",
                            "subject_name": "Alice",
                            "role": "reader",
                        }
                    ]
                },
            ),
            (
                200,
                {
                    "principal_id": "alice-id",
                    "groups": [{"id": "team-id"}],
                    "roles": ["reader"],
                },
            ),
            (
                200,
                {
                    "group_id": "team-id",
                    "group_name": "Engineering",
                    "principals": [{"id": "alice-id", "name": "Alice"}],
                    "roles": ["reader"],
                },
            ),
        ]
    )
    config = {
        "header_provider": DynamicHeaders(),
        "extra_headers": {"x-lancedb-database": "wrong-static-db"},
    }
    obj = Object.table(database="tenant/db", namespace_path=["a", "b"], name="events")

    async def exercise():
        if asynchronous:
            catalog = await lancedb.connect_catalog_async(
                endpoint, client_config=config
            )
        else:
            catalog = lancedb.connect_catalog(endpoint, client_config=config)
        authz = catalog.authz
        assert authz is catalog.authz
        assert requests == []  # Constructing handles does not probe a database.

        async def call(method, *args, **kwargs):
            value = method(*args, **kwargs)
            return await value if asynchronous else value

        await call(
            authz.add_acl,
            object=obj,
            subject=Subject.group_name("Engineering"),
            privilege=Privilege.SELECT,
        )
        await call(
            authz.delete_acl,
            object=obj,
            subject=Subject.group_id("team-id"),
            privilege="SELECT",
        )
        await call(
            authz.add_role_binding,
            subject=Subject.principal_name("Alice"),
            role="reader",
        )
        await call(
            authz.delete_role_binding,
            subject=Subject.principal_api_key("subject-secret"),
            role="reader",
        )
        iterator = authz.list_acls(object=obj, subject=Subject.role("reader"))
        entries = (
            [entry async for entry in iterator] if asynchronous else list(iterator)
        )
        assert entries == [
            AccessControlEntry(
                str(obj), Subject.group_id("team-id"), None, "PRIVILEGE20"
            )
        ]
        iterator = authz.list_role_bindings(
            subject=Subject.group_id("team-id"), role="reader"
        )
        bindings = (
            [binding async for binding in iterator] if asynchronous else list(iterator)
        )
        assert bindings == [
            RoleBinding(Subject.principal_id("alice-id"), "Alice", "reader")
        ]
        assert await call(
            authz.get_principal, Subject.principal_id("alice-id")
        ) == PrincipalInfo("alice-id", None, [IdentityInfo("team-id")], ["reader"])
        assert await call(
            authz.get_group, Subject.group_name("Engineering")
        ) == GroupInfo(
            "team-id", "Engineering", [IdentityInfo("alice-id", "Alice")], ["reader"]
        )

    asyncio.run(exercise())
    assert [path for path, _, _ in requests] == [
        "/admin/authz/acl/add",
        "/admin/authz/acl/delete",
        "/admin/authz/role_binding/add",
        "/admin/authz/role_binding/delete",
        "/admin/authz/acl/list",
        "/admin/authz/role_binding/list",
        "/admin/authz/principal",
        "/admin/authz/group",
    ]
    assert [body for _, _, body in requests] == [
        {"object": str(obj), "subject": "G:Engineering", "privilege": "SELECT"},
        {"object": str(obj), "subject": "g:team-id", "privilege": "SELECT"},
        {"subject": "P:Alice", "role": "reader"},
        {"subject": "a:subject-secret", "role": "reader"},
        {"object": str(obj), "subject": "r:reader", "max_results": 100},
        {"subject": "g:team-id", "role": "reader", "max_results": 100},
        {"principal": "p:alice-id"},
        {"group": "G:Engineering"},
    ]
    for _, headers, _ in requests:
        headers = {key.lower(): value for key, value in headers.items()}
        assert "x-lancedb-database" not in headers
        assert "x-lancedb-database-prefix" not in headers
        assert headers["authorization"] == "Bearer rotating-token"


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("limit", [None, 0, 1, 3])
def test_lazy_expanded_acl_pagination(catalog_server, asynchronous, limit):
    endpoint, requests, responses = catalog_server

    def entry(privilege):
        return {
            "object": "database:db",
            "subject_id": "r:reader",
            "privilege": privilege,
        }

    responses.extend(
        [
            (
                200,
                {
                    "entries": [entry("SELECT"), entry("INSERT")],
                    "continuation_token": "opaque/first",
                },
            ),
            (200, {"entries": [], "continuation_token": "opaque/second"}),
            (200, {"entries": [entry("UPDATE"), entry("DELETE")]}),
        ]
    )

    async def exercise():
        catalog = (
            await lancedb.connect_catalog_async(endpoint, api_key="caller-secret")
            if asynchronous
            else lancedb.connect_catalog(endpoint, api_key="caller-secret")
        )
        iterator = catalog.authz.list_acls(subject=Subject.role("reader"), limit=limit)
        assert requests == []
        return [entry async for entry in iterator] if asynchronous else list(iterator)

    values = asyncio.run(exercise())
    expected = ["SELECT", "INSERT", "UPDATE", "DELETE"]
    assert [entry.privilege for entry in values] == (
        expected if limit is None else expected[:limit]
    )
    assert len(requests) == (0 if limit == 0 else 1 if limit == 1 else 3)
    for _, headers, body in requests:
        assert headers["x-api-key"] == "caller-secret"
        assert body["subject"] == "r:reader"
    if len(requests) == 3:
        assert requests[1][2]["continuation_token"] == "opaque/first"
        assert requests[2][2]["continuation_token"] == "opaque/second"


@pytest.mark.parametrize("status", [400, 401, 403, 404, 409, 501, 503])
def test_http_errors_preserve_status_without_replaying_writes(catalog_server, status):
    endpoint, requests, responses = catalog_server
    responses.append((status, {"error": "authorization failure"}))
    authz = lancedb.connect_catalog(endpoint).authz
    with pytest.raises(HttpError) as error:
        authz.add_acl(
            object="system", subject=Subject.role("operator"), privilege="OPERATE"
        )
    assert error.value.status_code == status
    assert error.value.request_id
    assert len(requests) == 1


@pytest.mark.parametrize("asynchronous", [False, True])
def test_role_pagination_and_late_failure(catalog_server, asynchronous):
    endpoint, requests, responses = catalog_server
    responses.extend(
        [
            (
                200,
                {
                    "bindings": [{"subject_id": "g:team", "role": "reader"}],
                    "continuation_token": "next",
                },
            ),
            (403, {"error": "denied"}),
        ]
    )

    async def exercise():
        catalog = (
            await lancedb.connect_catalog_async(endpoint)
            if asynchronous
            else lancedb.connect_catalog(endpoint)
        )
        iterator = catalog.authz.list_role_bindings(role="reader")
        first = await anext(iterator) if asynchronous else next(iterator)
        assert first.subject == Subject.group_id("team")
        assert len(requests) == 1
        with pytest.raises(HttpError) as error:
            if asynchronous:
                await anext(iterator)
            else:
                next(iterator)
        assert error.value.status_code == 403

    asyncio.run(exercise())
    assert requests[1][2] == {
        "role": "reader",
        "max_results": 100,
        "continuation_token": "next",
    }


def test_early_close_does_not_fetch_next_page(catalog_server):
    endpoint, requests, responses = catalog_server
    responses.append(
        (
            200,
            {
                "bindings": [{"subject_id": "g:team", "role": "reader"}],
                "continuation_token": "next",
            },
        )
    )
    iterator = lancedb.connect_catalog(endpoint).authz.list_role_bindings()
    next(iterator)
    iterator.close()
    assert len(requests) == 1


def test_selectors_objects_and_redaction():
    secret = Subject.principal_api_key("do-not-print-this")
    assert "do-not-print-this" not in repr(secret)
    assert "do-not-print-this" not in str(secret)
    with pytest.raises(FrozenInstanceError):
        secret.kind = "role"
    assert str(Object.system()) == "system"
    assert str(Object.database("tenant/db")) == "database:tenant/db"
    assert (
        str(Object.namespace(database="db", namespace_path=["a", "b"]))
        == "namespace:db:a$b"
    )
    assert str(Object.table(database="db", name="events")) == "table:db:public$events"
    assert str(Object.view(database="db", name="recent")) == "view:db:public$recent"
    assert Subject.principal_id("p:id")._wire() == "p:p:id"
    assert Subject.group_id("team-id").value == "team-id"
    for namespace in ([], ["a$b"], [""], "public", [".."]):
        with pytest.raises(ValueError):
            Object.table(database="db", namespace_path=namespace, name="events")
    for database in ("", "a//b", "a/../b", "a:b", "a%2Fb", "a$b"):
        with pytest.raises(ValueError):
            Object.database(database)


@pytest.mark.asyncio
async def test_validation_before_io_and_empty_lists():
    inner = AsyncMock()
    inner.list_acls.return_value = json.dumps({"entries": []})
    inner.list_role_bindings.return_value = json.dumps({"bindings": []})
    authz = AsyncAuthorization(inner)
    for limit in (-1, 1.5, True):
        with pytest.raises(ValueError):
            authz.list_acls(limit=limit)
        with pytest.raises(ValueError):
            Authorization(authz).list_role_bindings(limit=limit)
    with pytest.raises(ValueError):
        await authz.get_group(Subject.principal_id("alice"))
    with pytest.raises(ValueError):
        await authz.get_principal(Subject.group_id("team"))
    for method in (authz.add_role_binding, authz.delete_role_binding):
        with pytest.raises(ValueError):
            await method(subject=Subject.role("reader"), role="writer")
    with pytest.raises(ValueError):
        authz.list_role_bindings(subject=Subject.role("reader"))
    assert [entry async for entry in authz.list_acls(limit=0)] == []
    assert inner.mock_calls == []
    assert [entry async for entry in authz.list_acls()] == []
    assert [entry async for entry in authz.list_role_bindings()] == []


def test_api_key_subject_is_not_logged(catalog_server):
    import os
    import subprocess
    import sys

    endpoint, requests, responses = catalog_server
    responses.append((200, {"principal_id": "public-id", "groups": [], "roles": []}))
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import lancedb, sys; from lancedb.authz import Subject; "
            "lancedb.connect_catalog(sys.argv[1]).authz.get_principal("
            "Subject.principal_api_key('test-subject-secret-never-log'))",
            endpoint,
        ],
        env={**os.environ, "LANCEDB_LOG": "debug"},
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    )
    assert requests[0][2] == {"principal": "a:test-subject-secret-never-log"}
    assert "test-subject-secret-never-log" not in result.stdout + result.stderr


@pytest.mark.parametrize(
    "builder, kwargs, wire",
    [
        ("system", {}, "system"),
        ("secret", {"database": "db", "name": "api-key"}, "secret:db:public$api-key"),
        (
            "function",
            {"database": "db", "name": "caption"},
            "function:db:public$caption",
        ),
        (
            "secret",
            {"database": "tenant/db", "namespace_path": ["a", "b"], "name": "api-key"},
            "secret:tenant/db:a$b$api-key",
        ),
        (
            "function",
            {"database": "tenant/db", "namespace_path": ["a", "b"], "name": "caption"},
            "function:tenant/db:a$b$caption",
        ),
        ("database", {"name": "tenant/db"}, "database:tenant/db"),
        (
            "namespace",
            {"database": "tenant/db", "namespace_path": ["a", "b"]},
            "namespace:tenant/db:a$b",
        ),
        (
            "table",
            {"database": "tenant/db", "namespace_path": ["a", "b"], "name": "events"},
            "table:tenant/db:a$b$events",
        ),
        (
            "view",
            {"database": "db", "namespace_path": ["public"], "name": "recent"},
            "view:db:public$recent",
        ),
    ],
)
def test_rust_backed_object_builders(builder, kwargs, wire):
    import pickle
    from lancedb import _lancedb

    value = getattr(Object, builder)(**kwargs)
    assert str(value) == wire
    assert str(value._inner) == wire
    assert str(_lancedb.AuthzObject(wire)) == wire
    assert value == Object(wire)
    assert hash(value) == hash(Object(wire))
    assert pickle.loads(pickle.dumps(value)) == value


@pytest.mark.parametrize(
    "kind, prefix",
    [
        ("principal_id", "p:"),
        ("principal_name", "P:"),
        ("group_id", "g:"),
        ("group_name", "G:"),
        ("role", "r:"),
        ("principal_api_key", "a:"),
    ],
)
def test_rust_backed_subject_selectors(kind, prefix):
    import pickle
    from lancedb import _lancedb

    value = "p:literal@example.com / 名前"
    subject = getattr(Subject, kind)(value)
    assert subject.kind == kind
    assert subject.value == value
    assert subject._wire() == prefix + value
    assert _lancedb.AuthzSubject(kind, value).to_wire() == prefix + value
    assert pickle.loads(pickle.dumps(subject)) == subject
    assert hash(subject) == hash(getattr(Subject, kind)(value))
    with pytest.raises(ValueError):
        getattr(Subject, kind)("")
    if kind == "principal_api_key":
        assert value not in repr(subject._inner)
        assert value not in str(subject._inner)
    if kind in ("principal_id", "group_id", "role"):
        assert Subject._canonical(prefix + value) == subject
    else:
        with pytest.raises(ValueError) as error:
            Subject._canonical(prefix + value)
        assert value not in str(error.value)


@pytest.mark.parametrize(
    "wire",
    [
        "",
        "database:",
        "database:a//b",
        "database:a/../b",
        "database:a%2Fb",
        "namespace:db:a$$b",
        "table:db:events",
        "table:db:public$",
        "table:db:public$..",
        "table:db:a/b$events",
        "view:db:public$café",
        "secret:db:public$",
        "function:db:public$..",
        "secret:db:a$$key",
        "function:db:caption",
        "secret:db:public$bad/name",
        "function:db:public$bad:name",
        "system:db",
    ],
)
def test_invalid_raw_objects_fail_before_io(catalog_server, wire):
    endpoint, requests, _ = catalog_server
    authz = lancedb.connect_catalog(endpoint).authz
    with pytest.raises(ValueError):
        Object(wire)
    with pytest.raises(ValueError):
        authz.list_acls(object=wire)
    with pytest.raises(ValueError):
        authz.add_acl(object=wire, subject=Subject.role("reader"), privilege="SELECT")
    assert requests == []


@pytest.mark.parametrize(
    "wire",
    [
        "future:db:resource",
        "table_extension:db:resource",
        "secret:tenant/db:a$b$key",
        "function:tenant/db:a$b$caption",
    ],
)
def test_objects_round_trip_through_acl_requests(catalog_server, wire):
    endpoint, requests, responses = catalog_server
    responses.extend(
        [
            (200, {}),
            (
                200,
                {
                    "entries": [
                        {
                            "object": wire,
                            "subject_id": "r:reader",
                            "privilege": "SELECT",
                        }
                    ]
                },
            ),
            (200, {}),
        ]
    )
    authz = lancedb.connect_catalog(endpoint).authz
    obj = Object(wire)
    assert str(obj) == wire
    authz.add_acl(object=obj, subject=Subject.role("reader"), privilege="SELECT")
    entries = list(authz.list_acls(object=wire))
    assert entries[0].object == wire
    authz.delete_acl(
        object=entries[0].object, subject=Subject.role("reader"), privilege="SELECT"
    )
    assert len(requests) == 3
    for _, _, body in requests:
        assert body["object"] == wire


@pytest.mark.asyncio
async def test_native_requests_validate_selector_context_before_io(catalog_server):
    endpoint, requests, _ = catalog_server
    catalog = await lancedb.connect_catalog_async(endpoint)
    native = catalog.authz._inner
    for method, request in [
        (native.list_role_bindings, {"subject": "r:reader"}),
        (native.add_role_binding, {"subject": "r:reader", "role": "writer"}),
        (native.delete_role_binding, {"subject": "r:reader", "role": "writer"}),
        (native.get_principal, {"principal": "g:group"}),
        (native.get_group, {"group": "a:never-log-this-key"}),
    ]:
        with pytest.raises(ValueError) as error:
            await method(json.dumps(request))
        assert "never-log-this-key" not in str(error.value)
    assert requests == []


def test_privilege_values_use_rust_serialization():
    import pickle
    from lancedb import _lancedb

    names = [value.value for value in Privilege]
    assert names == _lancedb.AuthzPrivilege.known()
    for privilege in Privilege:
        assert json.loads(json.dumps(privilege)) == privilege.value
        assert str(_lancedb.AuthzPrivilege(privilege)) == privilege.value
        assert pickle.loads(pickle.dumps(privilege)) is privilege
    for index in range(64):
        alias = f"PRIVILEGE{index}"
        assert str(_lancedb.AuthzPrivilege(alias)) == alias


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize(
    "creation_privilege", [None, "CREATE_SECRET", "CREATE_FUNCTION"]
)
def test_privilege_names_and_future_values_round_trip(
    catalog_server, asynchronous, creation_privilege
):
    endpoint, requests, responses = catalog_server
    inputs = [
        Privilege.SELECT,
        "SELECT",
        "PRIVILEGE2",
        "PRIVILEGE20",
        "PRIVILEGE63",
        "PRIVILEGE64",
        "PRIVILEGE256",
        "PRIVILEGE18446744073709551616",
    ]
    expected = ["SELECT", *inputs[1:]]
    object_id = "table:db:public$t"
    if creation_privilege is not None:
        inputs = [Privilege[creation_privilege], creation_privilege]
        expected = [creation_privilege, creation_privilege]
        object_id = "namespace:db:public"
    for privilege in inputs:
        responses.extend(
            [
                (200, {}),
                (
                    200,
                    {
                        "entries": [
                            {
                                "object": object_id,
                                "subject_id": "r:reader",
                                "privilege": privilege,
                            }
                        ]
                    },
                ),
                (200, {}),
            ]
        )

    async def exercise():
        catalog = (
            await lancedb.connect_catalog_async(endpoint)
            if asynchronous
            else lancedb.connect_catalog(endpoint)
        )
        authz = catalog.authz
        for privilege, canonical in zip(inputs, expected):
            added = authz.add_acl(
                object=object_id,
                subject=Subject.role("reader"),
                privilege=privilege,
            )
            if asynchronous:
                await added
            iterator = authz.list_acls()
            entries = (
                [entry async for entry in iterator] if asynchronous else list(iterator)
            )
            assert entries[0].privilege == canonical
            deleted = authz.delete_acl(
                object=entries[0].object,
                subject=entries[0].subject,
                privilege=entries[0].privilege,
            )
            if asynchronous:
                await deleted

    asyncio.run(exercise())
    assert len(requests) == 3 * len(inputs)
    for index, canonical in enumerate(expected):
        assert requests[index * 3][2]["object"] == object_id
        assert requests[index * 3][2]["privilege"] == canonical
        assert requests[index * 3 + 2][2]["object"] == object_id
        assert requests[index * 3 + 2][2]["privilege"] == canonical


@pytest.mark.parametrize("asynchronous", [False, True])
def test_invalid_privileges_fail_before_io(catalog_server, asynchronous):
    endpoint, requests, _ = catalog_server
    invalid = [
        "",
        "select",
        "SELECT ",
        "UNKNOWN",
        "PRIVILEGE",
        "PRIVILEGE-1",
        "PRIVILEGE+1",
        "PRIVILEGE1x",
        None,
        2,
        True,
        ["SELECT"],
    ]

    async def exercise():
        catalog = (
            await lancedb.connect_catalog_async(endpoint)
            if asynchronous
            else lancedb.connect_catalog(endpoint)
        )
        async_authz = catalog.authz if asynchronous else catalog.authz._inner
        native = async_authz._inner
        for privilege in invalid:
            for method in [catalog.authz.add_acl, catalog.authz.delete_acl]:
                with pytest.raises(ValueError, match="privilege"):
                    result = method(
                        object="system",
                        subject=Subject.role("reader"),
                        privilege=privilege,
                    )
                    if asynchronous:
                        await result
            for method in [
                native.add_acl,
                native.delete_acl,
            ]:
                with pytest.raises(ValueError):
                    await method(
                        json.dumps(
                            {
                                "object": "system",
                                "subject": "r:reader",
                                "privilege": privilege,
                            }
                        )
                    )

    asyncio.run(exercise())
    assert requests == []


@pytest.mark.parametrize("asynchronous", [False, True])
def test_acl_page_preserves_unknown_subjects(catalog_server, asynchronous):
    import pickle

    endpoint, requests, responses = catalog_server
    responses.extend(
        [
            (
                200,
                {
                    "entries": [
                        {
                            "subject_id": "p:known",
                            "object": "system",
                            "privilege": "USAGE",
                        },
                        {
                            "subject_id": "x:future-subject",
                            "object": "system",
                            "privilege": "USAGE",
                        },
                    ]
                },
            ),
            (200, {}),
        ]
    )

    async def exercise():
        catalog = (
            await lancedb.connect_catalog_async(endpoint)
            if asynchronous
            else lancedb.connect_catalog(endpoint)
        )
        iterator = catalog.authz.list_acls()
        entries = (
            [entry async for entry in iterator] if asynchronous else list(iterator)
        )
        assert len(entries) == 2
        assert entries[0].subject == Subject.principal_id("known")
        subject = entries[1].subject
        assert subject.kind == "unknown"
        assert subject.value == "x:future-subject"
        assert subject._wire() == "x:future-subject"
        assert pickle.loads(pickle.dumps(subject)) == subject
        result = catalog.authz.delete_acl(
            object=entries[1].object, subject=subject, privilege=entries[1].privilege
        )
        if asynchronous:
            await result

    asyncio.run(exercise())
    assert len(requests) == 2
    assert requests[1][2]["subject"] == "x:future-subject"


@pytest.mark.parametrize("builder", ["secret", "function"])
def test_secret_and_function_builders_validate_components(builder):
    from lancedb import _lancedb

    for database, namespace, name in [
        ("a//b", ["public"], "valid"),
        ("db", [], "valid"),
        ("db", ["a$b"], "valid"),
        ("db", "public", "valid"),
        ("db", ["public"], "bad$name"),
        ("db", ["public"], ".."),
    ]:
        with pytest.raises(ValueError):
            getattr(Object, builder)(
                database=database, namespace_path=namespace, name=name
            )
        with pytest.raises(ValueError):
            getattr(_lancedb.AuthzObject, builder)(database, namespace, name)
