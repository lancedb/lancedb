# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Deployment-wide authorization through ``catalog.authz``.

ACLs grant one privilege on an object to a principal, group, or role. Role
bindings grant a role to a principal or group. Identity-provider group membership
is read-only. Name resolution and permission checks are performed by the server.
"""

from collections.abc import AsyncIterator, Iterator, Sequence
from dataclasses import dataclass, field
from enum import Enum
import json
from typing import Literal, Optional, TypeVar, Union, cast

from . import _lancedb
from .background_loop import LOOP

__all__ = [
    "Authorization",
    "AsyncAuthorization",
    "Subject",
    "Object",
    "Privilege",
    "AccessControlEntry",
    "RoleBinding",
    "IdentityInfo",
    "PrincipalInfo",
    "GroupInfo",
]

_PAGE_SIZE = 100
_T = TypeVar("_T")
_SubjectKind = Literal[
    "principal_id",
    "principal_name",
    "group_id",
    "group_name",
    "role",
    "principal_api_key",
    "unknown",
]


@dataclass(frozen=True)
class Subject:
    """An immutable identity selector. Prefer the named factory methods.

    IDs are stable; names must resolve unambiguously on the server. API-key
    selectors identify the subject of an operation, not its authenticated caller.
    Secret values are omitted from representations. Unknown canonical subject
    kinds returned by newer servers use ``kind="unknown"`` and preserve the full
    wire string in ``value``; they can be passed back to ACL methods.
    """

    kind: _SubjectKind
    _value: str = field(repr=False)

    _inner: _lancedb.AuthzSubject = field(init=False, repr=False, compare=False)

    def __post_init__(self):
        object.__setattr__(
            self, "_inner", _lancedb.AuthzSubject(self.kind, self._value)
        )

    @classmethod
    def principal_id(cls, id: str) -> "Subject":
        """Select a principal by canonical identity-provider ID."""
        return cls("principal_id", id)

    @classmethod
    def principal_name(cls, name: str) -> "Subject":
        """Select a principal by an unambiguous provider name."""
        return cls("principal_name", name)

    @classmethod
    def group_id(cls, id: str) -> "Subject":
        """Select a group by canonical identity-provider ID."""
        return cls("group_id", id)

    @classmethod
    def group_name(cls, name: str) -> "Subject":
        """Select a group by an unambiguous provider name."""
        return cls("group_name", name)

    @classmethod
    def role(cls, name: str) -> "Subject":
        """Select a LanceDB-managed role, without an ``r:`` prefix."""
        return cls("role", name)

    @classmethod
    def principal_api_key(cls, api_key: str) -> "Subject":
        """Select the principal holding this secret API key."""
        return cls("principal_api_key", api_key)

    def __repr__(self) -> str:
        value = self._inner.display_value
        return f"Subject.{self.kind}({value!r})"

    @property
    def value(self) -> str:
        """The selected value, or full wire string for unknown subject kinds.

        API-key values are secrets.
        """
        return self._value

    def __reduce__(self):
        return type(self), (self.kind, self._value)

    def _wire(self) -> str:
        return self._inner.to_wire()

    @classmethod
    def _canonical(cls, value: str) -> "Subject":
        inner = _lancedb.AuthzSubject.from_canonical(value)
        return cls(cast(_SubjectKind, inner.kind), inner.value)


@dataclass(frozen=True)
class Object:
    """An immutable resource name in the authorization syntax.

    Use the builders to avoid interpreting separators as part of a name.
    Methods also accept raw object strings, validated by the same Rust parser.
    Unknown resource types are preserved for compatibility with newer servers.
    """

    _value: str
    _inner: _lancedb.AuthzObject = field(init=False, repr=False, compare=False)

    def __post_init__(self):
        object.__setattr__(self, "_inner", _lancedb.AuthzObject(self._value))

    @classmethod
    def system(cls) -> "Object":
        """The deployment-wide system object."""
        return cls(str(_lancedb.AuthzObject.system()))

    @classmethod
    def database(cls, name: str) -> "Object":
        """A database; slash-containing names remain one logical database."""
        return cls(str(_lancedb.AuthzObject.database(name)))

    @classmethod
    def namespace(cls, *, database: str, namespace_path: Sequence[str]) -> "Object":
        """A namespace path in an explicitly selected database."""
        return cls(str(_lancedb.AuthzObject.namespace(database, namespace_path)))

    @classmethod
    def table(
        cls, *, database: str, name: str, namespace_path: Sequence[str] = ("public",)
    ) -> "Object":
        """A table, defaulting to the database's public namespace."""
        return cls(str(_lancedb.AuthzObject.table(database, namespace_path, name)))

    @classmethod
    def view(
        cls, *, database: str, name: str, namespace_path: Sequence[str] = ("public",)
    ) -> "Object":
        """A view, defaulting to the database's public namespace."""
        return cls(str(_lancedb.AuthzObject.view(database, namespace_path, name)))

    @classmethod
    def secret(
        cls, *, database: str, name: str, namespace_path: Sequence[str] = ("public",)
    ) -> "Object":
        """A secret, defaulting to the database's public namespace."""
        return cls(str(_lancedb.AuthzObject.secret(database, namespace_path, name)))

    @classmethod
    def function(
        cls, *, database: str, name: str, namespace_path: Sequence[str] = ("public",)
    ) -> "Object":
        """A function, defaulting to the database's public namespace."""
        return cls(str(_lancedb.AuthzObject.function(database, namespace_path, name)))

    def __str__(self) -> str:
        return str(self._inner)

    def __reduce__(self):
        return type(self), (self._value,)


class Privilege(str, Enum):
    """Known privileges, validated and serialized by Rust.

    ACL methods also accept strings such as ``"SELECT"`` and future privilege
    names such as ``"PRIVILEGE20"``. Future names are preserved verbatim;
    their meaning and validity are determined by the server.

    Grant ``CREATE_SECRET`` or ``CREATE_FUNCTION`` on a namespace to delegate
    creation of those resources without transferring namespace ownership.
    """

    def __new__(cls, value: str):
        canonical = str(_lancedb.AuthzPrivilege(value))
        member = str.__new__(cls, canonical)
        member._value_ = canonical
        return member

    OWNERSHIP = "OWNERSHIP"
    USAGE = "USAGE"
    SELECT = "SELECT"
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    CREATE_DATABASE = "CREATE_DATABASE"
    CREATE_NAMESPACE = "CREATE_NAMESPACE"
    CREATE_TABLE = "CREATE_TABLE"
    CREATE_MATERIALIZED_VIEW = "CREATE_MATERIALIZED_VIEW"
    CREATE_VIEW = "CREATE_VIEW"
    CREATE_SECRET = "CREATE_SECRET"
    CREATE_FUNCTION = "CREATE_FUNCTION"
    OPERATE = "OPERATE"


@dataclass(frozen=True)
class AccessControlEntry:
    """One object/subject/privilege grant; subject contains a canonical ID or role."""

    object: str
    subject: Subject
    subject_name: Optional[str]
    privilege: str


@dataclass(frozen=True)
class RoleBinding:
    """A direct principal/group role binding, not an expanded effective role."""

    subject: Subject
    subject_name: Optional[str]
    role: str


@dataclass(frozen=True)
class IdentityInfo:
    """A canonical identity-provider ID and its optional display name."""

    id: str
    name: Optional[str] = None


@dataclass(frozen=True)
class PrincipalInfo:
    """A principal's groups and effective roles, including group-inherited roles."""

    principal_id: str
    principal_name: Optional[str]
    groups: list[IdentityInfo]
    roles: list[str]


@dataclass(frozen=True)
class GroupInfo:
    """A group's provider-managed members and LanceDB-managed direct roles."""

    group_id: str
    group_name: Optional[str]
    principals: list[IdentityInfo]
    roles: list[str]


def _subject(subject: Subject, context: Optional[str] = None) -> str:
    if not isinstance(subject, Subject):
        raise ValueError("subject must be a Subject selector")
    return subject._inner.to_wire(context)


def _object(value: Union[str, Object]) -> str:
    if not isinstance(value, (str, Object)) or not str(value):
        raise ValueError("object must be a nonempty string or Object")
    return str(value) if isinstance(value, Object) else str(_lancedb.AuthzObject(value))


def _limit(limit: Optional[int]) -> None:
    if limit is not None and (
        isinstance(limit, bool) or not isinstance(limit, int) or limit < 0
    ):
        raise ValueError("limit must be a nonnegative integer or None")


def _privilege(value: Union[str, Privilege]) -> str:
    if not isinstance(value, str):
        raise ValueError("privilege must be a string or Privilege")
    return str(_lancedb.AuthzPrivilege(value))


def _entry(value: dict) -> AccessControlEntry:
    return AccessControlEntry(
        value["object"],
        Subject._canonical(value["subject_id"]),
        value.get("subject_name"),
        value["privilege"],
    )


def _binding(value: dict) -> RoleBinding:
    return RoleBinding(
        Subject._canonical(value["subject_id"]),
        value.get("subject_name"),
        value["role"],
    )


class AsyncAuthorization:
    """Async authorization returned by ``AsyncCatalog.authz``.

    HTTP failures raise [HttpError][lancedb.remote.errors.HttpError], preserving
    status and request ID. Invalid local arguments raise ValueError. Lists are
    lazy async iterators: failures can occur during iteration after some results
    have been yielded. Iteration does not provide a consistent snapshot.
    """

    def __init__(self, inner: _lancedb.AuthorizationClient):
        self._inner = inner

    async def _pages(self, method, request: dict, key: str, limit: Optional[int]):
        yielded = 0
        while limit is None or yielded < limit:
            request["max_results"] = (
                _PAGE_SIZE if limit is None else min(_PAGE_SIZE, limit - yielded)
            )
            response = json.loads(await method(json.dumps(request)))
            for value in response[key]:
                yield value
                yielded += 1
                if limit is not None and yielded >= limit:
                    return
            token = response.get("continuation_token")
            if not token:
                return
            if token == request.get("continuation_token"):
                raise RuntimeError(
                    "Authorization listing returned a repeated continuation token"
                )
            request["continuation_token"] = token

    def list_acls(
        self,
        *,
        object: Optional[Union[str, Object]] = None,
        subject: Optional[Subject] = None,
        limit: Optional[int] = None,
    ) -> AsyncIterator[AccessControlEntry]:
        """Iterate grants, optionally filtered by exact object and/or subject.

        Requires USAGE on the supplied object, or on system if object is omitted.
        ``limit`` bounds yielded entries across all pages; None lists all and zero
        makes no requests. Missing matches produce an empty iterator.
        """
        _limit(limit)
        request = {}
        if object is not None:
            request["object"] = _object(object)
        if subject is not None:
            request["subject"] = _subject(subject)

        async def entries():
            async for value in self._pages(
                self._inner.list_acls, request, "entries", limit
            ):
                yield _entry(value)

        return entries()

    async def add_acl(
        self,
        *,
        object: Union[str, Object],
        subject: Subject,
        privilege: Union[str, Privilege],
    ) -> None:
        """Grant one privilege; requires OWNERSHIP on the object.

        A duplicate grant raises HTTP 409. Granting OWNERSHIP transfers ownership.
        Writes are not automatically retried after ambiguous failures.
        """
        await self._inner.add_acl(
            json.dumps(
                {
                    "object": _object(object),
                    "subject": _subject(subject),
                    "privilege": _privilege(privilege),
                }
            )
        )

    async def delete_acl(
        self,
        *,
        object: Union[str, Object],
        subject: Subject,
        privilege: Union[str, Privilege],
    ) -> None:
        """Revoke one privilege; requires OWNERSHIP on the object.

        A missing grant raises HTTP 404. OWNERSHIP itself cannot be deleted.
        """
        await self._inner.delete_acl(
            json.dumps(
                {
                    "object": _object(object),
                    "subject": _subject(subject),
                    "privilege": _privilege(privilege),
                }
            )
        )

    def list_role_bindings(
        self,
        *,
        subject: Optional[Subject] = None,
        role: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> AsyncIterator[RoleBinding]:
        """Iterate direct bindings, optionally filtered by principal/group and role.

        Requires USAGE on system. Limit semantics match ``list_acls``. Principals'
        roles inherited through groups are available through ``get_principal``.
        """
        _limit(limit)
        request = {}
        if subject is not None:
            request["subject"] = _subject(subject, "role_member")
        if role is not None:
            request["role"] = role

        async def bindings():
            async for value in self._pages(
                self._inner.list_role_bindings, request, "bindings", limit
            ):
                yield _binding(value)

        return bindings()

    async def add_role_binding(self, *, subject: Subject, role: str) -> None:
        """Grant a role idempotently; requires OPERATE on system."""
        await self._inner.add_role_binding(
            json.dumps({"subject": _subject(subject, "role_member"), "role": role})
        )

    async def delete_role_binding(self, *, subject: Subject, role: str) -> None:
        """Idempotently revoke a role; requires OPERATE on system."""
        await self._inner.delete_role_binding(
            json.dumps({"subject": _subject(subject, "role_member"), "role": role})
        )

    async def get_principal(self, subject: Subject) -> PrincipalInfo:
        """Look up groups and effective roles; requires USAGE on system.

        Select by ID, name, or API key. Missing/ambiguous identities raise HTTP
        404/409; unavailable or unconfigured resolution raises HTTP 503/501.
        """
        value = json.loads(
            await self._inner.get_principal(
                json.dumps({"principal": _subject(subject, "principal")})
            )
        )
        return PrincipalInfo(
            value["principal_id"],
            value.get("principal_name"),
            [IdentityInfo(**group) for group in value["groups"]],
            value["roles"],
        )

    async def get_group(self, subject: Subject) -> GroupInfo:
        """Look up a group's members and roles by ID/name; requires USAGE on system.

        Membership is read-only and comes from the identity provider. Lookup
        errors have the same HTTP semantics as ``get_principal``.
        """
        value = json.loads(
            await self._inner.get_group(
                json.dumps({"group": _subject(subject, "group")})
            )
        )
        return GroupInfo(
            value["group_id"],
            value.get("group_name"),
            [IdentityInfo(**principal) for principal in value["principals"]],
            value["roles"],
        )


async def _next(iterator: AsyncIterator[_T]) -> _T:
    return await iterator.__anext__()


def _sync_iter(iterator: AsyncIterator[_T]) -> Iterator[_T]:
    try:
        while True:
            yield LOOP.run(_next(iterator))
    except StopAsyncIteration:
        return
    finally:
        close = getattr(iterator, "aclose", None)
        if close is not None:
            LOOP.run(close())


class Authorization:
    """Synchronous authorization returned by ``Catalog.authz``.

    Shares the async API's permissions, errors, and lazy pagination behavior.
    Group membership comes from the identity provider and cannot be edited here.
    """

    def __init__(self, inner: AsyncAuthorization):
        self._inner = inner

    def list_acls(
        self,
        *,
        object: Optional[Union[str, Object]] = None,
        subject: Optional[Subject] = None,
        limit: Optional[int] = None,
    ) -> Iterator[AccessControlEntry]:
        """Iterate grants.

        See [list_acls][lancedb.authz.AsyncAuthorization.list_acls] for permissions.
        """
        return _sync_iter(
            self._inner.list_acls(object=object, subject=subject, limit=limit)
        )

    def add_acl(
        self,
        *,
        object: Union[str, Object],
        subject: Subject,
        privilege: Union[str, Privilege],
    ) -> None:
        """Grant a privilege; duplicates raise HTTP 409.

        OWNERSHIP transfers ownership.
        """
        LOOP.run(
            self._inner.add_acl(object=object, subject=subject, privilege=privilege)
        )

    def delete_acl(
        self,
        *,
        object: Union[str, Object],
        subject: Subject,
        privilege: Union[str, Privilege],
    ) -> None:
        """Revoke a privilege. Missing grants raise HTTP 404.

        OWNERSHIP cannot be deleted.
        """
        LOOP.run(
            self._inner.delete_acl(object=object, subject=subject, privilege=privilege)
        )

    def list_role_bindings(
        self,
        *,
        subject: Optional[Subject] = None,
        role: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> Iterator[RoleBinding]:
        """Iterate direct role bindings, using the same limit semantics as list_acls."""
        return _sync_iter(
            self._inner.list_role_bindings(subject=subject, role=role, limit=limit)
        )

    def add_role_binding(self, *, subject: Subject, role: str) -> None:
        """Grant a role idempotently; requires OPERATE on system."""
        LOOP.run(self._inner.add_role_binding(subject=subject, role=role))

    def delete_role_binding(self, *, subject: Subject, role: str) -> None:
        """Idempotently revoke a role; requires OPERATE on system."""
        LOOP.run(self._inner.delete_role_binding(subject=subject, role=role))

    def get_principal(self, subject: Subject) -> PrincipalInfo:
        """Look up groups and effective roles; requires USAGE on system."""
        return LOOP.run(self._inner.get_principal(subject))

    def get_group(self, subject: Subject) -> GroupInfo:
        """Look up a group's members and roles; requires USAGE on system."""
        return LOOP.run(self._inner.get_group(subject))
