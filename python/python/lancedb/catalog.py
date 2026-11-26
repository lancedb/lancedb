# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Remote catalogs manage databases."""

import os
import warnings
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from functools import cached_property
from typing import Any, AsyncIterator, Iterator, Optional, Union

from . import _lancedb
from .background_loop import LOOP
from .authz import AsyncAuthorization, Authorization
from .common import URI, sanitize_uri
from .db import AsyncConnection, DBConnection
from .remote import ClientConfig, OAuthConfig
from .remote.db import RemoteDBConnection


class AsyncDatabaseNames(AsyncIterator[str]):
    """An async iterator of database names with pagination state.

    Returned by [list_databases][lancedb.catalog.AsyncCatalog.list_databases].
    """

    def __init__(self, inner: _lancedb.DatabaseNames):
        self._inner = inner

    def __aiter__(self) -> "AsyncDatabaseNames":
        return self

    async def __anext__(self) -> str:
        return await self._inner.__anext__()

    def num_page_results(self) -> int:
        """Return the number of names available without another REST request."""
        return self._inner.num_page_results()

    def page_token(self) -> Optional[str]:
        """Return the token for the next REST request.

        Before the first request, this is the supplied starting token (``None``
        starts at the beginning). After the final page is fetched, it is ``None``,
        even if names remain cached. Drain the cache before saving a token to
        avoid skipping those names when resuming. A failed request terminates
        iteration and leaves its token available for resuming a new iterator.
        """
        return self._inner.page_token()


class DatabaseNames(Iterator[str]):
    """A synchronous iterator of database names with pagination state.

    Returned by [Catalog.list_databases][lancedb.catalog.Catalog.list_databases].
    """

    def __init__(self, inner: AsyncDatabaseNames):
        self._inner = inner

    def __iter__(self) -> "DatabaseNames":
        return self

    def __next__(self) -> str:
        try:
            return LOOP.run(self._inner.__anext__())
        except StopAsyncIteration:
            raise StopIteration from None

    def num_page_results(self) -> int:
        """Return the number of names available without another REST request."""
        return self._inner.num_page_results()

    def page_token(self) -> Optional[str]:
        """Return the token for the next REST request.

        See [page_token][lancedb.catalog.AsyncDatabaseNames.page_token]
        for initial, cached, final-page, and error behavior.
        """
        return self._inner.page_token()


class AsyncCatalog:
    """An asynchronous remote catalog returned by
    [connect_catalog_async][lancedb.connect_catalog_async].

    Create/connect return ordinary [AsyncConnection][lancedb.db.AsyncConnection]
    instances. Drop uses restricted behavior: remove the database's tables first.
    """

    def __init__(self, inner: _lancedb.Catalog):
        self._inner = inner

    @cached_property
    def authz(self) -> AsyncAuthorization:
        """Authorization sharing this catalog's scope and credentials."""
        return AsyncAuthorization(self._inner.authz)

    @property
    def uri(self) -> str:
        """The catalog's root namespace endpoint."""
        return self._inner.uri

    async def create_database(
        self, name: str, *, exist_ok: bool = False
    ) -> AsyncConnection:
        """Create a database, or open an existing one when ``exist_ok=True``."""
        return AsyncConnection(
            await self._inner.create_database(name, exist_ok=exist_ok)
        )

    async def connect_database(self, name: str) -> AsyncConnection:
        """Connect to an existing database by its logical name."""
        return AsyncConnection(await self._inner.connect_database(name))

    def list_databases(
        self, *, page_token: Optional[str] = None, page_limit: Optional[int] = None
    ) -> AsyncDatabaseNames:
        """Iterate lazily over all database names using ``async for``.

        ``page_token`` resumes from a saved token; ``None`` starts at the beginning.
        ``page_limit`` sets the maximum names per REST response (not the total);
        ``None`` uses the server default. Request errors are raised during iteration.

        Examples
        --------
        ```python
        async for name in catalog.list_databases():
            print(name)
        ```
        """
        return AsyncDatabaseNames(
            self._inner.list_databases(page_token=page_token, page_limit=page_limit)
        )

    async def drop_database(
        self, name: str, *, ignore_missing: bool = False, cascade: bool = False
    ) -> None:
        """Drop a database. A nonempty database is an error unless ``cascade`` is set,
        which drops everything in it first; the name stays taken until the server
        finishes cleaning up their storage."""
        await self._inner.drop_database(
            name, ignore_missing=ignore_missing, cascade=cascade
        )


class Catalog:
    """A synchronous remote catalog returned by
    [connect_catalog][lancedb.connect_catalog].

    Examples
    --------
    ```python
    catalog = lancedb.connect_catalog("https://my-server.example", api_key="secret")
    db = catalog.create_database("analytics", exist_ok=True)
    for name in catalog.list_databases():
        print(name)
    ```
    """

    def __init__(
        self,
        inner: AsyncCatalog,
        *,
        api_key=None,
        client_config=None,
        sql_host_override: Optional[str] = None,
        oauth_config: Optional[OAuthConfig] = None,
        storage_options: Optional[dict[str, str]] = None,
    ):
        self._inner = inner
        self._api_key = api_key
        self._client_config = client_config
        self._sql_host_override = sql_host_override
        self._oauth_config = oauth_config
        self._storage_options = storage_options

    @cached_property
    def authz(self) -> Authorization:
        """Authorization sharing this catalog's scope and credentials."""
        return Authorization(self._inner.authz)

    @property
    def uri(self) -> str:
        """The catalog's root namespace endpoint."""
        return self._inner.uri

    def create_database(self, name: str, *, exist_ok: bool = False) -> DBConnection:
        """Create a database, or open an existing one when ``exist_ok=True``."""
        inner = LOOP.run(self._inner.create_database(name, exist_ok=exist_ok))
        return self._wrap_database(name, inner)

    def connect_database(self, name: str) -> DBConnection:
        """Connect to an existing database by its logical name."""
        return self._wrap_database(name, LOOP.run(self._inner.connect_database(name)))

    def _wrap_database(self, name: str, inner: AsyncConnection) -> DBConnection:
        return RemoteDBConnection._from_catalog(
            inner,
            name,
            self.uri,
            self._api_key,
            self._client_config,
            self._oauth_config,
            self._sql_host_override,
            self._storage_options,
        )

    def list_databases(
        self, *, page_token: Optional[str] = None, page_limit: Optional[int] = None
    ) -> DatabaseNames:
        """Iterate lazily over database names, fetching more results as needed.

        ``page_token`` resumes from a saved token; ``None`` starts at the beginning.
        ``page_limit`` sets the maximum names per REST response (not the total);
        ``None`` uses the server default. Request errors are raised during iteration.
        """
        return DatabaseNames(
            self._inner.list_databases(page_token=page_token, page_limit=page_limit)
        )

    def drop_database(
        self, name: str, *, ignore_missing: bool = False, cascade: bool = False
    ) -> None:
        """Drop a database. A nonempty database is an error unless ``cascade`` is set,
        which drops everything in it first; the name stays taken until the server
        finishes cleaning up their storage."""
        LOOP.run(
            self._inner.drop_database(
                name, ignore_missing=ignore_missing, cascade=cascade
            )
        )


async def connect_catalog_async(
    uri: Optional[URI] = None,
    *,
    api_key: Optional[str] = None,
    region: Optional[str] = None,
    host_override: Optional[str] = None,
    sql_host_override: Optional[str] = None,
    read_consistency_interval: Optional[timedelta] = None,
    client_config: Optional[Union[ClientConfig, dict[str, Any]]] = None,
    storage_options: Optional[dict[str, str]] = None,
    session: Optional[_lancedb.Session] = None,
    manifest_enabled: bool = False,
    namespace_client_properties: Optional[dict[str, str]] = None,
    oauth_config: Optional[OAuthConfig] = None,
) -> AsyncCatalog:
    """Connect to a server's root catalog using database connection settings.

    ``host_override`` selects the HTTP(S) catalog endpoint when provided; otherwise
    ``uri`` must be an HTTP(S) endpoint. A ``db://`` URI can be shared with
    [connect_async][lancedb.connect_async] when ``host_override`` is set: the URI's
    database is ignored and requests target the root catalog.

    ``region`` is ignored with a warning when supplied. Catalog connections use
    the explicit endpoint rather than a region-derived Cloud (legacy) endpoint.
    ``api_key`` defaults to the ``LANCEDB_API_KEY`` environment variable.
    Embedded-only ``session``, ``manifest_enabled`` and
    ``namespace_client_properties`` are not supported.

    Root requests omit database-selection headers. API key, client configuration,
    OAuth, storage options, and table read consistency settings are inherited by
    opened databases.
    Database names containing slashes remain single logical names.
    Set ``sql_host_override`` to the SQL service endpoint to execute SQL through
    returned connections when the catalog endpoint uses HTTPS.
    """
    if region is not None:
        warnings.warn(
            "region is ignored by connect_catalog_async; the catalog endpoint "
            "is selected by host_override or uri.",
            UserWarning,
            stacklevel=2,
        )
    for name, value in (
        ("session", session),
        ("manifest_enabled", manifest_enabled or None),
        ("namespace_client_properties", namespace_client_properties),
    ):
        if value is not None:
            raise NotImplementedError(
                f"{name} is not supported for catalog connections"
            )
    endpoint = host_override if host_override is not None else uri
    if endpoint is None:
        raise ValueError("uri or host_override is required for catalog connections")
    if isinstance(client_config, dict):
        client_config = ClientConfig(**client_config)
    if client_config is None:
        client_config = ClientConfig()
    if (
        api_key is None
        and oauth_config is None
        and client_config.header_provider is None
    ):
        api_key = os.environ.get("LANCEDB_API_KEY")
    inner = await _lancedb.connect_catalog(
        sanitize_uri(endpoint),
        api_key=api_key,
        client_config=client_config,
        sql_host_override=sql_host_override,
        read_consistency_interval=(
            read_consistency_interval.total_seconds()
            if read_consistency_interval is not None
            else None
        ),
        oauth_config=oauth_config,
        storage_options=storage_options,
    )
    return AsyncCatalog(inner)


def connect_catalog(
    uri: Optional[URI] = None,
    *,
    api_key: Optional[str] = None,
    region: Optional[str] = None,
    host_override: Optional[str] = None,
    sql_host_override: Optional[str] = None,
    read_consistency_interval: Optional[timedelta] = None,
    request_thread_pool: Optional[Union[int, ThreadPoolExecutor]] = None,
    client_config: Optional[Union[ClientConfig, dict[str, Any]]] = None,
    storage_options: Optional[dict[str, str]] = None,
    session: Optional[_lancedb.Session] = None,
    manifest_enabled: bool = False,
    namespace_client_impl: Optional[str] = None,
    namespace_client_properties: Optional[dict[str, str]] = None,
    namespace_client_pushdown_operations: Optional[list[str]] = None,
    oauth_config: Optional[OAuthConfig] = None,
    **kwargs: Any,
) -> Catalog:
    """Connect synchronously to a root catalog using database connection settings.

    See [connect_catalog_async][lancedb.connect_catalog_async] for options.
    The inputs match [connect][lancedb.connect]; ``region`` is ignored with a
    warning when supplied. Namespace client options and embedded session settings
    are not supported. ``request_thread_pool`` is deprecated and unused, as in
    ``connect``.
    Local filesystem and object-store catalogs are not supported.

    Examples
    --------
    ```python
    settings = {"api_key": "secret", "host_override": "https://my-server.example"}
    catalog = lancedb.connect_catalog("db://analytics", **settings)
    db = lancedb.connect("db://analytics", **settings)
    ```
    """
    unknown_kwargs = {
        key: value
        for key, value in kwargs.items()
        if key not in ("connection_timeout", "read_timeout")
    }
    if unknown_kwargs:
        raise ValueError(f"Unknown keyword arguments: {unknown_kwargs}")
    for name, value in (
        ("namespace_client_impl", namespace_client_impl),
        ("namespace_client_pushdown_operations", namespace_client_pushdown_operations),
    ):
        if value is not None:
            raise NotImplementedError(
                f"{name} is not supported for catalog connections"
            )
    if region is not None:
        warnings.warn(
            "region is ignored by connect_catalog; the catalog endpoint "
            "is selected by host_override or uri.",
            UserWarning,
            stacklevel=2,
        )
    if request_thread_pool is not None:
        warnings.warn(
            "request_thread_pool is no longer used and will be removed in "
            "a future release.",
            DeprecationWarning,
            stacklevel=2,
        )
    if isinstance(client_config, dict):
        client_config = ClientConfig(**client_config)
    if client_config is None:
        client_config = ClientConfig()
    for name, field in (
        ("connection_timeout", "connect_timeout"),
        ("read_timeout", "read_timeout"),
    ):
        if kwargs.get(name) is not None:
            warnings.warn(
                f"{name} is deprecated and will be removed in a future release. "
                f"Please use client_config.timeout_config.{field} instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            setattr(
                client_config.timeout_config,
                field,
                timedelta(seconds=kwargs[name]),
            )
    if (
        api_key is None
        and oauth_config is None
        and client_config.header_provider is None
    ):
        api_key = os.environ.get("LANCEDB_API_KEY")
    return Catalog(
        LOOP.run(
            connect_catalog_async(
                uri,
                api_key=api_key,
                host_override=host_override,
                client_config=client_config,
                sql_host_override=sql_host_override,
                read_consistency_interval=read_consistency_interval,
                storage_options=storage_options,
                session=session,
                manifest_enabled=manifest_enabled,
                namespace_client_properties=namespace_client_properties,
                oauth_config=oauth_config,
            )
        ),
        api_key=api_key,
        client_config=client_config,
        sql_host_override=sql_host_override,
        oauth_config=oauth_config,
        storage_options=storage_options,
    )
