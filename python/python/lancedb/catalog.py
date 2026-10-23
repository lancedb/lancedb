# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Remote catalogs manage databases through a server's root namespace."""

from functools import cached_property
from datetime import timedelta
from typing import Any, AsyncIterator, Iterator, Optional, Union

from . import _lancedb
from .background_loop import LOOP
from .authz import AsyncAuthorization, Authorization
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

    async def drop_database(self, name: str, *, ignore_missing: bool = False) -> None:
        """Drop an empty database. A nonempty database is an error."""
        await self._inner.drop_database(name, ignore_missing=ignore_missing)


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
    ):
        self._inner = inner
        self._api_key = api_key
        self._client_config = client_config
        self._sql_host_override = sql_host_override
        self._oauth_config = oauth_config

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

    def drop_database(self, name: str, *, ignore_missing: bool = False) -> None:
        """Drop an empty database. A nonempty database is an error."""
        LOOP.run(self._inner.drop_database(name, ignore_missing=ignore_missing))


async def connect_catalog_async(
    endpoint: str,
    *,
    api_key: Optional[str] = None,
    client_config: Optional[Union[ClientConfig, dict[str, Any]]] = None,
    sql_host_override: Optional[str] = None,
    read_consistency_interval: Optional[timedelta] = None,
    oauth_config: Optional[OAuthConfig] = None,
) -> AsyncCatalog:
    """Connect to an HTTP(S) server's root catalog.

    Root requests omit database-selection headers. API key, client configuration,
    OAuth, and table read consistency settings are inherited by opened databases.
    Database names containing slashes remain single logical names.
    Set ``sql_host_override`` to the SQL service endpoint to execute SQL through
    returned connections when the catalog endpoint uses HTTPS.
    """
    if isinstance(client_config, dict):
        client_config = ClientConfig(**client_config)
    if client_config is None:
        client_config = ClientConfig()
    inner = await _lancedb.connect_catalog(
        endpoint,
        api_key=api_key,
        client_config=client_config,
        sql_host_override=sql_host_override,
        read_consistency_interval=(
            read_consistency_interval.total_seconds()
            if read_consistency_interval is not None
            else None
        ),
        oauth_config=oauth_config,
    )
    return AsyncCatalog(inner)


def connect_catalog(
    endpoint: str,
    *,
    api_key: Optional[str] = None,
    client_config: Optional[Union[ClientConfig, dict[str, Any]]] = None,
    sql_host_override: Optional[str] = None,
    read_consistency_interval: Optional[timedelta] = None,
    oauth_config: Optional[OAuthConfig] = None,
) -> Catalog:
    """Connect synchronously to an HTTP(S) server's root catalog.

    See [connect_catalog_async][lancedb.connect_catalog_async] for options.
    Local filesystem and object-store catalogs are not supported.
    """
    return Catalog(
        LOOP.run(
            connect_catalog_async(
                endpoint,
                api_key=api_key,
                client_config=client_config,
                sql_host_override=sql_host_override,
                read_consistency_interval=read_consistency_interval,
                oauth_config=oauth_config,
            )
        ),
        api_key=api_key,
        client_config=client_config,
        sql_host_override=sql_host_override,
        oauth_config=oauth_config,
    )
