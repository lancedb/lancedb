# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Remote catalogs manage databases through a server's root namespace."""

from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Optional, Union

from . import _lancedb
from .background_loop import LOOP
from .db import AsyncConnection, DBConnection
from .remote import ClientConfig
from .remote.db import RemoteDBConnection


@dataclass
class ListDatabasesResponse:
    """A page of database names and an optional continuation token."""

    databases: list[str]
    page_token: Optional[str] = None


class AsyncCatalog:
    """An asynchronous remote catalog returned by
    [connect_catalog_async][lancedb.connect_catalog_async].

    Create/open return ordinary [AsyncConnection][lancedb.db.AsyncConnection]
    instances. Drop uses restricted behavior: remove the database's tables first.
    """

    def __init__(self, inner: _lancedb.Catalog):
        self._inner = inner

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

    async def open_database(self, name: str) -> AsyncConnection:
        """Open an existing database by its logical name."""
        return AsyncConnection(await self._inner.open_database(name))

    async def list_databases(
        self, *, limit: Optional[int] = None, page_token: Optional[str] = None
    ) -> ListDatabasesResponse:
        """List a page of databases. Pass the returned token for the next page."""
        names, token = await self._inner.list_databases(
            limit=limit, page_token=page_token
        )
        return ListDatabasesResponse(names, token)

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
    page = catalog.list_databases(limit=20)
    ```
    """

    def __init__(
        self,
        inner: AsyncCatalog,
        *,
        api_key=None,
        client_config=None,
        oauth_config=None,
    ):
        self._inner = inner
        self._api_key = api_key
        self._client_config = client_config
        self._oauth_config = oauth_config

    @property
    def uri(self) -> str:
        """The catalog's root namespace endpoint."""
        return self._inner.uri

    def create_database(self, name: str, *, exist_ok: bool = False) -> DBConnection:
        """Create a database, or open an existing one when ``exist_ok=True``."""
        inner = LOOP.run(self._inner.create_database(name, exist_ok=exist_ok))
        return self._wrap_database(name, inner)

    def open_database(self, name: str) -> DBConnection:
        """Open an existing database by its logical name."""
        return self._wrap_database(name, LOOP.run(self._inner.open_database(name)))

    def _wrap_database(self, name: str, inner: AsyncConnection) -> DBConnection:
        return RemoteDBConnection._from_catalog(
            inner,
            name,
            self.uri,
            self._api_key,
            self._client_config,
            self._oauth_config,
        )

    def list_databases(
        self, *, limit: Optional[int] = None, page_token: Optional[str] = None
    ) -> ListDatabasesResponse:
        """List a page of databases. Pass the returned token for the next page."""
        return LOOP.run(self._inner.list_databases(limit=limit, page_token=page_token))

    def drop_database(self, name: str, *, ignore_missing: bool = False) -> None:
        """Drop an empty database. A nonempty database is an error."""
        LOOP.run(self._inner.drop_database(name, ignore_missing=ignore_missing))


async def connect_catalog_async(
    endpoint: str,
    *,
    api_key: Optional[str] = None,
    client_config: Optional[Union[ClientConfig, dict[str, Any]]] = None,
    read_consistency_interval: Optional[timedelta] = None,
    oauth_config=None,
) -> AsyncCatalog:
    """Connect to an HTTP(S) server's root catalog.

    Root requests omit database-selection headers. API key, client configuration,
    OAuth, and table read consistency settings are inherited by opened databases.
    Database names containing slashes remain single logical names.
    """
    if isinstance(client_config, dict):
        client_config = ClientConfig(**client_config)
    inner = await _lancedb.connect_catalog(
        endpoint,
        api_key=api_key,
        client_config=client_config,
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
    read_consistency_interval: Optional[timedelta] = None,
    oauth_config=None,
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
                read_consistency_interval=read_consistency_interval,
                oauth_config=oauth_config,
            )
        ),
        api_key=api_key,
        client_config=client_config,
        oauth_config=oauth_config,
    )
