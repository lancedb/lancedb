# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Lazy, resumable resource listings."""

from __future__ import annotations

import asyncio
from typing import Any, AsyncIterator, Callable, Generic, Iterator, Optional, TypeVar

from .background_loop import LOOP

T = TypeVar("T")


class AsyncListing(AsyncIterator[T], Generic[T]):
    """A lazy async iterator with inspectable pagination state.

    Returned by connection resource-listing methods. For example::

        async for name in connection.list_views(page_limit=20):
            print(name)

    Requests happen during iteration; request errors terminate the iterator.
    """

    def __init__(self, inner: Any, convert: Callable[[Any], T] = lambda value: value):
        self._inner = inner
        self._convert = convert

    def __aiter__(self) -> AsyncListing[T]:
        return self

    async def __anext__(self) -> T:
        return self._convert(await self._inner.__anext__())

    def num_page_results(self) -> int:
        """Return the number of cached items available without another request."""
        return self._inner.num_page_results()

    def page_token(self) -> Optional[str]:
        """Return the token for the next request.

        Initially this is the supplied starting token (``None`` starts at the
        beginning). After fetching the final page it is ``None``, even if items
        remain cached. Drain the cache before saving a token to avoid skipping
        items when resuming. A failed request terminates iteration and retains
        its token for retrying with a new iterator.
        """
        return self._inner.page_token()


class Listing(Iterator[T], Generic[T]):
    """A lazy synchronous iterator with inspectable pagination state.

    For example::

        for name in connection.list_views(page_limit=20):
            print(name)

    Use ``list(connection.list_views())`` to collect all results.
    """

    def __init__(self, inner: AsyncListing[T]):
        self._inner = inner

    def __iter__(self) -> Listing[T]:
        return self

    def __next__(self) -> T:
        try:
            return LOOP.run(self._inner.__anext__())
        except StopAsyncIteration:
            raise StopIteration from None

    def num_page_results(self) -> int:
        """Return the number of cached items available without another request."""
        return self._inner.num_page_results()

    def page_token(self) -> Optional[str]:
        """Return the next request's token.

        See [AsyncListing.page_token][lancedb.listing.AsyncListing.page_token]
        for initial, cached, final-page, and error behavior.
        """
        return self._inner.page_token()


def validate_page_limit(limit: Optional[int]) -> None:
    """Validate the legacy limit argument before passing it to the bindings."""
    if limit is not None and (
        not isinstance(limit, int) or limit < 0 or limit > 2**31 - 1
    ):
        raise ValueError("limit must be between 0 and 2147483647")


class _AsyncPagedNameListing(AsyncIterator[str]):
    """An async listing with an inspectable copy of its current backend page."""

    def __init__(
        self, inner: Any, page_token: Optional[str] = None, *, empty: bool = False
    ):
        self._inner = inner
        self._token = page_token
        self._page: Optional[list[str]] = [] if empty else None
        self._position = 0
        self._done = empty
        self._failure: Optional[BaseException] = None
        self._lock = asyncio.Lock()

    @property
    def page_token(self) -> Optional[str]:
        """The continuation token for the next backend request."""
        return self._token

    def num_page_results(self) -> int:
        """Count the remaining names in the current page."""
        return 0 if self._page is None else len(self._page) - self._position

    def _page_copy(self) -> list[str]:
        if self._page is None:
            raise RuntimeError("Await or iterate the listing before reading its page")
        return self._page.copy()

    async def _fetch_page(self) -> None:
        try:
            page = await self._inner.next_page()
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            self._failure = exc
            self._done = True
            raise
        if page is None:
            self._page = []
            self._token = None
            self._done = True
        else:
            self._page = page["items"]
            self._token = page["page_token"]
            self._done = self._token is None
        self._position = 0

    async def _load_initial(self):
        async with self._lock:
            if self._failure is not None:
                raise self._failure
            if self._page is None:
                await self._fetch_page()
        return self

    def __await__(self):
        return self._load_initial().__await__()

    def __aiter__(self):
        return self

    async def __anext__(self) -> str:
        async with self._lock:
            if self._failure is not None:
                raise StopAsyncIteration
            if self._page is None:
                await self._fetch_page()
            assert self._page is not None
            while self._position >= len(self._page):
                if self._done:
                    raise StopAsyncIteration
                await self._fetch_page()
                assert self._page is not None
            item = self._page[self._position]
            self._position += 1
            return item


class AsyncTableListing(_AsyncPagedNameListing):
    """Awaitable table iterator; awaiting loads one page.

    For example::

        page = await db.list_tables(limit=20)
        print(page.tables, page.page_token)
        async for name in db.list_tables(limit=20):
            print(name)
    """

    @property
    def tables(self) -> list[str]:
        """A copy of every table name in the most recently fetched page."""
        return self._page_copy()


class AsyncNamespaceListing(_AsyncPagedNameListing):
    """Awaitable namespace iterator; awaiting loads one page.

    For example::

        page = await db.list_namespaces(limit=20)
        print(page.namespaces, page.page_token)
    """

    @property
    def namespaces(self) -> list[str]:
        """A copy of every name in the most recently fetched page."""
        return self._page_copy()


class _PagedNameListing(Iterator[str]):
    def __init__(self, inner: _AsyncPagedNameListing):
        self._inner = inner
        LOOP.run(inner._load_initial())

    def __iter__(self):
        return self

    def __next__(self) -> str:
        try:
            return LOOP.run(self._inner.__anext__())
        except StopAsyncIteration:
            raise StopIteration from None

    @property
    def page_token(self) -> Optional[str]:
        return self._inner.page_token

    def num_page_results(self) -> int:
        return self._inner.num_page_results()


class TableListing(_PagedNameListing):
    """Table iterator retaining the current page as a list.

    For example::

        page = db.list_tables(limit=20)
        print(page.tables, page.page_token)
        for name in db.list_tables(limit=20):
            print(name)
    """

    @property
    def tables(self) -> list[str]:
        return self._inner.tables


class NamespaceListing(_PagedNameListing):
    """Namespace iterator retaining the current page as a list.

    For example::

        page = db.list_namespaces(limit=20)
        print(page.namespaces, page.page_token)
    """

    @property
    def namespaces(self) -> list[str]:
        return self._inner.namespaces
