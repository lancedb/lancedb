# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Lazy, resumable resource listings."""

from __future__ import annotations

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
