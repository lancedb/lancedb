// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

/** Pagination controls for lazy resource listings. */
export interface ListingOptions {
  /** Starting continuation token; omitted starts at the beginning. */
  pageToken?: string;
  /** Maximum results per request, not a limit on the whole listing. */
  pageLimit?: number;
}

/** @internal */
export function validateListingOptions(options: ListingOptions): void {
  if (
    options.pageLimit !== undefined &&
    (!Number.isInteger(options.pageLimit) ||
      options.pageLimit <= 0 ||
      options.pageLimit > 2147483647)
  ) {
    throw new Error(
      "Listing page limit must be an integer between 1 and 2147483647",
    );
  }
}

/**
 * A lazy async iterator over database resources with pagination state.
 * Requests occur during iteration; request errors terminate the iterator.
 *
 * @example
 * ```ts
 * for await (const name of connection.listViews([], { pageLimit: 20 })) {
 *   console.log(name);
 * }
 * ```
 */
export class Listing<T> implements AsyncIterableIterator<T> {
  /** @hidden */
  constructor(
    private readonly inner: {
      next(): Promise<T | null | undefined>;
      numPageResults(): number;
      pageToken(): string | null | undefined;
    },
  ) {}

  [Symbol.asyncIterator](): AsyncIterableIterator<T> {
    return this;
  }

  /** Fetch the next item, requesting another page only when needed. */
  async next(): Promise<IteratorResult<T>> {
    const value = await this.inner.next();
    return value == null
      ? { done: true, value: undefined }
      : { done: false, value };
  }

  /** Number of cached items available without another request. */
  numPageResults(): number {
    return this.inner.numPageResults();
  }

  /**
   * Token for the next request. Initially this is the supplied starting token.
   * Returns undefined after fetching the final page, even with cached items.
   * Drain the cache before saving a token to avoid skipping items on resumption.
   * Failed requests terminate iteration and retain their token for a new iterator.
   */
  pageToken(): string | undefined {
    return this.inner.pageToken() ?? undefined;
  }
}
