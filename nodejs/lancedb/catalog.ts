// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import { Connection, LocalConnection } from "./connection";
import { HeaderProvider } from "./header";
import {
  JsHeaderProvider,
  Catalog as NativeCatalog,
  CatalogOptions as NativeCatalogOptions,
  DatabaseNames as NativeDatabaseNames,
} from "./native.js";
import { OAuthConfig } from "./oauth";

/** Options shared by a catalog and the database connections it returns. */
export interface CatalogOptions
  extends Omit<NativeCatalogOptions, "oauthConfig"> {
  oauthConfig?: OAuthConfig;
  /** Called for each request to supply authentication headers. */
  headerProvider?:
    | HeaderProvider
    | (() => Record<string, string> | Promise<Record<string, string>>);
}

/**
 * A lazy async iterator of database names with pagination state.
 * Returned by {@link Catalog.listDatabases}.
 */
export class DatabaseNames implements AsyncIterableIterator<string> {
  /** @hidden */
  constructor(private readonly inner: NativeDatabaseNames) {}

  [Symbol.asyncIterator](): AsyncIterableIterator<string> {
    return this;
  }

  /** Fetch the next name, requesting another page only when needed. */
  async next(): Promise<IteratorResult<string>> {
    const value = await this.inner.next();
    return value == null
      ? { done: true, value: undefined }
      : { done: false, value };
  }

  /** Number of names available without another REST request. */
  numPageResults(): number {
    return this.inner.numPageResults();
  }

  /**
   * Token for the next REST request. Initially this is the supplied starting token.
   * Returns undefined after the final page is fetched, even if names remain cached.
   * Drain the cache before saving a token to avoid skipping names when resuming.
   * A failed request terminates iteration and retains its token for a new iterator.
   */
  pageToken(): string | undefined {
    return this.inner.pageToken() ?? undefined;
  }
}

/** A remote catalog manages databases through the server's root namespace. */
export class Catalog {
  /** @hidden */
  constructor(private readonly inner: NativeCatalog) {}

  /** The root namespace endpoint. */
  get uri(): string {
    return this.inner.uri;
  }

  /** Create a database, or open an existing database when existOk is true. */
  async createDatabase(
    name: string,
    options: { existOk?: boolean } = {},
  ): Promise<Connection> {
    return new LocalConnection(
      await this.inner.createDatabase(name, options.existOk),
    );
  }

  /** Connect to an existing database by its logical name. */
  async connectDatabase(name: string): Promise<Connection> {
    return new LocalConnection(await this.inner.connectDatabase(name));
  }

  /** Drop an empty database. The server rejects nonempty databases. */
  async dropDatabase(
    name: string,
    options: { ignoreMissing?: boolean } = {},
  ): Promise<void> {
    await this.inner.dropDatabase(name, options.ignoreMissing);
  }

  /**
   * Iterate lazily over all database names using `for await...of`.
   * pageToken resumes from a saved token; omitted starts at the beginning.
   * pageLimit limits each REST response, not the total; omitted uses the server default.
   * Request errors are raised during iteration and terminate the iterator.
   *
   * @example
   * ```ts
   * for await (const name of catalog.listDatabases({ pageLimit: 20 })) {
   *   console.log(name);
   * }
   * ```
   */
  listDatabases(
    options: { pageLimit?: number; pageToken?: string } = {},
  ): DatabaseNames {
    if (
      options.pageLimit !== undefined &&
      (!Number.isInteger(options.pageLimit) ||
        options.pageLimit <= 0 ||
        options.pageLimit > 2147483647)
    ) {
      throw new Error(
        "Database list limit must be an integer between 1 and 2147483647",
      );
    }
    return new DatabaseNames(
      this.inner.listDatabases(options.pageToken, options.pageLimit),
    );
  }
}

/**
 * Connect to an HTTP(S) catalog endpoint. Catalog requests omit database-selection
 * headers; opened database connections inherit authentication and client options.
 *
 * @example
 * ```ts
 * const catalog = await connectCatalog("https://my-server.example", { apiKey: "secret" });
 * const db = await catalog.createDatabase("analytics", { existOk: true });
 * for await (const name of catalog.listDatabases({ pageLimit: 20 })) {
 *   console.log(name);
 * }
 * ```
 */
export async function connectCatalog(
  endpoint: string,
  options: CatalogOptions = {},
): Promise<Catalog> {
  const { headerProvider, ...nativeOptions } = options;
  const provider = headerProvider
    ? new JsHeaderProvider(async () =>
        typeof headerProvider === "function"
          ? headerProvider()
          : headerProvider.getHeaders(),
      )
    : undefined;
  return new Catalog(
    await NativeCatalog.new(endpoint, nativeOptions, provider),
  );
}
