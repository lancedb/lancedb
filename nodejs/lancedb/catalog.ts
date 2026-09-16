// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import { Connection, LocalConnection } from "./connection";
import { HeaderProvider } from "./header";
import {
  JsHeaderProvider,
  ListDatabasesResponse,
  Catalog as NativeCatalog,
  CatalogOptions as NativeCatalogOptions,
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

export type { ListDatabasesResponse } from "./native.js";

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

  /** List one page of databases; pass pageToken from a response for the next page. */
  async listDatabases(
    options: { limit?: number; pageToken?: string } = {},
  ): Promise<ListDatabasesResponse> {
    if (
      options.limit !== undefined &&
      (!Number.isInteger(options.limit) ||
        options.limit <= 0 ||
        options.limit > 2147483647)
    ) {
      throw new Error(
        "Database list limit must be an integer between 1 and 2147483647",
      );
    }
    return this.inner.listDatabases(options.limit, options.pageToken);
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
 * const page = await catalog.listDatabases({ limit: 20 });
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
