// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import {
  Connection,
  LocalConnection,
  cleanseStorageOptions,
} from "./connection";

import {
  ConnectionOptions,
  Connection as LanceDbConnection,
} from "./native.js";

export {
  WriteOptions,
  WriteMode,
  AddColumnsSql,
  ColumnAlteration,
  ConnectionOptions,
  IndexStatistics,
  IndexConfig,
  ClientConfig,
  TimeoutConfig,
  RetryConfig,
} from "./native.js";

export {
  makeArrowTable,
  MakeArrowTableOptions,
  Data,
  VectorColumnOptions,
} from "./arrow";

export {
  Connection,
  CreateTableOptions,
  TableNamesOptions,
} from "./connection";

export {
  ExecutableQuery,
  Query,
  QueryBase,
  VectorQuery,
  RecordBatchIterator,
} from "./query";

export { Index, IndexOptions, IvfPqOptions } from "./indices";

export { Table, AddDataOptions, UpdateOptions, OptimizeOptions } from "./table";

export * as embedding from "./embedding";
export * as rerankers from "./rerankers";

/**
 * Connect to a LanceDB instance at the given URI.
 *
 * Accepted formats:
 *
 * - `/path/to/database` - local database
 * - `s3://bucket/path/to/database` or `gs://bucket/path/to/database` - database on cloud storage
 * - `db://host:port` - remote database (LanceDB cloud)
 * @param {string} uri - The uri of the database. If the database uri starts
 * with `db://` then it connects to a remote database.
 * @see {@link ConnectionOptions} for more details on the URI format.
 * @example
 * ```ts
 * const conn = await connect("/path/to/database");
 * ```
 * @example
 * ```ts
 * const conn = await connect(
 *   "s3://bucket/path/to/database",
 *   {storageOptions: {timeout: "60s"}
 * });
 * ```
 */
export async function connect(
  uri: string,
  opts?: Partial<ConnectionOptions>,
): Promise<Connection>;
/**
 * Connect to a LanceDB instance at the given URI.
 *
 * Accepted formats:
 *
 * - `/path/to/database` - local database
 * - `s3://bucket/path/to/database` or `gs://bucket/path/to/database` - database on cloud storage
 * - `db://host:port` - remote database (LanceDB cloud)
 * @param  options - The options to use when connecting to the database
 * @see {@link ConnectionOptions} for more details on the URI format.
 * @example
 * ```ts
 * const conn = await connect({
 *   uri: "/path/to/database",
 *   storageOptions: {timeout: "60s"}
 * });
 * ```
 */
export async function connect(
  opts: Partial<ConnectionOptions> & { uri: string },
): Promise<Connection>;
export async function connect(
  uriOrOptions: string | (Partial<ConnectionOptions> & { uri: string }),
  opts: Partial<ConnectionOptions> = {},
): Promise<Connection> {
  let uri: string | undefined;
  if (typeof uriOrOptions !== "string") {
    const { uri: uri_, ...options } = uriOrOptions;
    uri = uri_;
    opts = options;
  } else {
    uri = uriOrOptions;
  }

  if (!uri) {
    throw new Error("uri is required");
  }

  opts = (opts as ConnectionOptions) ?? {};
  (<ConnectionOptions>opts).storageOptions = cleanseStorageOptions(
    (<ConnectionOptions>opts).storageOptions,
  );
  const nativeConn = await LanceDbConnection.new(uri, opts);
  return new LocalConnection(nativeConn);
}
