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
  AddColumnsSql,
  ConnectionOptions,
  IndexStatistics,
  IndexConfig,
  ClientConfig,
  TimeoutConfig,
  RetryConfig,
  OptimizeStats,
  CompactionStats,
  RemovalStats,
  TableStatistics,
  FragmentStatistics,
  FragmentSummaryStats,
  Tags,
  TagContents,
  MergeResult,
  AddResult,
  AddColumnsResult,
  AlterColumnsResult,
  DeleteResult,
  DropColumnsResult,
  UpdateResult,
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
  OpenTableOptions,
} from "./connection";

export {
  ExecutableQuery,
  Query,
  QueryBase,
  VectorQuery,
  QueryExecutionOptions,
  FullTextSearchOptions,
  RecordBatchIterator,
  FullTextQuery,
  MatchQuery,
  PhraseQuery,
  BoostQuery,
  MultiMatchQuery,
  FullTextQueryType,
} from "./query";

export {
  Index,
  IndexOptions,
  IvfPqOptions,
  IvfFlatOptions,
  HnswPqOptions,
  HnswSqOptions,
  FtsOptions,
} from "./indices";

export {
  Table,
  AddDataOptions,
  UpdateOptions,
  OptimizeOptions,
  Version,
  ColumnAlteration,
} from "./table";

export { MergeInsertBuilder } from "./merge";

export * as embedding from "./embedding";
export * as rerankers from "./rerankers";
export {
  SchemaLike,
  TableLike,
  FieldLike,
  RecordBatchLike,
  DataLike,
  IntoVector,
} from "./arrow";
export { IntoSql, packBits } from "./util";

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
 * @param  options - The options to use when connecting to the database
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
  options?: Partial<ConnectionOptions>,
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
  options: Partial<ConnectionOptions> & { uri: string },
): Promise<Connection>;
export async function connect(
  uriOrOptions: string | (Partial<ConnectionOptions> & { uri: string }),
  options: Partial<ConnectionOptions> = {},
): Promise<Connection> {
  let uri: string | undefined;
  if (typeof uriOrOptions !== "string") {
    const { uri: uri_, ...opts } = uriOrOptions;
    uri = uri_;
    options = opts;
  } else {
    uri = uriOrOptions;
  }

  if (!uri) {
    throw new Error("uri is required");
  }

  options = (options as ConnectionOptions) ?? {};
  (<ConnectionOptions>options).storageOptions = cleanseStorageOptions(
    (<ConnectionOptions>options).storageOptions,
  );
  const nativeConn = await LanceDbConnection.new(uri, options);
  return new LocalConnection(nativeConn);
}
