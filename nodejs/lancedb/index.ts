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
  JsHeaderProvider as NativeJsHeaderProvider,
  Session,
} from "./native.js";

import { HeaderProvider } from "./header";

// Re-export native header provider for use with connectWithHeaderProvider
export { JsHeaderProvider as NativeJsHeaderProvider } from "./native.js";

export {
  AddColumnsSql,
  ConnectionOptions,
  IndexStatistics,
  IndexConfig,
  ClientConfig,
  TimeoutConfig,
  RetryConfig,
  TlsConfig,
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
  SplitRandomOptions,
  SplitHashOptions,
  SplitSequentialOptions,
  ShuffleOptions,
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

export { Session } from "./native.js";

export {
  ExecutableQuery,
  Query,
  QueryBase,
  VectorQuery,
  TakeQuery,
  QueryExecutionOptions,
  FullTextSearchOptions,
  RecordBatchIterator,
  FullTextQuery,
  MatchQuery,
  PhraseQuery,
  BoostQuery,
  MultiMatchQuery,
  BooleanQuery,
  FullTextQueryType,
  Operator,
  Occur,
} from "./query";

export {
  Index,
  IndexOptions,
  IvfPqOptions,
  IvfRqOptions,
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

export {
  HeaderProvider,
  StaticHeaderProvider,
  OAuthHeaderProvider,
  TokenResponse,
} from "./header";

export { MergeInsertBuilder, WriteExecutionOptions } from "./merge";

export * as embedding from "./embedding";
export { permutationBuilder, PermutationBuilder } from "./permutation";
export * as rerankers from "./rerankers";
export {
  SchemaLike,
  TableLike,
  FieldLike,
  RecordBatchLike,
  DataLike,
  IntoVector,
  MultiVector,
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
 * @example
 * Using with a header provider for per-request authentication:
 * ```ts
 * const provider = new StaticHeaderProvider({
 *   "X-API-Key": "my-key"
 * });
 * const conn = await connectWithHeaderProvider(
 *   "db://host:port",
 *   options,
 *   provider
 * );
 * ```
 */
export async function connect(
  uri: string,
  options?: Partial<ConnectionOptions>,
  session?: Session,
  headerProvider?:
    | HeaderProvider
    | (() => Record<string, string>)
    | (() => Promise<Record<string, string>>),
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
 *
 * @example
 * ```ts
 * const session = Session.default();
 * const conn = await connect({
 *   uri: "/path/to/database",
 *   session: session
 * });
 * ```
 */
export async function connect(
  options: Partial<ConnectionOptions> & { uri: string },
): Promise<Connection>;
export async function connect(
  uriOrOptions: string | (Partial<ConnectionOptions> & { uri: string }),
  optionsOrSession?: Partial<ConnectionOptions> | Session,
  sessionOrHeaderProvider?:
    | Session
    | HeaderProvider
    | (() => Record<string, string>)
    | (() => Promise<Record<string, string>>),
  headerProvider?:
    | HeaderProvider
    | (() => Record<string, string>)
    | (() => Promise<Record<string, string>>),
): Promise<Connection> {
  let uri: string | undefined;
  let finalOptions: Partial<ConnectionOptions> = {};
  let finalHeaderProvider:
    | HeaderProvider
    | (() => Record<string, string>)
    | (() => Promise<Record<string, string>>)
    | undefined;

  if (typeof uriOrOptions !== "string") {
    // First overload: connect(options)
    const { uri: uri_, ...opts } = uriOrOptions;
    uri = uri_;
    finalOptions = opts;
  } else {
    // Second overload: connect(uri, options?, session?, headerProvider?)
    uri = uriOrOptions;

    // Handle optionsOrSession parameter
    if (optionsOrSession && "inner" in optionsOrSession) {
      // Second param is session, so no options provided
      finalOptions = {};
    } else {
      // Second param is options
      finalOptions = (optionsOrSession as Partial<ConnectionOptions>) || {};
    }

    // Handle sessionOrHeaderProvider parameter
    if (
      sessionOrHeaderProvider &&
      (typeof sessionOrHeaderProvider === "function" ||
        "getHeaders" in sessionOrHeaderProvider)
    ) {
      // Third param is header provider
      finalHeaderProvider = sessionOrHeaderProvider as
        | HeaderProvider
        | (() => Record<string, string>)
        | (() => Promise<Record<string, string>>);
    } else {
      // Third param is session, header provider is fourth param
      finalHeaderProvider = headerProvider;
    }
  }

  if (!uri) {
    throw new Error("uri is required");
  }

  finalOptions = (finalOptions as ConnectionOptions) ?? {};
  (<ConnectionOptions>finalOptions).storageOptions = cleanseStorageOptions(
    (<ConnectionOptions>finalOptions).storageOptions,
  );

  // Create native header provider if one was provided
  let nativeProvider: NativeJsHeaderProvider | undefined;
  if (finalHeaderProvider) {
    if (typeof finalHeaderProvider === "function") {
      nativeProvider = new NativeJsHeaderProvider(finalHeaderProvider);
    } else if (
      finalHeaderProvider &&
      typeof finalHeaderProvider.getHeaders === "function"
    ) {
      nativeProvider = new NativeJsHeaderProvider(async () =>
        finalHeaderProvider.getHeaders(),
      );
    }
  }

  const nativeConn = await LanceDbConnection.new(
    uri,
    finalOptions,
    nativeProvider,
  );
  return new LocalConnection(nativeConn);
}
