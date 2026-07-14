// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import {
  Connection,
  LocalConnection,
  cleanseStorageOptions,
} from "./connection";

import {
  ConnectNamespaceOptions,
  ConnectionOptions,
  Connection as LanceDbConnection,
  JsHeaderProvider as NativeJsHeaderProvider,
  Session,
} from "./native.js";

import { HeaderProvider } from "./header";

// Re-export native header provider for use with connectWithHeaderProvider
export { JsHeaderProvider as NativeJsHeaderProvider } from "./native.js";

// OpenTelemetry metrics bridge. Only the high-level entry point is public; the
// underlying recorder/catalog/snapshot functions remain internal plumbing that
// `otel.ts` consumes from the native module.
export { instrumentLanceDbMetrics } from "./otel";

export {
  AddColumnsSql,
  ConnectionOptions,
  ConnectNamespaceOptions,
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
  BranchContents,
  MergeResult,
  AddResult,
  AddColumnsResult,
  AlterColumnsResult,
  UpdateFieldMetadataResult,
  DeleteResult,
  DropColumnsResult,
  UpdateResult,
  SplitCalculatedOptions,
  SplitRandomOptions,
  SplitHashOptions,
  SplitSequentialOptions,
  ShuffleOptions,
  OAuthConfig as NativeOAuthConfig,
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
  ListNamespacesOptions,
  CreateNamespaceOptions,
  DropNamespaceOptions,
  ListNamespacesResponse,
  CreateNamespaceResponse,
  DropNamespaceResponse,
  DescribeNamespaceResponse,
  RenameTableOptions,
} from "./connection";

export { Session } from "./native.js";

export {
  ExecutableQuery,
  Query,
  QueryBase,
  VectorQuery,
  TakeQuery,
  QueryExecutionOptions,
  ColumnOrdering,
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
  Branches,
  AddDataOptions,
  UpdateOptions,
  OptimizeOptions,
  Version,
  WriteProgress,
  FtsToken,
  TokenizeFtsQueryOptions,
  LsmWriteSpec,
  ColumnAlteration,
  FieldMetadataUpdate,
} from "./table";

export {
  HeaderProvider,
  StaticHeaderProvider,
  OAuthHeaderProvider,
  TokenResponse,
} from "./header";

export { OAuthConfig, OAuthFlowType } from "./oauth";

export { MergeInsertBuilder, WriteExecutionOptions } from "./merge";

export * as embedding from "./embedding";
export { permutationBuilder, PermutationBuilder } from "./permutation";
export { Scannable, ScannableOptions } from "./scannable";
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
      nativeProvider = new NativeJsHeaderProvider(async () =>
        finalHeaderProvider(),
      );
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

/**
 * Configuration for the built-in directory namespace (`"dir"`).
 *
 * The directory namespace stores tables under a single root path (local
 * filesystem or object storage URI). See
 * {@link https://docs.lancedb.com/namespaces} for the documented surface;
 * less-common knobs live under {@link DirNamespaceConfig.extraProperties}.
 */
export interface DirNamespaceConfig {
  /** Root path or URI containing the LanceDB tables. */
  root: string;
  /**
   * Whether to maintain a namespace manifest at the root. Required for
   * child namespaces. Defaults to true on the impl side.
   */
  manifestEnabled?: boolean;
  /**
   * Additional raw properties passed verbatim to the namespace
   * implementation (e.g. `storage.*`, `credential_vendor.*`). Typed
   * fields above take precedence on key collision.
   */
  extraProperties?: Record<string, string>;
}

/**
 * Configuration for the built-in REST namespace (`"rest"`).
 *
 * The REST namespace talks to a remote catalog server over HTTP. See
 * {@link https://docs.lancedb.com/namespaces} for the documented surface;
 * less-common knobs (TLS, metrics) live under
 * {@link RestNamespaceConfig.extraProperties}.
 */
export interface RestNamespaceConfig {
  /** Catalog endpoint URL. */
  uri: string;
  /**
   * HTTP headers forwarded with each request. Keys are passed through
   * as-is (e.g. `"x-api-key"`, `"Authorization"`).
   */
  headers?: Record<string, string>;
  /**
   * Additional raw properties passed verbatim to the namespace
   * implementation (e.g. `tls.*`, `ops_metrics_enabled`, `delimiter`).
   * Typed fields above take precedence on key collision.
   */
  extraProperties?: Record<string, string>;
}

function dirConfigToProperties(
  config: DirNamespaceConfig,
): Record<string, string> {
  // Spread the whole input so that unknown keys (e.g. a raw `manifest_enabled`
  // passed via the dynamic-impl path) flow through instead of being dropped.
  // Typed transformations layer on top.
  const { manifestEnabled, extraProperties, ...rest } = config;
  const properties: Record<string, string> = {
    ...(extraProperties ?? {}),
    ...(rest as Record<string, string>),
  };
  if (manifestEnabled !== undefined) {
    properties.manifest_enabled = String(manifestEnabled);
  }
  return properties;
}

function restConfigToProperties(
  config: RestNamespaceConfig,
): Record<string, string> {
  const { headers, extraProperties, ...rest } = config;
  const properties: Record<string, string> = {
    ...(extraProperties ?? {}),
    ...(rest as Record<string, string>),
  };
  if (headers) {
    for (const [name, value] of Object.entries(headers)) {
      properties[`headers.${name}`] = value;
    }
  }
  return properties;
}

/**
 * Connect to a LanceDB database through a namespace.
 *
 * Unlike {@link connect}, which routes by URI scheme (local path vs.
 * `db://` cloud), `connectNamespace` always returns a namespace-backed
 * connection. The `implName` selects the namespace implementation:
 *
 * - `"dir"` — directory namespace, configured with {@link DirNamespaceConfig}.
 * - `"rest"` — remote REST catalog, configured with {@link RestNamespaceConfig}.
 * - Any other string — full module path for a custom implementation,
 *   configured with a free-form string-keyed `properties` map.
 *
 * @example Typed dir namespace
 * ```ts
 * const db = await connectNamespace("dir", { root: "/path/to/db" });
 * await db.createTable("users", [{ id: 1 }]);
 * ```
 *
 * @example Typed REST namespace with auth headers
 * ```ts
 * const db = await connectNamespace("rest", {
 *   uri: "https://catalog.example.com",
 *   headers: { "x-api-key": process.env.CATALOG_KEY ?? "" },
 * });
 * ```
 *
 * @example Custom implementation with raw properties
 * ```ts
 * const db = await connectNamespace("my.custom.Namespace", {
 *   endpoint: "...",
 * });
 * ```
 */
export function connectNamespace(
  implName: "dir",
  config: DirNamespaceConfig,
  options?: Partial<ConnectNamespaceOptions>,
): Promise<Connection>;
/**
 * Connect through the built-in REST namespace.
 *
 * Configured with {@link RestNamespaceConfig}. See the function-level
 * documentation above for the full surface, examples, and how this
 * relates to {@link connect}.
 *
 * @example
 * ```ts
 * const db = await connectNamespace("rest", {
 *   uri: "https://catalog.example.com",
 *   headers: { "x-api-key": process.env.CATALOG_KEY ?? "" },
 * });
 * ```
 */
export function connectNamespace(
  implName: "rest",
  config: RestNamespaceConfig,
  options?: Partial<ConnectNamespaceOptions>,
): Promise<Connection>;
/**
 * Connect through a custom namespace implementation by full module path,
 * configured with a free-form string-keyed `properties` map. Use the
 * typed overloads above for the built-in `"dir"` and `"rest"` impls.
 *
 * See the function-level documentation above for examples and how this
 * relates to {@link connect}.
 *
 * @example
 * ```ts
 * const db = await connectNamespace("my.custom.Namespace", {
 *   endpoint: "...",
 * });
 * ```
 */
export function connectNamespace(
  implName: string,
  properties: Record<string, string>,
  options?: Partial<ConnectNamespaceOptions>,
): Promise<Connection>;
export async function connectNamespace(
  implName: string,
  configOrProperties:
    | DirNamespaceConfig
    | RestNamespaceConfig
    | Record<string, string>,
  options?: Partial<ConnectNamespaceOptions>,
): Promise<Connection> {
  let properties: Record<string, string>;
  if (implName === "dir") {
    properties = dirConfigToProperties(
      configOrProperties as DirNamespaceConfig,
    );
  } else if (implName === "rest") {
    properties = restConfigToProperties(
      configOrProperties as RestNamespaceConfig,
    );
  } else {
    properties = configOrProperties as Record<string, string>;
  }

  const finalOptions: ConnectNamespaceOptions = (options ??
    {}) as ConnectNamespaceOptions;
  finalOptions.storageOptions = cleanseStorageOptions(
    finalOptions.storageOptions,
  );

  const nativeConn = await LanceDbConnection.newWithNamespace(
    implName,
    properties,
    finalOptions,
  );
  return new LocalConnection(nativeConn);
}
