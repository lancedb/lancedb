// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

/**
 * Typed errors for the Node SDK.
 *
 * Rust errors used to arrive as one `Error` whose message was the Display text
 * plus an indented `Caused by` chain. Native code now records the LanceDB error
 * kind and its fields in a one-line header, and {@link installLanceErrorTranslation}
 * turns that into one of the classes below before application code sees it.
 *
 * `error.code` is the Rust variant name (`"TableNotFound"`, `"Http"`, ...).
 * `error.message` is that variant's own message, without the nested cause text.
 * {@link LanceError} shows how to branch with `instanceof`.
 */

const HEADER = /^LanceError (\{.*\})(?:\n|$)/;
const MAX_CAUSE_DEPTH = 8;
/** Per-load marker so one SDK load does not wrap the same function twice. */
const WRAPPED = Symbol("lancedb.errorWrapped");
/**
 * Points a wrapper at the native function it replaced. Shared across SDK loads
 * so a later load wraps that function instead of stacking another wrapper.
 */
const ORIGINAL_FN = Symbol.for("lancedb.originalNativeFn");
/**
 * WeakMap of native objects to the translator for the SDK load that owns them.
 * Stored on the shared native constructor so every load sees the same map.
 */
const TRANSLATORS = Symbol.for("lancedb.errorTranslators");

type ErrorTranslator = (error: unknown) => unknown;

type TranslatorHost = {
  [TRANSLATORS]?: WeakMap<object, ErrorTranslator>;
};
/**
 * Marks a translated LanceDB error. A later SDK load cannot use `instanceof`
 * against a class from an earlier load, but this symbol is shared by the
 * process, so the later load can rebuild its own subclass.
 */
export const LANCE_ERROR_MARK = Symbol.for("lancedb.LanceError");

const METADATA_KEYS = [
  "requestId",
  "statusCode",
  "errorCode",
  "resourceKind",
  "resourceName",
  "namespacePath",
  "tableName",
  "columnName",
  "databaseName",
  "indexName",
  "functionName",
  "path",
  "jobId",
  "sqlJobId",
  "jobRequestId",
  "grpcCode",
  "phase",
  "retryable",
  "requestFailures",
  "maxRequestFailures",
  "connectFailures",
  "maxConnectFailures",
  "readFailures",
  "maxReadFailures",
] as const;

type AnyFn = (...args: unknown[]) => unknown;

export interface LanceErrorOptions extends ErrorOptions {
  /** Rust variant name. Subclasses set this from their static {@link LanceError.code}. */
  code?: string;
  requestId?: string;
  statusCode?: number;
  /** Machine-readable code reported by a remote server, when it sent one. */
  errorCode?: string;
  resourceKind?: string;
  resourceName?: string;
  namespacePath?: string[];
  tableName?: string;
  columnName?: string;
  databaseName?: string;
  indexName?: string;
  functionName?: string;
  path?: string;
  jobId?: string;
  sqlJobId?: string;
  jobRequestId?: string;
  grpcCode?: number;
  phase?: string;
  retryable?: boolean;
  requestFailures?: number;
  maxRequestFailures?: number;
  connectFailures?: number;
  maxConnectFailures?: number;
  readFailures?: number;
  maxReadFailures?: number;
}

interface ErrorPayload {
  code?: unknown;
  requestId?: unknown;
  statusCode?: unknown;
  errorCode?: unknown;
  resourceKind?: unknown;
  resourceName?: unknown;
  namespacePath?: unknown;
  tableName?: unknown;
  columnName?: unknown;
  databaseName?: unknown;
  indexName?: unknown;
  functionName?: unknown;
  path?: unknown;
  jobId?: unknown;
  sqlJobId?: unknown;
  jobRequestId?: unknown;
  grpcCode?: unknown;
  phase?: unknown;
  retryable?: unknown;
  requestFailures?: unknown;
  maxRequestFailures?: unknown;
  connectFailures?: unknown;
  maxConnectFailures?: unknown;
  readFailures?: unknown;
  maxReadFailures?: unknown;
}

/**
 * Base class for errors raised by the LanceDB native core.
 *
 * Subclasses exist so application code can branch with `instanceof`. A kind
 * that does not have its own class is still a {@link LanceError}; `code` then
 * carries the Rust variant name.
 *
 * Stream-time failures and native constructor validation can still arrive as a
 * plain `Error`. Handle that case as a fallback.
 *
 * ```ts
 * import * as lancedb from "@lancedb/lancedb";
 *
 * try {
 *   await db.openTable("missing");
 * } catch (error) {
 *   if (error instanceof lancedb.TableNotFoundError) {
 *     error.tableName; // "missing"
 *     error.cause; // underlying error, when Rust supplied one
 *   } else if (error instanceof Error) {
 *     // Stream-time failures and native constructor validation can still
 *     // arrive untyped.
 *     error.message;
 *   }
 * }
 * ```
 */
export class LanceError extends Error {
  static readonly code: string = "Error";

  readonly code: string;
  readonly requestId?: string;
  readonly statusCode?: number;
  readonly errorCode?: string;
  readonly resourceKind?: string;
  readonly resourceName?: string;
  readonly namespacePath?: string[];
  readonly tableName?: string;
  readonly columnName?: string;
  readonly databaseName?: string;
  readonly indexName?: string;
  readonly functionName?: string;
  readonly path?: string;
  readonly jobId?: string;
  readonly sqlJobId?: string;
  readonly jobRequestId?: string;
  readonly grpcCode?: number;
  readonly phase?: string;
  readonly retryable?: boolean;
  readonly requestFailures?: number;
  readonly maxRequestFailures?: number;
  readonly connectFailures?: number;
  readonly maxConnectFailures?: number;
  readonly readFailures?: number;
  readonly maxReadFailures?: number;

  constructor(message: string, options?: LanceErrorOptions) {
    super(message, { cause: options?.cause });
    const ctor = new.target as unknown as { code?: string };
    this.name = new.target.name;
    this.code = options?.code ?? ctor.code ?? LanceError.code;
    this.requestId = options?.requestId;
    this.statusCode = options?.statusCode;
    this.errorCode = options?.errorCode;
    this.resourceKind = options?.resourceKind;
    this.resourceName = options?.resourceName;
    this.namespacePath = options?.namespacePath;
    this.tableName = options?.tableName;
    this.columnName = options?.columnName;
    this.databaseName = options?.databaseName;
    this.indexName = options?.indexName;
    this.functionName = options?.functionName;
    this.path = options?.path;
    this.jobId = options?.jobId;
    this.sqlJobId = options?.sqlJobId;
    this.jobRequestId = options?.jobRequestId;
    this.grpcCode = options?.grpcCode;
    this.phase = options?.phase;
    this.retryable = options?.retryable;
    this.requestFailures = options?.requestFailures;
    this.maxRequestFailures = options?.maxRequestFailures;
    this.connectFailures = options?.connectFailures;
    this.maxConnectFailures = options?.maxConnectFailures;
    this.readFailures = options?.readFailures;
    this.maxReadFailures = options?.maxReadFailures;
    Object.defineProperty(this, LANCE_ERROR_MARK, { value: true });
  }
}

/** Input rejected before it reached storage. */
export class InvalidInputError extends LanceError {
  static override readonly code: string = "InvalidInput";
}

/** A table name failed validation. */
export class InvalidTableNameError extends InvalidInputError {
  static override readonly code = "InvalidTableName";
}

/** A computed-column or SQL expression was rejected. */
export class InvalidExpressionError extends InvalidInputError {
  static override readonly code = "InvalidExpression";
}

/** The data or query does not match the expected schema. */
export class SchemaError extends InvalidInputError {
  static override readonly code = "Schema";
}

/** The named table is not a materialized view. */
export class NotAMaterializedViewError extends InvalidInputError {
  static override readonly code = "NotAMaterializedView";
}

/** The named table does not exist. */
export class TableNotFoundError extends LanceError {
  static override readonly code = "TableNotFound";
}

/** The named table is present but could not be loaded. */
export class TableCorruptedError extends LanceError {
  static override readonly code = "TableCorrupted";
}

/** A table with this name already exists. */
export class TableAlreadyExistsError extends LanceError {
  static override readonly code = "TableAlreadyExists";
}

/** The named database does not exist. */
export class DatabaseNotFoundError extends LanceError {
  static override readonly code = "DatabaseNotFound";
}

/** A database with this name already exists. */
export class DatabaseAlreadyExistsError extends LanceError {
  static override readonly code = "DatabaseAlreadyExists";
}

/** The named index does not exist. */
export class IndexNotFoundError extends LanceError {
  static override readonly code = "IndexNotFound";
}

/** The named embedding function is not registered. */
export class EmbeddingFunctionNotFoundError extends LanceError {
  static override readonly code = "EmbeddingFunctionNotFound";
}

/** The named column does not exist. */
export class ColumnNotFoundError extends LanceError {
  static override readonly code: string = "ColumnNotFound";
}

/**
 * A query referenced a field the schema does not contain.
 *
 * This is also a {@link ColumnNotFoundError}. `code` remains `"FieldNotFound"`
 * so callers can tell a schema-qualified miss from a plain missing column.
 */
export class FieldNotFoundError extends ColumnNotFoundError {
  static override readonly code = "FieldNotFound";
}

/** A column with this name already exists. */
export class ColumnAlreadyExistsError extends LanceError {
  static override readonly code = "ColumnAlreadyExists";
}

/** The named column is not a computed column. */
export class NotAComputedColumnError extends LanceError {
  static override readonly code = "NotAComputedColumn";
}

/** The operation is not implemented for this table or build. */
export class NotSupportedError extends LanceError {
  static override readonly code = "NotSupported";
}

/** A local operation exceeded its time limit. */
export class TimeoutError extends LanceError {
  static override readonly code = "Timeout";
}

/** An unclassified failure inside the LanceDB runtime. */
export class LanceRuntimeError extends LanceError {
  static override readonly code = "Runtime";
}

/** Creating a dataset directory failed. */
export class CreateDirError extends LanceError {
  static override readonly code = "CreateDir";
}

/** Object storage returned an error. */
export class ObjectStoreError extends LanceError {
  static override readonly code = "ObjectStore";
}

/** The Lance format layer returned an error. `code` is `"Lance"`. */
export class LanceFormatError extends LanceError {
  static override readonly code = "Lance";
}

/** Arrow returned an error while reading or writing batches. */
export class LanceArrowError extends LanceError {
  static override readonly code = "Arrow";
}

/** A caller-supplied callback or external library failed. */
export class ExternalError extends LanceError {
  static override readonly code = "External";
}

/** A LanceDB error that does not fit a more specific kind. */
export class OtherError extends LanceError {
  static override readonly code = "Other";
}

/** An HTTP call to LanceDB Cloud or Enterprise failed. */
export class HttpError extends LanceError {
  static override readonly code = "Http";
}

/** A remote call exhausted its retry policy. `cause` is the last HTTP error. */
export class RetryError extends LanceError {
  static override readonly code = "Retry";
}

/** A remote SQL call failed. `grpcCode` is the numeric gRPC status. */
export class SqlError extends LanceError {
  static override readonly code = "Sql";
}

/** An asynchronous job finished in the failed state. */
export class JobFailedError extends LanceError {
  static override readonly code = "JobFailed";
}

/** An asynchronous job was cancelled. */
export class JobCancelledError extends LanceError {
  static override readonly code = "JobCancelled";
}

/** The named job does not exist. */
export class JobNotFoundError extends LanceError {
  static override readonly code = "JobNotFound";
}

type LanceErrorConstructor = new (
  message: string,
  options?: LanceErrorOptions,
) => LanceError;

const ERROR_CLASSES = new Map<string, LanceErrorConstructor>([
  ["InvalidInput", InvalidInputError],
  ["InvalidTableName", InvalidTableNameError],
  ["InvalidExpression", InvalidExpressionError],
  ["Schema", SchemaError],
  ["NotAMaterializedView", NotAMaterializedViewError],
  ["TableNotFound", TableNotFoundError],
  ["TableCorrupted", TableCorruptedError],
  ["TableAlreadyExists", TableAlreadyExistsError],
  ["DatabaseNotFound", DatabaseNotFoundError],
  ["DatabaseAlreadyExists", DatabaseAlreadyExistsError],
  ["IndexNotFound", IndexNotFoundError],
  ["EmbeddingFunctionNotFound", EmbeddingFunctionNotFoundError],
  ["ColumnNotFound", ColumnNotFoundError],
  ["FieldNotFound", FieldNotFoundError],
  ["ColumnAlreadyExists", ColumnAlreadyExistsError],
  ["NotAComputedColumn", NotAComputedColumnError],
  ["NotSupported", NotSupportedError],
  ["Timeout", TimeoutError],
  ["Runtime", LanceRuntimeError],
  ["CreateDir", CreateDirError],
  ["ObjectStore", ObjectStoreError],
  ["Lance", LanceFormatError],
  ["Arrow", LanceArrowError],
  ["External", ExternalError],
  ["Other", OtherError],
  ["Http", HttpError],
  ["Retry", RetryError],
  ["Sql", SqlError],
  ["JobFailed", JobFailedError],
  ["JobCancelled", JobCancelledError],
  ["JobNotFound", JobNotFoundError],
]);

/** True when `error` was raised by the LanceDB native core. */
export function isLanceError(error: unknown): error is LanceError {
  return error instanceof LanceError;
}

/** True for table, database, and column create conflicts. */
export function isAlreadyExistsError(
  error: unknown,
): error is
  | TableAlreadyExistsError
  | DatabaseAlreadyExistsError
  | ColumnAlreadyExistsError {
  return (
    error instanceof TableAlreadyExistsError ||
    error instanceof DatabaseAlreadyExistsError ||
    error instanceof ColumnAlreadyExistsError
  );
}

/**
 * Rebuild a native rejection as a {@link LanceError} subclass.
 *
 * Errors that did not come from Rust, including plain `Error`s thrown by the
 * TypeScript layer, are returned unchanged.
 *
 * An error already translated by another SDK load is not an `instanceof` this
 * load's classes. Its kind and fields are copied onto a new subclass so
 * `instanceof` and metadata such as `tableName` stay valid here.
 */
export function materializeLanceError(error: unknown, depth = 0): unknown {
  if (depth > MAX_CAUSE_DEPTH) {
    return error;
  }
  if (error instanceof LanceError) {
    return error;
  }
  if (isMarkedLanceError(error)) {
    return rebuildMarked(error, depth);
  }
  if (!isErrorLike(error)) {
    return error;
  }
  const cause =
    error.cause === undefined
      ? undefined
      : materializeLanceError(error.cause, depth + 1);
  const match = HEADER.exec(error.message);
  if (match === null) {
    // Native addons create Error in Node's realm. Jest and other vm contexts
    // have a different Error constructor, so preserve the message on a local one.
    return error instanceof Error && cause === error.cause
      ? error
      : copyForeignError(error, cause);
  }
  let payload: ErrorPayload;
  try {
    payload = JSON.parse(match[1]) as ErrorPayload;
  } catch {
    return error instanceof Error ? error : copyForeignError(error, cause);
  }
  if (typeof payload.code !== "string" || payload.code.length === 0) {
    return error instanceof Error ? error : copyForeignError(error, cause);
  }
  const message = error.message.slice(match[0].length) || payload.code;
  const wrapped = createLanceError(
    message,
    payload.code,
    payloadOptions(payload, cause),
  );
  copyStack(error, wrapped);
  return wrapped;
}

function isMarkedLanceError(
  error: unknown,
): error is Error & { code: string; cause?: unknown } {
  if (typeof error !== "object" || error === null) {
    return false;
  }
  const candidate = error as {
    code?: unknown;
    message?: unknown;
    [LANCE_ERROR_MARK]?: unknown;
  };
  return (
    candidate[LANCE_ERROR_MARK] === true &&
    typeof candidate.message === "string" &&
    typeof candidate.code === "string" &&
    candidate.code.length > 0
  );
}

function rebuildMarked(
  error: Error & { code: string; cause?: unknown },
  depth: number,
): LanceError {
  const cause =
    error.cause === undefined
      ? undefined
      : materializeLanceError(error.cause, depth + 1);
  const wrapped = createLanceError(
    error.message,
    error.code,
    payloadOptions(instancePayload(error), cause),
  );
  copyStack(error, wrapped);
  return wrapped;
}

function createLanceError(
  message: string,
  code: string,
  options: LanceErrorOptions,
): LanceError {
  const Ctor = ERROR_CLASSES.get(code);
  if (Ctor === undefined) {
    return new LanceError(message, { ...options, code });
  }
  return new Ctor(message, { ...options, code });
}

function instancePayload(error: object): ErrorPayload {
  const source = error as Record<string, unknown>;
  const payload: ErrorPayload = {};
  for (const key of METADATA_KEYS) {
    if (source[key] !== undefined) {
      payload[key] = source[key];
    }
  }
  return payload;
}

function copyStack(error: { stack?: unknown }, wrapped: LanceError): void {
  if (typeof error.stack !== "string") {
    return;
  }
  const newline = error.stack.indexOf("\n");
  const frames = newline === -1 ? "" : error.stack.slice(newline);
  wrapped.stack = `${wrapped.name}: ${wrapped.message}${frames}`;
}

function isErrorLike(value: unknown): value is Error {
  if (value instanceof Error) {
    return true;
  }
  if (typeof value !== "object" || value === null) {
    return false;
  }
  const candidate = value as { message?: unknown; name?: unknown };
  return (
    typeof candidate.message === "string" && typeof candidate.name === "string"
  );
}

function copyForeignError(error: Error, cause: unknown): Error {
  const copy = new Error(error.message, { cause });
  copy.name = error.name;
  if (typeof error.stack === "string") {
    copy.stack = error.stack;
  }
  const code = (error as { code?: unknown }).code;
  if (typeof code === "string") {
    (copy as { code?: string }).code = code;
  }
  return copy;
}

function payloadOptions(
  payload: ErrorPayload,
  cause: unknown,
): LanceErrorOptions {
  return {
    cause,
    requestId: asString(payload.requestId),
    statusCode: asNumber(payload.statusCode),
    errorCode: asString(payload.errorCode),
    resourceKind: asString(payload.resourceKind),
    resourceName: asString(payload.resourceName),
    namespacePath: asStringArray(payload.namespacePath),
    tableName: asString(payload.tableName),
    columnName: asString(payload.columnName),
    databaseName: asString(payload.databaseName),
    indexName: asString(payload.indexName),
    functionName: asString(payload.functionName),
    path: asString(payload.path),
    jobId: asString(payload.jobId),
    sqlJobId: asString(payload.sqlJobId),
    jobRequestId: asString(payload.jobRequestId),
    grpcCode: asNumber(payload.grpcCode),
    phase: asString(payload.phase),
    retryable: asBoolean(payload.retryable),
    requestFailures: asNumber(payload.requestFailures),
    maxRequestFailures: asNumber(payload.maxRequestFailures),
    connectFailures: asNumber(payload.connectFailures),
    maxConnectFailures: asNumber(payload.maxConnectFailures),
    readFailures: asNumber(payload.readFailures),
    maxReadFailures: asNumber(payload.maxReadFailures),
  };
}

function asString(value: unknown): string | undefined {
  return typeof value === "string" ? value : undefined;
}

function asNumber(value: unknown): number | undefined {
  return typeof value === "number" && Number.isFinite(value)
    ? value
    : undefined;
}

function asBoolean(value: unknown): boolean | undefined {
  return typeof value === "boolean" ? value : undefined;
}

function asStringArray(value: unknown): string[] | undefined {
  if (
    !Array.isArray(value) ||
    !value.every((item) => typeof item === "string")
  ) {
    return undefined;
  }
  return value;
}

/**
 * Wrap the native module so Rust errors surface as {@link LanceError}s.
 *
 * Called while the package entrypoint loads. Idempotent within one load.
 * A later load replaces these wrappers and calls the original native function,
 * so translation uses that load's classes instead of nesting on a previous one.
 */
export function installLanceErrorTranslation(moduleNamespace: object): void {
  for (const key of Object.getOwnPropertyNames(moduleNamespace)) {
    const value = (moduleNamespace as Record<string, unknown>)[key];
    if (typeof value !== "function") {
      continue;
    }
    const fn = value as AnyFn;
    if (isNativeClass(fn)) {
      wrapOwnFunctions(fn);
      wrapOwnFunctions((fn as { prototype: object }).prototype);
    } else {
      replaceFunction(moduleNamespace, key, fn);
    }
  }
}

function isNativeClass(value: AnyFn): boolean {
  const prototype = (value as { prototype?: object }).prototype;
  if (prototype != null) {
    const hasMethods = Object.getOwnPropertyNames(prototype).some(
      (name) => name !== "constructor",
    );
    if (hasMethods) {
      return true;
    }
  }
  // napi factory classes such as Index only expose static methods.
  return Object.getOwnPropertyNames(value).some((name) => {
    if (name === "length" || name === "name" || name === "prototype") {
      return false;
    }
    const property = (value as unknown as Record<string, unknown>)[name];
    return typeof property === "function";
  });
}

function wrapOwnFunctions(owner: object): void {
  for (const key of Object.getOwnPropertyNames(owner)) {
    if (key === "prototype" || key === "length" || key === "name") {
      continue;
    }
    const descriptor = Object.getOwnPropertyDescriptor(owner, key);
    if (descriptor === undefined || typeof descriptor.value !== "function") {
      continue;
    }
    replaceFunction(owner, key, descriptor.value as AnyFn);
  }
}

function replaceFunction(owner: object, key: string, fn: AnyFn): void {
  if (isWrapped(fn)) {
    return;
  }
  const original = originalNative(fn);
  const wrapped = function (this: unknown, ...args: unknown[]) {
    const fail = (error: unknown): never => {
      throw translatorFor(this)(error);
    };
    try {
      const result = original.apply(this, args);
      // Native promises are created in Node's realm. `instanceof Promise` is
      // false when the caller is running in another realm, such as Jest.
      if (isThenable(result)) {
        return Promise.resolve(result).then((value) => value, fail);
      }
      return result;
    } catch (error) {
      fail(error);
    }
  };
  Object.defineProperty(wrapped, "name", { value: original.name });
  Object.defineProperty(wrapped, WRAPPED, { value: true });
  Object.defineProperty(wrapped, ORIGINAL_FN, { value: original });
  const descriptor = Object.getOwnPropertyDescriptor(owner, key);
  try {
    Object.defineProperty(owner, key, {
      configurable: true,
      enumerable: descriptor?.enumerable ?? false,
      writable: descriptor?.writable ?? true,
      value: wrapped,
    });
  } catch {
    // A non-configurable native export keeps its original function.
  }
}

function isWrapped(fn: AnyFn): boolean {
  return Boolean((fn as unknown as Record<symbol, boolean>)[WRAPPED]);
}

function originalNative(fn: AnyFn): AnyFn {
  const original = (fn as unknown as Record<symbol, unknown>)[ORIGINAL_FN];
  return typeof original === "function" ? (original as AnyFn) : fn;
}

/**
 * Remember which SDK load should translate errors thrown by `value`.
 *
 * Native classes are shared across loads. The load that wraps a native object
 * in a TypeScript handle owns its errors, even if a later load replaces the
 * method wrapper.
 */
export function bindErrorTranslator<T>(value: T): T {
  if (
    typeof value !== "object" ||
    value === null ||
    typeof (value as { then?: unknown }).then === "function"
  ) {
    return value;
  }
  const host = translatorHost((value as object).constructor);
  if (host === undefined) {
    return value;
  }
  const map = translatorMap(host);
  if (map !== undefined) {
    map.set(value, materializeLanceError);
  }
  return value;
}

function translatorHost(ctor: unknown): TranslatorHost | undefined {
  if (typeof ctor !== "function") {
    return undefined;
  }
  return ctor as TranslatorHost;
}

function translatorMap(
  host: TranslatorHost,
): WeakMap<object, ErrorTranslator> | undefined {
  if (host[TRANSLATORS] != null) {
    return host[TRANSLATORS];
  }
  try {
    const map = new WeakMap<object, ErrorTranslator>();
    Object.defineProperty(host, TRANSLATORS, { value: map });
    return map;
  } catch {
    return undefined;
  }
}

function translatorFor(target: unknown): ErrorTranslator {
  if (typeof target === "object" && target !== null) {
    const host = translatorHost((target as object).constructor);
    const bound = host?.[TRANSLATORS]?.get(target);
    if (bound !== undefined) {
      return bound;
    }
  }
  return materializeLanceError;
}

function isThenable(value: unknown): value is PromiseLike<unknown> {
  return (
    (typeof value === "object" || typeof value === "function") &&
    value !== null &&
    typeof (value as { then?: unknown }).then === "function"
  );
}
