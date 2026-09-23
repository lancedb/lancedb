// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import { Field, LargeBinary, Struct, Utf8 } from "apache-arrow";
import { BlobFile as NativeBlobFile } from "./native";

const BLOB_V2_EXTENSION_NAME = "lance.blob.v2";

const INLINE_SIZE_THRESHOLD_KEY = "lance-encoding:blob-inline-size-threshold";
const DEDICATED_SIZE_THRESHOLD_KEY =
  "lance-encoding:blob-dedicated-size-threshold";
const PACK_FILE_SIZE_THRESHOLD_KEY =
  "lance-encoding:blob-pack-file-size-threshold";

/**
 * Bytes accepted for a blob value. `ArrayBuffer` is wrapped without copying.
 * `Blob` and `File` are read with `arrayBuffer()`, which only the async write
 * paths ({@link Connection.createTable}, {@link Table.add},
 * {@link Table.mergeInsert}) can do.
 */
export type BlobData = Buffer | Uint8Array | ArrayBuffer | Blob;

/** A URI accepted for a blob value. A `URL` is stored as its `href`. */
export type BlobUri = string | URL;

/**
 * A value accepted for a `lance.blob.v2` column (see {@link blob}).
 *
 * Either inline bytes, a URI pointing at external bytes, or a struct that sets
 * exactly one of `data` or `uri`. `null` and `undefined` write a null blob.
 *
 * @example
 * ```ts
 * import { pathToFileURL } from "node:url";
 * import type { BlobInput } from "@lancedb/lancedb";
 *
 * const rows: { id: bigint; image: BlobInput }[] = [
 *   { id: 1n, image: await (await fetch(url)).arrayBuffer() },
 *   { id: 2n, image: pathToFileURL("/data/cat.png") },
 *   { id: 3n, image: { data: new Blob(["hello"]) } },
 * ];
 * await table.add(rows);
 * ```
 */
export type BlobInput =
  | BlobData
  | BlobUri
  | { data: BlobData; uri?: null }
  | { data?: null; uri: BlobUri }
  | null
  | undefined;

/** A blob value normalized to the columns of the blob struct. */
export type BlobValue = {
  data: Buffer | Uint8Array | null;
  uri: string | null;
};

export type BlobOptions = {
  /** Defaults to true. */
  nullable?: boolean;
  /**
   * Max payload bytes kept inline in the data file. Zero is allowed. Must be a
   * safe integer.
   */
  inlineSizeThreshold?: number;
  /**
   * Max payload bytes stored in a packed sidecar before a dedicated file. Must
   * be a positive safe integer.
   */
  dedicatedSizeThreshold?: number;
  /**
   * Max bytes in one packed sidecar before starting another. Must be a positive
   * safe integer.
   */
  packFileSizeThreshold?: number;
};

/**
 * Declares a `lance.blob.v2` column.
 *
 * Query results are descriptors, not payload bytes. Use {@link Table.fetchBlobs}
 * or {@link Table.fetchBlobFiles} to read bytes.
 *
 * @example
 * ```ts
 * import { readFile } from "node:fs/promises";
 * import { Field, Int64, Schema } from "apache-arrow";
 * import { blob, connect } from "@lancedb/lancedb";
 *
 * const db = await connect("./data");
 * const video = await readFile("clip.mp4");
 * const table = await db.createTable(
 *   "videos",
 *   [{ id: 1n, video }],
 *   {
 *     schema: new Schema([
 *       new Field("id", new Int64()),
 *       blob("video"),
 *     ]),
 *   },
 * );
 *
 * const rows = await table.query().select(["id"]).withRowId().toArray();
 * const rowIds = rows.map((row) => row._rowid as bigint);
 * const bytes = await table.fetchBlobs("video", rowIds);
 *
 * const [handle] = await table.fetchBlobFiles("video", rowIds);
 * const size = handle!.size();
 * const header = await handle!.readRange(0n, size < 65536n ? size : 65536n);
 * ```
 */
export function blob(name: string, options: BlobOptions = {}): Field {
  const metadata = new Map<string, string>([
    ["ARROW:extension:name", BLOB_V2_EXTENSION_NAME],
  ]);
  setThreshold(
    metadata,
    INLINE_SIZE_THRESHOLD_KEY,
    "inlineSizeThreshold",
    options.inlineSizeThreshold,
    0,
  );
  setThreshold(
    metadata,
    DEDICATED_SIZE_THRESHOLD_KEY,
    "dedicatedSizeThreshold",
    options.dedicatedSizeThreshold,
    1,
  );
  setThreshold(
    metadata,
    PACK_FILE_SIZE_THRESHOLD_KEY,
    "packFileSizeThreshold",
    options.packFileSizeThreshold,
    1,
  );
  return new Field(
    name,
    new Struct([
      new Field("data", new LargeBinary(), true),
      new Field("uri", new Utf8(), true),
    ]),
    options.nullable ?? true,
    metadata,
  );
}

/**
 * Checks for the `lance.blob.v2` extension marker. Does not validate the
 * field's storage type.
 */
export function isBlobField(field: Field): boolean {
  return field.metadata?.get("ARROW:extension:name") === BLOB_V2_EXTENSION_NAME;
}

/**
 * A lazy handle to blob bytes. Create one with {@link Table.fetchBlobFiles}.
 *
 * @hideconstructor
 */
export class BlobFile {
  private readonly inner: NativeBlobFile;

  private constructor(inner: NativeBlobFile) {
    if (!(inner instanceof NativeBlobFile)) {
      throw new Error("BlobFile handles come from Table.fetchBlobFiles");
    }
    this.inner = inner;
  }

  /** @ignore */
  static fromNative(inner: NativeBlobFile): BlobFile {
    return new BlobFile(inner);
  }

  /** Returns the blob size in bytes. */
  size(): bigint {
    return this.inner.size();
  }

  /**
   * Reads from the cursor to the end and advances the cursor.
   *
   * A second call returns an empty buffer. {@link BlobFile.readRange} does
   * not move the cursor.
   */
  read(): Promise<Buffer> {
    return this.inner.read();
  }

  /**
   * Reads the half-open byte range `[start, end)`.
   *
   * Fails when `end` is past the blob size. Does not move the cursor.
   */
  readRange(start: bigint, end: bigint): Promise<Buffer> {
    return this.inner.readRange(start, end);
  }
}

/**
 * Rewrites the synchronous-only widenings of {@link BlobInput} (`ArrayBuffer`,
 * `URL`) into `Uint8Array` and URI strings, keeping the value's shape. Other
 * values are returned unchanged and validated later by {@link coerceBlobValue}.
 *
 * Throws on `Blob` / `File`, which need {@link resolveBlobInput} first.
 */
export function normalizeBlobInput(value: unknown): unknown {
  if (needsBlobResolution(value)) {
    throw new Error(
      "Blob and File values must be read asynchronously. Pass them to " +
        "createTable, add, or mergeInsert, or convert them with " +
        "`new Uint8Array(await value.arrayBuffer())`",
    );
  }
  if (value instanceof ArrayBuffer) {
    return new Uint8Array(value);
  }
  if (value instanceof URL) {
    return value.href;
  }
  if (isBlobStruct(value)) {
    const out: Record<string, unknown> = { ...value };
    let changed = false;
    if (value.data instanceof ArrayBuffer) {
      out.data = new Uint8Array(value.data);
      changed = true;
    }
    if (value.uri instanceof URL) {
      out.uri = value.uri.href;
      changed = true;
    }
    return changed ? out : value;
  }
  return value;
}

/** Whether {@link resolveBlobInput} has anything to read for this value. */
export function needsBlobResolution(value: unknown): boolean {
  return isBlobLike(value) || (isBlobStruct(value) && isBlobLike(value.data));
}

/**
 * Reads `Blob` / `File` values, at the top level or in `data`, into
 * `Uint8Array`. Other values are returned unchanged.
 */
export async function resolveBlobInput(value: unknown): Promise<unknown> {
  if (isBlobLike(value)) {
    return new Uint8Array(await value.arrayBuffer());
  }
  if (isBlobStruct(value) && isBlobLike(value.data)) {
    return {
      ...value,
      data: new Uint8Array(await value.data.arrayBuffer()),
    };
  }
  return value;
}

export function coerceBlobValue(input: unknown): BlobValue | null {
  const value = normalizeBlobInput(input);
  if (value == null) {
    return null;
  }
  if (isBlobBytes(value)) {
    return { data: value, uri: null };
  }
  if (ArrayBuffer.isView(value)) {
    throw new Error(
      "Blob data must be Buffer, Uint8Array, ArrayBuffer, or Blob",
    );
  }
  if (typeof value === "string") {
    if (value === "") {
      throw new Error("Blob uri cannot be empty");
    }
    return { data: null, uri: value };
  }
  if (typeof value === "object") {
    const record = value as Record<string, unknown>;
    if (!("data" in record) && !("uri" in record)) {
      throw new Error(
        "Blob struct values must include a 'data' or 'uri' field",
      );
    }
    const uri = record.uri;
    if (uri === "") {
      throw new Error("Blob uri cannot be empty");
    }
    if (uri != null && typeof uri !== "string") {
      throw new Error(
        `Blob uri must be a string, URL, or null, got ${typeof uri}`,
      );
    }
    const data = record.data;
    if (data != null && !isBlobBytes(data)) {
      throw new Error(
        "Blob data must be Buffer, Uint8Array, ArrayBuffer, Blob, or null",
      );
    }
    const bytes = (data as Buffer | Uint8Array | null | undefined) ?? null;
    const uriValue = uri ?? null;
    if ((bytes == null) === (uriValue == null)) {
      throw new Error(
        "Blob struct values must set exactly one of 'data' or 'uri'",
      );
    }
    return { data: bytes, uri: uriValue };
  }
  throw new Error(
    "Blob column values must be Buffer, Uint8Array, ArrayBuffer, Blob, a URI string or URL, null, or { data?, uri? }",
  );
}

function isBlobLike(value: unknown): value is Blob {
  return typeof Blob !== "undefined" && value instanceof Blob;
}

function isBlobStruct(value: unknown): value is Record<string, unknown> {
  if (typeof value !== "object" || value === null) {
    return false;
  }
  const prototype = Object.getPrototypeOf(value);
  return (
    (prototype === Object.prototype || prototype === null) &&
    ("data" in value || "uri" in value)
  );
}

function isBlobBytes(value: unknown): value is Buffer | Uint8Array {
  return Buffer.isBuffer(value) || value instanceof Uint8Array;
}

function setThreshold(
  metadata: Map<string, string>,
  key: string,
  optionName: string,
  value: number | undefined,
  minimum: number,
): void {
  if (value === undefined) {
    return;
  }
  if (!Number.isSafeInteger(value)) {
    throw new Error(`${optionName} must be a safe integer`);
  }
  if (value < minimum) {
    throw new Error(
      minimum <= 0
        ? `${optionName} must be non-negative`
        : `${optionName} must be positive`,
    );
  }
  metadata.set(key, String(value));
}
