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

export type BlobInput = {
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
   * Reads from the cursor and advances the cursor.
   *
   * Reads to the end when `maxBytes` is omitted, or at most `maxBytes` bytes
   * otherwise. Returns an empty buffer at the end of the blob.
   * {@link BlobFile.readRange} does not move the cursor.
   */
  read(maxBytes?: bigint, options?: BlobReadOptions): Promise<Buffer> {
    return runWithSignal(options?.signal, (signal) =>
      this.inner.read(maxBytes, signal),
    );
  }

  /**
   * Reads the half-open byte range `[start, end)`.
   *
   * Fails when `end` is past the blob size. Does not move the cursor.
   */
  readRange(
    start: bigint,
    end: bigint,
    options?: BlobReadOptions,
  ): Promise<Buffer> {
    return runWithSignal(options?.signal, (signal) =>
      this.inner.readRange(start, end, signal),
    );
  }

  /**
   * Reads several half-open byte ranges. Returns one buffer per range, in
   * the order given.
   *
   * Fails when any `end` is past the blob size. Does not move the cursor.
   */
  readRanges(
    ranges: BlobRange[],
    options?: BlobReadOptions,
  ): Promise<Buffer[]> {
    return runWithSignal(options?.signal, (signal) =>
      this.inner.readRanges(ranges, signal),
    );
  }

  /** Moves the cursor to `position`, in bytes from the start of the blob. */
  seek(position: bigint): Promise<void> {
    return this.inner.seek(position);
  }

  /** Returns the cursor position in bytes. */
  tell(): Promise<bigint> {
    return this.inner.tell();
  }

  /**
   * Releases the handle. Reads after `close()` fail. Calling it again does
   * nothing.
   */
  close(): Promise<void> {
    return this.inner.close();
  }

  /** Returns true after {@link BlobFile.close}. */
  isClosed(): boolean {
    return this.inner.isClosed();
  }
}

/** Options for blob reads. */
export type BlobReadOptions = {
  /**
   * Cancels the read. The call rejects with `signal.reason`, and the native
   * read stops, including in-flight requests to a remote table.
   */
  signal?: AbortSignal;
};

/** @ignore */
export async function runWithSignal<T>(
  signal: AbortSignal | undefined,
  run: (signal: AbortSignal | undefined) => Promise<T>,
): Promise<T> {
  if (signal === undefined) {
    return run(undefined);
  }
  signal.throwIfAborted();
  // napi-rs replaces `onabort` on the signal it receives, so pass a private
  // signal and forward the caller's abort to it.
  const controller = new AbortController();
  const onAbort = () => controller.abort();
  signal.addEventListener("abort", onAbort, { once: true });
  try {
    return await run(controller.signal);
  } catch (err) {
    if (signal.aborted) {
      throw signal.reason;
    }
    throw err;
  } finally {
    signal.removeEventListener("abort", onAbort);
  }
}

/** A half-open byte range `[start, end)` for {@link BlobFile.readRanges}. */
export type BlobRange = {
  start: bigint;
  end: bigint;
};

export function coerceBlobValue(value: unknown): BlobInput | null {
  if (value == null) {
    return null;
  }
  if (isBlobBytes(value)) {
    return { data: value, uri: null };
  }
  if (ArrayBuffer.isView(value)) {
    throw new Error("Blob data must be Buffer or Uint8Array");
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
      throw new Error(`Blob uri must be a string or null, got ${typeof uri}`);
    }
    const data = record.data;
    if (data != null && !isBlobBytes(data)) {
      throw new Error("Blob data must be Buffer, Uint8Array, or null");
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
    "Blob column values must be Buffer, Uint8Array, a URI string, null, or { data?, uri? }",
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
