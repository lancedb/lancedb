// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import { Session as NativeSession } from "./native";

/**
 * Configuration options for creating a Session.
 */
export interface SessionOptions {
  /**
   * The size of the index cache in bytes.
   *
   * The index cache stores decompressed vector indices in memory for faster access.
   * A larger cache can improve performance for datasets with many indices, but uses more RAM.
   *
   * Default: 6GB (6 * 1024 * 1024 * 1024 bytes)
   */
  indexCacheSizeBytes?: number | bigint;

  /**
   * The size of the metadata cache in bytes.
   *
   * The metadata cache stores file metadata and schema information in memory.
   * A larger cache can improve performance for datasets with many files or frequent schema access.
   *
   * Default: 1GB (1024 * 1024 * 1024 bytes)
   */
  metadataCacheSizeBytes?: number | bigint;
}

/**
 * A session manages caches and object stores across LanceDB operations.
 *
 * Sessions allow you to configure cache sizes for index and metadata caches,
 * which can significantly impact performance for large datasets. By creating
 * a session with custom cache sizes, you can optimize memory usage and
 * performance for your specific use case.
 *
 * @example
 * ```typescript
 * import { Session, connect } from "@lancedb/lancedb";
 *
 * // Create a session with custom cache sizes
 * const session = new Session({
 *   indexCacheSizeBytes: 8n * 1024n * 1024n * 1024n, // 8GB
 *   metadataCacheSizeBytes: 2n * 1024n * 1024n * 1024n, // 2GB
 * });
 *
 * // Use the session when connecting
 * const db = await connect("./my-lancedb", undefined, session);
 * ```
 */
export class Session {
  private _native: NativeSession;

  /**
   * Create a new Session with the specified cache sizes.
   *
   * @param options - Configuration options for the session
   */
  constructor(options: SessionOptions = {}) {
    const indexCacheSize =
      options.indexCacheSizeBytes !== undefined
        ? BigInt(options.indexCacheSizeBytes)
        : undefined;
    const metadataCacheSize =
      options.metadataCacheSizeBytes !== undefined
        ? BigInt(options.metadataCacheSizeBytes)
        : undefined;

    this._native = new NativeSession(indexCacheSize, metadataCacheSize);
  }

  /**
   * Create a Session with default cache sizes.
   *
   * This is equivalent to creating a session with 6GB index cache
   * and 1GB metadata cache.
   *
   * @returns A new Session with default cache sizes
   */
  static default(): Session {
    const session = new Session();
    session._native = NativeSession.default();
    return session;
  }

  /**
   * Get the current size of the session caches in bytes.
   *
   * @returns The total size of all caches in the session
   */
  get sizeBytes(): bigint {
    return this._native.sizeBytes();
  }

  /**
   * Get the approximate number of items cached in the session.
   *
   * @returns The number of cached items across all caches
   */
  get approxNumItems(): number {
    return this._native.approxNumItems();
  }

  /**
   * @internal
   * Get the native session instance for internal use.
   */
  get _nativeSession(): NativeSession {
    return this._native;
  }
}
