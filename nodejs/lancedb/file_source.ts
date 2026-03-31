// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import { tableFromIPC } from "./arrow";
import { NativeFileSource, readFiles as nativeReadFiles } from "./native.js";

export { NativeFileSource };

/**
 * A data source backed by files on disk or object storage.
 *
 * Created by {@link readFiles} and accepted by `Table.add`.
 */
export class FileSource {
  /** @internal */
  readonly _native: NativeFileSource;

  constructor(native: NativeFileSource) {
    this._native = native;
  }

  /** The Arrow schema inferred from the files. */
  get schema() {
    return tableFromIPC(this._native.schema()).schema;
  }
}

/**
 * Read files matching a glob pattern or path as a data source for `Table.add`.
 *
 * The format is auto-detected from the file extension:
 * - `.parquet` — Apache Parquet
 * - `.csv` — Comma-separated values
 * - `.lance` — Lance dataset (single path only, glob not supported)
 *
 * Schema is inferred from the files eagerly; data is streamed lazily when
 * the source is passed to `table.add()`.
 *
 * @example
 * ```ts
 * const source = await readFiles("./data/*.parquet");
 * console.log(source.schema);
 * await table.add(source);
 * ```
 */
export async function readFiles(pattern: string): Promise<FileSource> {
  return new FileSource(await nativeReadFiles(pattern));
}
