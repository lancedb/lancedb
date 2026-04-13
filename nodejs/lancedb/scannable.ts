// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import {
  Table as ArrowTable,
  RecordBatch,
  RecordBatchFileWriter,
  RecordBatchReader,
  RecordBatchStreamWriter,
  Schema,
} from "apache-arrow";
import { NapiScannable } from "./native.js";

export interface ScannableOptions {
  /** Hint about the number of rows. Not validated against the stream. */
  numRows?: number;
  /**
   * Whether the source can be scanned more than once. Defaults to `true` for
   * `fromTable` / `fromFactory` and `false` for `fromIterable` /
   * `fromRecordBatchReader`.
   */
  rescannable?: boolean;
}

/**
 * A data source that can be scanned as a stream of Arrow `RecordBatch`es.
 *
 * `Scannable` wraps the schema + optional row count + rescannable flag and
 * a callback that yields batches one at a time. It is passed to consumers
 * (e.g. `Table.add`, `createTable`, `mergeInsert` — follow-up work) that
 * need to pull data without materializing the full dataset in JS memory.
 *
 * Batches cross the JS↔Rust boundary as Arrow IPC Stream messages; a fresh
 * writer serializes each batch, and the Rust side decodes it with
 * `arrow_ipc::reader::StreamReader`. One batch is in flight at a time.
 */
export class Scannable {
  readonly schema: Schema;
  readonly numRows: number | null;
  readonly rescannable: boolean;

  /** @hidden */
  private readonly native: NapiScannable;

  private constructor(
    native: NapiScannable,
    schema: Schema,
    numRows: number | null,
    rescannable: boolean,
  ) {
    this.native = native;
    this.schema = schema;
    this.numRows = numRows;
    this.rescannable = rescannable;
  }

  /** @hidden Access the native handle for passing through to Rust consumers. */
  get inner(): NapiScannable {
    return this.native;
  }

  /**
   * Build a Scannable from an explicit schema and a factory that returns a
   * fresh batch iterator on each call.
   *
   * The factory is invoked once per scan. Each iterator yields
   * `RecordBatch`es matching the declared schema. Use this when you need
   * direct control over the pull loop — for example, to wrap a streaming
   * source whose batches are produced lazily.
   *
   * @param schema - The Arrow schema of the produced batches.
   * @param factory - Called at the start of each scan to produce a batch
   *   iterator. Must be idempotent when `rescannable` is true.
   * @param opts.numRows - Optional row count hint.
   * @param opts.rescannable - Defaults to `true`. Set to `false` if calling
   *   `factory()` twice would not reproduce the same data.
   */
  static fromFactory(
    schema: Schema,
    factory: () =>
      | AsyncIterable<RecordBatch>
      | Iterable<RecordBatch>
      | AsyncIterator<RecordBatch>
      | Iterator<RecordBatch>,
    opts: ScannableOptions = {},
  ): Scannable {
    const numRows = opts.numRows ?? null;
    if (numRows != null && !Number.isInteger(numRows)) {
      throw new TypeError("numRows must be an integer");
    }
    const rescannable = opts.rescannable ?? true;

    let iter: AsyncIterator<RecordBatch> | Iterator<RecordBatch> | null = null;
    const getNextBatch = async (): Promise<Buffer | null> => {
      if (iter === null) {
        iter = normalizeIterator(factory());
      }
      const result = await iter.next();
      if (result.done) {
        iter = null;
        return null;
      }
      return encodeBatch(result.value);
    };

    const schemaBuf = encodeSchema(schema);
    const native = new NapiScannable(
      schemaBuf,
      numRows,
      rescannable,
      getNextBatch,
    );
    return new Scannable(native, schema, numRows, rescannable);
  }

  /**
   * Build a Scannable from an in-memory Arrow `Table`. Rescannable by
   * default — the table's batches are replayed on each scan.
   */
  static fromTable(table: ArrowTable, opts: ScannableOptions = {}): Scannable {
    return Scannable.fromFactory(table.schema, () => table.batches, {
      numRows: opts.numRows ?? table.numRows,
      rescannable: opts.rescannable ?? true,
    });
  }

  /**
   * Build a Scannable from an iterable of `RecordBatch`es. The iterable is
   * consumed once; `rescannable` defaults to `false`. Pass an explicit
   * schema so the consumer can validate before any batch is pulled.
   */
  static fromIterable(
    schema: Schema,
    iter: AsyncIterable<RecordBatch> | Iterable<RecordBatch>,
    opts: ScannableOptions = {},
  ): Scannable {
    return Scannable.fromFactory(schema, () => iter, {
      numRows: opts.numRows,
      rescannable: opts.rescannable ?? false,
    });
  }

  /**
   * Build a Scannable from an Arrow `RecordBatchReader`. A reader can only
   * be consumed once; `rescannable` defaults to `false`.
   *
   * The reader must already be opened (via `.open()`) so its `.schema` is
   * populated — `RecordBatchReader.from(...)` returns an unopened reader.
   */
  static fromRecordBatchReader(
    reader: RecordBatchReader,
    opts: ScannableOptions = {},
  ): Scannable {
    return Scannable.fromFactory(reader.schema, () => reader, {
      numRows: opts.numRows,
      rescannable: opts.rescannable ?? false,
    });
  }
}

function normalizeIterator<T>(
  source: AsyncIterable<T> | Iterable<T> | AsyncIterator<T> | Iterator<T>,
): AsyncIterator<T> | Iterator<T> {
  if (source == null) {
    throw new TypeError("Scannable factory returned null/undefined");
  }
  if (
    typeof (source as AsyncIterable<T>)[Symbol.asyncIterator] === "function"
  ) {
    return (source as AsyncIterable<T>)[Symbol.asyncIterator]();
  }
  if (typeof (source as Iterable<T>)[Symbol.iterator] === "function") {
    return (source as Iterable<T>)[Symbol.iterator]();
  }
  // Already an iterator (has `.next`).
  if (typeof (source as Iterator<T>).next === "function") {
    return source as Iterator<T>;
  }
  throw new TypeError("Scannable factory returned a non-iterable value");
}

async function encodeBatch(batch: RecordBatch): Promise<Buffer> {
  const writer = RecordBatchStreamWriter.writeAll([batch]);
  const bytes = await writer.toUint8Array();
  return Buffer.from(bytes);
}

// Uses IPC File format (not Stream) because the Rust side parses it with
// `ipc_file_to_schema`, which expects File format.
function encodeSchema(schema: Schema): Buffer {
  const writer = new RecordBatchFileWriter();
  writer.reset(undefined, schema);
  writer.finish();
  writer.close();
  return Buffer.from(writer.toUint8Array(true));
}
