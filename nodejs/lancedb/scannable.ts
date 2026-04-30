// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import {
  Table as ArrowTable,
  RecordBatch,
  RecordBatchReader,
  Schema,
} from "apache-arrow";
import {
  fromRecordBatchToStreamBuffer,
  fromTableToBuffer,
  makeEmptyTable,
} from "./arrow";
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
   * @param opts - Optional hints. `rescannable` defaults to `true`; set to
   *   `false` if calling `factory()` twice would not reproduce the same data.
   */
  static async fromFactory(
    schema: Schema,
    factory: () =>
      | AsyncIterable<RecordBatch>
      | Iterable<RecordBatch>
      | AsyncIterator<RecordBatch>
      | Iterator<RecordBatch>,
    opts: ScannableOptions = {},
  ): Promise<Scannable> {
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
      return fromRecordBatchToStreamBuffer(result.value);
    };

    const schemaBuf = await fromTableToBuffer(makeEmptyTable(schema));
    const native = new NapiScannable(
      schemaBuf,
      numRows,
      rescannable,
      getNextBatch,
    );
    return new Scannable(native, schema, numRows, rescannable);
  }

  /**
   * Build a Scannable from an in-memory Arrow `Table`. Always rescannable;
   * the table's batches are replayed on each scan.
   *
   * The table's row count is authoritative: `opts.numRows` must either be
   * omitted or equal to `table.numRows`. `opts.rescannable` of `false` is
   * rejected because in-memory Tables are always rescannable.
   */
  static async fromTable(
    table: ArrowTable,
    opts: ScannableOptions = {},
  ): Promise<Scannable> {
    if (opts.numRows != null && opts.numRows !== table.numRows) {
      throw new TypeError(
        `opts.numRows (${opts.numRows}) does not match table.numRows (${table.numRows}). ` +
          `The table's row count is authoritative; omit numRows or pass the matching value.`,
      );
    }
    if (opts.rescannable === false) {
      throw new TypeError(
        `fromTable does not accept rescannable: false. ` +
          `In-memory Arrow Tables are always rescannable; omit the option or pass true.`,
      );
    }
    return Scannable.fromFactory(table.schema, () => table.batches, {
      numRows: table.numRows,
      rescannable: true,
    });
  }

  /**
   * Build a Scannable from an iterable of `RecordBatch`es. `rescannable`
   * defaults to `false`. Pass an explicit schema so the consumer can
   * validate before any batch is pulled.
   *
   * `opts.rescannable: true` is honest for replayable iterables (Arrays,
   * Sets, or custom iterables whose `[Symbol.iterator]()` returns a fresh
   * iterator each call). It is rejected for one-shot iterables (generators,
   * async generators, or already-an-iterator inputs) because their
   * `[Symbol.iterator]()` returns the same exhausted object on the second
   * scan. For replayable sources outside this shape, use
   * `fromFactory(schema, () => createIter(), { rescannable: true })`.
   *
   * Note: when `opts.rescannable` is `true`, the constructor calls
   * `[Symbol.iterator]()` once on the input to perform the structural check.
   */
  static async fromIterable(
    schema: Schema,
    iter: AsyncIterable<RecordBatch> | Iterable<RecordBatch>,
    opts: ScannableOptions = {},
  ): Promise<Scannable> {
    if (opts.rescannable === true && isOneShotIterable(iter)) {
      throw new TypeError(
        `fromIterable: rescannable: true is not honest for one-shot iterables ` +
          `(generators, async generators, or iterators where [Symbol.iterator]() ` +
          `returns the same object). The source would be exhausted after the first scan. ` +
          `Use fromFactory(schema, () => createIter(), { rescannable: true }) for sources ` +
          `where each call mints a fresh iterator.`,
      );
    }
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
  ): Promise<Scannable> {
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

// A "self-iterator" returns the same object from `[Symbol.iterator]()` /
// `[Symbol.asyncIterator]()`. Generators behave this way, so they exhaust
// after one pass. Replayable iterables (Array, Set, custom) return a fresh
// iterator each call. Detection mirrors `normalizeIterator`'s ordering so
// classification matches scan-time behavior.
function isOneShotIterable(
  source: AsyncIterable<unknown> | Iterable<unknown>,
): boolean {
  // null/undefined are not one-shot in any meaningful sense; let
  // `normalizeIterator` raise the actual error at scan time.
  if (source == null) return false;
  const ref = source as unknown;
  if (
    typeof (source as AsyncIterable<unknown>)[Symbol.asyncIterator] ===
    "function"
  ) {
    const it = (source as AsyncIterable<unknown>)[
      Symbol.asyncIterator
    ]() as unknown;
    return it === ref;
  }
  if (typeof (source as Iterable<unknown>)[Symbol.iterator] === "function") {
    const it = (source as Iterable<unknown>)[Symbol.iterator]() as unknown;
    return it === ref;
  }
  // Already-an-iterator (has `.next` but no `Symbol.iterator`) is by
  // definition one-shot.
  if (typeof (source as { next?: unknown }).next === "function") return true;
  return false;
}
