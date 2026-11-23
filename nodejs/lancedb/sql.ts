// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import {
  Binary,
  Bool,
  type DataType,
  Float64,
  Int64,
  Null,
  RecordBatchFileWriter,
  type RecordBatchLike,
  Table,
  type TableLike,
  TimestampMillisecond,
  Utf8,
  vectorFromArray,
} from "./arrow";
import { sanitizeTable } from "./sanitize";

/** Scalar SQL values. Use a one-row Arrow table for explicit Arrow types. */
export type QueryParameterValue =
  | string
  | number
  | bigint
  | boolean
  | Date
  | Uint8Array
  | null;

/** Values for `$1`/`$name` placeholders, sent separately from SQL text.
 * Arrays bind by position; objects bind by name. Arrow columns bind both ways.
 * Safe integers and bigint use Int64; fractional numbers use Float64.
 * Arrow input preserves explicit types.
 */
export type QueryParameters =
  | readonly QueryParameterValue[]
  | Readonly<Record<string, QueryParameterValue>>
  | TableLike
  | RecordBatchLike;

/** @internal */
export async function toParameterBuffer(
  parameters: QueryParameters,
): Promise<Buffer> {
  let table: Table;
  if (
    "schema" in parameters &&
    parameters.schema !== null &&
    typeof parameters.schema === "object" &&
    "fields" in parameters.schema
  ) {
    const arrow = parameters as TableLike | RecordBatchLike;
    table = sanitizeTable(
      "batches" in arrow ? arrow : { schema: arrow.schema, batches: [arrow] },
    );
  } else {
    const entries = Array.isArray(parameters)
      ? parameters.map((value, index) => [String(index + 1), value] as const)
      : Object.entries(parameters);
    const columns = Object.fromEntries(
      entries.map(([name, input]) => {
        let value = input;
        let type: DataType;
        if (value === null) type = new Null();
        else if (typeof value === "string") type = new Utf8();
        else if (typeof value === "boolean") type = new Bool();
        else if (typeof value === "number") {
          if (
            !Number.isFinite(value) ||
            (Number.isInteger(value) && !Number.isSafeInteger(value))
          ) {
            throw new Error(
              "Numeric parameters must be finite and integers must be safe; use bigint for Int64",
            );
          }
          type = Number.isInteger(value) ? new Int64() : new Float64();
          if (Number.isInteger(value)) value = BigInt(value);
        } else if (typeof value === "bigint") {
          if (value < -(1n << 63n) || value >= 1n << 63n) {
            throw new Error("bigint parameters must fit Int64");
          }
          type = new Int64();
        } else if (value instanceof Date && Number.isFinite(value.getTime())) {
          type = new TimestampMillisecond();
        } else if (value instanceof Uint8Array) type = new Binary();
        else
          throw new Error(
            "Query parameters must be scalar values or a one-row Arrow table",
          );
        return [name, vectorFromArray([value], type)];
      }),
    );
    table = new Table(columns);
  }
  if (table.numRows !== 1) {
    throw new Error(
      `Query parameters must contain exactly one row, got ${table.numRows}`,
    );
  }
  return Buffer.from(
    await RecordBatchFileWriter.writeAll(table).toUint8Array(),
  );
}
