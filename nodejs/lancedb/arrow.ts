// Copyright 2024 Lance Developers.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import {
  Int64,
  Field,
  FixedSizeList,
  Float,
  Float32,
  Schema,
  Table as ArrowTable,
  Table,
  Vector,
  vectorFromArray,
  tableToIPC,
  DataType,
} from "apache-arrow";

/** Data type accepted by NodeJS SDK */
export type Data = Record<string, unknown>[] | ArrowTable;

export class VectorColumnOptions {
  /** Vector column type. */
  type: Float = new Float32();

  constructor(values?: Partial<VectorColumnOptions>) {
    Object.assign(this, values);
  }
}

/** Options to control the makeArrowTable call. */
export class MakeArrowTableOptions {
  /** Provided schema. */
  schema?: Schema;

  /** Vector columns */
  vectorColumns: Record<string, VectorColumnOptions> = {
    vector: new VectorColumnOptions(),
  };

  constructor(values?: Partial<MakeArrowTableOptions>) {
    Object.assign(this, values);
  }
}

/**
 * An enhanced version of the {@link makeTable} function from Apache Arrow
 * that supports nested fields and embeddings columns.
 *
 * Note that it currently does not support nulls.
 *
 * @param data input data
 * @param options options to control the makeArrowTable call.
 *
 * @example
 *
 * ```ts
 *
 * import { fromTableToBuffer, makeArrowTable } from "../arrow";
 * import { Field, FixedSizeList, Float16, Float32, Int32, Schema } from "apache-arrow";
 *
 * const schema = new Schema([
 *   new Field("a", new Int32()),
 *   new Field("b", new Float32()),
 *   new Field("c", new FixedSizeList(3, new Field("item", new Float16()))),
 *  ]);
 *  const table = makeArrowTable([
 *    { a: 1, b: 2, c: [1, 2, 3] },
 *    { a: 4, b: 5, c: [4, 5, 6] },
 *    { a: 7, b: 8, c: [7, 8, 9] },
 *  ], { schema });
 * ```
 *
 * It guesses the vector columns if the schema is not provided. For example,
 * by default it assumes that the column named `vector` is a vector column.
 *
 * ```ts
 *
 * const schema = new Schema([
 new Field("a", new Float64()),
 new Field("b", new Float64()),
 new Field(
 "vector",
 new FixedSizeList(3, new Field("item", new Float32()))
 ),
 ]);
 const table = makeArrowTable([
 { a: 1, b: 2, vector: [1, 2, 3] },
 { a: 4, b: 5, vector: [4, 5, 6] },
 { a: 7, b: 8, vector: [7, 8, 9] },
 ]);
 assert.deepEqual(table.schema, schema);
 * ```
 *
 * You can specify the vector column types and names using the options as well
 *
 * ```typescript
 *
 * const schema = new Schema([
 new Field('a', new Float64()),
 new Field('b', new Float64()),
 new Field('vec1', new FixedSizeList(3, new Field('item', new Float16()))),
 new Field('vec2', new FixedSizeList(3, new Field('item', new Float16())))
 ]);
 * const table = makeArrowTable([
 { a: 1, b: 2, vec1: [1, 2, 3], vec2: [2, 4, 6] },
 { a: 4, b: 5, vec1: [4, 5, 6], vec2: [8, 10, 12] },
 { a: 7, b: 8, vec1: [7, 8, 9], vec2: [14, 16, 18] }
 ], {
 vectorColumns: {
 vec1: { type: new Float16() },
 vec2: { type: new Float16() }
 }
 }
 * assert.deepEqual(table.schema, schema)
 * ```
 */
export function makeArrowTable(
  data: Record<string, any>[],
  options?: Partial<MakeArrowTableOptions>
): Table {
  if (data.length === 0) {
    throw new Error("At least one record needs to be provided");
  }
  const opt = new MakeArrowTableOptions(options ?? {});
  const columns: Record<string, Vector> = {};
  // TODO: sample dataset to find missing columns
  const columnNames = Object.keys(data[0]);
  for (const colName of columnNames) {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-return
    let values = data.map((datum) => datum[colName]);
    let vector: Vector;

    if (opt.schema !== undefined) {
      // Explicit schema is provided, highest priority
      const fieldType: DataType | undefined = opt.schema.fields.filter((f) => f.name === colName)[0]?.type as DataType;
      if (fieldType instanceof Int64) {
        // wrap in BigInt to avoid bug: https://github.com/apache/arrow/issues/40051
        // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
        values = values.map((v) => BigInt(v));
      }
      vector = vectorFromArray(values, fieldType);
    } else {
      const vectorColumnOptions = opt.vectorColumns[colName];
      if (vectorColumnOptions !== undefined) {
        const fslType = new FixedSizeList(
          (values[0] as any[]).length,
          new Field("item", vectorColumnOptions.type, false)
        );
        vector = vectorFromArray(values, fslType);
      } else {
        // Normal case
        vector = vectorFromArray(values);
      }
    }
    columns[colName] = vector;
  }

  return new Table(columns);
}

/**
 * Convert an Arrow Table to a Buffer.
 *
 * @param data Arrow Table
 * @param schema Arrow Schema, optional
 * @returns Buffer node
 */
export function toBuffer(data: Data, schema?: Schema): Buffer {
  let tbl: Table;
  if (data instanceof Table) {
    tbl = data;
  } else {
    tbl = makeArrowTable(data, { schema });
  }
  return Buffer.from(tableToIPC(tbl));
}
