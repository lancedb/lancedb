// Copyright 2023 Lance Developers.
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
  Field,
  type FixedSizeListBuilder,
  Float32,
  makeBuilder,
  RecordBatchFileWriter,
  Utf8,
  type Vector,
  FixedSizeList,
  vectorFromArray,
  type Schema,
  Table as ArrowTable,
  RecordBatchStreamWriter,
  List,
  Float64,
  RecordBatch,
  makeData,
  Struct,
  type Float
} from 'apache-arrow'
import { type EmbeddingFunction } from './index'

/** Options to control the makeArrowTable call. */
export class MakeArrowTableOptions {
  /** Provided schema. */
  schema?: Schema

  /// Default vector column types.
  vectorDataType: Float = new Float32()

  /** Vector columns */
  vectorColumns: string[] = ['vector']

  constructor (values: Partial<MakeArrowTableOptions>) {
    Object.assign(this, values)
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
  ], { vectorColumns: ['vec1', 'vec2'], vectorDataType: new Float16() });
 * assert.deepEqual(table.schema, schema)
 * ```
 */
export function makeArrowTable (
  data: Array<Record<string, any>>,
  options?: Partial<MakeArrowTableOptions>
): ArrowTable {
  if (data.length === 0) {
    throw new Error('At least one record needs to be provided')
  }
  const opt = new MakeArrowTableOptions(options !== undefined ? options : {})
  const columns: Record<string, Vector> = {}
  // TODO: sample dataset to find missing columns
  const columnNames = Object.keys(data[0])
  for (const colName of columnNames) {
    const values = data.map((datum) => datum[colName])
    let vector: Vector

    if (opt.schema !== undefined) {
      // Explicit schema is provided, highest priority
      vector = vectorFromArray(
        values,
        opt.schema?.fields.filter((f) => f.name === colName)[0]?.type
      )
    } else if (opt.vectorColumns.includes(colName)) {
      const fslType = new FixedSizeList(
        values[0].length,
        new Field('item', opt.vectorDataType, false)
      )
      vector = vectorFromArray(values, fslType)
    } else {
      // Normal case
      vector = vectorFromArray(values)
    }
    columns[colName] = vector
  }

  return new ArrowTable(columns)
}

// Converts an Array of records into an Arrow Table, optionally applying an embeddings function to it.
export async function convertToTable<T> (
  data: Array<Record<string, unknown>>,
  embeddings?: EmbeddingFunction<T>
): Promise<ArrowTable> {
  if (data.length === 0) {
    throw new Error('At least one record needs to be provided')
  }

  const columns = Object.keys(data[0])
  const records: Record<string, Vector> = {}

  for (const columnsKey of columns) {
    if (columnsKey === 'vector') {
      const vectorSize = (data[0].vector as any[]).length
      const listBuilder = newVectorBuilder(vectorSize)
      for (const datum of data) {
        if ((datum[columnsKey] as any[]).length !== vectorSize) {
          throw new Error(`Invalid vector size, expected ${vectorSize}`)
        }

        listBuilder.append(datum[columnsKey])
      }
      records[columnsKey] = listBuilder.finish().toVector()
    } else {
      const values = []
      for (const datum of data) {
        values.push(datum[columnsKey])
      }

      if (columnsKey === embeddings?.sourceColumn) {
        const vectors = await embeddings.embed(values as T[])
        records.vector = vectorFromArray(
          vectors,
          newVectorType(vectors[0].length)
        )
      }

      if (typeof values[0] === 'string') {
        // `vectorFromArray` converts strings into dictionary vectors, forcing it back to a string column
        records[columnsKey] = vectorFromArray(values, new Utf8())
      } else if (Array.isArray(values[0])) {
        const elementType = getElementType(values[0])
        let innerType
        if (elementType === 'string') {
          innerType = new Utf8()
        } else if (elementType === 'number') {
          innerType = new Float64()
        } else {
          // TODO: pass in schema if it exists, else keep going to the next element
          throw new Error(`Unsupported array element type ${elementType}`)
        }
        const listBuilder = makeBuilder({
          type: new List(new Field('item', innerType, true))
        })
        for (const value of values) {
          listBuilder.append(value)
        }
        records[columnsKey] = listBuilder.finish().toVector()
      } else {
        // TODO if this is a struct field then recursively align the subfields
        records[columnsKey] = vectorFromArray(values)
      }
    }
  }

  return new ArrowTable(records)
}

function getElementType (arr: any[]): string {
  if (arr.length === 0) {
    return 'undefined'
  }

  return typeof arr[0]
}

// Creates a new Arrow ListBuilder that stores a Vector column
function newVectorBuilder (dim: number): FixedSizeListBuilder<Float32> {
  return makeBuilder({
    type: newVectorType(dim)
  })
}

// Creates the Arrow Type for a Vector column with dimension `dim`
function newVectorType (dim: number): FixedSizeList<Float32> {
  // Somewhere we always default to have the elements nullable, so we need to set it to true
  // otherwise we often get schema mismatches because the stored data always has schema with nullable elements
  const children = new Field<Float32>('item', new Float32(), true)
  return new FixedSizeList(dim, children)
}

// Converts an Array of records into Arrow IPC format
export async function fromRecordsToBuffer<T> (
  data: Array<Record<string, unknown>>,
  embeddings?: EmbeddingFunction<T>,
  schema?: Schema
): Promise<Buffer> {
  let table = await convertToTable(data, embeddings)
  if (schema !== undefined) {
    table = alignTable(table, schema)
  }
  const writer = RecordBatchFileWriter.writeAll(table)
  return Buffer.from(await writer.toUint8Array())
}

// Converts an Array of records into Arrow IPC stream format
export async function fromRecordsToStreamBuffer<T> (
  data: Array<Record<string, unknown>>,
  embeddings?: EmbeddingFunction<T>,
  schema?: Schema
): Promise<Buffer> {
  let table = await convertToTable(data, embeddings)
  if (schema !== undefined) {
    table = alignTable(table, schema)
  }
  const writer = RecordBatchStreamWriter.writeAll(table)
  return Buffer.from(await writer.toUint8Array())
}

// Converts an Arrow Table into Arrow IPC format
export async function fromTableToBuffer<T> (
  table: ArrowTable,
  embeddings?: EmbeddingFunction<T>,
  schema?: Schema
): Promise<Buffer> {
  if (embeddings !== undefined) {
    const source = table.getChild(embeddings.sourceColumn)

    if (source === null) {
      throw new Error(
        `The embedding source column ${embeddings.sourceColumn} was not found in the Arrow Table`
      )
    }

    const vectors = await embeddings.embed(source.toArray() as T[])
    const column = vectorFromArray(vectors, newVectorType(vectors[0].length))
    table = table.assign(new ArrowTable({ vector: column }))
  }
  if (schema !== undefined) {
    table = alignTable(table, schema)
  }
  const writer = RecordBatchFileWriter.writeAll(table)
  return Buffer.from(await writer.toUint8Array())
}

// Converts an Arrow Table into Arrow IPC stream format
export async function fromTableToStreamBuffer<T> (
  table: ArrowTable,
  embeddings?: EmbeddingFunction<T>,
  schema?: Schema
): Promise<Buffer> {
  if (embeddings !== undefined) {
    const source = table.getChild(embeddings.sourceColumn)

    if (source === null) {
      throw new Error(
        `The embedding source column ${embeddings.sourceColumn} was not found in the Arrow Table`
      )
    }

    const vectors = await embeddings.embed(source.toArray() as T[])
    const column = vectorFromArray(vectors, newVectorType(vectors[0].length))
    table = table.assign(new ArrowTable({ vector: column }))
  }
  if (schema !== undefined) {
    table = alignTable(table, schema)
  }
  const writer = RecordBatchStreamWriter.writeAll(table)
  return Buffer.from(await writer.toUint8Array())
}

function alignBatch (batch: RecordBatch, schema: Schema): RecordBatch {
  const alignedChildren = []
  for (const field of schema.fields) {
    const indexInBatch = batch.schema.fields?.findIndex(
      (f) => f.name === field.name
    )
    if (indexInBatch < 0) {
      throw new Error(
        `The column ${field.name} was not found in the Arrow Table`
      )
    }
    alignedChildren.push(batch.data.children[indexInBatch])
  }
  const newData = makeData({
    type: new Struct(schema.fields),
    length: batch.numRows,
    nullCount: batch.nullCount,
    children: alignedChildren
  })
  return new RecordBatch(schema, newData)
}

function alignTable (table: ArrowTable, schema: Schema): ArrowTable {
  const alignedBatches = table.batches.map((batch) =>
    alignBatch(batch, schema)
  )
  return new ArrowTable(schema, alignedBatches)
}

// Creates an empty Arrow Table
export function createEmptyTable (schema: Schema): ArrowTable {
  return new ArrowTable(schema)
}
