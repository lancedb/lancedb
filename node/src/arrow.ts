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
  Field, type FixedSizeListBuilder,
  Float32,
  makeBuilder,
  RecordBatchFileWriter,
  Utf8,
  type Vector,
  FixedSizeList,
  vectorFromArray, type Schema, Table as ArrowTable, RecordBatchStreamWriter, List
} from 'apache-arrow'
import { type EmbeddingFunction } from './index'

// Converts an Array of records into an Arrow Table, optionally applying an embeddings function to it.
export async function convertToTable<T> (data: Array<Record<string, unknown>>, embeddings?: EmbeddingFunction<T>): Promise<ArrowTable> {
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
        records.vector = vectorFromArray(vectors, newVectorType(vectors[0].length))
      }

      if (typeof values[0] === 'string') {
        // `vectorFromArray` converts strings into dictionary vectors, forcing it back to a string column
        records[columnsKey] = vectorFromArray(values, new Utf8())
      } else if (Array.isArray(values[0]) && typeof values[0][0] === 'string') {
        const listBuilder = makeBuilder({
          type: new List(new Field('item', new Utf8()))
        })
        for (const value of values) {
          listBuilder.append(value)
        }
        records[columnsKey] = listBuilder.finish().toVector()
      } else {
        records[columnsKey] = vectorFromArray(values)
      }
    }
  }

  return new ArrowTable(records)
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
export async function fromRecordsToBuffer<T> (data: Array<Record<string, unknown>>, embeddings?: EmbeddingFunction<T>): Promise<Buffer> {
  const table = await convertToTable(data, embeddings)
  const writer = RecordBatchFileWriter.writeAll(table)
  return Buffer.from(await writer.toUint8Array())
}

// Converts an Array of records into Arrow IPC stream format
export async function fromRecordsToStreamBuffer<T> (data: Array<Record<string, unknown>>, embeddings?: EmbeddingFunction<T>): Promise<Buffer> {
  const table = await convertToTable(data, embeddings)
  const writer = RecordBatchStreamWriter.writeAll(table)
  return Buffer.from(await writer.toUint8Array())
}

// Converts an Arrow Table into Arrow IPC format
export async function fromTableToBuffer<T> (table: ArrowTable, embeddings?: EmbeddingFunction<T>): Promise<Buffer> {
  if (embeddings !== undefined) {
    const source = table.getChild(embeddings.sourceColumn)

    if (source === null) {
      throw new Error(`The embedding source column ${embeddings.sourceColumn} was not found in the Arrow Table`)
    }

    const vectors = await embeddings.embed(source.toArray() as T[])
    const column = vectorFromArray(vectors, newVectorType(vectors[0].length))
    table = table.assign(new ArrowTable({ vector: column }))
  }
  const writer = RecordBatchFileWriter.writeAll(table)
  return Buffer.from(await writer.toUint8Array())
}

// Converts an Arrow Table into Arrow IPC stream format
export async function fromTableToStreamBuffer<T> (table: ArrowTable, embeddings?: EmbeddingFunction<T>): Promise<Buffer> {
  if (embeddings !== undefined) {
    const source = table.getChild(embeddings.sourceColumn)

    if (source === null) {
      throw new Error(`The embedding source column ${embeddings.sourceColumn} was not found in the Arrow Table`)
    }

    const vectors = await embeddings.embed(source.toArray() as T[])
    const column = vectorFromArray(vectors, newVectorType(vectors[0].length))
    table = table.assign(new ArrowTable({ vector: column }))
  }
  const writer = RecordBatchStreamWriter.writeAll(table)
  return Buffer.from(await writer.toUint8Array())
}

// Creates an empty Arrow Table
export function createEmptyTable (schema: Schema): ArrowTable {
  return new ArrowTable(schema)
}
