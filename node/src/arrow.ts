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
  Float32,
  List,
  type ListBuilder,
  makeBuilder,
  RecordBatchFileWriter,
  Table,
  Utf8,
  type Vector,
  vectorFromArray
} from 'apache-arrow'
import { type EmbeddingFunction } from './index'
import { OnBadVectors, type Result } from './common'

export async function convertToTable<T> (
  data: Array<Record<string, unknown>>, embeddings?: EmbeddingFunction<T>,
  onBadVectors: OnBadVectors = OnBadVectors.DROP,
  fillValue: number = 0.0): Promise<Table> {
  if (data.length === 0) {
    throw new Error('At least one record needs to be provided')
  }

  await embed(data, embeddings)

  // sanitize the data, ensuring the vectors are all the same size and don't contain nulls or undefined
  const rs = _sanitizeVector(data, 'vector', onBadVectors, fillValue)
  if (!rs.ok) {
    throw rs.error
  } else {
    data = rs.value
  }

  const columns = new Set<string>()
  data.forEach(d => { Object.keys(d).forEach(k => columns.add(k)) })
  const records: Record<string, Vector> = {}
  for (const columnsKey of columns) {
    if (columnsKey === 'vector') {
      const listBuilder = newVectorListBuilder()
      for (const datum of data) {
        listBuilder.append(datum[columnsKey])
      }
      records[columnsKey] = listBuilder.finish().toVector()
    } else {
      const values = data.map(d => d[columnsKey])
      if (typeof values[0] === 'string') {
        // `vectorFromArray` converts strings into dictionary vectors, forcing it back to a string column
        records[columnsKey] = vectorFromArray(values, new Utf8())
      } else {
        records[columnsKey] = vectorFromArray(values)
      }
    }
  }

  return new Table(records)
}

async function embed<T> (data: Array<Record<string, unknown>>, embeddings?: EmbeddingFunction<T>): Promise<void> {
  // create embeddings if needed
  if (embeddings !== undefined) {
    const values: T[] = data.map(d => d[embeddings.sourceColumn] as T)
    const vectors = await embeddings.embed(values)
    if (vectors.length !== data.length) {
      throw new Error('Embedding function returned wrong number of vectors')
    }
    data.forEach((d, i) => {
      d.vector = vectors[i]
    })
  }
}

function _sanitizeVector<T extends number> (
  data: Array<Record<string, unknown>>,
  vectorColumnName: string,
  onBadVectors: OnBadVectors,
  fillValue?: T): Result<Array<Record<string, unknown>>, Error> {
  const lengths = data.map(d => (d[vectorColumnName] as number[]).length)
  const maxNdims = lengths.reduce((a, b) => Math.max(a, b))

  if (onBadVectors === OnBadVectors.ERROR) {
    for (const rec of data) {
      const vec = rec[vectorColumnName] as any[]
      if (vec.length !== maxNdims) {
        return { ok: false, error: new Error(`Invalid vector size, expected ${maxNdims}`) }
      }
      if (vec.findIndex(v => v === null || v === undefined) >= 0) {
        return { ok: false, error: new Error('Vector contains null') }
      }
    }
    return { ok: true, value: data }
  } else if (onBadVectors === OnBadVectors.DROP) {
    return { ok: true, value: data.filter(d => isVectorValid(d[vectorColumnName] as any[], maxNdims)) }
  } else if (onBadVectors === OnBadVectors.FILL) {
    if (fillValue === undefined) {
      throw new TypeError('If onBadVectors is FILL, fillValue must be provided')
    }
    return { ok: true, value: data.map(d => fillVector(d, vectorColumnName, maxNdims, fillValue)) }
  } else {
    throw new Error('Invalid value for onBadVector')
  }
}

function isVectorValid (vec: any[], maxNdims: number): boolean {
  return vec.length === maxNdims && vec.findIndex(v => v === null || v === undefined) < 0
}

// Fill vectors that have nulls or
function fillVector (rec: Record<string, unknown>, vectorColumnName: string,
  maxNdims: number, fillValue: number): Record<string, unknown> {
  let vec = rec[vectorColumnName] as any[]
  vec = vec.map(v => (v === null || v === undefined) ? fillValue : v)
  if (vec.length < maxNdims) {
    vec = vec.concat(Array(maxNdims - vec.length).fill(fillValue))
  }
  rec[vectorColumnName] = vec
  return rec
}

// Creates a new Arrow ListBuilder that stores a Vector column
function newVectorListBuilder (): ListBuilder<Float32, any> {
  const children = new Field<Float32>('item', new Float32())
  const list = new List(children)
  return makeBuilder({
    type: list
  })
}

export async function fromRecordsToBuffer<T> (
  data: Array<Record<string, unknown>>, embeddings?: EmbeddingFunction<T>,
  onBadVectors: OnBadVectors = OnBadVectors.DROP,
  fillValue: number = 0.0): Promise<Buffer> {
  const table = await convertToTable(data, embeddings, onBadVectors, fillValue)
  const writer = RecordBatchFileWriter.writeAll(table)
  return Buffer.from(await writer.toUint8Array())
}
