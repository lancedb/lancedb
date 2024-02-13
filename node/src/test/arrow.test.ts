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

import { describe } from 'mocha'
import { assert, expect, use as chaiUse } from 'chai'
import * as chaiAsPromised from 'chai-as-promised'

import { convertToTable, fromTableToBuffer, makeArrowTable, makeEmptyTable } from '../arrow'
import {
  Field,
  FixedSizeList,
  Float16,
  Float32,
  Int32,
  tableFromIPC,
  Schema,
  Float64,
  type Table,
  Binary,
  Bool,
  Utf8,
  Struct,
  List,
  DataType,
  Dictionary,
  Int64
} from 'apache-arrow'
import { type EmbeddingFunction } from '../embedding/embedding_function'

chaiUse(chaiAsPromised)

function sampleRecords (): Array<Record<string, any>> {
  return [
    {
      binary: Buffer.alloc(5),
      boolean: false,
      number: 7,
      string: 'hello',
      struct: { x: 0, y: 0 },
      list: ['anime', 'action', 'comedy']
    }
  ]
}

// Helper method to verify various ways to create a table
async function checkTableCreation (tableCreationMethod: (records: any, recordsReversed: any, schema: Schema) => Promise<Table>): Promise<void> {
  const records = sampleRecords()
  const recordsReversed = [{
    list: ['anime', 'action', 'comedy'],
    struct: { x: 0, y: 0 },
    string: 'hello',
    number: 7,
    boolean: false,
    binary: Buffer.alloc(5)
  }]
  const schema = new Schema([
    new Field('binary', new Binary(), false),
    new Field('boolean', new Bool(), false),
    new Field('number', new Float64(), false),
    new Field('string', new Utf8(), false),
    new Field('struct', new Struct([
      new Field('x', new Float64(), false),
      new Field('y', new Float64(), false)
    ])),
    new Field('list', new List(new Field('item', new Utf8(), false)), false)
  ])

  const table = await tableCreationMethod(records, recordsReversed, schema)
  schema.fields.forEach((field, idx) => {
    const actualField = table.schema.fields[idx]
    assert.isFalse(actualField.nullable)
    assert.equal(table.getChild(field.name)?.type.toString(), field.type.toString())
    assert.equal(table.getChildAt(idx)?.type.toString(), field.type.toString())
  })
}

describe('The function makeArrowTable', function () {
  it('will use data types from a provided schema instead of inference', async function () {
    const schema = new Schema([
      new Field('a', new Int32()),
      new Field('b', new Float32()),
      new Field('c', new FixedSizeList(3, new Field('item', new Float16()))),
      new Field('d', new Int64())
    ])
    const table = makeArrowTable(
      [
        { a: 1, b: 2, c: [1, 2, 3], d: 9 },
        { a: 4, b: 5, c: [4, 5, 6], d: 10 },
        { a: 7, b: 8, c: [7, 8, 9], d: null }
      ],
      { schema }
    )

    const buf = await fromTableToBuffer(table)
    assert.isAbove(buf.byteLength, 0)

    const actual = tableFromIPC(buf)
    assert.equal(actual.numRows, 3)
    const actualSchema = actual.schema
    assert.deepEqual(actualSchema, schema)
  })

  it('will assume the column `vector` is FixedSizeList<Float32> by default', async function () {
    const schema = new Schema([
      new Field('a', new Float64()),
      new Field('b', new Float64()),
      new Field(
        'vector',
        new FixedSizeList(3, new Field('item', new Float32(), true))
      )
    ])
    const table = makeArrowTable([
      { a: 1, b: 2, vector: [1, 2, 3] },
      { a: 4, b: 5, vector: [4, 5, 6] },
      { a: 7, b: 8, vector: [7, 8, 9] }
    ])

    const buf = await fromTableToBuffer(table)
    assert.isAbove(buf.byteLength, 0)

    const actual = tableFromIPC(buf)
    assert.equal(actual.numRows, 3)
    const actualSchema = actual.schema
    assert.deepEqual(actualSchema, schema)
  })

  it('can support multiple vector columns', async function () {
    const schema = new Schema([
      new Field('a', new Float64()),
      new Field('b', new Float64()),
      new Field('vec1', new FixedSizeList(3, new Field('item', new Float16(), true))),
      new Field('vec2', new FixedSizeList(3, new Field('item', new Float16(), true)))
    ])
    const table = makeArrowTable(
      [
        { a: 1, b: 2, vec1: [1, 2, 3], vec2: [2, 4, 6] },
        { a: 4, b: 5, vec1: [4, 5, 6], vec2: [8, 10, 12] },
        { a: 7, b: 8, vec1: [7, 8, 9], vec2: [14, 16, 18] }
      ],
      {
        vectorColumns: {
          vec1: { type: new Float16() },
          vec2: { type: new Float16() }
        }
      }
    )

    const buf = await fromTableToBuffer(table)
    assert.isAbove(buf.byteLength, 0)

    const actual = tableFromIPC(buf)
    assert.equal(actual.numRows, 3)
    const actualSchema = actual.schema
    assert.deepEqual(actualSchema, schema)
  })

  it('will allow different vector column types', async function () {
    const table = makeArrowTable(
      [
        { fp16: [1], fp32: [1], fp64: [1] }
      ],
      {
        vectorColumns: {
          fp16: { type: new Float16() },
          fp32: { type: new Float32() },
          fp64: { type: new Float64() }
        }
      }
    )

    assert.equal(table.getChild('fp16')?.type.children[0].type.toString(), new Float16().toString())
    assert.equal(table.getChild('fp32')?.type.children[0].type.toString(), new Float32().toString())
    assert.equal(table.getChild('fp64')?.type.children[0].type.toString(), new Float64().toString())
  })

  it('will use dictionary encoded strings if asked', async function () {
    const table = makeArrowTable([{ str: 'hello' }])
    assert.isTrue(DataType.isUtf8(table.getChild('str')?.type))

    const tableWithDict = makeArrowTable([{ str: 'hello' }], { dictionaryEncodeStrings: true })
    assert.isTrue(DataType.isDictionary(tableWithDict.getChild('str')?.type))

    const schema = new Schema([
      new Field('str', new Dictionary(new Utf8(), new Int32()))
    ])

    const tableWithDict2 = makeArrowTable([{ str: 'hello' }], { schema })
    assert.isTrue(DataType.isDictionary(tableWithDict2.getChild('str')?.type))
  })

  it('will infer data types correctly', async function () {
    await checkTableCreation(async (records) => makeArrowTable(records))
  })

  it('will allow a schema to be provided', async function () {
    await checkTableCreation(async (records, _, schema) => makeArrowTable(records, { schema }))
  })

  it('will use the field order of any provided schema', async function () {
    await checkTableCreation(async (_, recordsReversed, schema) => makeArrowTable(recordsReversed, { schema }))
  })

  it('will make an empty table', async function () {
    await checkTableCreation(async (_, __, schema) => makeArrowTable([], { schema }))
  })
})

class DummyEmbedding implements EmbeddingFunction<string> {
  public readonly sourceColumn = 'string'
  public readonly embeddingDimension = 2
  public readonly embeddingDataType = new Float16()

  async embed (data: string[]): Promise<number[][]> {
    return data.map(
      () => [0.0, 0.0]
    )
  }
}

class DummyEmbeddingWithNoDimension implements EmbeddingFunction<string> {
  public readonly sourceColumn = 'string'

  async embed (data: string[]): Promise<number[][]> {
    return data.map(
      () => [0.0, 0.0]
    )
  }
}

describe('convertToTable', function () {
  it('will infer data types correctly', async function () {
    await checkTableCreation(async (records) => await convertToTable(records))
  })

  it('will allow a schema to be provided', async function () {
    await checkTableCreation(async (records, _, schema) => await convertToTable(records, undefined, { schema }))
  })

  it('will use the field order of any provided schema', async function () {
    await checkTableCreation(async (_, recordsReversed, schema) => await convertToTable(recordsReversed, undefined, { schema }))
  })

  it('will make an empty table', async function () {
    await checkTableCreation(async (_, __, schema) => await convertToTable([], undefined, { schema }))
  })

  it('will apply embeddings', async function () {
    const records = sampleRecords()
    const table = await convertToTable(records, new DummyEmbedding())
    assert.isTrue(DataType.isFixedSizeList(table.getChild('vector')?.type))
    assert.equal(table.getChild('vector')?.type.children[0].type.toString(), new Float16().toString())
  })

  it('will fail if missing the embedding source column', async function () {
    return await expect(convertToTable([{ id: 1 }], new DummyEmbedding())).to.be.rejectedWith("'string' was not present")
  })

  it('use embeddingDimension if embedding missing from table', async function () {
    const schema = new Schema([
      new Field('string', new Utf8(), false)
    ])
    // Simulate getting an empty Arrow table (minus embedding) from some other source
    // In other words, we aren't starting with records
    const table = makeEmptyTable(schema)

    // If the embedding specifies the dimension we are fine
    await fromTableToBuffer(table, new DummyEmbedding())

    // We can also supply a schema and should be ok
    const schemaWithEmbedding = new Schema([
      new Field('string', new Utf8(), false),
      new Field('vector', new FixedSizeList(2, new Field('item', new Float16(), false)), false)
    ])
    await fromTableToBuffer(table, new DummyEmbeddingWithNoDimension(), schemaWithEmbedding)

    // Otherwise we will get an error
    return await expect(fromTableToBuffer(table, new DummyEmbeddingWithNoDimension())).to.be.rejectedWith('does not specify `embeddingDimension`')
  })

  it('will apply embeddings to an empty table', async function () {
    const schema = new Schema([
      new Field('string', new Utf8(), false),
      new Field('vector', new FixedSizeList(2, new Field('item', new Float16(), false)), false)
    ])
    const table = await convertToTable([], new DummyEmbedding(), { schema })
    assert.isTrue(DataType.isFixedSizeList(table.getChild('vector')?.type))
    assert.equal(table.getChild('vector')?.type.children[0].type.toString(), new Float16().toString())
  })

  it('will complain if embeddings present but schema missing embedding column', async function () {
    const schema = new Schema([
      new Field('string', new Utf8(), false)
    ])
    return await expect(convertToTable([], new DummyEmbedding(), { schema })).to.be.rejectedWith('column vector was missing')
  })

  it('will provide a nice error if run twice', async function () {
    const records = sampleRecords()
    const table = await convertToTable(records, new DummyEmbedding())
    // fromTableToBuffer will try and apply the embeddings again
    return await expect(fromTableToBuffer(table, new DummyEmbedding())).to.be.rejectedWith('already existed')
  })
})

describe('makeEmptyTable', function () {
  it('will make an empty table', async function () {
    await checkTableCreation(async (_, __, schema) => makeEmptyTable(schema))
  })
})
