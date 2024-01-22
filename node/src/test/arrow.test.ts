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
import { assert } from 'chai'

import { fromTableToBuffer, makeArrowTable } from '../arrow'
import {
  Field,
  FixedSizeList,
  Float16,
  Float32,
  Int32,
  tableFromIPC,
  Schema,
  Float64
} from 'apache-arrow'

describe('Apache Arrow tables', function () {
  it('customized schema', async function () {
    const schema = new Schema([
      new Field('a', new Int32()),
      new Field('b', new Float32()),
      new Field('c', new FixedSizeList(3, new Field('item', new Float16())))
    ])
    const table = makeArrowTable(
      [
        { a: 1, b: 2, c: [1, 2, 3] },
        { a: 4, b: 5, c: [4, 5, 6] },
        { a: 7, b: 8, c: [7, 8, 9] }
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

  it('default vector column', async function () {
    const schema = new Schema([
      new Field('a', new Float64()),
      new Field('b', new Float64()),
      new Field(
        'vector',
        new FixedSizeList(3, new Field('item', new Float32()))
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

  it('2 vector columns', async function () {
    const schema = new Schema([
      new Field('a', new Float64()),
      new Field('b', new Float64()),
      new Field('vec1', new FixedSizeList(3, new Field('item', new Float16()))),
      new Field('vec2', new FixedSizeList(3, new Field('item', new Float16())))
    ])
    const table = makeArrowTable(
      [
        { a: 1, b: 2, vec1: [1, 2, 3], vec2: [2, 4, 6] },
        { a: 4, b: 5, vec1: [4, 5, 6], vec2: [8, 10, 12] },
        { a: 7, b: 8, vec1: [7, 8, 9], vec2: [14, 16, 18] }
      ],
      { vectorColumns: ['vec1', 'vec2'], vectorDataType: new Float16() }
    )

    const buf = await fromTableToBuffer(table)
    assert.isAbove(buf.byteLength, 0)

    const actual = tableFromIPC(buf)
    assert.equal(actual.numRows, 3)
    const actualSchema = actual.schema
    assert.deepEqual(actualSchema, schema)
  })
})
