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

import { describe } from 'mocha'
import { assert } from 'chai'

import * as lancedb from '../index'
import {makeVector, tableFromArrays} from 'apache-arrow'

describe('LanceDB client', function () {
  describe('open a connection to lancedb', function () {
    const con = lancedb.connect('.../../sample-lancedb')

    it.skip('should have a valid url', function () {
      assert.equal(con.uri, '.../../sample-lancedb')
    })

    it.skip('should return the existing table names', function () {
      assert.deepEqual(con.tableNames(), ['my_table'])
    })

    describe.skip('open a table from a connection', function () {
      const tablePromise = con.openTable('my_table')

      it('should have a valid name', async function () {
        const table = await tablePromise
        assert.equal(table.name, 'my_table')
      })

      class MyResult {
        vector: Float32Array = new Float32Array(0)
        price: number = 0
        item: string = ''
        utf8: string = ''
      }

      it('execute a query', async function () {
        const table = await tablePromise
        const builder = table.search([0.1, 0.3])
        const results = await builder.execute() as MyResult[]

        assert.equal(results.length, 2)
        assert.equal(results[0].item, 'foo')
        assert.equal(results[0].price, 10)
        assert.approximately(results[0].vector[0], 3.1, 0.1)
        assert.approximately(results[0].vector[1], 4.1, 0.1)
      })

      it('execute a query and type cast the result', async function () {
        const table = await tablePromise

        const builder = table.search([0.1, 0.3])
        const results = await builder.execute_cast<MyResult>()
        assert.equal(results.length, 2)
        assert.equal(results[0].item, 'foo')
        assert.equal(results[0].price, 10)
        assert.approximately(results[0].vector[0], 3.1, 0.1)
        assert.approximately(results[0].vector[1], 4.1, 0.1)
      })
    })

    describe('create table', function () {
      it.skip('creates a new table from arrow arrays', async function () {
        // This doesn't work, fails with `Error: internal error in Neon module: called `Option::unwrap()` on a `None` value`
        // const vectorsArr = Array.of([0.1, 0.2], [1.1, 1.2])
        const vectorsArr = Array.of(0.1, 0.2)
        const idsArr = Array.of(1, 2)

        // For now I'm creating the arrow Table on the user side, but this need to change
        // After the bug with Lists is resolved
        const rainfall = tableFromArrays({
          vectors: vectorsArr,
          ids: idsArr
        })

        const table = await con.createTableArrow('vectors', rainfall)
        assert.equal(table.name, 'vectors')
      })

      it('creates a new table from javascript objects', async function () {
        const data = [
          { id: 1, vector: [0.1, 0.2], price: 10 },
          { id: 2, vector: [1.1, 1.2], price: 50 }
        ]

        const tableName = `vectors_${Math.floor(Math.random() * 100)}`
        const table = await con.createTable(tableName, data)
        assert.equal(table.name, tableName)

        const builder = table.search([0.1, 0.3])
        const results = await builder.execute()
        console.table(results)
      })
    })
  })
})
