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
import { track } from 'temp'

import * as lancedb from '../index'
import { MetricType, Query } from '../index'

describe('LanceDB client', function () {
  describe('when creating a connection to lancedb', function () {
    it('should have a valid url', async function () {
      const uri = await createTestDB()
      const con = await lancedb.connect(uri)
      assert.equal(con.uri, uri)
    })

    it('should return the existing table names', async function () {
      const uri = await createTestDB()
      const con = await lancedb.connect(uri)
      assert.deepEqual(await con.tableNames(), ['vectors'])
    })
  })

  describe('when querying an existing dataset', function () {
    it('should open a table', async function () {
      const uri = await createTestDB()
      const con = await lancedb.connect(uri)
      const table = await con.openTable('vectors')
      assert.equal(table.name, 'vectors')
    })

    it('execute a query', async function () {
      const uri = await createTestDB()
      const con = await lancedb.connect(uri)
      const table = await con.openTable('vectors')
      const results = await table.search([0.1, 0.3]).execute()

      assert.equal(results.length, 2)
      assert.equal(results[0].price, 10)
      const vector = results[0].vector as Float32Array
      assert.approximately(vector[0], 0.0, 0.2)
      assert.approximately(vector[0], 0.1, 0.3)
    })

    it('limits # of results', async function () {
      const uri = await createTestDB()
      const con = await lancedb.connect(uri)
      const table = await con.openTable('vectors')
      const results = await table.search([0.1, 0.3]).limit(1).execute()
      assert.equal(results.length, 1)
      assert.equal(results[0].id, 1)
    })

    it('uses a filter', async function () {
      const uri = await createTestDB()
      const con = await lancedb.connect(uri)
      const table = await con.openTable('vectors')
      const results = await table.search([0.1, 0.3]).filter('id == 2').execute()
      assert.equal(results.length, 1)
      assert.equal(results[0].id, 2)
    })
  })

  describe('when creating a new dataset', function () {
    it('creates a new table from javascript objects', async function () {
      const dir = await track().mkdir('lancejs')
      const con = await lancedb.connect(dir)

      const data = [
        { id: 1, vector: [0.1, 0.2], price: 10 },
        { id: 2, vector: [1.1, 1.2], price: 50 }
      ]

      const tableName = `vectors_${Math.floor(Math.random() * 100)}`
      const table = await con.createTable(tableName, data)
      assert.equal(table.name, tableName)

      const results = await table.search([0.1, 0.3]).execute()
      assert.equal(results.length, 2)
    })

    it('appends records to an existing table ', async function () {
      const dir = await track().mkdir('lancejs')
      const con = await lancedb.connect(dir)

      const data = [
        { id: 1, vector: [0.1, 0.2], price: 10 },
        { id: 2, vector: [1.1, 1.2], price: 50 }
      ]

      const table = await con.createTable('vectors', data)
      const results = await table.search([0.1, 0.3]).execute()
      assert.equal(results.length, 2)

      const dataAdd = [
        { id: 3, vector: [2.1, 2.2], price: 10 },
        { id: 4, vector: [3.1, 3.2], price: 50 }
      ]
      await table.add(dataAdd)
      const resultsAdd = await table.search([0.1, 0.3]).execute()
      assert.equal(resultsAdd.length, 4)
    })

    it('overwrite all records in a table', async function () {
      const uri = await createTestDB()
      const con = await lancedb.connect(uri)

      const table = await con.openTable('vectors')
      const results = await table.search([0.1, 0.3]).execute()
      assert.equal(results.length, 2)

      const dataOver = [
        { vector: [2.1, 2.2], price: 10, name: 'foo' },
        { vector: [3.1, 3.2], price: 50, name: 'bar' }
      ]
      await table.overwrite(dataOver)
      const resultsAdd = await table.search([0.1, 0.3]).execute()
      assert.equal(resultsAdd.length, 2)
    })
  })
})

describe('Query object', function () {
  it('sets custom parameters', async function () {
    const query = new Query(undefined, [0.1, 0.3])
      .limit(1)
      .metricType(MetricType.Cosine)
      .refineFactor(100)
      .nprobes(20) as Record<string, any>
    assert.equal(query._limit, 1)
    assert.equal(query._metricType, MetricType.Cosine)
    assert.equal(query._refineFactor, 100)
    assert.equal(query._nprobes, 20)
  })
})

async function createTestDB (): Promise<string> {
  const dir = await track().mkdir('lancejs')
  const con = await lancedb.connect(dir)

  const data = [
    { id: 1, vector: [0.1, 0.2], name: 'foo', price: 10, is_active: true },
    { id: 2, vector: [1.1, 1.2], name: 'bar', price: 50, is_active: false }
  ]

  await con.createTable('vectors', data)
  return dir
}
