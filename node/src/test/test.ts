// Copyright 2023 LanceDB Developers.
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
import { track } from 'temp'
import * as chai from 'chai'
import * as chaiAsPromised from 'chai-as-promised'

import * as lancedb from '../index'
import { type AwsCredentials, type EmbeddingFunction, MetricType, WriteMode } from '../index'
import { Query } from '../query'

const expect = chai.expect
const assert = chai.assert
chai.use(chaiAsPromised)

describe('LanceDB client', function () {
  describe('when creating a connection to lancedb', function () {
    it('should have a valid url', async function () {
      const uri = await createTestDB()
      const con = await lancedb.connect(uri)
      assert.equal(con.uri, uri)
    })

    it('should accept an options object', async function () {
      const uri = await createTestDB()
      const con = await lancedb.connect({ uri })
      assert.equal(con.uri, uri)
    })

    it('should accept custom aws credentials', async function () {
      const uri = await createTestDB()
      const awsCredentials: AwsCredentials = {
        accessKeyId: '',
        secretKey: ''
      }
      const con = await lancedb.connect({ uri, awsCredentials })
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

    it('uses a filter / where clause', async function () {
      // eslint-disable-next-line @typescript-eslint/explicit-function-return-type
      const assertResults = (results: Array<Record<string, unknown>>) => {
        assert.equal(results.length, 1)
        assert.equal(results[0].id, 2)
      }

      const uri = await createTestDB()
      const con = await lancedb.connect(uri)
      const table = await con.openTable('vectors')
      let results = await table.search([0.1, 0.1]).filter('id == 2').execute()
      assertResults(results)
      results = await table.search([0.1, 0.1]).where('id == 2').execute()
      assertResults(results)
    })

    it('select only a subset of columns', async function () {
      const uri = await createTestDB()
      const con = await lancedb.connect(uri)
      const table = await con.openTable('vectors')
      const results = await table.search([0.1, 0.1]).select(['is_active']).execute()
      assert.equal(results.length, 2)
      // vector and score are always returned
      assert.isDefined(results[0].vector)
      assert.isDefined(results[0].score)
      assert.isDefined(results[0].is_active)

      assert.isUndefined(results[0].id)
      assert.isUndefined(results[0].name)
      assert.isUndefined(results[0].price)
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
      assert.equal(await table.countRows(), 2)
    })

    it('use overwrite flag to overwrite existing table', async function () {
      const dir = await track().mkdir('lancejs')
      const con = await lancedb.connect(dir)

      const data = [
        { id: 1, vector: [0.1, 0.2], price: 10 },
        { id: 2, vector: [1.1, 1.2], price: 50 }
      ]

      const tableName = 'overwrite'
      await con.createTable(tableName, data, WriteMode.Create)

      const newData = [
        { id: 1, vector: [0.1, 0.2], price: 10 },
        { id: 2, vector: [1.1, 1.2], price: 50 },
        { id: 3, vector: [1.1, 1.2], price: 50 }
      ]

      await expect(con.createTable(tableName, newData)).to.be.rejectedWith(Error, 'already exists')

      const table = await con.createTable(tableName, newData, WriteMode.Overwrite)
      assert.equal(table.name, tableName)
      assert.equal(await table.countRows(), 3)
    })

    it('appends records to an existing table ', async function () {
      const dir = await track().mkdir('lancejs')
      const con = await lancedb.connect(dir)

      const data = [
        { id: 1, vector: [0.1, 0.2], price: 10, name: 'a' },
        { id: 2, vector: [1.1, 1.2], price: 50, name: 'b' }
      ]

      const table = await con.createTable('vectors', data)
      assert.equal(await table.countRows(), 2)

      const dataAdd = [
        { id: 3, vector: [2.1, 2.2], price: 10, name: 'c' },
        { id: 4, vector: [3.1, 3.2], price: 50, name: 'd' }
      ]
      await table.add(dataAdd)
      assert.equal(await table.countRows(), 4)
    })

    it('overwrite all records in a table', async function () {
      const uri = await createTestDB()
      const con = await lancedb.connect(uri)

      const table = await con.openTable('vectors')
      assert.equal(await table.countRows(), 2)

      const dataOver = [
        { vector: [2.1, 2.2], price: 10, name: 'foo' },
        { vector: [3.1, 3.2], price: 50, name: 'bar' }
      ]
      await table.overwrite(dataOver)
      assert.equal(await table.countRows(), 2)
    })

    it('can delete records from a table', async function () {
      const uri = await createTestDB()
      const con = await lancedb.connect(uri)

      const table = await con.openTable('vectors')
      assert.equal(await table.countRows(), 2)

      await table.delete('price = 10')
      assert.equal(await table.countRows(), 1)
    })
  })

  describe('when creating a vector index', function () {
    it('overwrite all records in a table', async function () {
      const uri = await createTestDB(32, 300)
      const con = await lancedb.connect(uri)
      const table = await con.openTable('vectors')
      await table.createIndex({ type: 'ivf_pq', column: 'vector', num_partitions: 2, max_iters: 2, num_sub_vectors: 2 })
    }).timeout(10_000) // Timeout is high partially because GH macos runner is pretty slow

    it('replace an existing index', async function () {
      const uri = await createTestDB(16, 300)
      const con = await lancedb.connect(uri)
      const table = await con.openTable('vectors')

      await table.createIndex({ type: 'ivf_pq', column: 'vector', num_partitions: 2, max_iters: 2, num_sub_vectors: 2 })

      // Replace should fail if the index already exists
      await expect(table.createIndex({
        type: 'ivf_pq', column: 'vector', num_partitions: 2, max_iters: 2, num_sub_vectors: 2, replace: false
      })
      ).to.be.rejectedWith('LanceError(Index)')

      // Default replace = true
      await table.createIndex({ type: 'ivf_pq', column: 'vector', num_partitions: 2, max_iters: 2, num_sub_vectors: 2 })
    }).timeout(50_000)
  })

  describe('when using a custom embedding function', function () {
    class TextEmbedding implements EmbeddingFunction<string> {
      sourceColumn: string

      constructor (targetColumn: string) {
        this.sourceColumn = targetColumn
      }

      _embedding_map = new Map<string, number[]>([
        ['foo', [2.1, 2.2]],
        ['bar', [3.1, 3.2]]
      ])

      async embed (data: string[]): Promise<number[][]> {
        return data.map(datum => this._embedding_map.get(datum) ?? [0.0, 0.0])
      }
    }

    it('should encode the original data into embeddings', async function () {
      const dir = await track().mkdir('lancejs')
      const con = await lancedb.connect(dir)
      const embeddings = new TextEmbedding('name')

      const data = [
        { price: 10, name: 'foo' },
        { price: 50, name: 'bar' }
      ]
      const table = await con.createTable('vectors', data, WriteMode.Create, embeddings)
      const results = await table.search('foo').execute()
      assert.equal(results.length, 2)
    })
  })
})

describe('Query object', function () {
  it('sets custom parameters', async function () {
    const query = new Query([0.1, 0.3])
      .limit(1)
      .metricType(MetricType.Cosine)
      .refineFactor(100)
      .select(['a', 'b'])
      .nprobes(20) as Record<string, any>
    assert.equal(query._limit, 1)
    assert.equal(query._metricType, MetricType.Cosine)
    assert.equal(query._refineFactor, 100)
    assert.equal(query._nprobes, 20)
    assert.deepEqual(query._select, ['a', 'b'])
  })
})

async function createTestDB (numDimensions: number = 2, numRows: number = 2): Promise<string> {
  const dir = await track().mkdir('lancejs')
  const con = await lancedb.connect(dir)

  const data = []
  for (let i = 0; i < numRows; i++) {
    const vector = []
    for (let j = 0; j < numDimensions; j++) {
      vector.push(i + (j * 0.1))
    }
    data.push({ id: i + 1, name: `name_${i}`, price: i + 10, is_active: (i % 2 === 0), vector })
  }

  await con.createTable('vectors', data)
  return dir
}

describe('Drop table', function () {
  it('drop a table', async function () {
    const dir = await track().mkdir('lancejs')
    const con = await lancedb.connect(dir)

    const data = [
      { price: 10, name: 'foo', vector: [1, 2, 3] },
      { price: 50, name: 'bar', vector: [4, 5, 6] }
    ]
    await con.createTable('t1', data)
    await con.createTable('t2', data)

    assert.deepEqual(await con.tableNames(), ['t1', 't2'])

    await con.dropTable('t1')
    assert.deepEqual(await con.tableNames(), ['t2'])
  })
})
