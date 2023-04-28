import { describe } from 'mocha'
import { assert } from 'chai'

import * as lancedb from '../index'

describe('LanceDB client', function () {
  describe('open a connection to lancedb', function () {
    const con = lancedb.connect('/tmp/lance')

    it('should have a valid url', function () {
      assert.equal(con.uri, '/tmp/lance')
    })

    it('should return the existing table names', function () {
      assert.deepEqual(con.tableNames(), ['vectors'])
    })

    describe('open a table from a connection', function () {
      const table = con.openTable('vectors')

      it('should have a valid name', function () {
        assert.equal(table.name, 'vectors')
      })

      it('execute a query', function () {
        const builder = table.query([0, 1, 0])
        const results = builder.execute()
        assert.equal(results.length, 1)
        assert.deepEqual(results[0], { vector: [1.0, 2.0, 3.0], id: 1, text: 'Hello World' })
      })

      it('execute a query and type cast the result', function () {
        class MyResult {
          vector: number[] = []
          id: number = 0
          text: string = ''
        }

        const builder = table.query([0, 1, 0])
        const results = builder.execute_cast<MyResult>()
        assert.equal(results.length, 1)
        assert.equal(results[0].id, 1)
        assert.deepEqual(results[0].vector, [1.0, 2.0, 3.0])
        assert.equal(results[0].text, 'Hello World')
      })
    })
  })
})
