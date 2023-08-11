import { describe } from 'mocha'
import { assert } from 'chai'

import * as lancedb from '../../index'

describe('LanceDB Remote client', function () {
  if (process.env.LANCEDB_URI != null && process.env.LANCEDB_REGION != null && process.env.LANCEDB_APIKEY != null) {
    it('should perform a simple search', async function () {
      const db = await lancedb.connect({ uri: process.env.LANCEDB_URI, region: process.env.LANCEDB_REGION, apiKey: process.env.LANCEDB_APIKEY })
      const tableName = 'gc-2023-08-11'
      console.log(await db.tableNames())

      const table = await db.openTable(tableName)
      assert.equal(table.name, tableName)

      const results = await table.search([0.1, 0.2]).execute()
      console.log(results)

      // await db.dropTable(tableName)
    })
  } else {
    describe.skip('Skip remote test', function () {})
  }
})
