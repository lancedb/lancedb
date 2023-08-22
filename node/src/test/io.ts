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

// IO tests

import { describe } from 'mocha'
import { assert } from 'chai'

import * as lancedb from '../index'
import { type ConnectionOptions } from '../index'

describe('LanceDB S3 client', function () {
  if (process.env.TEST_S3_BASE_URL != null) {
    const baseUri = process.env.TEST_S3_BASE_URL
    it('should have a valid url', async function () {
      const opts = { uri: `${baseUri}/valid_url` }
      const table = await createTestDB(opts, 2, 20)
      const con = await lancedb.connect(opts)
      assert.equal(con.uri, opts.uri)

      const results = await table.search([0.1, 0.3]).limit(5).execute()
      assert.equal(results.length, 5)
    }).timeout(10_000)
  } else {
    describe.skip('Skip S3 test', function () {})
  }

  if (process.env.TEST_S3_BASE_URL != null && process.env.TEST_AWS_ACCESS_KEY_ID != null && process.env.TEST_AWS_SECRET_ACCESS_KEY != null) {
    const baseUri = process.env.TEST_S3_BASE_URL
    it('use custom credentials', async function () {
      const opts: ConnectionOptions = {
        uri: `${baseUri}/custom_credentials`,
        awsCredentials: {
          accessKeyId: process.env.TEST_AWS_ACCESS_KEY_ID as string,
          secretKey: process.env.TEST_AWS_SECRET_ACCESS_KEY as string
        }
      }
      const table = await createTestDB(opts, 2, 20)
      console.log(table)
      const con = await lancedb.connect(opts)
      console.log(con)
      assert.equal(con.uri, opts.uri)

      const results = await table.search([0.1, 0.3]).limit(5).execute()
      assert.equal(results.length, 5)
    }).timeout(10_000)
  } else {
    describe.skip('Skip S3 test', function () {})
  }
})

async function createTestDB (opts: ConnectionOptions, numDimensions: number = 2, numRows: number = 2): Promise<lancedb.Table> {
  const con = await lancedb.connect(opts)

  const data = []
  for (let i = 0; i < numRows; i++) {
    const vector = []
    for (let j = 0; j < numDimensions; j++) {
      vector.push(i + (j * 0.1))
    }
    data.push({ id: i + 1, name: `name_${i}`, price: i + 10, is_active: (i % 2 === 0), vector })
  }

  return await con.createTable('vectors_2', data)
}
