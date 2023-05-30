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
import { MetricType, Query } from '../index'

describe('LanceDB S3 client', function () {
    describe('when creating a connection on S3', function () {
      it('should have a valid url', async function () {
        const uri = "s3://eto-ops-testing/lancedb";
        await createTestDB(uri)
        console.log("Created table on S3", uri);
        const con = await lancedb.connect(uri)
        assert.equal(con.uri, uri)
      })
    })
})

async function createTestDB (uri: string, numDimensions: number = 2, numRows: number = 2) {
    const con = await lancedb.connect(uri)

    const data = []
    for (let i = 0; i < numRows; i++) {
      const vector = []
      for (let j = 0; j < numDimensions; j++) {
        vector.push(i + (j * 0.1))
      }
      data.push({ id: i + 1, name: `name_${i}`, price: i + 10, is_active: (i % 2 === 0), vector })
    }

    await con.createTable('vectors', data)
  }

