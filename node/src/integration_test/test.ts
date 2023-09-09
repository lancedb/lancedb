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
import * as chai from 'chai'
import * as chaiAsPromised from 'chai-as-promised'
import { v4 as uuidv4 } from 'uuid'

import * as lancedb from '../index'

const assert = chai.assert
chai.use(chaiAsPromised)

describe('LanceDB AWS Integration test', function () {
  it('s3+ddb schema is processed correctly', async function () {
    this.timeout(5000)

    // WARNING: specifying engine is NOT a publicly supported feature in lancedb yet
    // THE API WILL CHANGE
    const conn = await lancedb.connect('s3://lancedb-integtest?engine=ddb&ddbTableName=lancedb-integtest')
    const data = [{ vector: Array(128).fill(1.0) }]

    const tableName = uuidv4()
    let table = await conn.createTable(tableName, data, { writeMode: lancedb.WriteMode.Overwrite })

    const futs = [table.add(data), table.add(data), table.add(data), table.add(data), table.add(data)]
    await Promise.allSettled(futs)

    table = await conn.openTable(tableName)
    assert.equal(await table.countRows(), 6)
  })
})
