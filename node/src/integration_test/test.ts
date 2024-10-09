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
import { assert } from 'chai'
import * as chaiAsPromised from 'chai-as-promised'
import { v4 as uuidv4 } from 'uuid'

import * as lancedb from '../index'
import { tmpdir } from 'os'
import * as fs from 'fs'
import * as path from 'path'

chai.use(chaiAsPromised)

describe('LanceDB AWS Integration test', function () {
  it('s3+ddb schema is processed correctly', async function () {
    this.timeout(15000)

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

describe('LanceDB Mirrored Store Integration test', function () {
  it('s3://...?mirroredStore=... param is processed correctly', async function () {
    this.timeout(600000)

    const dir = tmpdir()
    console.log(dir)
    const conn = await lancedb.connect({ uri: `s3://lancedb-integtest?mirroredStore=${dir}`, storageOptions: { allowHttp: 'true' } })
    const data = Array(200).fill({ vector: Array(128).fill(1.0), id: 0 })
    data.push(...Array(200).fill({ vector: Array(128).fill(1.0), id: 1 }))
    data.push(...Array(200).fill({ vector: Array(128).fill(1.0), id: 2 }))
    data.push(...Array(200).fill({ vector: Array(128).fill(1.0), id: 3 }))

    const tableName = uuidv4()

    // try create table and check if it's mirrored
    const t = await conn.createTable(tableName, data, { writeMode: lancedb.WriteMode.Overwrite })

    const mirroredPath = path.join(dir, `${tableName}.lance`)
    fs.readdir(mirroredPath, { withFileTypes: true }, (err, files) => {
      if (err != null) throw err
      // there should be three dirs
      assert.equal(files.length, 3)
      assert.isTrue(files[0].isDirectory())
      assert.isTrue(files[1].isDirectory())

      fs.readdir(path.join(mirroredPath, '_transactions'), { withFileTypes: true }, (err, files) => {
        if (err != null) throw err
        assert.equal(files.length, 1)
        assert.isTrue(files[0].name.endsWith('.txn'))
      })

      fs.readdir(path.join(mirroredPath, '_versions'), { withFileTypes: true }, (err, files) => {
        if (err != null) throw err
        assert.equal(files.length, 1)
        assert.isTrue(files[0].name.endsWith('.manifest'))
      })

      fs.readdir(path.join(mirroredPath, 'data'), { withFileTypes: true }, (err, files) => {
        if (err != null) throw err
        assert.equal(files.length, 1)
        assert.isTrue(files[0].name.endsWith('.lance'))
      })
    })

    // try create index and check if it's mirrored
    await t.createIndex({ column: 'vector', type: 'ivf_pq' })

    fs.readdir(mirroredPath, { withFileTypes: true }, (err, files) => {
      if (err != null) throw err
      // there should be four dirs
      assert.equal(files.length, 4)
      assert.isTrue(files[0].isDirectory())
      assert.isTrue(files[1].isDirectory())
      assert.isTrue(files[2].isDirectory())

      // Two TXs now
      fs.readdir(path.join(mirroredPath, '_transactions'), { withFileTypes: true }, (err, files) => {
        if (err != null) throw err
        assert.equal(files.length, 2)
        assert.isTrue(files[0].name.endsWith('.txn'))
        assert.isTrue(files[1].name.endsWith('.txn'))
      })

      fs.readdir(path.join(mirroredPath, 'data'), { withFileTypes: true }, (err, files) => {
        if (err != null) throw err
        assert.equal(files.length, 1)
        assert.isTrue(files[0].name.endsWith('.lance'))
      })

      fs.readdir(path.join(mirroredPath, '_indices'), { withFileTypes: true }, (err, files) => {
        if (err != null) throw err
        assert.equal(files.length, 1)
        assert.isTrue(files[0].isDirectory())

        fs.readdir(path.join(mirroredPath, '_indices', files[0].name), { withFileTypes: true }, (err, files) => {
          if (err != null) throw err

          assert.equal(files.length, 1)
          assert.isTrue(files[0].isFile())
          assert.isTrue(files[0].name.endsWith('.idx'))
        })
      })
    })

    // try delete and check if it's mirrored
    await t.delete('id = 0')

    fs.readdir(mirroredPath, { withFileTypes: true }, (err, files) => {
      if (err != null) throw err
      // there should be five dirs
      assert.equal(files.length, 5)
      assert.isTrue(files[0].isDirectory())
      assert.isTrue(files[1].isDirectory())
      assert.isTrue(files[2].isDirectory())
      assert.isTrue(files[3].isDirectory())
      assert.isTrue(files[4].isDirectory())

      // Three TXs now
      fs.readdir(path.join(mirroredPath, '_transactions'), { withFileTypes: true }, (err, files) => {
        if (err != null) throw err
        assert.equal(files.length, 3)
        assert.isTrue(files[0].name.endsWith('.txn'))
        assert.isTrue(files[1].name.endsWith('.txn'))
      })

      fs.readdir(path.join(mirroredPath, 'data'), { withFileTypes: true }, (err, files) => {
        if (err != null) throw err
        assert.equal(files.length, 1)
        assert.isTrue(files[0].name.endsWith('.lance'))
      })

      fs.readdir(path.join(mirroredPath, '_indices'), { withFileTypes: true }, (err, files) => {
        if (err != null) throw err
        assert.equal(files.length, 1)
        assert.isTrue(files[0].isDirectory())

        fs.readdir(path.join(mirroredPath, '_indices', files[0].name), { withFileTypes: true }, (err, files) => {
          if (err != null) throw err

          assert.equal(files.length, 1)
          assert.isTrue(files[0].isFile())
          assert.isTrue(files[0].name.endsWith('.idx'))
        })
      })

      fs.readdir(path.join(mirroredPath, '_deletions'), { withFileTypes: true }, (err, files) => {
        if (err != null) throw err
        assert.equal(files.length, 1)
        assert.isTrue(files[0].name.endsWith('.arrow'))
      })
    })
  })
})
