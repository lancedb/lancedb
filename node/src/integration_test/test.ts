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

    const dir = await fs.promises.mkdtemp(path.join(tmpdir(), 'lancedb-mirror-'))
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

    const files = await fs.promises.readdir(mirroredPath, { withFileTypes: true })
    // there should be three dirs
    assert.equal(files.length, 3, 'files after table creation')
    assert.isTrue(files[0].isDirectory())
    assert.isTrue(files[1].isDirectory())

    const transactionFiles = await fs.promises.readdir(path.join(mirroredPath, '_transactions'), { withFileTypes: true })
    assert.equal(transactionFiles.length, 1, 'transactionFiles after table creation')
    assert.isTrue(transactionFiles[0].name.endsWith('.txn'))

    const versionFiles = await fs.promises.readdir(path.join(mirroredPath, '_versions'), { withFileTypes: true })
    assert.equal(versionFiles.length, 1, 'versionFiles after table creation')
    assert.isTrue(versionFiles[0].name.endsWith('.manifest'))

    const dataFiles = await fs.promises.readdir(path.join(mirroredPath, 'data'), { withFileTypes: true })
    assert.equal(dataFiles.length, 1, 'dataFiles after table creation')
    assert.isTrue(dataFiles[0].name.endsWith('.lance'))

    // try create index and check if it's mirrored
    await t.createIndex({ column: 'vector', type: 'ivf_pq' })

    const filesAfterIndex = await fs.promises.readdir(mirroredPath, { withFileTypes: true })
    // there should be four dirs
    assert.equal(filesAfterIndex.length, 4, 'filesAfterIndex')
    assert.isTrue(filesAfterIndex[0].isDirectory())
    assert.isTrue(filesAfterIndex[1].isDirectory())
    assert.isTrue(filesAfterIndex[2].isDirectory())

    // Two TXs now
    const transactionFilesAfterIndex = await fs.promises.readdir(path.join(mirroredPath, '_transactions'), { withFileTypes: true })
    assert.equal(transactionFilesAfterIndex.length, 2, 'transactionFilesAfterIndex')
    assert.isTrue(transactionFilesAfterIndex[0].name.endsWith('.txn'))
    assert.isTrue(transactionFilesAfterIndex[1].name.endsWith('.txn'))

    const dataFilesAfterIndex = await fs.promises.readdir(path.join(mirroredPath, 'data'), { withFileTypes: true })
    assert.equal(dataFilesAfterIndex.length, 1, 'dataFilesAfterIndex')
    assert.isTrue(dataFilesAfterIndex[0].name.endsWith('.lance'))

    const indicesFiles = await fs.promises.readdir(path.join(mirroredPath, '_indices'), { withFileTypes: true })
    assert.equal(indicesFiles.length, 1, 'indicesFiles')
    assert.isTrue(indicesFiles[0].isDirectory())

    const indexFiles = await fs.promises.readdir(path.join(mirroredPath, '_indices', indicesFiles[0].name), { withFileTypes: true })
    console.log(`DEBUG indexFiles in ${indicesFiles[0].name}:`, indexFiles.map(f => `${f.name} (${f.isFile() ? 'file' : 'dir'})`))
    assert.equal(indexFiles.length, 2, 'indexFiles')
    const fileNames = indexFiles.map(f => f.name).sort()
    assert.isTrue(fileNames.includes('auxiliary.idx'), 'auxiliary.idx should be present')
    assert.isTrue(fileNames.includes('index.idx'), 'index.idx should be present')
    assert.isTrue(indexFiles.every(f => f.isFile()), 'all index files should be files')

    // try delete and check if it's mirrored
    await t.delete('id = 0')

    const filesAfterDelete = await fs.promises.readdir(mirroredPath, { withFileTypes: true })
    // there should be five dirs
    assert.equal(filesAfterDelete.length, 5, 'filesAfterDelete')
    assert.isTrue(filesAfterDelete[0].isDirectory())
    assert.isTrue(filesAfterDelete[1].isDirectory())
    assert.isTrue(filesAfterDelete[2].isDirectory())
    assert.isTrue(filesAfterDelete[3].isDirectory())
    assert.isTrue(filesAfterDelete[4].isDirectory())

    // Three TXs now
    const transactionFilesAfterDelete = await fs.promises.readdir(path.join(mirroredPath, '_transactions'), { withFileTypes: true })
    assert.equal(transactionFilesAfterDelete.length, 3, 'transactionFilesAfterDelete')
    assert.isTrue(transactionFilesAfterDelete[0].name.endsWith('.txn'))
    assert.isTrue(transactionFilesAfterDelete[1].name.endsWith('.txn'))

    const dataFilesAfterDelete = await fs.promises.readdir(path.join(mirroredPath, 'data'), { withFileTypes: true })
    assert.equal(dataFilesAfterDelete.length, 1, 'dataFilesAfterDelete')
    assert.isTrue(dataFilesAfterDelete[0].name.endsWith('.lance'))

    const indicesFilesAfterDelete = await fs.promises.readdir(path.join(mirroredPath, '_indices'), { withFileTypes: true })
    assert.equal(indicesFilesAfterDelete.length, 1, 'indicesFilesAfterDelete')
    assert.isTrue(indicesFilesAfterDelete[0].isDirectory())

    const indexFilesAfterDelete = await fs.promises.readdir(path.join(mirroredPath, '_indices', indicesFilesAfterDelete[0].name), { withFileTypes: true })
    console.log(`DEBUG indexFilesAfterDelete in ${indicesFilesAfterDelete[0].name}:`, indexFilesAfterDelete.map(f => `${f.name} (${f.isFile() ? 'file' : 'dir'})`))
    assert.equal(indexFilesAfterDelete.length, 2, 'indexFilesAfterDelete')
    const fileNamesAfterDelete = indexFilesAfterDelete.map(f => f.name).sort()
    assert.isTrue(fileNamesAfterDelete.includes('auxiliary.idx'), 'auxiliary.idx should be present after delete')
    assert.isTrue(fileNamesAfterDelete.includes('index.idx'), 'index.idx should be present after delete')
    assert.isTrue(indexFilesAfterDelete.every(f => f.isFile()), 'all index files should be files after delete')

    const deletionFiles = await fs.promises.readdir(path.join(mirroredPath, '_deletions'), { withFileTypes: true })
    assert.equal(deletionFiles.length, 1, 'deletionFiles')
    assert.isTrue(deletionFiles[0].name.endsWith('.arrow'))
  })
})
