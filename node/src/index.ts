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

import {
  RecordBatchFileWriter,
  type Table as ArrowTable,
  tableFromIPC,
  Vector
} from 'apache-arrow'
import { fromRecordsToBuffer } from './arrow'

// eslint-disable-next-line @typescript-eslint/no-var-requires
const { databaseNew, databaseTableNames, databaseOpenTable, tableCreate, tableSearch, tableAdd } = require('../native.js')

/**
 * Connect to a LanceDB instance at the given URI
 * @param uri The uri of the database.
 */
export async function connect (uri: string): Promise<Connection> {
  return new Connection(uri)
}

/**
 * A connection to a LanceDB database.
 */
export class Connection {
  private readonly _uri: string
  private readonly _db: any

  constructor (uri: string) {
    this._uri = uri
    this._db = databaseNew(uri)
  }

  get uri (): string {
    return this._uri
  }

  /**
     * Get the names of all tables in the database.
     */
  async tableNames (): Promise<string[]> {
    return databaseTableNames.call(this._db)
  }

  /**
     * Open a table in the database.
     * @param name The name of the table.
     */
  async openTable (name: string): Promise<Table> {
    const tbl = await databaseOpenTable.call(this._db, name)
    return new Table(tbl, name)
  }

  async createTable (name: string, data: Array<Record<string, unknown>>): Promise<Table> {
    await tableCreate.call(this._db, name, await fromRecordsToBuffer(data))
    return await this.openTable(name)
  }

  async createTableArrow (name: string, table: ArrowTable): Promise<Table> {
    const writer = RecordBatchFileWriter.writeAll(table)
    await tableCreate.call(this._db, name, Buffer.from(await writer.toUint8Array()))
    return await this.openTable(name)
  }
}

/**
 * A table in a LanceDB database.
 */
export class Table {
  private readonly _tbl: any
  private readonly _name: string

  constructor (tbl: any, name: string) {
    this._tbl = tbl
    this._name = name
  }

  get name (): string {
    return this._name
  }

  /**
     * Create a search query to find the nearest neighbors of the given query vector.
     * @param queryVector The query vector.
     */
  search (queryVector: number[]): Query {
    return new Query(this._tbl, queryVector)
  }

  /**
   * Insert records into this Table
   * @param data Records to be inserted into the Table
   *
   * @param mode Append / Overwrite existing records. Default: Append
   * @return The number of rows added to the table
   */
  async add (data: Array<Record<string, unknown>>): Promise<number> {
    return tableAdd.call(this._tbl, await fromRecordsToBuffer(data), WriteMode.Append.toString())
  }

  async overwrite (data: Array<Record<string, unknown>>): Promise<number> {
    return tableAdd.call(this._tbl, await fromRecordsToBuffer(data), WriteMode.Overwrite.toString())
  }
}

/**
 * A builder for nearest neighbor queries for LanceDB.
 */
export class Query {
  private readonly _tbl: any
  private readonly _queryVector: number[]
  private _limit: number
  private _refineFactor?: number
  private _nprobes: number
  private readonly _columns?: string[]
  private _filter?: string
  private _metricType?: MetricType

  constructor (tbl: any, queryVector: number[]) {
    this._tbl = tbl
    this._queryVector = queryVector
    this._limit = 10
    this._nprobes = 20
    this._refineFactor = undefined
    this._columns = undefined
    this._filter = undefined
  }

  limit (value: number): Query {
    this._limit = value
    return this
  }

  refineFactor (value: number): Query {
    this._refineFactor = value
    return this
  }

  nprobes (value: number): Query {
    this._nprobes = value
    return this
  }

  filter (value: string): Query {
    this._filter = value
    return this
  }

  metricType (value: MetricType): Query {
    this._metricType = value
    return this
  }

  /**
     * Execute the query and return the results as an Array of Objects
     */
  async execute<T = Record<string, unknown>> (): Promise<T[]> {
    const buffer = await tableSearch.call(this._tbl, this)
    const data = tableFromIPC(buffer)
    return data.toArray().map((entry: Record<string, unknown>) => {
      const newObject: Record<string, unknown> = {}
      Object.keys(entry).forEach((key: string) => {
        if (entry[key] instanceof Vector) {
          newObject[key] = (entry[key] as Vector).toArray()
        } else {
          newObject[key] = entry[key]
        }
      })
      return newObject as unknown as T
    })
  }
}

export enum WriteMode {
  Overwrite = 'overwrite',
  Append = 'append'
}

export enum MetricType {
  L2 = 'l2',
  Cosine = 'cosine'
}
