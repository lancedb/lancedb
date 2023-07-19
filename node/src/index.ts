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
  type Table as ArrowTable
} from 'apache-arrow'
import { fromRecordsToBuffer } from './arrow'
import type { EmbeddingFunction } from './embedding/embedding_function'
import { RemoteConnection } from './remote'
import { Query } from './query'

// eslint-disable-next-line @typescript-eslint/no-var-requires
const { databaseNew, databaseTableNames, databaseOpenTable, databaseDropTable, tableCreate, tableAdd, tableCreateVectorIndex, tableCountRows, tableDelete } = require('../native.js')

export type { EmbeddingFunction }
export { OpenAIEmbeddingFunction } from './embedding/openai'

export interface AwsCredentials {
  accessKeyId: string

  secretKey: string

  sessionToken?: string
}

export interface ConnectionOptions {
  uri: string

  awsCredentials?: AwsCredentials

  // API key for the remote connections
  apiKey?: string
  // Region to connect
  region?: string

  // override the host for the remote connections
  hostOverride?: string
}

/**
 * Connect to a LanceDB instance at the given URI
 * @param uri The uri of the database.
 */
export async function connect (uri: string): Promise<Connection>
export async function connect (opts: Partial<ConnectionOptions>): Promise<Connection>
export async function connect (arg: string | Partial<ConnectionOptions>): Promise<Connection> {
  let opts: ConnectionOptions
  if (typeof arg === 'string') {
    opts = { uri: arg }
  } else {
    // opts = { uri: arg.uri, awsCredentials = arg.awsCredentials }
    opts = Object.assign({
      uri: '',
      awsCredentials: undefined,
      apiKey: undefined,
      region: 'us-west-2'
    }, arg)
  }

  if (opts.uri.startsWith('db://')) {
    // Remote connection
    return new RemoteConnection(opts)
  }
  const db = await databaseNew(opts.uri)
  return new LocalConnection(db, opts)
}

/**
 * A LanceDB Connection that allows you to open tables and create new ones.
 *
 * Connection could be local against filesystem or remote against a server.
 */
export interface Connection {
  uri: string

  tableNames(): Promise<string[]>

  /**
   * Open a table in the database.
   *
   * @param name The name of the table.
   * @param embeddings An embedding function to use on this table
   */
  openTable<T>(name: string, embeddings?: EmbeddingFunction<T>): Promise<Table<T>>

  /**
   * Creates a new Table and initialize it with new data.
   *
   * @param {string} name - The name of the table.
   * @param data - Non-empty Array of Records to be inserted into the table
   * @param {WriteMode} mode - The write mode to use when creating the table.
   * @param {EmbeddingFunction} embeddings - An embedding function to use on this table
   */
  createTable<T>(name: string, data: Array<Record<string, unknown>>, mode?: WriteMode, embeddings?: EmbeddingFunction<T>): Promise<Table<T>>

  createTableArrow(name: string, table: ArrowTable): Promise<Table>

  /**
   * Drop an existing table.
   * @param name The name of the table to drop.
   */
  dropTable(name: string): Promise<void>

}

/**
 * A LanceDB Table is the collection of Records. Each Record has one or more vector fields.
 */
export interface Table<T = number[]> {
  name: string

  /**
   * Creates a search query to find the nearest neighbors of the given search term
   * @param query The query search term
   */
  search: (query: T) => Query<T>

  /**
   * Insert records into this Table.
   *
   * @param data Records to be inserted into the Table
   * @return The number of rows added to the table
   */
  add: (data: Array<Record<string, unknown>>) => Promise<number>

  /**
   * Insert records into this Table, replacing its contents.
   *
   * @param data Records to be inserted into the Table
   * @return The number of rows added to the table
   */
  overwrite: (data: Array<Record<string, unknown>>) => Promise<number>

  /**
   * Create an ANN index on this Table vector index.
   *
   * @param indexParams The parameters of this Index, @see VectorIndexParams.
   */
  createIndex: (indexParams: VectorIndexParams) => Promise<any>

  /**
   * Returns the number of rows in this table.
   */
  countRows: () => Promise<number>

  /**
   * Delete rows from this table.
   *
   * This can be used to delete a single row, many rows, all rows, or
   * sometimes no rows (if your predicate matches nothing).
   *
   * @param filter  A filter in the same format used by a sql WHERE clause. The
   *                filter must not be empty.
   *
   * @examples
   *
   * ```ts
   * const con = await lancedb.connect("./.lancedb")
   * const data = [
   *    {id: 1, vector: [1, 2]},
   *    {id: 2, vector: [3, 4]},
   *    {id: 3, vector: [5, 6]},
   * ];
   * const tbl = await con.createTable("my_table", data)
   * await tbl.delete("id = 2")
   * await tbl.countRows() // Returns 2
   * ```
   *
   * If you have a list of values to delete, you can combine them into a
   * stringified list and use the `IN` operator:
   *
   * ```ts
   * const to_remove = [1, 5];
   * await tbl.delete(`id IN (${to_remove.join(",")})`)
   * await tbl.countRows() // Returns 1
   * ```
   */
  delete: (filter: string) => Promise<void>
}

/**
 * A connection to a LanceDB database.
 */
export class LocalConnection implements Connection {
  private readonly _options: ConnectionOptions
  private readonly _db: any

  constructor (db: any, options: ConnectionOptions) {
    this._options = options
    this._db = db
  }

  get uri (): string {
    return this._options.uri
  }

  /**
   * Get the names of all tables in the database.
   */
  async tableNames (): Promise<string[]> {
    return databaseTableNames.call(this._db)
  }

  /**
   * Open a table in the database.
   *
   * @param name The name of the table.
   */
  async openTable (name: string): Promise<Table>

  /**
   * Open a table in the database.
   *
   * @param name The name of the table.
   * @param embeddings An embedding function to use on this Table
   */
  async openTable<T> (name: string, embeddings: EmbeddingFunction<T>): Promise<Table<T>>
  async openTable<T> (name: string, embeddings?: EmbeddingFunction<T>): Promise<Table<T>>
  async openTable<T> (name: string, embeddings?: EmbeddingFunction<T>): Promise<Table<T>> {
    const tbl = await databaseOpenTable.call(this._db, name)
    if (embeddings !== undefined) {
      return new LocalTable(tbl, name, this._options, embeddings)
    } else {
      return new LocalTable(tbl, name, this._options)
    }
  }

  /**
   * Creates a new Table and initialize it with new data.
   *
   * @param name The name of the table.
   * @param data Non-empty Array of Records to be inserted into the Table
   * @param mode The write mode to use when creating the table.
   */
  async createTable (name: string, data: Array<Record<string, unknown>>, mode?: WriteMode): Promise<Table>
  async createTable (name: string, data: Array<Record<string, unknown>>, mode: WriteMode): Promise<Table>

  /**
   * Creates a new Table and initialize it with new data.
   *
   * @param name The name of the table.
   * @param data Non-empty Array of Records to be inserted into the Table
   * @param mode The write mode to use when creating the table.
   * @param embeddings An embedding function to use on this Table
   */
  async createTable<T> (name: string, data: Array<Record<string, unknown>>, mode: WriteMode, embeddings: EmbeddingFunction<T>): Promise<Table<T>>
  async createTable<T> (name: string, data: Array<Record<string, unknown>>, mode: WriteMode, embeddings?: EmbeddingFunction<T>): Promise<Table<T>>
  async createTable<T> (name: string, data: Array<Record<string, unknown>>, mode: WriteMode, embeddings?: EmbeddingFunction<T>): Promise<Table<T>> {
    if (mode === undefined) {
      mode = WriteMode.Create
    }

    const createArgs = [this._db, name, await fromRecordsToBuffer(data, embeddings), mode.toLowerCase()]
    if (this._options.awsCredentials !== undefined) {
      createArgs.push(this._options.awsCredentials.accessKeyId)
      createArgs.push(this._options.awsCredentials.secretKey)
      if (this._options.awsCredentials.sessionToken !== undefined) {
        createArgs.push(this._options.awsCredentials.sessionToken)
      }
    }

    const tbl = await tableCreate.call(...createArgs)

    if (embeddings !== undefined) {
      return new LocalTable(tbl, name, this._options, embeddings)
    } else {
      return new LocalTable(tbl, name, this._options)
    }
  }

  async createTableArrow (name: string, table: ArrowTable): Promise<Table> {
    const writer = RecordBatchFileWriter.writeAll(table)
    await tableCreate.call(this._db, name, Buffer.from(await writer.toUint8Array()))
    return await this.openTable(name)
  }

  /**
   * Drop an existing table.
   * @param name The name of the table to drop.
   */
  async dropTable (name: string): Promise<void> {
    await databaseDropTable.call(this._db, name)
  }
}

export class LocalTable<T = number[]> implements Table<T> {
  private readonly _tbl: any
  private readonly _name: string
  private readonly _embeddings?: EmbeddingFunction<T>
  private readonly _options: ConnectionOptions

  constructor (tbl: any, name: string, options: ConnectionOptions)
  /**
   * @param tbl
   * @param name
   * @param options
   * @param embeddings An embedding function to use when interacting with this table
   */
  constructor (tbl: any, name: string, options: ConnectionOptions, embeddings: EmbeddingFunction<T>)
  constructor (tbl: any, name: string, options: ConnectionOptions, embeddings?: EmbeddingFunction<T>) {
    this._tbl = tbl
    this._name = name
    this._embeddings = embeddings
    this._options = options
  }

  get name (): string {
    return this._name
  }

  /**
   * Creates a search query to find the nearest neighbors of the given search term
   * @param query The query search term
   */
  search (query: T): Query<T> {
    return new Query(query, this._tbl, this._embeddings)
  }

  /**
   * Insert records into this Table.
   *
   * @param data Records to be inserted into the Table
   * @return The number of rows added to the table
   */
  async add (data: Array<Record<string, unknown>>): Promise<number> {
    const callArgs = [this._tbl, await fromRecordsToBuffer(data, this._embeddings), WriteMode.Append.toString()]
    if (this._options.awsCredentials !== undefined) {
      callArgs.push(this._options.awsCredentials.accessKeyId)
      callArgs.push(this._options.awsCredentials.secretKey)
      if (this._options.awsCredentials.sessionToken !== undefined) {
        callArgs.push(this._options.awsCredentials.sessionToken)
      }
    }
    return tableAdd.call(...callArgs)
  }

  /**
   * Insert records into this Table, replacing its contents.
   *
   * @param data Records to be inserted into the Table
   * @return The number of rows added to the table
   */
  async overwrite (data: Array<Record<string, unknown>>): Promise<number> {
    const callArgs = [this._tbl, await fromRecordsToBuffer(data, this._embeddings), WriteMode.Overwrite.toString()]
    if (this._options.awsCredentials !== undefined) {
      callArgs.push(this._options.awsCredentials.accessKeyId)
      callArgs.push(this._options.awsCredentials.secretKey)
      if (this._options.awsCredentials.sessionToken !== undefined) {
        callArgs.push(this._options.awsCredentials.sessionToken)
      }
    }
    return tableAdd.call(this._tbl, await fromRecordsToBuffer(data, this._embeddings), WriteMode.Overwrite.toString())
  }

  /**
   * Create an ANN index on this Table vector index.
   *
   * @param indexParams The parameters of this Index, @see VectorIndexParams.
   */
  async createIndex (indexParams: VectorIndexParams): Promise<any> {
    return tableCreateVectorIndex.call(this._tbl, indexParams)
  }

  /**
   * Returns the number of rows in this table.
   */
  async countRows (): Promise<number> {
    return tableCountRows.call(this._tbl)
  }

  /**
   * Delete rows from this table.
   *
   * @param filter A filter in the same format used by a sql WHERE clause.
   */
  async delete (filter: string): Promise<void> {
    return tableDelete.call(this._tbl, filter)
  }
}

/// Config to build IVF_PQ index.
///
export interface IvfPQIndexConfig {
  /**
   * The column to be indexed
   */
  column?: string

  /**
   * A unique name for the index
   */
  index_name?: string

  /**
   * Metric type, L2 or Cosine
   */
  metric_type?: MetricType

  /**
   * The number of partitions this index
   */
  num_partitions?: number

  /**
   * The max number of iterations for kmeans training.
   */
  max_iters?: number

  /**
   * Train as optimized product quantization.
   */
  use_opq?: boolean

  /**
   * Number of subvectors to build PQ code
   */
  num_sub_vectors?: number
  /**
   * The number of bits to present one PQ centroid.
   */
  num_bits?: number

  /**
   * Max number of iterations to train OPQ, if `use_opq` is true.
   */
  max_opq_iters?: number

  /**
   * Replace an existing index with the same name if it exists.
   */
  replace?: boolean

  type: 'ivf_pq'
}

export type VectorIndexParams = IvfPQIndexConfig

/**
 * Write mode for writing a table.
 */
export enum WriteMode {
  /** Create a new {@link Table}. */
  Create = 'create',
  /** Overwrite the existing {@link Table} if presented. */
  Overwrite = 'overwrite',
  /** Append new data to the table. */
  Append = 'append'
}

/**
 * Distance metrics type.
 */
export enum MetricType {
  /**
   * Euclidean distance
   */
  L2 = 'l2',

  /**
   * Cosine distance
   */
  Cosine = 'cosine',

  /**
   * Dot product
   */
  Dot = 'dot'
}
