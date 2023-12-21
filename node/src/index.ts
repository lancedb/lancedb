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
  type Schema,
  Table as ArrowTable
} from 'apache-arrow'
import { createEmptyTable, fromRecordsToBuffer, fromTableToBuffer } from './arrow'
import type { EmbeddingFunction } from './embedding/embedding_function'
import { RemoteConnection } from './remote'
import { Query } from './query'
import { isEmbeddingFunction } from './embedding/embedding_function'
import { type Literal, toSQL } from './util'

// eslint-disable-next-line @typescript-eslint/no-var-requires
const { databaseNew, databaseTableNames, databaseOpenTable, databaseDropTable, tableCreate, tableAdd, tableCreateScalarIndex, tableCreateVectorIndex, tableCountRows, tableDelete, tableUpdate, tableCleanupOldVersions, tableCompactFiles, tableListIndices, tableIndexStats } = require('../native.js')

export { Query }
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

  awsRegion?: string

  // API key for the remote connections
  apiKey?: string
  // Region to connect
  region?: string

  // override the host for the remote connections
  hostOverride?: string
}

function getAwsArgs (opts: ConnectionOptions): any[] {
  const callArgs = []
  const awsCredentials = opts.awsCredentials
  if (awsCredentials !== undefined) {
    callArgs.push(awsCredentials.accessKeyId)
    callArgs.push(awsCredentials.secretKey)
    callArgs.push(awsCredentials.sessionToken)
  } else {
    callArgs.push(undefined)
    callArgs.push(undefined)
    callArgs.push(undefined)
  }

  callArgs.push(opts.awsRegion)
  return callArgs
}

export interface CreateTableOptions<T> {
  // Name of Table
  name: string

  // Data to insert into the Table
  data?: Array<Record<string, unknown>> | ArrowTable | undefined

  // Optional Arrow Schema for this table
  schema?: Schema | undefined

  // Optional embedding function used to create embeddings
  embeddingFunction?: EmbeddingFunction<T> | undefined

  // WriteOptions for this operation
  writeOptions?: WriteOptions | undefined
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
   * Creates a new Table, optionally initializing it with new data.
   *
   * @param {string} name - The name of the table.
   * @param data - Array of Records to be inserted into the table
   * @param schema - An Arrow Schema that describe this table columns
   * @param {EmbeddingFunction} embeddings - An embedding function to use on this table
   * @param {WriteOptions} writeOptions - The write options to use when creating the table.
   */
  createTable<T> ({ name, data, schema, embeddingFunction, writeOptions }: CreateTableOptions<T>): Promise<Table<T>>

  /**
   * Creates a new Table and initialize it with new data.
   *
   * @param {string} name - The name of the table.
   * @param data - Non-empty Array of Records to be inserted into the table
   */
  createTable (name: string, data: Array<Record<string, unknown>>): Promise<Table>

  /**
   * Creates a new Table and initialize it with new data.
   *
   * @param {string} name - The name of the table.
   * @param data - Non-empty Array of Records to be inserted into the table
   * @param {WriteOptions} options - The write options to use when creating the table.
   */
  createTable (name: string, data: Array<Record<string, unknown>>, options: WriteOptions): Promise<Table>

  /**
   * Creates a new Table and initialize it with new data.
   *
   * @param {string} name - The name of the table.
   * @param data - Non-empty Array of Records to be inserted into the table
   * @param {EmbeddingFunction} embeddings - An embedding function to use on this table
   */
  createTable<T> (name: string, data: Array<Record<string, unknown>>, embeddings: EmbeddingFunction<T>): Promise<Table<T>>
  /**
   * Creates a new Table and initialize it with new data.
   *
   * @param {string} name - The name of the table.
   * @param data - Non-empty Array of Records to be inserted into the table
   * @param {EmbeddingFunction} embeddings - An embedding function to use on this table
   * @param {WriteOptions} options - The write options to use when creating the table.
   */
  createTable<T> (name: string, data: Array<Record<string, unknown>>, embeddings: EmbeddingFunction<T>, options: WriteOptions): Promise<Table<T>>

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
   * Create a scalar index on this Table for the given column
   *
   * @param column The column to index
   * @param replace If false, fail if an index already exists on the column
   *
   * Scalar indices, like vector indices, can be used to speed up scans.  A scalar
   * index can speed up scans that contain filter expressions on the indexed column.
   * For example, the following scan will be faster if the column `my_col` has
   * a scalar index:
   *
   * ```ts
   * const con = await lancedb.connect('./.lancedb');
   * const table = await con.openTable('images');
   * const results = await table.where('my_col = 7').execute();
   * ```
   *
   * Scalar indices can also speed up scans containing a vector search and a
   * prefilter:
   *
   * ```ts
   * const con = await lancedb.connect('././lancedb');
   * const table = await con.openTable('images');
   * const results = await table.search([1.0, 2.0]).where('my_col != 7').prefilter(true);
   * ```
   *
   * Scalar indices can only speed up scans for basic filters using
   * equality, comparison, range (e.g. `my_col BETWEEN 0 AND 100`), and set
   * membership (e.g. `my_col IN (0, 1, 2)`)
   *
   * Scalar indices can be used if the filter contains multiple indexed columns and
   * the filter criteria are AND'd or OR'd together
   * (e.g. `my_col < 0 AND other_col> 100`)
   *
   * Scalar indices may be used if the filter contains non-indexed columns but,
   * depending on the structure of the filter, they may not be usable.  For example,
   * if the column `not_indexed` does not have a scalar index then the filter
   * `my_col = 0 OR not_indexed = 1` will not be able to use any scalar index on
   * `my_col`.
   *
   * @examples
   *
   * ```ts
   * const con = await lancedb.connect('././lancedb')
   * const table = await con.openTable('images')
   * await table.createScalarIndex('my_col')
   * ```
   */
  createScalarIndex: (column: string, replace: boolean) => Promise<void>

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

  /**
   * Update rows in this table.
   *
   * This can be used to update a single row, many rows, all rows, or
   * sometimes no rows (if your predicate matches nothing).
   *
   * @param args see {@link UpdateArgs} and {@link UpdateSqlArgs} for more details
   *
   * @examples
   *
   * ```ts
   * const con = await lancedb.connect("./.lancedb")
   * const data = [
   *    {id: 1, vector: [3, 3], name: 'Ye'},
   *    {id: 2, vector: [4, 4], name: 'Mike'},
   * ];
   * const tbl = await con.createTable("my_table", data)
   *
   * await tbl.update({
   *   where: "id = 2",
   *   values: { vector: [2, 2], name: "Michael" },
   * })
   *
   * let results = await tbl.search([1, 1]).execute();
   * // Returns [
   * //   {id: 2, vector: [2, 2], name: 'Michael'}
   * //   {id: 1, vector: [3, 3], name: 'Ye'}
   * // ]
   * ```
   *
   */
  update: (args: UpdateArgs | UpdateSqlArgs) => Promise<void>

  /**
   * List the indicies on this table.
   */
  listIndices: () => Promise<VectorIndex[]>

  /**
   * Get statistics about an index.
   */
  indexStats: (indexUuid: string) => Promise<IndexStats>
}

export interface UpdateArgs {
  /**
   * A filter in the same format used by a sql WHERE clause. The filter may be empty,
   * in which case all rows will be updated.
   */
  where?: string

  /**
   * A key-value map of updates. The keys are the column names, and the values are the
   * new values to set
   */
  values: Record<string, Literal>
}

export interface UpdateSqlArgs {
  /**
   * A filter in the same format used by a sql WHERE clause. The filter may be empty,
   * in which case all rows will be updated.
   */
  where?: string

  /**
   * A key-value map of updates. The keys are the column names, and the values are the
   * new values to set as SQL expressions.
   */
  valuesSql: Record<string, string>
}

export interface VectorIndex {
  columns: string[]
  name: string
  uuid: string
}

export interface IndexStats {
  numIndexedRows: number | null
  numUnindexedRows: number | null
}

/**
 * A connection to a LanceDB database.
 */
export class LocalConnection implements Connection {
  private readonly _options: () => ConnectionOptions
  private readonly _db: any

  constructor (db: any, options: ConnectionOptions) {
    this._options = () => options
    this._db = db
  }

  get uri (): string {
    return this._options().uri
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
    const tbl = await databaseOpenTable.call(this._db, name, ...getAwsArgs(this._options()))
    if (embeddings !== undefined) {
      return new LocalTable(tbl, name, this._options(), embeddings)
    } else {
      return new LocalTable(tbl, name, this._options())
    }
  }

  async createTable<T> (name: string | CreateTableOptions<T>, data?: Array<Record<string, unknown>>, optsOrEmbedding?: WriteOptions | EmbeddingFunction<T>, opt?: WriteOptions): Promise<Table<T>> {
    if (typeof name === 'string') {
      let writeOptions: WriteOptions = new DefaultWriteOptions()
      if (opt !== undefined && isWriteOptions(opt)) {
        writeOptions = opt
      } else if (optsOrEmbedding !== undefined && isWriteOptions(optsOrEmbedding)) {
        writeOptions = optsOrEmbedding
      }

      let embeddings: undefined | EmbeddingFunction<T>
      if (optsOrEmbedding !== undefined && isEmbeddingFunction(optsOrEmbedding)) {
        embeddings = optsOrEmbedding
      }
      return await this.createTableImpl({ name, data, embeddingFunction: embeddings, writeOptions })
    }
    return await this.createTableImpl(name)
  }

  private async createTableImpl<T> ({ name, data, schema, embeddingFunction, writeOptions = new DefaultWriteOptions() }: {
    name: string
    data?: Array<Record<string, unknown>> | ArrowTable | undefined
    schema?: Schema | undefined
    embeddingFunction?: EmbeddingFunction<T> | undefined
    writeOptions?: WriteOptions | undefined
  }): Promise<Table<T>> {
    let buffer: Buffer

    function isEmpty (data: Array<Record<string, unknown>> | ArrowTable<any>): boolean {
      if (data instanceof ArrowTable) {
        return data.data.length === 0
      }
      return data.length === 0
    }

    if ((data === undefined) || isEmpty(data)) {
      if (schema === undefined) {
        throw new Error('Either data or schema needs to defined')
      }
      buffer = await fromTableToBuffer(createEmptyTable(schema))
    } else if (data instanceof ArrowTable) {
      buffer = await fromTableToBuffer(data, embeddingFunction)
    } else {
      // data is Array<Record<...>>
      buffer = await fromRecordsToBuffer(data, embeddingFunction)
    }

    const tbl = await tableCreate.call(this._db, name, buffer, writeOptions?.writeMode?.toString(), ...getAwsArgs(this._options()))
    if (embeddingFunction !== undefined) {
      return new LocalTable(tbl, name, this._options(), embeddingFunction)
    } else {
      return new LocalTable(tbl, name, this._options())
    }
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
  private _tbl: any
  private readonly _name: string
  private readonly _embeddings?: EmbeddingFunction<T>
  private readonly _options: () => ConnectionOptions

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
    this._options = () => options
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
   * Creates a filter query to find all rows matching the specified criteria
   * @param value The filter criteria (like SQL where clause syntax)
   */
  filter (value: string): Query<T> {
    return new Query(undefined, this._tbl, this._embeddings).filter(value)
  }

  where = this.filter

  /**
   * Insert records into this Table.
   *
   * @param data Records to be inserted into the Table
   * @return The number of rows added to the table
   */
  async add (data: Array<Record<string, unknown>>): Promise<number> {
    return tableAdd.call(
      this._tbl,
      await fromRecordsToBuffer(data, this._embeddings),
      WriteMode.Append.toString(),
      ...getAwsArgs(this._options())
    ).then((newTable: any) => { this._tbl = newTable })
  }

  /**
   * Insert records into this Table, replacing its contents.
   *
   * @param data Records to be inserted into the Table
   * @return The number of rows added to the table
   */
  async overwrite (data: Array<Record<string, unknown>>): Promise<number> {
    return tableAdd.call(
      this._tbl,
      await fromRecordsToBuffer(data, this._embeddings),
      WriteMode.Overwrite.toString(),
      ...getAwsArgs(this._options())
    ).then((newTable: any) => { this._tbl = newTable })
  }

  /**
   * Create an ANN index on this Table vector index.
   *
   * @param indexParams The parameters of this Index, @see VectorIndexParams.
   */
  async createIndex (indexParams: VectorIndexParams): Promise<any> {
    return tableCreateVectorIndex.call(this._tbl, indexParams).then((newTable: any) => { this._tbl = newTable })
  }

  async createScalarIndex (column: string, replace: boolean): Promise<void> {
    return tableCreateScalarIndex.call(this._tbl, column, replace)
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
    return tableDelete.call(this._tbl, filter).then((newTable: any) => { this._tbl = newTable })
  }

  /**
   * Update rows in this table.
   *
   * @param args see {@link UpdateArgs} and {@link UpdateSqlArgs} for more details
   *
   * @returns
   */
  async update (args: UpdateArgs | UpdateSqlArgs): Promise<void> {
    let filter: string | null
    let updates: Record<string, string>

    if ('valuesSql' in args) {
      filter = args.where ?? null
      updates = args.valuesSql
    } else {
      filter = args.where ?? null
      updates = {}
      for (const [key, value] of Object.entries(args.values)) {
        updates[key] = toSQL(value)
      }
    }

    return tableUpdate.call(this._tbl, filter, updates).then((newTable: any) => { this._tbl = newTable })
  }

  /**
   * Clean up old versions of the table, freeing disk space.
   *
   * @param olderThan The minimum age in minutes of the versions to delete. If not
   *                  provided, defaults to two weeks.
   * @param deleteUnverified Because they may be part of an in-progress
   *                  transaction, uncommitted files newer than 7 days old are
   *                  not deleted by default. This means that failed transactions
   *                  can leave around data that takes up disk space for up to
   *                  7 days. You can override this safety mechanism by setting
   *                 this option to `true`, only if you promise there are no
   *                 in progress writes while you run this operation. Failure to
   *                 uphold this promise can lead to corrupted tables.
   * @returns
   */
  async cleanupOldVersions (olderThan?: number, deleteUnverified?: boolean): Promise<CleanupStats> {
    return tableCleanupOldVersions.call(this._tbl, olderThan, deleteUnverified)
      .then((res: { newTable: any, metrics: CleanupStats }) => {
        this._tbl = res.newTable
        return res.metrics
      })
  }

  /**
   * Run the compaction process on the table.
   *
   * This can be run after making several small appends to optimize the table
   * for faster reads.
   *
   * @param options Advanced options configuring compaction. In most cases, you
   *               can omit this arguments, as the default options are sensible
   *               for most tables.
   * @returns Metrics about the compaction operation.
   */
  async compactFiles (options?: CompactionOptions): Promise<CompactionMetrics> {
    const optionsArg = options ?? {}
    return tableCompactFiles.call(this._tbl, optionsArg)
      .then((res: { newTable: any, metrics: CompactionMetrics }) => {
        this._tbl = res.newTable
        return res.metrics
      })
  }

  async listIndices (): Promise<VectorIndex[]> {
    return tableListIndices.call(this._tbl)
  }

  async indexStats (indexUuid: string): Promise<IndexStats> {
    return tableIndexStats.call(this._tbl, indexUuid)
  }
}

export interface CleanupStats {
  /**
   * The number of bytes removed from disk.
   */
  bytesRemoved: number
  /**
   * The number of old table versions removed.
   */
  oldVersions: number
}

export interface CompactionOptions {
  /**
   * The number of rows per fragment to target. Fragments that have fewer rows
   * will be compacted into adjacent fragments to produce larger fragments.
   * Defaults to 1024 * 1024.
   */
  targetRowsPerFragment?: number
  /**
   * The maximum number of rows per group. Defaults to 1024.
   */
  maxRowsPerGroup?: number
  /**
   * If true, fragments that have rows that are deleted may be compacted to
   * remove the deleted rows. This can improve the performance of queries.
   * Default is true.
   */
  materializeDeletions?: boolean
  /**
   * A number between 0 and 1, representing the proportion of rows that must be
   * marked deleted before a fragment is a candidate for compaction to remove
   * the deleted rows. Default is 10%.
   */
  materializeDeletionsThreshold?: number
  /**
   * The number of threads to use for compaction. If not provided, defaults to
   * the number of cores on the machine.
   */
  numThreads?: number
}

export interface CompactionMetrics {
  /**
   * The number of fragments that were removed.
   */
  fragmentsRemoved: number
  /**
   * The number of new fragments that were created.
   */
  fragmentsAdded: number
  /**
   * The number of files that were removed. Each fragment may have more than one
   * file.
   */
  filesRemoved: number
  /**
   * The number of files added. This is typically equal to the number of
   * fragments added.
   */
  filesAdded: number
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

  /**
   * Cache size of the index
   */
  index_cache_size?: number

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
 * Write options when creating a Table.
 */
export interface WriteOptions {
  /** A {@link WriteMode} to use on this operation */
  writeMode?: WriteMode
}

export class DefaultWriteOptions implements WriteOptions {
  writeMode = WriteMode.Create
}

export function isWriteOptions (value: any): value is WriteOptions {
  return Object.keys(value).length === 1 &&
      (value.writeMode === undefined || typeof value.writeMode === 'string')
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
