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
const { databaseNew, databaseTableNames, databaseOpenTable, tableCreate, tableSearch, tableAdd, tableCreateVectorIndex } = require('../native.js')

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
  private _db: any

  constructor (uri: string) {
    this._uri = uri
    this._db = null;
  }

  get uri (): string {
    return this._uri
  }

  // TODO: fix return type
  private async db() : Promise<any> {
    if (this._db == null) {
      this._db = await databaseNew(this._uri)
    }
    return this._db
  }

  /**
     * Get the names of all tables in the database.
     */
  async tableNames (): Promise<string[]> {
    const db = await this.db();
    return await databaseTableNames.call(db)
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
  async openTable<T> (name: string, embeddings?: EmbeddingFunction<T>): Promise<Table<T>> {
    const tbl = await databaseOpenTable.call(this._db, name)
    if (embeddings !== undefined) {
      return new Table(tbl, name, embeddings)
    } else {
      return new Table(tbl, name)
    }
  }

  /**
   * Creates a new Table and initialize it with new data.
   *
   * @param name The name of the table.
   * @param data Non-empty Array of Records to be inserted into the Table
   */

  async createTable (name: string, data: Array<Record<string, unknown>>): Promise<Table>
  /**
   * Creates a new Table and initialize it with new data.
   *
   * @param name The name of the table.
   * @param data Non-empty Array of Records to be inserted into the Table
   * @param embeddings An embedding function to use on this Table
   */
  async createTable<T> (name: string, data: Array<Record<string, unknown>>, embeddings: EmbeddingFunction<T>): Promise<Table<T>>
  async createTable<T> (name: string, data: Array<Record<string, unknown>>, embeddings?: EmbeddingFunction<T>): Promise<Table<T>> {
    const db = await this.db();
    const tbl = await tableCreate.call(db, name, await fromRecordsToBuffer(data, embeddings))
    if (embeddings !== undefined) {
      return new Table(tbl, name, embeddings)
    } else {
      return new Table(tbl, name)
    }
  }

  async createTableArrow (name: string, table: ArrowTable): Promise<Table> {
    const db = await this.db();
    const writer = RecordBatchFileWriter.writeAll(table)
    await tableCreate.call(db, name, Buffer.from(await writer.toUint8Array()))
    return await this.openTable(name)
  }
}

export class Table<T = number[]> {
  private readonly _tbl: any
  private readonly _name: string
  private readonly _embeddings?: EmbeddingFunction<T>

  constructor (tbl: any, name: string)
  /**
   * @param tbl
   * @param name
   * @param embeddings An embedding function to use when interacting with this table
   */
  constructor (tbl: any, name: string, embeddings: EmbeddingFunction<T>)
  constructor (tbl: any, name: string, embeddings?: EmbeddingFunction<T>) {
    this._tbl = tbl
    this._name = name
    this._embeddings = embeddings
  }

  get name (): string {
    return this._name
  }

  /**
   * Creates a search query to find the nearest neighbors of the given search term
   * @param query The query search term
   */
  search (query: T): Query {
    let queryVector: number[]
    if (this._embeddings !== undefined) {
      queryVector = this._embeddings.embed([query])[0]
    } else {
      queryVector = query as number[]
    }
    return new Query(this._tbl, queryVector)
  }

  /**
   * Insert records into this Table.
   *
   * @param data Records to be inserted into the Table
   * @return The number of rows added to the table
   */
  async add (data: Array<Record<string, unknown>>): Promise<number> {
    return tableAdd.call(this._tbl, await fromRecordsToBuffer(data, this._embeddings), WriteMode.Append.toString())
  }

  /**
   * Insert records into this Table, replacing its contents.
   *
   * @param data Records to be inserted into the Table
   * @return The number of rows added to the table
   */
  async overwrite (data: Array<Record<string, unknown>>): Promise<number> {
    return tableAdd.call(this._tbl, await fromRecordsToBuffer(data, this._embeddings), WriteMode.Overwrite.toString())
  }

  /**
   * Create an ANN index on this Table vector index.
   *
   * @param indexParams The parameters of this Index, @see VectorIndexParams.
   */
  async create_index (indexParams: VectorIndexParams): Promise<any> {
    return tableCreateVectorIndex.call(this._tbl, indexParams)
  }
}

interface IvfPQIndexConfig {
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

  type: 'ivf_pq'
}

export type VectorIndexParams = IvfPQIndexConfig

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
    this._metricType = undefined
  }

  /***
   * Sets the number of results that will be returned
   * @param value number of results
   */
  limit (value: number): Query {
    this._limit = value
    return this
  }

  /**
   * Refine the results by reading extra elements and re-ranking them in memory.
   * @param value refine factor to use in this query.
   */
  refineFactor (value: number): Query {
    this._refineFactor = value
    return this
  }

  /**
   * The number of probes used. A higher number makes search more accurate but also slower.
   * @param value The number of probes used.
   */
  nprobes (value: number): Query {
    this._nprobes = value
    return this
  }

  /**
   * A filter statement to be applied to this query.
   * @param value A filter in the same format used by a sql WHERE clause.
   */
  filter (value: string): Query {
    this._filter = value
    return this
  }

  /**
   * The MetricType used for this Query.
   * @param value The metric to the. @see MetricType for the different options
   */
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

/**
 * An embedding function that automatically creates vector representation for a given column.
 */
export interface EmbeddingFunction<T> {
  /**
   * The name of the column that will be used as input for the Embedding Function.
   */
  sourceColumn: string

  /**
   * Creates a vector representation for the given values.
   */
  embed: (data: T[]) => number[][]
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
  Cosine = 'cosine'
}
