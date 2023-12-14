
import {
  type Schema,
  type Table as ArrowTable
} from 'apache-arrow'

import { type Literal } from './util'
import type { EmbeddingFunction } from './embedding/embedding_function'
import { type Query } from './query'

export interface AwsCredentials {
  accessKeyId: string

  secretKey: string

  sessionToken?: string
}

/**
 * Write options when creating a Table.
 */
export interface WriteOptions {
  /** A {@link WriteMode} to use on this operation */
  writeMode?: WriteMode
}

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
     *   filter: "id = 2",
     *   updates: { vector: [2, 2], name: "Michael" },
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
