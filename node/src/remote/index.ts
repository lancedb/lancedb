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

import {
  type EmbeddingFunction,
  type Table,
  type VectorIndexParams,
  type Connection,
  type ConnectionOptions,
  type CreateTableOptions,
  type VectorIndex,
  type WriteOptions,
  type IndexStats,
  type UpdateArgs,
  type UpdateSqlArgs,
  makeArrowTable,
  type MergeInsertArgs,
  type ColumnAlteration
} from '../index'
import { Query } from '../query'

import { Vector, Table as ArrowTable } from 'apache-arrow'
import { HttpLancedbClient } from './client'
import { isEmbeddingFunction } from '../embedding/embedding_function'
import {
  createEmptyTable,
  fromRecordsToStreamBuffer,
  fromTableToStreamBuffer
} from '../arrow'
import { toSQL, TTLCache } from '../util'
import { type HttpMiddleware } from '../middleware'

/**
 * Remote connection.
 */
export class RemoteConnection implements Connection {
  private _client: HttpLancedbClient
  private readonly _dbName: string
  private readonly _tableCache = new TTLCache(300_000)

  constructor (opts: ConnectionOptions) {
    if (!opts.uri.startsWith('db://')) {
      throw new Error(`Invalid remote DB URI: ${opts.uri}`)
    }
    if (opts.apiKey == null || opts.apiKey === '') {
      opts = Object.assign({}, opts, { apiKey: process.env.LANCEDB_API_KEY })
    }
    if (opts.apiKey === undefined || opts.region === undefined) {
      throw new Error(
        'API key and region are must be passed for remote connections. ' +
        'API key can also be set through LANCEDB_API_KEY env variable.')
    }

    this._dbName = opts.uri.slice('db://'.length)
    let server: string
    if (opts.hostOverride === undefined) {
      server = `https://${this._dbName}.${opts.region}.api.lancedb.com`
    } else {
      server = opts.hostOverride
    }
    this._client = new HttpLancedbClient(
      server,
      opts.apiKey,
      opts.timeout,
      opts.hostOverride === undefined ? undefined : this._dbName
    )
  }

  get uri (): string {
    // add the lancedb+ prefix back
    return 'db://' + this._client.uri
  }

  async tableNames (
    pageToken: string = '',
    limit: number = 10
  ): Promise<string[]> {
    const response = await this._client.get('/v1/table/', {
      limit: `${limit}`,
      page_token: pageToken
    })
    const body = await response.body()
    for (const table of body.tables) {
      this._tableCache.set(table, true)
    }
    return body.tables
  }

  async openTable (name: string): Promise<Table>
  async openTable<T>(
    name: string,
    embeddings: EmbeddingFunction<T>
  ): Promise<Table<T>>
  async openTable<T>(
    name: string,
    embeddings?: EmbeddingFunction<T>
  ): Promise<Table<T>> {
      // check if the table exists
      if (this._tableCache.get(name) === undefined) {
        await this._client.post(`/v1/table/${encodeURIComponent(name)}/describe/`)
        this._tableCache.set(name, true)
      }

    if (embeddings !== undefined) {
      return new RemoteTable(this._client, name, embeddings)
    } else {
      return new RemoteTable(this._client, name)
    }
  }

  async createTable<T>(
    nameOrOpts: string | CreateTableOptions<T>,
    data?: Array<Record<string, unknown>> | ArrowTable,
    optsOrEmbedding?: WriteOptions | EmbeddingFunction<T>,
    opt?: WriteOptions
  ): Promise<Table<T>> {
    // Logic copied from LocatlConnection, refactor these to a base class + connectionImpl pattern
    let schema
    let embeddings: undefined | EmbeddingFunction<T>
    let tableName: string
    if (typeof nameOrOpts === 'string') {
      if (
        optsOrEmbedding !== undefined &&
        isEmbeddingFunction(optsOrEmbedding)
      ) {
        embeddings = optsOrEmbedding
      }
      tableName = nameOrOpts
    } else {
      schema = nameOrOpts.schema
      embeddings = nameOrOpts.embeddingFunction
      tableName = nameOrOpts.name
      if (data === undefined) {
        data = nameOrOpts.data
      }
    }

    let buffer: Buffer

    function isEmpty (
      data: Array<Record<string, unknown>> | ArrowTable<any>
    ): boolean {
      if (data instanceof ArrowTable) {
        return data.numRows === 0
      }
      return data.length === 0
    }

    if (data === undefined || isEmpty(data)) {
      if (schema === undefined) {
        throw new Error('Either data or schema needs to defined')
      }
      buffer = await fromTableToStreamBuffer(createEmptyTable(schema))
    } else if (data instanceof ArrowTable) {
      buffer = await fromTableToStreamBuffer(data, embeddings)
    } else {
      // data is Array<Record<...>>
      buffer = await fromRecordsToStreamBuffer(data, embeddings)
    }

    const res = await this._client.post(
      `/v1/table/${encodeURIComponent(tableName)}/create/`,
      buffer,
      undefined,
      'application/vnd.apache.arrow.stream'
    )
    if (res.status !== 200) {
      throw new Error(
        `Server Error, status: ${res.status}, ` +
          // eslint-disable-next-line @typescript-eslint/restrict-template-expressions
          `message: ${res.statusText}: ${await res.body()}`
      )
    }

    this._tableCache.set(tableName, true)
    if (embeddings === undefined) {
      return new RemoteTable(this._client, tableName)
    } else {
      return new RemoteTable(this._client, tableName, embeddings)
    }
  }

  async dropTable (name: string): Promise<void> {
    await this._client.post(`/v1/table/${encodeURIComponent(name)}/drop/`)
    this._tableCache.delete(name)
  }

  withMiddleware (middleware: HttpMiddleware): Connection {
    const wrapped = this.clone()
    wrapped._client = wrapped._client.withMiddleware(middleware)
    return wrapped
  }

  private clone (): RemoteConnection {
    const clone: RemoteConnection = Object.create(RemoteConnection.prototype)
    return Object.assign(clone, this)
  }
}

export class RemoteQuery<T = number[]> extends Query<T> {
  constructor (
    query: T,
    private readonly _client: HttpLancedbClient,
    private readonly _name: string,
    embeddings?: EmbeddingFunction<T>
  ) {
    super(query, undefined, embeddings)
  }

  // TODO: refactor this to a base class + queryImpl pattern
  async execute<T = Record<string, unknown>>(): Promise<T[]> {
    const embeddings = this._embeddings
    const query = (this as any)._query
    let queryVector: number[]

    if (embeddings !== undefined) {
      queryVector = (await embeddings.embed([query]))[0]
    } else {
      queryVector = query as number[]
    }

    const data = await this._client.search(
      this._name,
      queryVector,
      (this as any)._limit,
      (this as any)._nprobes,
      (this as any)._prefilter,
      (this as any)._refineFactor,
      (this as any)._select,
      (this as any)._filter
    )

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

// we are using extend until we have next next version release
// Table and Connection has both been refactored to interfaces
export class RemoteTable<T = number[]> implements Table<T> {
  private _client: HttpLancedbClient
  private readonly _embeddings?: EmbeddingFunction<T>
  private readonly _name: string

  constructor (client: HttpLancedbClient, name: string)
  constructor (
    client: HttpLancedbClient,
    name: string,
    embeddings: EmbeddingFunction<T>
  )
  constructor (
    client: HttpLancedbClient,
    name: string,
    embeddings?: EmbeddingFunction<T>
  ) {
    this._client = client
    this._name = name
    this._embeddings = embeddings
  }

  get name (): string {
    return this._name
  }

  get schema (): Promise<any> {
    return this._client
      .post(`/v1/table/${encodeURIComponent(this._name)}/describe/`)
      .then(async (res) => {
        if (res.status !== 200) {
          throw new Error(
            `Server Error, status: ${res.status}, ` +
              // eslint-disable-next-line @typescript-eslint/restrict-template-expressions
              `message: ${res.statusText}: ${await res.body()}`
          )
        }
        return (await res.body())?.schema
      })
  }

  search (query: T): Query<T> {
    return new RemoteQuery(query, this._client, encodeURIComponent(this._name)) //, this._embeddings_new)
  }

  filter (where: string): Query<T> {
    throw new Error('Not implemented')
  }

  async mergeInsert (on: string, data: Array<Record<string, unknown>> | ArrowTable, args: MergeInsertArgs): Promise<void> {
    let tbl: ArrowTable
    if (data instanceof ArrowTable) {
      tbl = data
    } else {
      tbl = makeArrowTable(data, await this.schema)
    }

    const queryParams: any = {
      on
    }
    if (args.whenMatchedUpdateAll !== false && args.whenMatchedUpdateAll !== null && args.whenMatchedUpdateAll !== undefined) {
      queryParams.when_matched_update_all = 'true'
      if (typeof args.whenMatchedUpdateAll === 'string') {
        queryParams.when_matched_update_all_filt = args.whenMatchedUpdateAll
      }
    } else {
      queryParams.when_matched_update_all = 'false'
    }
    if (args.whenNotMatchedInsertAll ?? false) {
      queryParams.when_not_matched_insert_all = 'true'
    } else {
      queryParams.when_not_matched_insert_all = 'false'
    }
    if (args.whenNotMatchedBySourceDelete !== false && args.whenNotMatchedBySourceDelete !== null && args.whenNotMatchedBySourceDelete !== undefined) {
      queryParams.when_not_matched_by_source_delete = 'true'
      if (typeof args.whenNotMatchedBySourceDelete === 'string') {
        queryParams.when_not_matched_by_source_delete_filt = args.whenNotMatchedBySourceDelete
      }
    } else {
      queryParams.when_not_matched_by_source_delete = 'false'
    }

    const buffer = await fromTableToStreamBuffer(tbl, this._embeddings)
    const res = await this._client.post(
      `/v1/table/${encodeURIComponent(this._name)}/merge_insert/`,
      buffer,
      queryParams,
      'application/vnd.apache.arrow.stream'
    )
    if (res.status !== 200) {
      throw new Error(
        `Server Error, status: ${res.status}, ` +
          // eslint-disable-next-line @typescript-eslint/restrict-template-expressions
          `message: ${res.statusText}: ${await res.body()}`
      )
    }
  }

  async add (data: Array<Record<string, unknown>> | ArrowTable): Promise<number> {
    let tbl: ArrowTable
    if (data instanceof ArrowTable) {
      tbl = data
    } else {
      tbl = makeArrowTable(data, await this.schema)
    }

    const buffer = await fromTableToStreamBuffer(tbl, this._embeddings)
    const res = await this._client.post(
      `/v1/table/${encodeURIComponent(this._name)}/insert/`,
      buffer,
      {
        mode: 'append'
      },
      'application/vnd.apache.arrow.stream'
    )
    if (res.status !== 200) {
      throw new Error(
        `Server Error, status: ${res.status}, ` +
          // eslint-disable-next-line @typescript-eslint/restrict-template-expressions
          `message: ${res.statusText}: ${await res.body()}`
      )
    }
    return tbl.numRows
  }

  async overwrite (data: Array<Record<string, unknown>> | ArrowTable): Promise<number> {
    let tbl: ArrowTable
    if (data instanceof ArrowTable) {
      tbl = data
    } else {
      tbl = makeArrowTable(data)
    }
    const buffer = await fromTableToStreamBuffer(tbl, this._embeddings)
    const res = await this._client.post(
      `/v1/table/${encodeURIComponent(this._name)}/insert/`,
      buffer,
      {
        mode: 'overwrite'
      },
      'application/vnd.apache.arrow.stream'
    )
    if (res.status !== 200) {
      throw new Error(
        `Server Error, status: ${res.status}, ` +
          // eslint-disable-next-line @typescript-eslint/restrict-template-expressions
          `message: ${res.statusText}: ${await res.body()}`
      )
    }
    return tbl.numRows
  }

  async createIndex (indexParams: VectorIndexParams): Promise<void> {
    const unsupportedParams = [
      'index_name',
      'num_partitions',
      'max_iters',
      'use_opq',
      'num_sub_vectors',
      'num_bits',
      'max_opq_iters',
      'replace'
    ]
    for (const param of unsupportedParams) {
      // eslint-disable-next-line @typescript-eslint/strict-boolean-expressions
      if (indexParams[param as keyof VectorIndexParams]) {
        throw new Error(`${param} is not supported for remote connections`)
      }
    }

    const column = indexParams.column ?? 'vector'
    const indexType = 'vector'
    const metricType = indexParams.metric_type ?? 'L2'
    const indexCacheSize = indexParams.index_cache_size ?? null

    const data = {
      column,
      index_type: indexType,
      metric_type: metricType,
      index_cache_size: indexCacheSize
    }
    const res = await this._client.post(
      `/v1/table/${encodeURIComponent(this._name)}/create_index/`,
      data
    )
    if (res.status !== 200) {
      throw new Error(
        `Server Error, status: ${res.status}, ` +
          // eslint-disable-next-line @typescript-eslint/restrict-template-expressions
          `message: ${res.statusText}: ${await res.body()}`
      )
    }
  }

  async createScalarIndex (column: string): Promise<void> {
    const indexType = 'scalar'

    const data = {
      column,
      index_type: indexType,
      replace: true
    }
    const res = await this._client.post(
      `/v1/table/${encodeURIComponent(this._name)}/create_scalar_index/`,
      data
    )
    if (res.status !== 200) {
      throw new Error(
        `Server Error, status: ${res.status}, ` +
          // eslint-disable-next-line @typescript-eslint/restrict-template-expressions
          `message: ${res.statusText}: ${await res.body()}`
      )
    }
  }

  async countRows (filter?: string): Promise<number> {
    const result = await this._client.post(`/v1/table/${encodeURIComponent(this._name)}/count_rows/`, {
      predicate: filter
    })
    return (await result.body())
  }

  async delete (filter: string): Promise<void> {
    await this._client.post(`/v1/table/${encodeURIComponent(this._name)}/delete/`, {
      predicate: filter
    })
  }

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
    await this._client.post(`/v1/table/${encodeURIComponent(this._name)}/update/`, {
      predicate: filter,
      updates: Object.entries(updates).map(([key, value]) => [key, value])
    })
  }

  async listIndices (): Promise<VectorIndex[]> {
    const results = await this._client.post(
      `/v1/table/${encodeURIComponent(this._name)}/index/list/`
    )
    return (await results.body()).indexes?.map((index: any) => ({
      columns: index.columns,
      name: index.index_name,
      uuid: index.index_uuid,
      status: index.status
    }))
  }

  async indexStats (indexUuid: string): Promise<IndexStats> {
    const results = await this._client.post(
      `/v1/table/${encodeURIComponent(this._name)}/index/${indexUuid}/stats/`
    )
    const body = await results.body()
    return {
      numIndexedRows: body?.num_indexed_rows,
      numUnindexedRows: body?.num_unindexed_rows,
      indexType: body?.index_type,
      distanceType: body?.distance_type,
      completedAt: body?.completed_at
    }
  }

  async addColumns (newColumnTransforms: Array<{ name: string, valueSql: string }>): Promise<void> {
    throw new Error('Add columns is not yet supported in LanceDB Cloud.')
  }

  async alterColumns (columnAlterations: ColumnAlteration[]): Promise<void> {
    throw new Error('Alter columns is not yet supported in LanceDB Cloud.')
  }

  async dropColumns (columnNames: string[]): Promise<void> {
    throw new Error('Drop columns is not yet supported in LanceDB Cloud.')
  }

  withMiddleware(middleware: HttpMiddleware): Table<T> {
    const wrapped = this.clone()
    wrapped._client = wrapped._client.withMiddleware(middleware)
    return wrapped
  }

  private clone (): RemoteTable<T> {
    const clone: RemoteTable<T> = Object.create(RemoteTable.prototype)
    return Object.assign(clone, this)
  }
}
