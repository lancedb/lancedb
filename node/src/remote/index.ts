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
  type EmbeddingFunction, type Table, type VectorIndexParams, type Connection,
  type ConnectionOptions, type CreateTableOptions, type VectorIndex,
  type WriteOptions,
  type IndexStats,
  type UpdateArgs, type UpdateSqlArgs
} from '../index'
import { Query } from '../query'

import { Vector, Table as ArrowTable } from 'apache-arrow'
import { HttpLancedbClient } from './client'
import { isEmbeddingFunction } from '../embedding/embedding_function'
import { createEmptyTable, fromRecordsToStreamBuffer, fromTableToStreamBuffer } from '../arrow'
import { toSQL } from '../util'

/**
 * Remote connection.
 */
export class RemoteConnection implements Connection {
  private readonly _client: HttpLancedbClient
  private readonly _dbName: string

  constructor (opts: ConnectionOptions) {
    if (!opts.uri.startsWith('db://')) {
      throw new Error(`Invalid remote DB URI: ${opts.uri}`)
    }
    if (opts.apiKey === undefined || opts.region === undefined) {
      throw new Error('API key and region are not supported for remote connections')
    }

    this._dbName = opts.uri.slice('db://'.length)
    let server: string
    if (opts.hostOverride === undefined) {
      server = `https://${this._dbName}.${opts.region}.api.lancedb.com`
    } else {
      server = opts.hostOverride
    }
    this._client = new HttpLancedbClient(server, opts.apiKey, opts.hostOverride === undefined ? undefined : this._dbName)
  }

  get uri (): string {
    // add the lancedb+ prefix back
    return 'db://' + this._client.uri
  }

  async tableNames (pageToken: string = '', limit: number = 10): Promise<string[]> {
    const response = await this._client.get('/v1/table/', { limit, page_token: pageToken })
    return response.data.tables
  }

  async openTable (name: string): Promise<Table>
  async openTable<T> (name: string, embeddings: EmbeddingFunction<T>): Promise<Table<T>>
  async openTable<T> (name: string, embeddings?: EmbeddingFunction<T>): Promise<Table<T>> {
    if (embeddings !== undefined) {
      return new RemoteTable(this._client, name, embeddings)
    } else {
      return new RemoteTable(this._client, name)
    }
  }

  async createTable<T> (nameOrOpts: string | CreateTableOptions<T>, data?: Array<Record<string, unknown>>, optsOrEmbedding?: WriteOptions | EmbeddingFunction<T>, opt?: WriteOptions): Promise<Table<T>> {
    // Logic copied from LocatlConnection, refactor these to a base class + connectionImpl pattern
    let schema
    let embeddings: undefined | EmbeddingFunction<T>
    let tableName: string
    if (typeof nameOrOpts === 'string') {
      if (optsOrEmbedding !== undefined && isEmbeddingFunction(optsOrEmbedding)) {
        embeddings = optsOrEmbedding
      }
      tableName = nameOrOpts
    } else {
      schema = nameOrOpts.schema
      embeddings = nameOrOpts.embeddingFunction
      tableName = nameOrOpts.name
    }

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
      buffer = await fromTableToStreamBuffer(createEmptyTable(schema))
    } else if (data instanceof ArrowTable) {
      buffer = await fromTableToStreamBuffer(data, embeddings)
    } else {
      // data is Array<Record<...>>
      buffer = await fromRecordsToStreamBuffer(data, embeddings)
    }

    const res = await this._client.post(
      `/v1/table/${tableName}/create/`,
      buffer,
      undefined,
      'application/vnd.apache.arrow.stream'
    )
    if (res.status !== 200) {
      throw new Error(`Server Error, status: ${res.status}, ` +
      // eslint-disable-next-line @typescript-eslint/restrict-template-expressions
        `message: ${res.statusText}: ${res.data}`)
    }

    if (embeddings === undefined) {
      return new RemoteTable(this._client, tableName)
    } else {
      return new RemoteTable(this._client, tableName, embeddings)
    }
  }

  async dropTable (name: string): Promise<void> {
    await this._client.post(`/v1/table/${name}/drop/`)
  }
}

export class RemoteQuery<T = number[]> extends Query<T> {
  constructor (query: T, private readonly _client: HttpLancedbClient,
    private readonly _name: string, embeddings?: EmbeddingFunction<T>) {
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
  private readonly _client: HttpLancedbClient
  private readonly _embeddings?: EmbeddingFunction<T>
  private readonly _name: string

  constructor (client: HttpLancedbClient, name: string)
  constructor (client: HttpLancedbClient, name: string, embeddings: EmbeddingFunction<T>)
  constructor (client: HttpLancedbClient, name: string, embeddings?: EmbeddingFunction<T>) {
    this._client = client
    this._name = name
    this._embeddings = embeddings
  }

  get name (): string {
    return this._name
  }

  get schema (): Promise<any> {
    return this._client.post(`/v1/table/${this._name}/describe/`).then(res => {
      if (res.status !== 200) {
        throw new Error(`Server Error, status: ${res.status}, ` +
        // eslint-disable-next-line @typescript-eslint/restrict-template-expressions
          `message: ${res.statusText}: ${res.data}`)
      }
      return res.data?.schema
    })
  }

  search (query: T): Query<T> {
    return new RemoteQuery(query, this._client, this._name)//, this._embeddings_new)
  }

  async add (data: Array<Record<string, unknown>>): Promise<number> {
    const buffer = await fromRecordsToStreamBuffer(data, this._embeddings)
    const res = await this._client.post(
      `/v1/table/${this._name}/insert/`,
      buffer,
      {
        mode: 'append'
      },
      'application/vnd.apache.arrow.stream'
    )
    if (res.status !== 200) {
      throw new Error(`Server Error, status: ${res.status}, ` +
      // eslint-disable-next-line @typescript-eslint/restrict-template-expressions
        `message: ${res.statusText}: ${res.data}`)
    }
    return data.length
  }

  async overwrite (data: Array<Record<string, unknown>>): Promise<number> {
    const buffer = await fromRecordsToStreamBuffer(data, this._embeddings)
    const res = await this._client.post(
      `/v1/table/${this._name}/insert/`,
      buffer,
      {
        mode: 'overwrite'
      },
      'application/vnd.apache.arrow.stream'
    )
    if (res.status !== 200) {
      throw new Error(`Server Error, status: ${res.status}, ` +
      // eslint-disable-next-line @typescript-eslint/restrict-template-expressions
        `message: ${res.statusText}: ${res.data}`)
    }
    return data.length
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
    const indexType = 'vector' // only vector index is supported for remote connections
    const metricType = indexParams.metric_type ?? 'L2'
    const indexCacheSize = indexParams ?? null

    const data = {
      column,
      index_type: indexType,
      metric_type: metricType,
      index_cache_size: indexCacheSize
    }
    const res = await this._client.post(`/v1/table/${this._name}/create_index/`, data)
    if (res.status !== 200) {
      throw new Error(`Server Error, status: ${res.status}, ` +
      // eslint-disable-next-line @typescript-eslint/restrict-template-expressions
        `message: ${res.statusText}: ${res.data}`)
    }
  }

  async createScalarIndex (column: string, replace: boolean): Promise<void> {
    throw new Error('Not implemented')
  }

  async countRows (): Promise<number> {
    const result = await this._client.post(`/v1/table/${this._name}/describe/`)
    return result.data?.stats?.num_rows
  }

  async delete (filter: string): Promise<void> {
    await this._client.post(`/v1/table/${this._name}/delete/`, { predicate: filter })
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
    await this._client.post(`/v1/table/${this._name}/update/`, {
      predicate: filter,
      updates: Object.entries(updates).map(([key, value]) => [key, value])
    })
  }

  async listIndices (): Promise<VectorIndex[]> {
    const results = await this._client.post(`/v1/table/${this._name}/index/list/`)
    return results.data.indexes?.map((index: any) => ({
      columns: index.columns,
      name: index.index_name,
      uuid: index.index_uuid
    }))
  }

  async indexStats (indexUuid: string): Promise<IndexStats> {
    const results = await this._client.post(`/v1/table/${this._name}/index/${indexUuid}/stats/`)
    return {
      numIndexedRows: results.data.num_indexed_rows,
      numUnindexedRows: results.data.num_unindexed_rows
    }
  }
}
