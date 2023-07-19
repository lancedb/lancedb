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
  type ConnectionOptions
} from '../index'
import { Query } from '../query'

import { type Table as ArrowTable, Vector } from 'apache-arrow'
import { HttpLancedbClient } from './client'

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

  async tableNames (): Promise<string[]> {
    const response = await this._client.get('/v1/table/')
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

  async createTable (name: string, data: Array<Record<string, unknown>>): Promise<Table>
  async createTable<T> (name: string, data: Array<Record<string, unknown>>, embeddings: EmbeddingFunction<T>): Promise<Table<T>>
  async createTable<T> (name: string, data: Array<Record<string, unknown>>, embeddings?: EmbeddingFunction<T>): Promise<Table<T>> {
    throw new Error('Not implemented')
  }

  async createTableArrow (name: string, table: ArrowTable): Promise<Table> {
    throw new Error('Not implemented')
  }

  async dropTable (name: string): Promise<void> {
    throw new Error('Not implemented')
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

  search (query: T): Query<T> {
    return new RemoteQuery(query, this._client, this._name)//, this._embeddings_new)
  }

  async add (data: Array<Record<string, unknown>>): Promise<number> {
    throw new Error('Not implemented')
  }

  async overwrite (data: Array<Record<string, unknown>>): Promise<number> {
    throw new Error('Not implemented')
  }

  async createIndex (indexParams: VectorIndexParams): Promise<any> {
    throw new Error('Not implemented')
  }

  async countRows (): Promise<number> {
    throw new Error('Not implemented')
  }

  async delete (filter: string): Promise<void> {
    throw new Error('Not implemented')
  }
}
