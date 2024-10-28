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

import { Vector, tableFromIPC } from 'apache-arrow'
import { type EmbeddingFunction } from './embedding/embedding_function'
import { type MetricType } from '.'

// eslint-disable-next-line @typescript-eslint/no-var-requires
const { tableSearch } = require('../native.js')

/**
 * A builder for nearest neighbor queries for LanceDB.
 */
export class Query<T = number[]> {
  private readonly _query?: T
  private readonly _tbl?: any
  private _queryVector?: number[]
  private _limit?: number
  private _refineFactor?: number
  private _nprobes: number
  private _select?: string[]
  private _filter?: string
  private _metricType?: MetricType
  private _prefilter: boolean
  private _fastSearch: boolean
  protected readonly _embeddings?: EmbeddingFunction<T>

  constructor (query?: T, tbl?: any, embeddings?: EmbeddingFunction<T>) {
    this._tbl = tbl
    this._query = query
    this._limit = 10
    this._nprobes = 20
    this._refineFactor = undefined
    this._select = undefined
    this._filter = undefined
    this._metricType = undefined
    this._embeddings = embeddings
    this._prefilter = false
    this._fastSearch = false
  }

  /***
     * Sets the number of results that will be returned
     * default value is 10
     * @param value number of results
     */
  limit (value: number): Query<T> {
    this._limit = value
    return this
  }

  /**
     * Refine the results by reading extra elements and re-ranking them in memory.
     * @param value refine factor to use in this query.
     */
  refineFactor (value: number): Query<T> {
    this._refineFactor = value
    return this
  }

  /**
     * The number of probes used. A higher number makes search more accurate but also slower.
     * @param value The number of probes used.
     */
  nprobes (value: number): Query<T> {
    this._nprobes = value
    return this
  }

  /**
     * A filter statement to be applied to this query.
     * @param value A filter in the same format used by a sql WHERE clause.
     */
  filter (value: string): Query<T> {
    this._filter = value
    return this
  }

  where = this.filter

  /** Return only the specified columns.
     *
     * @param value Only select the specified columns. If not specified, all columns will be returned.
     */
  select (value: string[]): Query<T> {
    this._select = value
    return this
  }

  /**
     * The MetricType used for this Query.
     * @param value The metric to the. @see MetricType for the different options
     */
  metricType (value: MetricType): Query<T> {
    this._metricType = value
    return this
  }

  prefilter (value: boolean): Query<T> {
    this._prefilter = value
    return this
  }

  /**
   * Skip searching un-indexed data. This can make search faster, but will miss
   * any data that is not yet indexed.
   */
  fastSearch (value: boolean): Query<T> {
    this._fastSearch = value
    return this
  }

  /**
     * Execute the query and return the results as an Array of Objects
     */
  async execute<T = Record<string, unknown>> (): Promise<T[]> {
    if (this._query !== undefined) {
      if (this._embeddings !== undefined) {
        this._queryVector = (await this._embeddings.embed([this._query]))[0]
      } else {
        this._queryVector = this._query as number[]
      }
    }

    const isElectron = this.isElectron()
    const buffer = await tableSearch.call(this._tbl, this, isElectron)
    const data = tableFromIPC(buffer)

    return data.toArray().map((entry: Record<string, unknown>) => {
      const newObject: Record<string, unknown> = {}
      Object.keys(entry).forEach((key: string) => {
        if (entry[key] instanceof Vector) {
          // toJSON() returns f16 array correctly
          newObject[key] = (entry[key] as any).toJSON()
        } else {
          newObject[key] = entry[key] as any
        }
      })
      return newObject as unknown as T
    })
  }

  // See https://github.com/electron/electron/issues/2288
  private isElectron (): boolean {
    try {
      // eslint-disable-next-line no-prototype-builtins
      return (process?.versions?.hasOwnProperty('electron') || navigator?.userAgent?.toLowerCase()?.includes(' electron'))
    } catch (e) {
      return false
    }
  }
}
