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

import axios, { type AxiosResponse } from 'axios'

import { tableFromIPC, type Table as ArrowTable } from 'apache-arrow'

export class HttpLancedbClient {
  private readonly _url: string

  public constructor (
    url: string,
    private readonly _apiKey: string,
    private readonly _dbName?: string
  ) {
    this._url = url
  }

  get uri (): string {
    return this._url
  }

  public async search (
    tableName: string,
    vector: number[],
    k: number,
    nprobes: number,
    refineFactor?: number,
    columns?: string[],
    filter?: string
  ): Promise<ArrowTable<any>> {
    const response = await axios.post(
              `${this._url}/v1/table/${tableName}`,
              {
                vector,
                k,
                nprobes,
                refineFactor,
                columns,
                filter
              },
              {
                headers: {
                  'Content-Type': 'application/json',
                  'x-api-key': this._apiKey,
                  ...(this._dbName !== undefined ? { 'x-lancedb-database': this._dbName } : {})
                },
                responseType: 'arraybuffer',
                timeout: 10000
              }
    ).catch((err) => {
      console.error('error: ', err)
      return err.response
    })
    if (response.status !== 200) {
      const errorData = new TextDecoder().decode(response.data)
      throw new Error(
        `Server Error, status: ${response.status as number}, ` +
        `message: ${response.statusText as string}: ${errorData}`
      )
    }

    const table = tableFromIPC(response.data)
    return table
  }

  /**
   * Sent GET request.
   */
  public async get (path: string, params?: Record<string, string | number>): Promise<AxiosResponse> {
    const response = await axios.get(
      `${this._url}${path}`,
      {
        headers: {
          'Content-Type': 'application/json',
          'x-api-key': this._apiKey
        },
        params,
        timeout: 10000
      }
    ).catch((err) => {
      console.error('error: ', err)
      return err.response
    })
    if (response.status !== 200) {
      const errorData = new TextDecoder().decode(response.data)
      throw new Error(
        `Server Error, status: ${response.status as number}, ` +
        `message: ${response.statusText as string}: ${errorData}`
      )
    }
    return response
  }
}
