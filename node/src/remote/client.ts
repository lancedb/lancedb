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

import axios from 'axios'

import { tableFromIPC, type Table as ArrowTable } from 'apache-arrow'

export class HttpLancedbClient {
  private readonly _url: string

  public constructor (url: string) {
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
              `${this._url}/v1/table/${tableName}/`,
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
                  'Content-Type': 'application/json'
                },
                responseType: 'arraybuffer',
                timeout: 10000
              }
    )

    if (response.status !== 200) {
      throw new Error(`Server Error, status: ${response.status}, message: ${response.statusText}`)
    }

    const table = tableFromIPC(response.data)
    return table
  }
}
