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

import { Configuration, DataApi, type ResponseError, TablesApi } from '../generated-sources/openapi'

export class HttpLancedbClient {
  private readonly _url: string
  private readonly _tablesApi: TablesApi
  private readonly _dataApi: DataApi

  public constructor (url: string,
    apiKey: string,
    dbName?: string) {
    const configuration = new Configuration({
      basePath: url,
      apiKey,
      headers: {
        ...(dbName !== undefined ? { 'x-lancedb-database': dbName } : {})
      }
    })

    this._url = url
    this._tablesApi = new TablesApi(configuration)
    this._dataApi = new DataApi(configuration)
  }

  get tablesApi (): TablesApi {
    return this._tablesApi
  }

  get dataApi (): DataApi {
    return this._dataApi
  }

  get uri (): string {
    return this._url
  }
}

export function ErrorHandler () {
  return async (e: ResponseError) => {
    console.log('handling errors')
    const errorData = await e.response.text()
    throw new Error(
        `Server Error, status: ${e.response.status}, ` +
        `message: ${e.response.statusText}: ${errorData}`
    )
  }
}
