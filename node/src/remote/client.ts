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

import axios, { type AxiosResponse, type ResponseType } from 'axios'

import { tableFromIPC, type Table as ArrowTable } from 'apache-arrow'

import { type RemoteResponse, type RemoteRequest, Method } from '../middleware'

interface HttpLancedbClientMiddleware {
  onRemoteRequest(
    req: RemoteRequest,
    next: (req: RemoteRequest) => Promise<RemoteResponse>,
  ): Promise<RemoteResponse>
}

/**
 * Invoke the middleware chain and at the end call the remote endpoint
 */
async function callWithMiddlewares (
  req: RemoteRequest,
  middlewares: HttpLancedbClientMiddleware[],
  opts?: MiddlewareInvocationOptions
): Promise<RemoteResponse> {
  async function call (
    i: number,
    req: RemoteRequest
  ): Promise<RemoteResponse> {
    // if we have reached the end of the middleware chain, make the request
    if (i > middlewares.length) {
      const headers = Object.fromEntries(req.headers.entries())
      const params = Object.fromEntries(req.params?.entries() ?? [])
      const timeout = opts?.timeout
      let res
      if (req.method === Method.POST) {
        res = await axios.post(
          req.uri,
          req.body,
          {
            headers,
            params,
            timeout,
            responseType: opts?.responseType
          }
        )
      } else {
        res = await axios.get(
          req.uri,
          {
            headers,
            params,
            timeout
          }
        )
      }

      return toLanceRes(res)
    }

    // call next middleware in chain
    return await middlewares[i - 1].onRemoteRequest(
      req,
      async (req) => {
        return await call(i + 1, req)
      }
    )
  }

  return await call(1, req)
}

interface MiddlewareInvocationOptions {
  responseType?: ResponseType
  timeout?: number,
}

/**
 * Marshall the library response into a LanceDB response
 */
function toLanceRes (res: AxiosResponse): RemoteResponse {
  const headers = new Map()
  for (const h in res.headers) {
    headers.set(h, res.headers[h])
  }

  return {
    status: res.status,
    statusText: res.statusText,
    headers,
    body: async () => {
      return res.data
    }
  }
}

async function decodeErrorData(
  res: RemoteResponse,
  responseType?: ResponseType
): Promise<string> {
  const errorData = await res.body()
  if (responseType === 'arraybuffer') {
      return new TextDecoder().decode(errorData)
  } else {
    if (typeof errorData === 'object') {
      return JSON.stringify(errorData)
    }

    return errorData
  }
}

export class HttpLancedbClient {
  private readonly _url: string
  private readonly _apiKey: () => string
  private readonly _middlewares: HttpLancedbClientMiddleware[]
  private readonly _timeout: number | undefined

  public constructor (
    url: string,
    apiKey: string,
    timeout?: number,
    private readonly _dbName?: string,
    
  ) {
    this._url = url
    this._apiKey = () => apiKey
    this._middlewares = []
    this._timeout = timeout
  }

  get uri (): string {
    return this._url
  }

  public async search (
    tableName: string,
    vector: number[],
    k: number,
    nprobes: number,
    prefilter: boolean,
    refineFactor?: number,
    columns?: string[],
    filter?: string
  ): Promise<ArrowTable<any>> {
    const result = await this.post(
      `/v1/table/${tableName}/query/`,
      {
        vector,
        k,
        nprobes,
        refineFactor,
        columns,
        filter,
        prefilter
      },
      undefined,
      undefined,
      'arraybuffer'
    )
    const table = tableFromIPC(await result.body())
    return table
  }

  /**
   * Sent GET request.
   */
  public async get (path: string, params?: Record<string, string>): Promise<RemoteResponse> {
    const req = {
      uri: `${this._url}${path}`,
      method: Method.GET,
      headers: new Map(Object.entries({
        'Content-Type': 'application/json',
        'x-api-key': this._apiKey(),
        ...(this._dbName !== undefined ? { 'x-lancedb-database': this._dbName } : {})
      })),
      params: new Map(Object.entries(params ?? {}))
    }

    let response
    try {
      response = await callWithMiddlewares(req, this._middlewares)
      return response
    } catch (err: any) {
      console.error('error: ', err)
      if (err.response === undefined) {
        throw new Error(`Network Error: ${err.message as string}`)
      }

      response = toLanceRes(err.response)
    }

    if (response.status !== 200) {
      const errorData = await decodeErrorData(response)
      throw new Error(
        `Server Error, status: ${response.status}, ` +
        `message: ${response.statusText}: ${errorData}`
      )
    }

    return response
  }

  /**
   * Sent POST request.
   */
  public async post (
    path: string,
    data?: any,
    params?: Record<string, string>,
    content?: string | undefined,
    responseType?: ResponseType | undefined
  ): Promise<RemoteResponse> {
    const req = {
      uri: `${this._url}${path}`,
      method: Method.POST,
      headers: new Map(Object.entries({
        'Content-Type': content ?? 'application/json',
        'x-api-key': this._apiKey(),
        ...(this._dbName !== undefined ? { 'x-lancedb-database': this._dbName } : {})
      })),
      params: new Map(Object.entries(params ?? {})),
      body: data
    }

    let response
    try {
      response = await callWithMiddlewares(req, this._middlewares, {
        responseType,
        timeout: this._timeout,
      })

      // return response
    } catch (err: any) {
      console.error('error: ', err)
      if (err.response === undefined) {
        throw new Error(`Network Error: ${err.message as string}`)
      }
      response = toLanceRes(err.response)
    }

    if (response.status !== 200) {
      const errorData = await decodeErrorData(response, responseType)
      throw new Error(
        `Server Error, status: ${response.status}, ` +
        `message: ${response.statusText}: ${errorData}`
      )
    }

    return response
  }

  /**
   * Instrument this client with middleware
   * @param mw - The middleware that instruments the client
   * @returns - an instance of this client instrumented with the middleware
   */
  public withMiddleware (mw: HttpLancedbClientMiddleware): HttpLancedbClient {
    const wrapped = this.clone()
    wrapped._middlewares.push(mw)
    return wrapped
  }

  /**
   * Make a clone of this client
   */
  private clone (): HttpLancedbClient {
    const clone = new HttpLancedbClient(this._url, this._apiKey(), this._timeout, this._dbName)
    for (const mw of this._middlewares) {
      clone._middlewares.push(mw)
    }
    return clone
  }
}
