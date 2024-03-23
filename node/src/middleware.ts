// Copyright 2024 LanceDB Developers.
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

/**
 * Middleware for Remote LanceDB Connection or Table
 */
export interface HttpMiddleware {
  /**
   * A callback that can be used to instrument the behavior of http requests to remote
   * tables. It can be used to add headers, modify the request, or even short-circuit
   * the request and return a response without making the request to the remote endpoint.
   * It can also be used to modify the response from the remote endpoint.
   *
   * @param {RemoteResponse} res - Request to the remote endpoint
   * @param {onRemoteRequestNext} next - Callback to advance the middleware chain
   */
  onRemoteRequest(
    req: RemoteRequest,
    next: (req: RemoteRequest) => Promise<RemoteResponse>,
  ): Promise<RemoteResponse>
};

export enum Method {
  GET,
  POST
}

/**
 * A LanceDB Remote HTTP Request
 */
export interface RemoteRequest {
  uri: string
  method: Method
  headers: Map<string, string>
  params?: Map<string, string>
  body?: any
}

/**
 * A LanceDB Remote HTTP Response
 */
export interface RemoteResponse {
  status: number
  statusText: string
  headers: Map<string, string>
  body: () => Promise<any>
}
