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

import axios, {
  AxiosError,
  type AxiosResponse,
  type ResponseType,
} from "axios";
import { Table as ArrowTable } from "../arrow";
import { tableFromIPC } from "../arrow";
import { VectorQuery } from "../query";

export class RestfulLanceDBClient {
  #dbName: string;
  #region: string;
  #apiKey: string;
  #hostOverride?: string;
  #closed: boolean = false;
  #timeout: number = 12 * 1000; // 12 seconds;
  #session?: import("axios").AxiosInstance;

  constructor(
    dbName: string,
    apiKey: string,
    region: string,
    hostOverride?: string,
    timeout?: number,
  ) {
    this.#dbName = dbName;
    this.#apiKey = apiKey;
    this.#region = region;
    this.#hostOverride = hostOverride ?? this.#hostOverride;
    this.#timeout = timeout ?? this.#timeout;
    this.#session = undefined;
  }

  get session(): import("axios").AxiosInstance {
    if (this.#session === undefined) {
      this.#session = axios.create({
        baseURL: this.url,
        headers: {
          // biome-ignore lint: external API
          "x-api-key": this.#apiKey,
        },
        transformResponse: decodeErrorData,
        timeout: this.#timeout,
      });
    }
    return this.#session;
  }

  get url(): string {
    return (
      this.#hostOverride ??
      `https://${this.#dbName}.${this.#region}.api.lancedb.com`
    );
  }

  isOpen(): boolean {
    return !this.#closed;
  }

  private checkNotClosed(): void {
    if (this.#closed) {
      throw new Error("Connection is closed");
    }
  }

  close(): void {
    this.#session = undefined;
    this.#closed = true;
  }

  // biome-ignore lint/suspicious/noExplicitAny: <explanation>
  async get(uri: string, params?: Record<string, any>): Promise<any> {
    this.checkNotClosed();
    uri = new URL(uri, this.url).toString();
    let response;
    try {
      response = await this.session.get(uri, {
        params,
      });
    } catch (e) {
      if (e instanceof AxiosError && e.response) {
        response = e.response;
      } else {
        throw e;
      }
    }

    RequestError.checkStatus(response!);

    return response!.data;
  }

  // biome-ignore lint/suspicious/noExplicitAny: api response
  async post(uri: string, body?: any): Promise<any>;
  async post(
    uri: string,
    // biome-ignore lint/suspicious/noExplicitAny: api request
    body: any,
    additional: {
      config?: { responseType: "arraybuffer" };
      headers?: Record<string, string>;
      params?: Record<string, string>;
    },
  ): Promise<Buffer>;
  async post(
    uri: string,
    // biome-ignore lint/suspicious/noExplicitAny: api request
    body?: any,
    additional?: {
      config?: { responseType: ResponseType };
      headers?: Record<string, string>;
      params?: Record<string, string>;
    },
    // biome-ignore lint/suspicious/noExplicitAny: api response
  ): Promise<any> {
    this.checkNotClosed();
    uri = new URL(uri, this.url).toString();

    if (additional === undefined) {
      additional = {};
    }
    additional.config = additional.config ?? { responseType: "json" };
    additional.headers = additional.headers ?? {};
    additional.headers["Content-Type"] =
      additional.headers["Content-Type"] || "application/json";

    let response;
    try {
      response = await this.session.post(uri, body, {
        headers: additional.headers,
        responseType: additional!.config!.responseType,
        params: new Map(Object.entries(additional.params ?? {})),
      });
    } catch (e) {
      if (e instanceof AxiosError && e.response) {
        response = e.response;
      } else {
        throw e;
      }
    }
    RequestError.checkStatus(response!);
    return response!.data;
  }

  async listTables(limit = 10, pageToken = ""): Promise<string[]> {
    const json = await this.get("/v1/table", { limit, pageToken });
    return json.tables;
  }

  async query(tableName: string, query: VectorQuery): Promise<ArrowTable> {
    const tbl = await this.post(`/v1/table/${tableName}/query`, query, {
      config: {
        responseType: "arraybuffer",
      },
    });
    return tableFromIPC(tbl);
  }
}

function decodeErrorData(data: unknown) {
  if (Buffer.isBuffer(data)) {
    const decoded = data.toString("utf-8");
    return decoded;
  }
  return data;
}

export class RequestError extends Error {
  constructor(
    public message: string,
    public status?: number,
    public response?: AxiosResponse,
  ) {
    super(message);
    this.name = "RequestError";
    this.message = message;
  }

  public static checkStatus(response: AxiosResponse) {
    if (response.status >= 300) {
      throw new RequestError(response.statusText, response.status, response);
    }
  }

  public toString(): string {
    return `${this.name}: ${this.message}`;
  }
}
