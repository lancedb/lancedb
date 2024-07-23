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
  }

  // todo: cache the session.
  get session(): import("axios").AxiosInstance {
    if (this.#session !== undefined) {
      return this.#session;
    } else {
      return axios.create({
        baseURL: this.url,
        headers: {
          // biome-ignore lint: external API
          Authorization: `Bearer ${this.#apiKey}`,
        },
        transformResponse: decodeErrorData,
        timeout: this.#timeout,
      });
    }
  }

  get url(): string {
    return (
      this.#hostOverride ??
      `https://${this.#dbName}.${this.#region}.api.lancedb.com`
    );
  }

  get headers(): { [key: string]: string } {
    const headers: { [key: string]: string } = {
      "x-api-key": this.#apiKey,
      "x-request-id": "na",
    };
    if (this.#region == "local") {
      headers["Host"] = `${this.#dbName}.${this.#region}.api.lancedb.com`;
    }
    if (this.#hostOverride) {
      headers["x-lancedb-database"] = this.#dbName;
    }
    return headers;
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
        headers: this.headers,
        params,
      });
    } catch (e) {
      if (e instanceof AxiosError && e.response) {
        response = e.response;
      } else {
        throw e;
      }
    }

    RestfulLanceDBClient.checkStatus(response!);
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
    additional = Object.assign(
      { config: { responseType: "json" } },
      additional,
    );

    const headers = { ...this.headers, ...additional.headers };

    if (!headers["Content-Type"]) {
      headers["Content-Type"] = "application/json";
    }
    let response;
    try {
      response = await this.session.post(uri, body, {
        headers,
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
    RestfulLanceDBClient.checkStatus(response!);
    if (additional!.config!.responseType === "arraybuffer") {
      return response!.data;
    } else {
      return JSON.parse(response!.data);
    }
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

  static checkStatus(response: AxiosResponse): void {
    if (response.status === 404) {
      throw new Error(`Not found: ${response.data}`);
    } else if (response.status >= 400 && response.status < 500) {
      throw new Error(
        `Bad Request: ${response.status}, error: ${response.data}`,
      );
    } else if (response.status >= 500 && response.status < 600) {
      throw new Error(
        `Internal Server Error: ${response.status}, error: ${response.data}`,
      );
    } else if (response.status !== 200) {
      throw new Error(
        `Unknown Error: ${response.status}, error: ${response.data}`,
      );
    }
  }
}

function decodeErrorData(data: unknown) {
  if (Buffer.isBuffer(data)) {
    const decoded = data.toString("utf-8");
    return decoded;
  }
  return data;
}
