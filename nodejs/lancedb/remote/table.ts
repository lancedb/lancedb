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

import { Table as ArrowTable } from "apache-arrow";

import { Data, IntoVector } from "../arrow";

import { VectorQuery } from "../query";
import { AddDataOptions, Table, UpdateOptions } from "../table";
import { cachedProperty } from "../util";
import { RestfulLanceDBClient } from "./client";

export class RemoteTable extends Table {
  #client: RestfulLanceDBClient;
  #name: string;

  public constructor(client: RestfulLanceDBClient, tableName: string) {
    super();
    this.#client = client;
    this.#name = tableName;
  }

  isOpen(): boolean {
    return !this.#client.isOpen();
  }
  close(): void {
    this.#client.close();
  }
  display(): string {
    return `RemoteTable(${this.#name})`;
  }

  @cachedProperty
  async schema(): Promise<import("apache-arrow").Schema> {
    const resp = (await this.#client.post(
      `/v1/table/${this.#name}/describe/`,
      // biome-ignore lint/suspicious/noExplicitAny: <explanation>
    )) as any;
    // TODO: parse this into a valid arrow schema
    // biome-ignore lint/suspicious/noExplicitAny: <explanation>
    return resp.schema as any;
  }
  async add(data: Data, options?: Partial<AddDataOptions>): Promise<void> {
    const { buf, mode } = await Table.parseTableData(
      data,
      // biome-ignore lint/suspicious/noExplicitAny: <explanation>
      options as any,
      true,
    );
    await this.#client.post(
      `/v1/table/${encodeURIComponent(this.#name)}/insert/`,
      buf,
      {
        params: {
          mode,
        },
        headers: {
          "Content-Type": "application/vnd.apache.arrow.stream",
        },
      },
    );
  }
  async update(
    updates: Map<string, string> | Record<string, string>,
    options?: Partial<UpdateOptions>,
  ): Promise<void> {
    await this.#client.post(
      `/v1/table/${encodeURIComponent(this.#name)}/update/`,
      {
        predicate: options?.where ?? null,
        updates: Object.entries(updates).map(([key, value]) => [key, value]),
      },
    );
  }
  async countRows(filter?: unknown): Promise<number> {
    const payload = { predicate: filter };
    return (await this.#client.post(
      `/v1/table/${this.#name}/count_rows/`,
      payload,
      // biome-ignore lint/suspicious/noExplicitAny: <explanation>
    )) as any;
  }

  async delete(predicate: unknown): Promise<void> {
    const payload = { predicate };
    await this.#client.post(`/v1/table/${this.#name}/delete/`, payload);
  }
  createIndex(_column: unknown, _options?: unknown): Promise<void> {
    throw new Error("createIndex() is not yet supported on the LanceDB cloud");
  }
  query(): import("..").Query {
    throw new Error("query() is not yet supported on the LanceDB cloud");
  }
  search(query: IntoVector): VectorQuery;
  search(query: string): Promise<VectorQuery>;
  search(_query: string | IntoVector): VectorQuery | Promise<VectorQuery> {
    throw new Error("search() is not yet supported on the LanceDB cloud");
  }
  vectorSearch(_vector: unknown): import("..").VectorQuery {
    throw new Error("vectorSearch() is not yet supported on the LanceDB cloud");
  }
  addColumns(_newColumnTransforms: unknown): Promise<void> {
    throw new Error("addColumns() is not yet supported on the LanceDB cloud");
  }
  alterColumns(_columnAlterations: unknown): Promise<void> {
    throw new Error("alterColumns() is not yet supported on the LanceDB cloud");
  }
  dropColumns(_columnNames: unknown): Promise<void> {
    throw new Error("dropColumns() is not yet supported on the LanceDB cloud");
  }
  async version(): Promise<number> {
    console.log("name= ", this.#name);
    const resp = (await this.#client.post(
      `/v1/table/${this.#name}/describe/`,
      // biome-ignore lint/suspicious/noExplicitAny: <explanation>
    )) as any;
    return resp.version;
  }
  checkout(_version: unknown): Promise<void> {
    throw new Error("Method not implemented.");
  }
  checkoutLatest(): Promise<void> {
    throw new Error("Method not implemented.");
  }
  restore(): Promise<void> {
    throw new Error("Method not implemented.");
  }
  optimize(_options?: unknown): Promise<import("../native").OptimizeStats> {
    throw new Error("Method not implemented.");
  }
  async listIndices(): Promise<import("../native").IndexConfig[]> {
    return (await this.#client.post(
      `/v1/table/${this.#name}/index/list/`,
      // biome-ignore lint/suspicious/noExplicitAny: <explanation>
    )) as any;
  }
  toArrow(): Promise<ArrowTable> {
    throw new Error("toArrow() is not yet supported on the LanceDB cloud");
  }
}
