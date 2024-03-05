// Copyright 2024 Lance Developers.
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

import { fromTableToBuffer, makeArrowTable, makeEmptyTable } from "./arrow";
import { Connection as LanceDbConnection } from "./native";
import { Table } from "./table";
import { Table as ArrowTable, Schema } from "apache-arrow";

export interface CreateTableOptions {
  /**
   * The mode to use when creating the table.
   *
   * If this is set to "create" and the table already exists then either
   * an error will be thrown or, if existOk is true, then nothing will
   * happen.  Any provided data will be ignored.
   *
   * If this is set to "overwrite" then any existing table will be replaced.
   */
  mode: "create" | "overwrite";
  /**
   * If this is true and the table already exists and the mode is "create"
   * then no error will be raised.
   */
  existOk: boolean;
}

export interface TableNamesOptions {
  /**
   * If present, only return names that come lexicographically after the
   * supplied value.
   *
   * This can be combined with limit to implement pagination by setting this to
   * the last table name from the previous page.
   */
  startAfter?: string;
  /** An optional limit to the number of results to return. */
  limit?: number;
}

/**
 * A LanceDB Connection that allows you to open tables and create new ones.
 *
 * Connection could be local against filesystem or remote against a server.
 *
 * A Connection is intended to be a long lived object and may hold open
 * resources such as HTTP connection pools.  This is generally fine and
 * a single connection should be shared if it is going to be used many
 * times. However, if you are finished with a connection, you may call
 * close to eagerly free these resources.  Any call to a Connection
 * method after it has been closed will result in an error.
 *
 * Closing a connection is optional.  Connections will automatically
 * be closed when they are garbage collected.
 *
 * Any created tables are independent and will continue to work even if
 * the underlying connection has been closed.
 */
export class Connection {
  readonly inner: LanceDbConnection;

  constructor(inner: LanceDbConnection) {
    this.inner = inner;
  }

  /** Return true if the connection has not been closed */
  isOpen(): boolean {
    return this.inner.isOpen();
  }

  /** Close the connection, releasing any underlying resources.
   *
   * It is safe to call this method multiple times.
   *
   * Any attempt to use the connection after it is closed will result in an error.
   */
  close(): void {
    this.inner.close();
  }

  /** Return a brief description of the connection */
  display(): string {
    return this.inner.display();
  }

  /** List all the table names in this database.
   *
   * Tables will be returned in lexicographical order.
   *
   * @param options Optional parameters to control the listing.
   */
  async tableNames(options?: Partial<TableNamesOptions>): Promise<string[]> {
    return this.inner.tableNames(options?.startAfter, options?.limit);
  }

  /**
   * Open a table in the database.
   *
   * @param name The name of the table.
   * @param embeddings An embedding function to use on this table
   */
  async openTable(name: string): Promise<Table> {
    const innerTable = await this.inner.openTable(name);
    return new Table(innerTable);
  }

  /**
   * Creates a new Table and initialize it with new data.
   *
   * @param {string} name - The name of the table.
   * @param data - Non-empty Array of Records to be inserted into the table
   */
  async createTable(
    name: string,
    data: Record<string, unknown>[] | ArrowTable,
    options?: Partial<CreateTableOptions>,
  ): Promise<Table> {
    let mode: string = options?.mode ?? "create";
    const existOk = options?.existOk ?? false;

    if (mode === "create" && existOk) {
      mode = "exist_ok";
    }

    let table: ArrowTable;
    if (data instanceof ArrowTable) {
      table = data;
    } else {
      table = makeArrowTable(data);
    }
    const buf = await fromTableToBuffer(table);
    const innerTable = await this.inner.createTable(name, buf, mode);
    return new Table(innerTable);
  }

  /**
   * Creates a new empty Table
   *
   * @param {string} name - The name of the table.
   * @param schema - The schema of the table
   */
  async createEmptyTable(
    name: string,
    schema: Schema,
    options?: Partial<CreateTableOptions>,
  ): Promise<Table> {
    let mode: string = options?.mode ?? "create";
    const existOk = options?.existOk ?? false;

    if (mode === "create" && existOk) {
      mode = "exist_ok";
    }

    const table = makeEmptyTable(schema);
    const buf = await fromTableToBuffer(table);
    const innerTable = await this.inner.createEmptyTable(name, buf, mode);
    return new Table(innerTable);
  }

  /**
   * Drop an existing table.
   * @param name The name of the table to drop.
   */
  async dropTable(name: string): Promise<void> {
    return this.inner.dropTable(name);
  }
}
