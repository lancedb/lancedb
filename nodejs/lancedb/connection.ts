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
import { ConnectionOptions, Connection as LanceDbConnection } from "./native";
import { Table } from "./table";
import { Table as ArrowTable, Schema } from "apache-arrow";

/**
 * Connect to a LanceDB instance at the given URI.
 *
 * Accepted formats:
 *
 * - `/path/to/database` - local database
 * - `s3://bucket/path/to/database` or `gs://bucket/path/to/database` - database on cloud storage
 * - `db://host:port` - remote database (LanceDB cloud)
 * @param {string} uri - The uri of the database. If the database uri starts
 * with `db://` then it connects to a remote database.
 * @see {@link ConnectionOptions} for more details on the URI format.
 */
export async function connect(
  uri: string,
  opts?: Partial<ConnectionOptions>,
): Promise<Connection> {
  opts = opts ?? {};
  opts.storageOptions = cleanseStorageOptions(opts.storageOptions);
  const nativeConn = await LanceDbConnection.new(uri, opts);
  return new Connection(nativeConn);
}

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

  /**
   * Configuration for object storage.
   *
   * Options already set on the connection will be inherited by the table,
   * but can be overridden here.
   *
   * The available options are described at https://lancedb.github.io/lancedb/guides/storage/
   */
  storageOptions?: Record<string, string>;
}

export interface OpenTableOptions {
  /**
   * Configuration for object storage.
   *
   * Options already set on the connection will be inherited by the table,
   * but can be overridden here.
   *
   * The available options are described at https://lancedb.github.io/lancedb/guides/storage/
   */
  storageOptions?: Record<string, string>;
  /**
   * Set the size of the index cache, specified as a number of entries
   *
   * The exact meaning of an "entry" will depend on the type of index:
   * - IVF: there is one entry for each IVF partition
   * - BTREE: there is one entry for the entire index
   *
   * This cache applies to the entire opened table, across all indices.
   * Setting this value higher will increase performance on larger datasets
   * at the expense of more RAM
   */
  indexCacheSize?: number;
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

  /**
   * Close the connection, releasing any underlying resources.
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

  /**
   * List all the table names in this database.
   *
   * Tables will be returned in lexicographical order.
   * @param {Partial<TableNamesOptions>} options - options to control the
   * paging / start point
   */
  async tableNames(options?: Partial<TableNamesOptions>): Promise<string[]> {
    return this.inner.tableNames(options?.startAfter, options?.limit);
  }

  /**
   * Open a table in the database.
   * @param {string} name - The name of the table
   */
  async openTable(
    name: string,
    options?: Partial<OpenTableOptions>,
  ): Promise<Table> {
    const innerTable = await this.inner.openTable(
      name,
      cleanseStorageOptions(options?.storageOptions),
      options?.indexCacheSize,
    );
    return new Table(innerTable);
  }

  /**
   * Creates a new Table and initialize it with new data.
   * @param {string} name - The name of the table.
   * @param {Record<string, unknown>[] | ArrowTable} data - Non-empty Array of Records
   * to be inserted into the table
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
    const innerTable = await this.inner.createTable(
      name,
      buf,
      mode,
      cleanseStorageOptions(options?.storageOptions),
    );
    return new Table(innerTable);
  }

  /**
   * Creates a new empty Table
   * @param {string} name - The name of the table.
   * @param {Schema} schema - The schema of the table
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
    const innerTable = await this.inner.createEmptyTable(
      name,
      buf,
      mode,
      cleanseStorageOptions(options?.storageOptions),
    );
    return new Table(innerTable);
  }

  /**
   * Drop an existing table.
   * @param {string} name The name of the table to drop.
   */
  async dropTable(name: string): Promise<void> {
    return this.inner.dropTable(name);
  }
}

/**
 * Takes storage options and makes all the keys snake case.
 */
function cleanseStorageOptions(
  options?: Record<string, string>,
): Record<string, string> | undefined {
  if (options === undefined) {
    return undefined;
  }
  const result: Record<string, string> = {};
  for (const [key, value] of Object.entries(options)) {
    if (value !== undefined) {
      const newKey = camelToSnakeCase(key);
      result[newKey] = value;
    }
  }
  return result;
}

/**
 * Convert a string to snake case. It might already be snake case, in which case it is
 * returned unchanged.
 */
function camelToSnakeCase(camel: string): string {
  if (camel.includes("_")) {
    // Assume if there is at least one underscore, it is already snake case
    return camel;
  }
  if (camel.toLocaleUpperCase() === camel) {
    // Assume if the string is all uppercase, it is already snake case
    return camel;
  }

  let result = camel.replace(/[A-Z]/g, (letter) => `_${letter.toLowerCase()}`);
  if (result.startsWith("_")) {
    result = result.slice(1);
  }
  return result;
}
