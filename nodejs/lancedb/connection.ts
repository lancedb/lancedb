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

import { Data, Schema, SchemaLike, TableLike } from "./arrow";
import { fromTableToBuffer, makeEmptyTable } from "./arrow";
import { EmbeddingFunctionConfig, getRegistry } from "./embedding/registry";
import { Connection as LanceDbConnection } from "./native";
import { LocalTable, Table } from "./table";

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
  /**
   * The version of the data storage format to use.
   *
   * The default is `legacy`, which is Lance format v1.
   * `stable` is the new format, which is Lance format v2.
   */
  dataStorageVersion?: string;

  /**
   * If true then data files will be written with the legacy format
   *
   * The default is true while the new format is in beta
   *
   * Deprecated.
   */
  useLegacyFormat?: boolean;
  schema?: SchemaLike;
  embeddingFunction?: EmbeddingFunctionConfig;
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
export abstract class Connection {
  [Symbol.for("nodejs.util.inspect.custom")](): string {
    return this.display();
  }

  /**
   * Return true if the connection has not been closed
   */
  abstract isOpen(): boolean;

  /**
   * Close the connection, releasing any underlying resources.
   *
   * It is safe to call this method multiple times.
   *
   * Any attempt to use the connection after it is closed will result in an error.
   */
  abstract close(): void;

  /**
   * Return a brief description of the connection
   */
  abstract display(): string;

  /**
   * List all the table names in this database.
   *
   * Tables will be returned in lexicographical order.
   * @param {Partial<TableNamesOptions>} options - options to control the
   * paging / start point
   *
   */
  abstract tableNames(options?: Partial<TableNamesOptions>): Promise<string[]>;

  /**
   * Open a table in the database.
   * @param {string} name - The name of the table
   */
  abstract openTable(
    name: string,
    options?: Partial<OpenTableOptions>,
  ): Promise<Table>;

  /**
   * Creates a new Table and initialize it with new data.
   * @param {object} options - The options object.
   * @param {string} options.name - The name of the table.
   * @param {Data} options.data - Non-empty Array of Records to be inserted into the table
   *
   */
  abstract createTable(
    options: {
      name: string;
      data: Data;
    } & Partial<CreateTableOptions>,
  ): Promise<Table>;
  /**
   * Creates a new Table and initialize it with new data.
   * @param {string} name - The name of the table.
   * @param {Record<string, unknown>[] | TableLike} data - Non-empty Array of Records
   * to be inserted into the table
   */
  abstract createTable(
    name: string,
    data: Record<string, unknown>[] | TableLike,
    options?: Partial<CreateTableOptions>,
  ): Promise<Table>;

  /**
   * Creates a new empty Table
   * @param {string} name - The name of the table.
   * @param {Schema} schema - The schema of the table
   */
  abstract createEmptyTable(
    name: string,
    schema: import("./arrow").SchemaLike,
    options?: Partial<CreateTableOptions>,
  ): Promise<Table>;

  /**
   * Drop an existing table.
   * @param {string} name The name of the table to drop.
   */
  abstract dropTable(name: string): Promise<void>;
}

export class LocalConnection extends Connection {
  readonly inner: LanceDbConnection;

  constructor(inner: LanceDbConnection) {
    super();
    this.inner = inner;
  }

  isOpen(): boolean {
    return this.inner.isOpen();
  }

  close(): void {
    this.inner.close();
  }

  display(): string {
    return this.inner.display();
  }

  async tableNames(options?: Partial<TableNamesOptions>): Promise<string[]> {
    return this.inner.tableNames(options?.startAfter, options?.limit);
  }

  async openTable(
    name: string,
    options?: Partial<OpenTableOptions>,
  ): Promise<Table> {
    const innerTable = await this.inner.openTable(
      name,
      cleanseStorageOptions(options?.storageOptions),
      options?.indexCacheSize,
    );

    return new LocalTable(innerTable);
  }

  async createTable(
    nameOrOptions:
      | string
      | ({ name: string; data: Data } & Partial<CreateTableOptions>),
    data?: Record<string, unknown>[] | TableLike,
    options?: Partial<CreateTableOptions>,
  ): Promise<Table> {
    if (typeof nameOrOptions !== "string" && "name" in nameOrOptions) {
      const { name, data, ...options } = nameOrOptions;

      return this.createTable(name, data, options);
    }
    if (data === undefined) {
      throw new Error("data is required");
    }
    const { buf, mode } = await Table.parseTableData(data, options);
    let dataStorageVersion = "legacy";
    if (options?.dataStorageVersion !== undefined) {
      dataStorageVersion = options.dataStorageVersion;
    } else if (options?.useLegacyFormat !== undefined) {
      dataStorageVersion = options.useLegacyFormat ? "legacy" : "stable";
    }

    const innerTable = await this.inner.createTable(
      nameOrOptions,
      buf,
      mode,
      cleanseStorageOptions(options?.storageOptions),
      dataStorageVersion,
    );

    return new LocalTable(innerTable);
  }

  async createEmptyTable(
    name: string,
    schema: import("./arrow").SchemaLike,
    options?: Partial<CreateTableOptions>,
  ): Promise<Table> {
    let mode: string = options?.mode ?? "create";
    const existOk = options?.existOk ?? false;

    if (mode === "create" && existOk) {
      mode = "exist_ok";
    }
    let metadata: Map<string, string> | undefined = undefined;
    if (options?.embeddingFunction !== undefined) {
      const embeddingFunction = options.embeddingFunction;
      const registry = getRegistry();
      metadata = registry.getTableMetadata([embeddingFunction]);
    }

    let dataStorageVersion = "legacy";
    if (options?.dataStorageVersion !== undefined) {
      dataStorageVersion = options.dataStorageVersion;
    } else if (options?.useLegacyFormat !== undefined) {
      dataStorageVersion = options.useLegacyFormat ? "legacy" : "stable";
    }

    const table = makeEmptyTable(schema, metadata);
    const buf = await fromTableToBuffer(table);
    const innerTable = await this.inner.createEmptyTable(
      name,
      buf,
      mode,
      cleanseStorageOptions(options?.storageOptions),
      dataStorageVersion,
    );
    return new LocalTable(innerTable);
  }

  async dropTable(name: string): Promise<void> {
    return this.inner.dropTable(name);
  }
}

/**
 * Takes storage options and makes all the keys snake case.
 */
export function cleanseStorageOptions(
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
