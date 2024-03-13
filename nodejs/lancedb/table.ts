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

import { Schema, tableFromIPC } from "apache-arrow";
import {
  AddColumnsSql,
  ColumnAlteration,
  IndexConfig,
  Table as _NativeTable,
} from "./native";
import { Query } from "./query";
import { IndexOptions } from "./indices";
import { Data, fromDataToBuffer } from "./arrow";

export { IndexConfig } from "./native";
/**
 * Options for adding data to a table.
 */
export interface AddDataOptions {
  /** If "append" (the default) then the new data will be added to the table
   *
   * If "overwrite" then the new data will replace the existing data in the table.
   */
  mode: "append" | "overwrite";
}

export interface UpdateOptions {
  /**
   * A filter that limits the scope of the update.
   *
   * This should be an SQL filter expression.
   *
   * Only rows that satisfy the expression will be updated.
   *
   * For example, this could be 'my_col == 0' to replace all instances
   * of 0 in a column with some other default value.
   */
  where: string;
}

/**
 * A Table is a collection of Records in a LanceDB Database.
 *
 * A Table object is expected to be long lived and reused for multiple operations.
 * Table objects will cache a certain amount of index data in memory.  This cache
 * will be freed when the Table is garbage collected.  To eagerly free the cache you
 * can call the `close` method.  Once the Table is closed, it cannot be used for any
 * further operations.
 *
 * Closing a table is optional.  It not closed, it will be closed when it is garbage
 * collected.
 */
export class Table {
  private readonly inner: _NativeTable;

  /** Construct a Table. Internal use only. */
  constructor(inner: _NativeTable) {
    this.inner = inner;
  }

  /** Return true if the table has not been closed */
  isOpen(): boolean {
    return this.inner.isOpen();
  }

  /** Close the table, releasing any underlying resources.
   *
   * It is safe to call this method multiple times.
   *
   * Any attempt to use the table after it is closed will result in an error.
   */
  close(): void {
    this.inner.close();
  }

  /** Return a brief description of the table */
  display(): string {
    return this.inner.display();
  }

  /** Get the schema of the table. */
  async schema(): Promise<Schema> {
    const schemaBuf = await this.inner.schema();
    const tbl = tableFromIPC(schemaBuf);
    return tbl.schema;
  }

  /**
   * Insert records into this Table.
   *
   * @param {Data} data Records to be inserted into the Table
   * @return The number of rows added to the table
   */
  async add(data: Data, options?: Partial<AddDataOptions>): Promise<void> {
    const mode = options?.mode ?? "append";

    const buffer = await fromDataToBuffer(data);
    await this.inner.add(buffer, mode);
  }

  /**
   * Update existing records in the Table
   *
   * An update operation can be used to adjust existing values.  Use the
   * returned builder to specify which columns to update.  The new value
   * can be a literal value (e.g. replacing nulls with some default value)
   * or an expression applied to the old value (e.g. incrementing a value)
   *
   * An optional condition can be specified (e.g. "only update if the old
   * value is 0")
   *
   * Note: if your condition is something like "some_id_column == 7" and
   * you are updating many rows (with different ids) then you will get
   * better performance with a single [`merge_insert`] call instead of
   * repeatedly calilng this method.
   *
   * @param updates the columns to update
   *
   * Keys in the map should specify the name of the column to update.
   * Values in the map provide the new value of the column.  These can
   * be SQL literal strings (e.g. "7" or "'foo'") or they can be expressions
   * based on the row being updated (e.g. "my_col + 1")
   *
   * @param options additional options to control the update behavior
   */
  async update(
    updates: Map<string, string> | Record<string, string>,
    options?: Partial<UpdateOptions>,
  ) {
    const onlyIf = options?.where;
    let columns: [string, string][];
    if (updates instanceof Map) {
      columns = Array.from(updates.entries());
    } else {
      columns = Object.entries(updates);
    }
    await this.inner.update(onlyIf, columns);
  }

  /** Count the total number of rows in the dataset. */
  async countRows(filter?: string): Promise<number> {
    return await this.inner.countRows(filter);
  }

  /** Delete the rows that satisfy the predicate. */
  async delete(predicate: string): Promise<void> {
    await this.inner.delete(predicate);
  }

  /** Create an index to speed up queries.
   *
   * Indices can be created on vector columns or scalar columns.
   * Indices on vector columns will speed up vector searches.
   * Indices on scalar columns will speed up filtering (in both
   * vector and non-vector searches)
   *
   * @example
   *
   * If the column has a vector (fixed size list) data type then
   * an IvfPq vector index will be created.
   *
   * ```typescript
   * const table = await conn.openTable("my_table");
   * await table.createIndex(["vector"]);
   * ```
   *
   * For advanced control over vector index creation you can specify
   * the index type and options.
   * ```typescript
   * const table = await conn.openTable("my_table");
   * await table.createIndex(["vector"], I)
   *   .ivf_pq({ num_partitions: 128, num_sub_vectors: 16 })
   *   .build();
   * ```
   *
   * Or create a Scalar index
   *
   * ```typescript
   * await table.createIndex("my_float_col").build();
   * ```
   */
  async createIndex(column: string, options?: Partial<IndexOptions>) {
    // Bit of a hack to get around the fact that TS has no package-scope.
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const nativeIndex = (options?.config as any)?.inner;
    await this.inner.createIndex(nativeIndex, column, options?.replace);
  }

  /**
   * Create a generic {@link Query} Builder.
   *
   * When appropriate, various indices and statistics based pruning will be used to
   * accelerate the query.
   *
   * @example
   *
   * ### Run a SQL-style query
   * ```typescript
   * for await (const batch of table.query()
   *                          .filter("id > 1").select(["id"]).limit(20)) {
   *  console.log(batch);
   * }
   * ```
   *
   * ### Run Top-10 vector similarity search
   * ```typescript
   * for await (const batch of table.query()
   *                    .nearestTo([1, 2, 3])
   *                    .refineFactor(5).nprobe(10)
   *                    .limit(10)) {
   *  console.log(batch);
   * }
   *```
   *
   * ### Scan the full dataset
   * ```typescript
   * for await (const batch of table.query()) {
   *   console.log(batch);
   * }
   *
   * ### Return the full dataset as Arrow Table
   * ```typescript
   * let arrowTbl = await table.query().nearestTo([1.0, 2.0, 0.5, 6.7]).toArrow();
   * ```
   *
   * @returns {@link Query}
   */
  query(): Query {
    return new Query(this.inner);
  }

  /** Search the table with a given query vector.
   *
   * This is a convenience method for preparing an ANN {@link Query}.
   */
  search(vector: number[], column?: string): Query {
    const q = this.query();
    q.nearestTo(vector);
    if (column !== undefined) {
      q.column(column);
    }
    return q;
  }

  // TODO: Support BatchUDF
  /**
   * Add new columns with defined values.
   *
   * @param newColumnTransforms pairs of column names and the SQL expression to use
   *                            to calculate the value of the new column. These
   *                            expressions will be evaluated for each row in the
   *                            table, and can reference existing columns in the table.
   */
  async addColumns(newColumnTransforms: AddColumnsSql[]): Promise<void> {
    await this.inner.addColumns(newColumnTransforms);
  }

  /**
   * Alter the name or nullability of columns.
   *
   * @param columnAlterations One or more alterations to apply to columns.
   */
  async alterColumns(columnAlterations: ColumnAlteration[]): Promise<void> {
    await this.inner.alterColumns(columnAlterations);
  }

  /**
   * Drop one or more columns from the dataset
   *
   * This is a metadata-only operation and does not remove the data from the
   * underlying storage. In order to remove the data, you must subsequently
   * call ``compact_files`` to rewrite the data without the removed columns and
   * then call ``cleanup_files`` to remove the old files.
   *
   * @param columnNames The names of the columns to drop. These can be nested
   *                    column references (e.g. "a.b.c") or top-level column
   *                    names (e.g. "a").
   */
  async dropColumns(columnNames: string[]): Promise<void> {
    await this.inner.dropColumns(columnNames);
  }

  /** Retrieve the version of the table
   *
   * LanceDb supports versioning.  Every operation that modifies the table increases
   * version.  As long as a version hasn't been deleted you can `[Self::checkout]` that
   * version to view the data at that point.  In addition, you can `[Self::restore]` the
   * version to replace the current table with a previous version.
   */
  async version(): Promise<number> {
    return await this.inner.version();
  }

  /** Checks out a specific version of the Table
   *
   * Any read operation on the table will now access the data at the checked out version.
   * As a consequence, calling this method will disable any read consistency interval
   * that was previously set.
   *
   * This is a read-only operation that turns the table into a sort of "view"
   * or "detached head".  Other table instances will not be affected.  To make the change
   * permanent you can use the `[Self::restore]` method.
   *
   * Any operation that modifies the table will fail while the table is in a checked
   * out state.
   *
   * To return the table to a normal state use `[Self::checkout_latest]`
   */
  async checkout(version: number): Promise<void> {
    await this.inner.checkout(version);
  }

  /** Ensures the table is pointing at the latest version
   *
   * This can be used to manually update a table when the read_consistency_interval is None
   * It can also be used to undo a `[Self::checkout]` operation
   */
  async checkoutLatest(): Promise<void> {
    await this.inner.checkoutLatest();
  }

  /** Restore the table to the currently checked out version
   *
   * This operation will fail if checkout has not been called previously
   *
   * This operation will overwrite the latest version of the table with a
   * previous version.  Any changes made since the checked out version will
   * no longer be visible.
   *
   * Once the operation concludes the table will no longer be in a checked
   * out state and the read_consistency_interval, if any, will apply.
   */
  async restore(): Promise<void> {
    await this.inner.restore();
  }

  /**
   * List all indices that have been created with Self::create_index
   */
  async listIndices(): Promise<IndexConfig[]> {
    return await this.inner.listIndices();
  }
}
