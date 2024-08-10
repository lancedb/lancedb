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

import {
  Table as ArrowTable,
  Data,
  IntoVector,
  Schema,
  TableLike,
  fromDataToBuffer,
  fromTableToBuffer,
  fromTableToStreamBuffer,
  isArrowTable,
  makeArrowTable,
  tableFromIPC,
} from "./arrow";
import { CreateTableOptions } from "./connection";

import { EmbeddingFunctionConfig, getRegistry } from "./embedding/registry";
import { IndexOptions } from "./indices";
import { MergeInsertBuilder } from "./merge";
import {
  AddColumnsSql,
  ColumnAlteration,
  IndexConfig,
  IndexStatistics,
  OptimizeStats,
  Table as _NativeTable,
} from "./native";
import { Query, VectorQuery } from "./query";
import { sanitizeTable } from "./sanitize";
import { IntoSql, toSQL } from "./util";
export { IndexConfig } from "./native";

/**
 * Options for adding data to a table.
 */
export interface AddDataOptions {
  /**
   * If "append" (the default) then the new data will be added to the table
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

export interface OptimizeOptions {
  /**
   * If set then all versions older than the given date
   * be removed.  The current version will never be removed.
   * The default is 7 days
   * @example
   * // Delete all versions older than 1 day
   * const olderThan = new Date();
   * olderThan.setDate(olderThan.getDate() - 1));
   * tbl.cleanupOlderVersions(olderThan);
   *
   * // Delete all versions except the current version
   * tbl.cleanupOlderVersions(new Date());
   */
  cleanupOlderThan: Date;
  deleteUnverified: boolean;
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
export abstract class Table {
  [Symbol.for("nodejs.util.inspect.custom")](): string {
    return this.display();
  }
  /** Returns the name of the table */
  abstract get name(): string;

  /** Return true if the table has not been closed */
  abstract isOpen(): boolean;
  /**
   * Close the table, releasing any underlying resources.
   *
   * It is safe to call this method multiple times.
   *
   * Any attempt to use the table after it is closed will result in an error.
   */
  abstract close(): void;
  /** Return a brief description of the table */
  abstract display(): string;
  /** Get the schema of the table. */
  abstract schema(): Promise<Schema>;
  /**
   * Insert records into this Table.
   * @param {Data} data Records to be inserted into the Table
   */
  abstract add(data: Data, options?: Partial<AddDataOptions>): Promise<void>;
  /**
   * Update existing records in the Table
   * @param opts.values The values to update. The keys are the column names and the values
   * are the values to set.
   * @example
   * ```ts
   * table.update({where:"x = 2", values:{"vector": [10, 10]}})
   * ```
   */
  abstract update(
    opts: {
      values: Map<string, IntoSql> | Record<string, IntoSql>;
    } & Partial<UpdateOptions>,
  ): Promise<void>;
  /**
   * Update existing records in the Table
   * @param opts.valuesSql The values to update. The keys are the column names and the values
   * are the values to set. The values are SQL expressions.
   * @example
   * ```ts
   * table.update({where:"x = 2", valuesSql:{"x": "x + 1"}})
   * ```
   */
  abstract update(
    opts: {
      valuesSql: Map<string, string> | Record<string, string>;
    } & Partial<UpdateOptions>,
  ): Promise<void>;
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
   * @param {Map<string, string> | Record<string, string>} updates - the
   * columns to update
   *
   * Keys in the map should specify the name of the column to update.
   * Values in the map provide the new value of the column.  These can
   * be SQL literal strings (e.g. "7" or "'foo'") or they can be expressions
   * based on the row being updated (e.g. "my_col + 1")
   * @param {Partial<UpdateOptions>} options - additional options to control
   * the update behavior
   */
  abstract update(
    updates: Map<string, string> | Record<string, string>,
    options?: Partial<UpdateOptions>,
  ): Promise<void>;

  /** Count the total number of rows in the dataset. */
  abstract countRows(filter?: string): Promise<number>;
  /** Delete the rows that satisfy the predicate. */
  abstract delete(predicate: string): Promise<void>;
  /**
   * Create an index to speed up queries.
   *
   * Indices can be created on vector columns or scalar columns.
   * Indices on vector columns will speed up vector searches.
   * Indices on scalar columns will speed up filtering (in both
   * vector and non-vector searches)
   *
   * @note We currently don't support custom named indexes,
   * The index name will always be `${column}_idx`
   * @example
   * // If the column has a vector (fixed size list) data type then
   * // an IvfPq vector index will be created.
   * const table = await conn.openTable("my_table");
   * await table.createIndex("vector");
   * @example
   * // For advanced control over vector index creation you can specify
   * // the index type and options.
   * const table = await conn.openTable("my_table");
   * await table.createIndex("vector", {
   *   config: lancedb.Index.ivfPq({
   *     numPartitions: 128,
   *     numSubVectors: 16,
   *   }),
   * });
   * @example
   * // Or create a Scalar index
   * await table.createIndex("my_float_col");
   */
  abstract createIndex(
    column: string,
    options?: Partial<IndexOptions>,
  ): Promise<void>;
  /**
   * Create a {@link Query} Builder.
   *
   * Queries allow you to search your existing data.  By default the query will
   * return all the data in the table in no particular order.  The builder
   * returned by this method can be used to control the query using filtering,
   * vector similarity, sorting, and more.
   *
   * Note: By default, all columns are returned.  For best performance, you should
   * only fetch the columns you need.
   *
   * When appropriate, various indices and statistics based pruning will be used to
   * accelerate the query.
   * @example
   * // SQL-style filtering
   * //
   * // This query will return up to 1000 rows whose value in the `id` column
   * // is greater than 5. LanceDb supports a broad set of filtering functions.
   * for await (const batch of table
   *   .query()
   *   .where("id > 1")
   *   .select(["id"])
   *   .limit(20)) {
   *   console.log(batch);
   * }
   * @example
   * // Vector Similarity Search
   * //
   * // This example will find the 10 rows whose value in the "vector" column are
   * // closest to the query vector [1.0, 2.0, 3.0].  If an index has been created
   * // on the "vector" column then this will perform an ANN search.
   * //
   * // The `refineFactor` and `nprobes` methods are used to control the recall /
   * // latency tradeoff of the search.
   * for await (const batch of table
   *   .query()
   *   .where("id > 1")
   *   .select(["id"])
   *   .limit(20)) {
   *   console.log(batch);
   * }
   * @example
   * // Scan the full dataset
   * //
   * // This query will return everything in the table in no particular order.
   * for await (const batch of table.query()) {
   *   console.log(batch);
   * }
   * @returns {Query} A builder that can be used to parameterize the query
   */
  abstract query(): Query;

  /**
   * Create a search query to find the nearest neighbors
   * of the given query
   * @param {string | IntoVector} query - the query, a vector or string
   * @param {string} queryType - the type of the query, "vector", "fts", or "auto"
   * @param {string | string[]} ftsColumns - the columns to search in for full text search
   *    for now, only one column can be searched at a time.
   *
   * when "auto" is used, if the query is a string and an embedding function is defined, it will be treated as a vector query
   * if the query is a string and no embedding function is defined, it will be treated as a full text search query
   */
  abstract search(
    query: string | IntoVector,
    queryType?: string,
    ftsColumns?: string | string[],
  ): VectorQuery | Query;
  /**
   * Search the table with a given query vector.
   *
   * This is a convenience method for preparing a vector query and
   * is the same thing as calling `nearestTo` on the builder returned
   * by `query`.  @see {@link Query#nearestTo} for more details.
   */
  abstract vectorSearch(vector: IntoVector): VectorQuery;
  /**
   * Add new columns with defined values.
   * @param {AddColumnsSql[]} newColumnTransforms pairs of column names and
   * the SQL expression to use to calculate the value of the new column. These
   * expressions will be evaluated for each row in the table, and can
   * reference existing columns in the table.
   */
  abstract addColumns(newColumnTransforms: AddColumnsSql[]): Promise<void>;

  /**
   * Alter the name or nullability of columns.
   * @param {ColumnAlteration[]} columnAlterations One or more alterations to
   * apply to columns.
   */
  abstract alterColumns(columnAlterations: ColumnAlteration[]): Promise<void>;
  /**
   * Drop one or more columns from the dataset
   *
   * This is a metadata-only operation and does not remove the data from the
   * underlying storage. In order to remove the data, you must subsequently
   * call ``compact_files`` to rewrite the data without the removed columns and
   * then call ``cleanup_files`` to remove the old files.
   * @param {string[]} columnNames The names of the columns to drop. These can
   * be nested column references (e.g. "a.b.c") or top-level column names
   * (e.g. "a").
   */
  abstract dropColumns(columnNames: string[]): Promise<void>;
  /** Retrieve the version of the table */

  abstract version(): Promise<number>;
  /**
   * Checks out a specific version of the table _This is an in-place operation._
   *
   * This allows viewing previous versions of the table. If you wish to
   * keep writing to the dataset starting from an old version, then use
   * the `restore` function.
   *
   * Calling this method will set the table into time-travel mode. If you
   * wish to return to standard mode, call `checkoutLatest`.
   * @param {number} version The version to checkout
   * @example
   * ```typescript
   * import * as lancedb from "@lancedb/lancedb"
   * const db = await lancedb.connect("./.lancedb");
   * const table = await db.createTable("my_table", [
   *   { vector: [1.1, 0.9], type: "vector" },
   * ]);
   *
   * console.log(await table.version()); // 1
   * console.log(table.display());
   * await table.add([{ vector: [0.5, 0.2], type: "vector" }]);
   * await table.checkout(1);
   * console.log(await table.version()); // 2
   * ```
   */
  abstract checkout(version: number): Promise<void>;
  /**
   * Checkout the latest version of the table. _This is an in-place operation._
   *
   * The table will be set back into standard mode, and will track the latest
   * version of the table.
   */
  abstract checkoutLatest(): Promise<void>;

  /**
   * Restore the table to the currently checked out version
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
  abstract restore(): Promise<void>;
  /**
   * Optimize the on-disk data and indices for better performance.
   *
   * Modeled after ``VACUUM`` in PostgreSQL.
   *
   *  Optimization covers three operations:
   *
   *  - Compaction: Merges small files into larger ones
   *  - Prune: Removes old versions of the dataset
   *  - Index: Optimizes the indices, adding new data to existing indices
   *
   *
   *  Experimental API
   *  ----------------
   *
   *  The optimization process is undergoing active development and may change.
   *  Our goal with these changes is to improve the performance of optimization and
   *  reduce the complexity.
   *
   *  That being said, it is essential today to run optimize if you want the best
   *  performance.  It should be stable and safe to use in production, but it our
   *  hope that the API may be simplified (or not even need to be called) in the
   *  future.
   *
   *  The frequency an application shoudl call optimize is based on the frequency of
   *  data modifications.  If data is frequently added, deleted, or updated then
   *  optimize should be run frequently.  A good rule of thumb is to run optimize if
   *  you have added or modified 100,000 or more records or run more than 20 data
   *  modification operations.
   */
  abstract optimize(options?: Partial<OptimizeOptions>): Promise<OptimizeStats>;
  /** List all indices that have been created with {@link Table.createIndex} */
  abstract listIndices(): Promise<IndexConfig[]>;
  /** Return the table as an arrow table */
  abstract toArrow(): Promise<ArrowTable>;

  abstract mergeInsert(on: string | string[]): MergeInsertBuilder;

  /** List all the stats of a specified index
   *
   * @param {string} name The name of the index.
   * @returns {IndexStatistics | undefined} The stats of the index. If the index does not exist, it will return undefined
   */
  abstract indexStats(name: string): Promise<IndexStatistics | undefined>;

  static async parseTableData(
    data: Record<string, unknown>[] | TableLike,
    options?: Partial<CreateTableOptions>,
    streaming = false,
  ) {
    let mode: string = options?.mode ?? "create";
    const existOk = options?.existOk ?? false;

    if (mode === "create" && existOk) {
      mode = "exist_ok";
    }

    let table: ArrowTable;
    if (isArrowTable(data)) {
      table = sanitizeTable(data);
    } else {
      table = makeArrowTable(data as Record<string, unknown>[], options);
    }
    if (streaming) {
      const buf = await fromTableToStreamBuffer(
        table,
        options?.embeddingFunction,
        options?.schema,
      );
      return { buf, mode };
    } else {
      const buf = await fromTableToBuffer(
        table,
        options?.embeddingFunction,
        options?.schema,
      );
      return { buf, mode };
    }
  }
}

export class LocalTable extends Table {
  private readonly inner: _NativeTable;

  constructor(inner: _NativeTable) {
    super();
    this.inner = inner;
  }
  get name(): string {
    return this.inner.name;
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

  private async getEmbeddingFunctions(): Promise<
    Map<string, EmbeddingFunctionConfig>
  > {
    const schema = await this.schema();
    const registry = getRegistry();
    return registry.parseFunctions(schema.metadata);
  }

  /** Get the schema of the table. */
  async schema(): Promise<Schema> {
    const schemaBuf = await this.inner.schema();
    const tbl = tableFromIPC(schemaBuf);
    return tbl.schema;
  }

  async add(data: Data, options?: Partial<AddDataOptions>): Promise<void> {
    const mode = options?.mode ?? "append";
    const schema = await this.schema();
    const registry = getRegistry();
    const functions = await registry.parseFunctions(schema.metadata);

    const buffer = await fromDataToBuffer(
      data,
      functions.values().next().value,
      schema,
    );
    await this.inner.add(buffer, mode);
  }

  async update(
    optsOrUpdates:
      | (Map<string, string> | Record<string, string>)
      | ({
          values: Map<string, IntoSql> | Record<string, IntoSql>;
        } & Partial<UpdateOptions>)
      | ({
          valuesSql: Map<string, string> | Record<string, string>;
        } & Partial<UpdateOptions>),
    options?: Partial<UpdateOptions>,
  ) {
    const isValues =
      "values" in optsOrUpdates && typeof optsOrUpdates.values !== "string";
    const isValuesSql =
      "valuesSql" in optsOrUpdates &&
      typeof optsOrUpdates.valuesSql !== "string";
    const isMap = (obj: unknown): obj is Map<string, string> => {
      return obj instanceof Map;
    };

    let predicate;
    let columns: [string, string][];
    switch (true) {
      case isMap(optsOrUpdates):
        columns = Array.from(optsOrUpdates.entries());
        predicate = options?.where;
        break;
      case isValues && isMap(optsOrUpdates.values):
        columns = Array.from(optsOrUpdates.values.entries()).map(([k, v]) => [
          k,
          toSQL(v),
        ]);
        predicate = optsOrUpdates.where;
        break;
      case isValues && !isMap(optsOrUpdates.values):
        columns = Object.entries(optsOrUpdates.values).map(([k, v]) => [
          k,
          toSQL(v),
        ]);
        predicate = optsOrUpdates.where;
        break;

      case isValuesSql && isMap(optsOrUpdates.valuesSql):
        columns = Array.from(optsOrUpdates.valuesSql.entries());
        predicate = optsOrUpdates.where;
        break;
      case isValuesSql && !isMap(optsOrUpdates.valuesSql):
        columns = Object.entries(optsOrUpdates.valuesSql).map(([k, v]) => [
          k,
          v,
        ]);
        predicate = optsOrUpdates.where;
        break;
      default:
        columns = Object.entries(optsOrUpdates as Record<string, string>);
        predicate = options?.where;
    }
    await this.inner.update(predicate, columns);
  }

  async countRows(filter?: string): Promise<number> {
    return await this.inner.countRows(filter);
  }

  async delete(predicate: string): Promise<void> {
    await this.inner.delete(predicate);
  }

  async createIndex(column: string, options?: Partial<IndexOptions>) {
    // Bit of a hack to get around the fact that TS has no package-scope.
    // biome-ignore lint/suspicious/noExplicitAny: skip
    const nativeIndex = (options?.config as any)?.inner;
    await this.inner.createIndex(nativeIndex, column, options?.replace);
  }

  query(): Query {
    return new Query(this.inner);
  }

  search(
    query: string | IntoVector,
    queryType: string = "auto",
    ftsColumns?: string | string[],
  ): VectorQuery | Query {
    if (typeof query !== "string") {
      if (queryType === "fts") {
        throw new Error("Cannot perform full text search on a vector query");
      }
      return this.vectorSearch(query);
    }

    // If the query is a string, we need to determine if it is a vector query or a full text search query
    if (queryType === "fts") {
      return this.query().fullTextSearch(query, {
        columns: ftsColumns,
      });
    }

    // The query type is auto or vector
    // fall back to full text search if no embedding functions are defined and the query is a string
    if (queryType === "auto" && getRegistry().length() === 0) {
      return this.query().fullTextSearch(query, {
        columns: ftsColumns,
      });
    }

    const queryPromise = this.getEmbeddingFunctions().then(
      async (functions) => {
        // TODO: Support multiple embedding functions
        const embeddingFunc: EmbeddingFunctionConfig | undefined = functions
          .values()
          .next().value;
        if (!embeddingFunc) {
          return Promise.reject(
            new Error("No embedding functions are defined in the table"),
          );
        }
        return await embeddingFunc.function.computeQueryEmbeddings(query);
      },
    );

    return this.query().nearestTo(queryPromise);
  }

  vectorSearch(vector: IntoVector): VectorQuery {
    return this.query().nearestTo(vector);
  }

  // TODO: Support BatchUDF

  async addColumns(newColumnTransforms: AddColumnsSql[]): Promise<void> {
    await this.inner.addColumns(newColumnTransforms);
  }

  async alterColumns(columnAlterations: ColumnAlteration[]): Promise<void> {
    await this.inner.alterColumns(columnAlterations);
  }

  async dropColumns(columnNames: string[]): Promise<void> {
    await this.inner.dropColumns(columnNames);
  }

  async version(): Promise<number> {
    return await this.inner.version();
  }

  async checkout(version: number): Promise<void> {
    await this.inner.checkout(version);
  }

  async checkoutLatest(): Promise<void> {
    await this.inner.checkoutLatest();
  }

  async restore(): Promise<void> {
    await this.inner.restore();
  }

  async optimize(options?: Partial<OptimizeOptions>): Promise<OptimizeStats> {
    let cleanupOlderThanMs;
    if (
      options?.cleanupOlderThan !== undefined &&
      options?.cleanupOlderThan !== null
    ) {
      cleanupOlderThanMs =
        new Date().getTime() - options.cleanupOlderThan.getTime();
    }
    return await this.inner.optimize(
      cleanupOlderThanMs,
      options?.deleteUnverified,
    );
  }

  async listIndices(): Promise<IndexConfig[]> {
    return await this.inner.listIndices();
  }

  async toArrow(): Promise<ArrowTable> {
    return await this.query().toArrow();
  }

  async indexStats(name: string): Promise<IndexStatistics | undefined> {
    const stats = await this.inner.indexStats(name);
    if (stats === null) {
      return undefined;
    }
    return stats;
  }
  mergeInsert(on: string | string[]): MergeInsertBuilder {
    on = Array.isArray(on) ? on : [on];
    return new MergeInsertBuilder(this.inner.mergeInsert(on));
  }
}
