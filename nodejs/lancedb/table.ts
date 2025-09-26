// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import {
  Table as ArrowTable,
  Data,
  DataType,
  IntoVector,
  MultiVector,
  Schema,
  dataTypeToJson,
  fromDataToBuffer,
  isMultiVector,
  tableFromIPC,
} from "./arrow";

import { EmbeddingFunctionConfig, getRegistry } from "./embedding/registry";
import { IndexOptions } from "./indices";
import { MergeInsertBuilder } from "./merge";
import {
  AddColumnsResult,
  AddColumnsSql,
  AddResult,
  AlterColumnsResult,
  DeleteResult,
  DropColumnsResult,
  IndexConfig,
  IndexStatistics,
  OptimizeStats,
  TableStatistics,
  Tags,
  UpdateMapEntry,
  UpdateResult,
  Table as _NativeTable,
} from "./native";
import {
  FullTextQuery,
  Query,
  TakeQuery,
  VectorQuery,
  instanceOfFullTextQuery,
} from "./query";
import { sanitizeType } from "./sanitize";
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
   * tbl.optimize({cleanupOlderThan: olderThan});
   *
   * // Delete all versions except the current version
   * tbl.optimize({cleanupOlderThan: new Date()});
   */
  cleanupOlderThan: Date;
  deleteUnverified: boolean;
}

export interface Version {
  version: number;
  timestamp: Date;
  metadata: Record<string, string>;
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
 * Tables are created using the methods {@link Connection#createTable}
 * and {@link Connection#createEmptyTable}. Existing tables are opened
 * using {@link Connection#openTable}.
 *
 * Closing a table is optional.  It not closed, it will be closed when it is garbage
 * collected.
 *
 * @hideconstructor
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
   * @returns {Promise<AddResult>} A promise that resolves to an object
   * containing the new version number of the table
   */
  abstract add(
    data: Data,
    options?: Partial<AddDataOptions>,
  ): Promise<AddResult>;
  /**
   * Update existing records in the Table
   * @param opts.values The values to update. The keys are the column names and the values
   * are the values to set.
   * @returns {Promise<UpdateResult>} A promise that resolves to an object containing
   * the number of rows updated and the new version number
   * @example
   * ```ts
   * table.update({where:"x = 2", values:{"vector": [10, 10]}})
   * ```
   */
  abstract update(
    opts: {
      values: Map<string, IntoSql> | Record<string, IntoSql>;
    } & Partial<UpdateOptions>,
  ): Promise<UpdateResult>;
  /**
   * Update existing records in the Table
   * @param opts.valuesSql The values to update. The keys are the column names and the values
   * are the values to set. The values are SQL expressions.
   * @returns {Promise<UpdateResult>} A promise that resolves to an object containing
   * the number of rows updated and the new version number
   * @example
   * ```ts
   * table.update({where:"x = 2", valuesSql:{"x": "x + 1"}})
   * ```
   */
  abstract update(
    opts: {
      valuesSql: Map<string, string> | Record<string, string>;
    } & Partial<UpdateOptions>,
  ): Promise<UpdateResult>;
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
   * @returns {Promise<UpdateResult>} A promise that resolves to an object
   * containing the number of rows updated and the new version number
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
  ): Promise<UpdateResult>;

  /**
   * Update table metadata.
   *
   * @param updates - The metadata updates to apply. Keys are metadata keys,
   *                  values are the new values. Use `null` to remove a key.
   * @param replace - If true, replace the entire metadata map. If false, merge
   *                  updates with existing metadata. Defaults to false.
   * @returns A promise that resolves to the updated metadata map.
   * @example
   * ```ts
   * // Add metadata
   * await table.updateMetadata({"description": "My test table", "version": "1.0"});
   *
   * // Update specific keys
   * await table.updateMetadata({"version": "1.1", "author": "me"});
   *
   * // Remove a key
   * await table.updateMetadata({"author": null});
   * ```
   */
  abstract updateMetadata(
    updates: Map<string, string | null> | Record<string, string | null>,
    replace?: boolean,
  ): Promise<Record<string, string>>;

  /**
   * Update schema metadata.
   *
   * @param updates - The schema metadata updates to apply. Keys are metadata keys,
   *                  values are the new values. Use `null` to remove a key.
   * @param replace - If true, replace the entire schema metadata map. If false,
   *                  merge updates with existing metadata. Defaults to false.
   * @returns A promise that resolves to the updated schema metadata map.
   * @example
   * ```ts
   * // Add schema metadata
   * await table.updateSchemaMetadata({"format_version": "2.0"});
   * ```
   */
  abstract updateSchemaMetadata(
    updates: Map<string, string | null> | Record<string, string | null>,
    replace?: boolean,
  ): Promise<Record<string, string>>;

  /** Count the total number of rows in the dataset. */
  abstract countRows(filter?: string): Promise<number>;
  /**
   * Delete the rows that satisfy the predicate.
   * @returns {Promise<DeleteResult>} A promise that resolves to an object
   * containing the new version number of the table
   */
  abstract delete(predicate: string): Promise<DeleteResult>;
  /**
   * Create an index to speed up queries.
   *
   * Indices can be created on vector columns or scalar columns.
   * Indices on vector columns will speed up vector searches.
   * Indices on scalar columns will speed up filtering (in both
   * vector and non-vector searches)
   *
   * We currently don't support custom named indexes.
   * The index name will always be `${column}_idx`.
   *
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
   * Drop an index from the table.
   *
   * @param name The name of the index.
   *
   * This does not delete the index from disk, it just removes it from the table.
   * To delete the index, run {@link Table#optimize} after dropping the index.
   *
   * Use {@link Table.listIndices} to find the names of the indices.
   */
  abstract dropIndex(name: string): Promise<void>;

  /**
   * Prewarm an index in the table.
   *
   * @param name The name of the index.
   *
   * This will load the index into memory.  This may reduce the cold-start time for
   * future queries.  If the index does not fit in the cache then this call may be
   * wasteful.
   */
  abstract prewarmIndex(name: string): Promise<void>;

  /**
   * Waits for asynchronous indexing to complete on the table.
   *
   * @param indexNames The name of the indices to wait for
   * @param timeoutSeconds The number of seconds to wait before timing out
   *
   * This will raise an error if the indices are not created and fully indexed within the timeout.
   */
  abstract waitForIndex(
    indexNames: string[],
    timeoutSeconds: number,
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
   * Create a query that returns a subset of the rows in the table.
   * @param offsets The offsets of the rows to return.
   * @returns A builder that can be used to parameterize the query.
   */
  abstract takeOffsets(offsets: number[]): TakeQuery;

  /**
   * Create a query that returns a subset of the rows in the table.
   * @param rowIds The row ids of the rows to return.
   * @returns A builder that can be used to parameterize the query.
   */
  abstract takeRowIds(rowIds: number[]): TakeQuery;

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
    query: string | IntoVector | MultiVector | FullTextQuery,
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
  abstract vectorSearch(vector: IntoVector | MultiVector): VectorQuery;
  /**
   * Add new columns with defined values.
   * @param {AddColumnsSql[]} newColumnTransforms pairs of column names and
   * the SQL expression to use to calculate the value of the new column. These
   * expressions will be evaluated for each row in the table, and can
   * reference existing columns in the table.
   * @returns {Promise<AddColumnsResult>} A promise that resolves to an object
   * containing the new version number of the table after adding the columns.
   */
  abstract addColumns(
    newColumnTransforms: AddColumnsSql[],
  ): Promise<AddColumnsResult>;

  /**
   * Alter the name or nullability of columns.
   * @param {ColumnAlteration[]} columnAlterations One or more alterations to
   * apply to columns.
   * @returns {Promise<AlterColumnsResult>} A promise that resolves to an object
   * containing the new version number of the table after altering the columns.
   */
  abstract alterColumns(
    columnAlterations: ColumnAlteration[],
  ): Promise<AlterColumnsResult>;
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
   * @returns {Promise<DropColumnsResult>} A promise that resolves to an object
   * containing the new version number of the table after dropping the columns.
   */
  abstract dropColumns(columnNames: string[]): Promise<DropColumnsResult>;
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
   * @param {number | string} version The version to checkout, could be version number or tag
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
  abstract checkout(version: number | string): Promise<void>;

  /**
   * Checkout the latest version of the table. _This is an in-place operation._
   *
   * The table will be set back into standard mode, and will track the latest
   * version of the table.
   */
  abstract checkoutLatest(): Promise<void>;

  /**
   * List all the versions of the table
   */
  abstract listVersions(): Promise<Version[]>;

  /**
   * Get a tags manager for this table.
   *
   * Tags allow you to label specific versions of a table with a human-readable name.
   * The returned tags manager can be used to list, create, update, or delete tags.
   *
   * @returns {Tags} A tags manager for this table
   * @example
   * ```typescript
   * const tagsManager = await table.tags();
   * await tagsManager.create("v1", 1);
   * const tags = await tagsManager.list();
   * console.log(tags); // { "v1": { version: 1, manifestSize: ... } }
   * ```
   */
  abstract tags(): Promise<Tags>;

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
   *
   * Use {@link Table.listIndices} to find the names of the indices.
   */
  abstract indexStats(name: string): Promise<IndexStatistics | undefined>;

  /** Returns table and fragment statistics
   *
   * @returns {TableStatistics} The table and fragment statistics
   *
   */
  abstract stats(): Promise<TableStatistics>;
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

  async add(data: Data, options?: Partial<AddDataOptions>): Promise<AddResult> {
    const mode = options?.mode ?? "append";
    const schema = await this.schema();

    const buffer = await fromDataToBuffer(data, undefined, schema);
    return await this.inner.add(buffer, mode);
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
  ): Promise<UpdateResult> {
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
    return await this.inner.update(predicate, columns);
  }

  async updateMetadata(
    updates: Map<string, string | null> | Record<string, string | null>,
    replace: boolean = false,
  ): Promise<Record<string, string>> {
    const entries: UpdateMapEntry[] = [];

    if (updates instanceof Map) {
      for (const [key, value] of updates.entries()) {
        entries.push({ key, value: value ?? undefined });
      }
    } else {
      for (const [key, value] of Object.entries(updates)) {
        entries.push({ key, value: value ?? undefined });
      }
    }

    return await this.inner.updateMetadata(entries, replace);
  }

  async updateSchemaMetadata(
    updates: Map<string, string | null> | Record<string, string | null>,
    replace: boolean = false,
  ): Promise<Record<string, string>> {
    const entries: UpdateMapEntry[] = [];

    if (updates instanceof Map) {
      for (const [key, value] of updates.entries()) {
        entries.push({ key, value: value ?? undefined });
      }
    } else {
      for (const [key, value] of Object.entries(updates)) {
        entries.push({ key, value: value ?? undefined });
      }
    }

    return await this.inner.updateSchemaMetadata(entries, replace);
  }

  async countRows(filter?: string): Promise<number> {
    return await this.inner.countRows(filter);
  }

  async delete(predicate: string): Promise<DeleteResult> {
    return await this.inner.delete(predicate);
  }

  async createIndex(column: string, options?: Partial<IndexOptions>) {
    // Bit of a hack to get around the fact that TS has no package-scope.
    // biome-ignore lint/suspicious/noExplicitAny: skip
    const nativeIndex = (options?.config as any)?.inner;
    await this.inner.createIndex(
      nativeIndex,
      column,
      options?.replace,
      options?.waitTimeoutSeconds,
      options?.name,
      options?.train,
    );
  }

  async dropIndex(name: string): Promise<void> {
    await this.inner.dropIndex(name);
  }

  async prewarmIndex(name: string): Promise<void> {
    await this.inner.prewarmIndex(name);
  }

  async waitForIndex(
    indexNames: string[],
    timeoutSeconds: number,
  ): Promise<void> {
    await this.inner.waitForIndex(indexNames, timeoutSeconds);
  }

  takeOffsets(offsets: number[]): TakeQuery {
    return new TakeQuery(this.inner.takeOffsets(offsets));
  }

  takeRowIds(rowIds: number[]): TakeQuery {
    return new TakeQuery(this.inner.takeRowIds(rowIds));
  }

  query(): Query {
    return new Query(this.inner);
  }

  search(
    query: string | IntoVector | MultiVector | FullTextQuery,
    queryType: string = "auto",
    ftsColumns?: string | string[],
  ): VectorQuery | Query {
    if (typeof query !== "string" && !instanceOfFullTextQuery(query)) {
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
    if (
      queryType === "auto" &&
      (getRegistry().length() === 0 || instanceOfFullTextQuery(query))
    ) {
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

  vectorSearch(vector: IntoVector | MultiVector): VectorQuery {
    if (isMultiVector(vector)) {
      const query = this.query().nearestTo(vector[0]);
      for (const v of vector.slice(1)) {
        query.addQueryVector(v);
      }
      return query;
    }

    return this.query().nearestTo(vector);
  }

  // TODO: Support BatchUDF

  async addColumns(
    newColumnTransforms: AddColumnsSql[],
  ): Promise<AddColumnsResult> {
    return await this.inner.addColumns(newColumnTransforms);
  }

  async alterColumns(
    columnAlterations: ColumnAlteration[],
  ): Promise<AlterColumnsResult> {
    const processedAlterations = columnAlterations.map((alteration) => {
      if (typeof alteration.dataType === "string") {
        return {
          ...alteration,
          dataType: JSON.stringify({ type: alteration.dataType }),
        };
      } else if (alteration.dataType === undefined) {
        return {
          ...alteration,
          dataType: undefined,
        };
      } else {
        const dataType = sanitizeType(alteration.dataType);
        return {
          ...alteration,
          dataType: JSON.stringify(dataTypeToJson(dataType)),
        };
      }
    });

    return await this.inner.alterColumns(processedAlterations);
  }

  async dropColumns(columnNames: string[]): Promise<DropColumnsResult> {
    return await this.inner.dropColumns(columnNames);
  }

  async version(): Promise<number> {
    return await this.inner.version();
  }

  async checkout(version: number | string): Promise<void> {
    if (typeof version === "string") {
      return this.inner.checkoutTag(version);
    }
    return this.inner.checkout(version);
  }

  async checkoutLatest(): Promise<void> {
    await this.inner.checkoutLatest();
  }

  async listVersions(): Promise<Version[]> {
    return (await this.inner.listVersions()).map((version) => ({
      version: version.version,
      timestamp: new Date(version.timestamp / 1000),
      metadata: version.metadata,
    }));
  }

  async restore(): Promise<void> {
    await this.inner.restore();
  }

  async tags(): Promise<Tags> {
    return await this.inner.tags();
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

  async stats(): Promise<TableStatistics> {
    return await this.inner.stats();
  }

  mergeInsert(on: string | string[]): MergeInsertBuilder {
    on = Array.isArray(on) ? on : [on];
    return new MergeInsertBuilder(this.inner.mergeInsert(on), this.schema());
  }

  /**
   * Check if the table uses the new manifest path scheme.
   *
   * This function will return true if the table uses the V2 manifest
   * path scheme.
   */
  async usesV2ManifestPaths(): Promise<boolean> {
    return await this.inner.usesV2ManifestPaths();
  }

  /**
   * Migrate the table to use the new manifest path scheme.
   *
   * This function will rename all V1 manifests to V2 manifest paths.
   * These paths provide more efficient opening of datasets with many versions
   * on object stores.
   *
   * This function is idempotent, and can be run multiple times without
   * changing the state of the object store.
   *
   * However, it should not be run while other concurrent operations are happening.
   * And it should also run until completion before resuming other operations.
   */
  async migrateManifestPathsV2(): Promise<void> {
    await this.inner.migrateManifestPathsV2();
  }
}

/**
 *  A definition of a column alteration. The alteration changes the column at
 * `path` to have the new name `name`, to be nullable if `nullable` is true,
 * and to have the data type `data_type`. At least one of `rename` or `nullable`
 * must be provided.
 */
export interface ColumnAlteration {
  /**
   * The path to the column to alter. This is a dot-separated path to the column.
   * If it is a top-level column then it is just the name of the column. If it is
   * a nested column then it is the path to the column, e.g. "a.b.c" for a column
   * `c` nested inside a column `b` nested inside a column `a`.
   */
  path: string;
  /**
   * The new name of the column. If not provided then the name will not be changed.
   * This must be distinct from the names of all other columns in the table.
   */
  rename?: string;
  /**
   * A new data type for the column. If not provided then the data type will not be changed.
   * Changing data types is limited to casting to the same general type. For example, these
   * changes are valid:
   * * `int32` -> `int64` (integers)
   * * `double` -> `float` (floats)
   * * `string` -> `large_string` (strings)
   * But these changes are not:
   * * `int32` -> `double` (mix integers and floats)
   * * `string` -> `int32` (mix strings and integers)
   */
  dataType?: string | DataType;
  /** Set the new nullability. Note that a nullable column cannot be made non-nullable. */
  nullable?: boolean;
}
