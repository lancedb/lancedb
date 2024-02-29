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
import { AddColumnsSql, ColumnAlteration, Table as _NativeTable } from "./native";
import { toBuffer, Data } from "./arrow";
import { Query } from "./query";
import { IndexBuilder } from "./indexer";

/**
 * A LanceDB Table is the collection of Records.
 *
 * Each Record has one or more vector fields.
 */
export class Table {
  private readonly inner: _NativeTable;

  /** Construct a Table. Internal use only. */
  constructor(inner: _NativeTable) {
    this.inner = inner;
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
  async add(data: Data): Promise<void> {
    const buffer = toBuffer(data);
    await this.inner.add(buffer);
  }

  /** Count the total number of rows in the dataset. */
  async countRows(filter?: string): Promise<number> {
    return await this.inner.countRows(filter);
  }

  /** Delete the rows that satisfy the predicate. */
  async delete(predicate: string): Promise<void> {
    await this.inner.delete(predicate);
  }

  /** Create an index over the columns.
   *
   * @param {string} column The column to create the index on. If not specified,
   *                        it will create an index on vector field.
   *
   * @example
   *
   * By default, it creates vector idnex on one vector column.
   *
   * ```typescript
   * const table = await conn.openTable("my_table");
   * await table.createIndex().build();
   * ```
   *
   * You can specify `IVF_PQ` parameters via `ivf_pq({})` call.
   * ```typescript
   * const table = await conn.openTable("my_table");
   * await table.createIndex("my_vec_col")
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
  createIndex(column?: string): IndexBuilder {
    let builder = new IndexBuilder(this.inner);
    if (column !== undefined) {
      builder = builder.column(column);
    }
    return builder;
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
}
