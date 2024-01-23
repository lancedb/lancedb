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

import { Table as _NativeTable } from "./native";
import { toBuffer, Data } from "./arrow";
import { Query } from "./query";

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
  async countRows(): Promise<bigint> {
    return await this.inner.countRows();
  }

  /** Delete the rows that satisfy the predicate. */
  async delete(predicate: string): Promise<void> {
    await this.inner.delete(predicate);
  }

  search(vector?: number[]): Query {
    const q = new Query(this);
    if (vector !== undefined) {
      q.vector(vector);
    }
    return q;
  }
}
