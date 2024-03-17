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

import { RecordBatch, tableFromIPC, Table as ArrowTable } from "apache-arrow";
import {
  RecordBatchIterator as NativeBatchIterator,
  Query as NativeQuery,
  Table as NativeTable,
} from "./native";

class RecordBatchIterator implements AsyncIterator<RecordBatch> {
  private promisedInner?: Promise<NativeBatchIterator>;
  private inner?: NativeBatchIterator;

  constructor(
    inner?: NativeBatchIterator,
    promise?: Promise<NativeBatchIterator>,
  ) {
    // TODO: check promise reliably so we dont need to pass two arguments.
    this.inner = inner;
    this.promisedInner = promise;
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  async next(): Promise<IteratorResult<RecordBatch<any>>> {
    if (this.inner === undefined) {
      this.inner = await this.promisedInner;
    }
    if (this.inner === undefined) {
      throw new Error("Invalid iterator state state");
    }
    const n = await this.inner.next();
    if (n == null) {
      return Promise.resolve({ done: true, value: null });
    }
    const tbl = tableFromIPC(n);
    if (tbl.batches.length != 1) {
      throw new Error("Expected only one batch");
    }
    return Promise.resolve({ done: false, value: tbl.batches[0] });
  }
}
/* eslint-enable */

/** Query executor */
export class Query implements AsyncIterable<RecordBatch> {
  private readonly inner: NativeQuery;

  constructor(tbl: NativeTable) {
    this.inner = tbl.query();
  }

  /** Set the column to run query. */
  column(column: string): Query {
    this.inner.column(column);
    return this;
  }

  /** Set the filter predicate, only returns the results that satisfy the filter.
   *
   */
  filter(predicate: string): Query {
    this.inner.filter(predicate);
    return this;
  }

  /**
   * Select the columns to return. If not set, all columns are returned.
   */
  select(columns: string[]): Query {
    this.inner.select(columns);
    return this;
  }

  /**
   * Set the limit of rows to return.
   */
  limit(limit: number): Query {
    this.inner.limit(limit);
    return this;
  }

  prefilter(prefilter: boolean): Query {
    this.inner.prefilter(prefilter);
    return this;
  }

  /**
   * Set the query vector.
   */
  nearestTo(vector: number[]): Query {
    this.inner.nearestTo(Float32Array.from(vector));
    return this;
  }

  /**
   * Set the number of IVF partitions to use for the query.
   */
  nprobes(nprobes: number): Query {
    this.inner.nprobes(nprobes);
    return this;
  }

  /**
   * Set the refine factor for the query.
   */
  refineFactor(refineFactor: number): Query {
    this.inner.refineFactor(refineFactor);
    return this;
  }

  /**
   * Execute the query and return the results as an AsyncIterator.
   */
  async executeStream(): Promise<RecordBatchIterator> {
    const inner = await this.inner.executeStream();
    return new RecordBatchIterator(inner);
  }

  /** Collect the results as an Arrow Table. */
  async toArrow(): Promise<ArrowTable> {
    const batches = [];
    for await (const batch of this) {
      batches.push(batch);
    }
    return new ArrowTable(batches);
  }

  /** Returns a JSON Array of All results.
   *
   */
  async toArray(): Promise<unknown[]> {
    const tbl = await this.toArrow();
    // eslint-disable-next-line @typescript-eslint/no-unsafe-return
    return tbl.toArray();
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  [Symbol.asyncIterator](): AsyncIterator<RecordBatch<any>> {
    const promise = this.inner.executeStream();
    return new RecordBatchIterator(undefined, promise);
  }
}
