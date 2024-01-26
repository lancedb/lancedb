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

import { RecordBatch, tableFromIPC } from "apache-arrow";
import { Table } from "./table";
import {
  RecordBatchIterator as NativeBatchIterator,
  Query as NativeQuery,
  Table as NativeTable,
} from "./native";

// TODO: re-eanble eslint once we have a real implementation
/* eslint-disable */
class RecordBatchIterator implements AsyncIterator<RecordBatch> {
  private inner: NativeBatchIterator;

  constructor() {
    this.inner = new NativeBatchIterator();
  }

  async next(): Promise<IteratorResult<RecordBatch<any>, any>> {
    let n = await this.inner.next();
    if (n == null) {
      return Promise.resolve({ done: true, value: null });
    }
    let tbl = tableFromIPC(n);
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
  nearest_to(vector: number[]): Query {
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
  refine_factor(refine_factor: number): Query {
    this.inner.refineFactor(refine_factor);
    return this;
  }

  /**
   * Execute the query and return the results as an AsyncIterator.
   */
  execute_stream(): RecordBatchIterator {
    throw new RecordBatchIterator();
  }

  [Symbol.asyncIterator](): AsyncIterator<RecordBatch<any>, any, undefined> {
    return this.execute_stream();
  }
}
