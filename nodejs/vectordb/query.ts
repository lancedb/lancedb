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

import { RecordBatch } from "apache-arrow";
import { Table } from "./table";

// TODO: re-eanble eslint once we have a real implementation
/* eslint-disable */
class RecordBatchIterator implements AsyncIterator<RecordBatch> {
  next(
    ...args: [] | [undefined]
  ): Promise<IteratorResult<RecordBatch<any>, any>> {
    throw new Error("Method not implemented.");
  }
  return?(value?: any): Promise<IteratorResult<RecordBatch<any>, any>> {
    throw new Error("Method not implemented.");
  }
  throw?(e?: any): Promise<IteratorResult<RecordBatch<any>, any>> {
    throw new Error("Method not implemented.");
  }
}
/* eslint-enable */

/** Query executor */
export class Query implements AsyncIterable<RecordBatch> {
  private readonly tbl: Table;
  private _filter?: string;
  private _limit?: number;

  // Vector search
  private _vector?: Float32Array;
  private _nprobes?: number;
  private _refine_factor?: number = 1;

  constructor(tbl: Table) {
    this.tbl = tbl;
  }

  /** Set the filter predicate, only returns the results that satisfy the filter.
   *
   */
  filter(predicate: string): Query {
    this._filter = predicate;
    return this;
  }

  /**
   * Set the limit of rows to return.
   */
  limit(limit: number): Query {
    this._limit = limit;
    return this;
  }

  /**
   * Set the query vector.
   */
  nearest_to(vector: number[]): Query {
    this._vector = Float32Array.from(vector);
    return this;
  }

  /**
   * Set the number of probes to use for the query.
   */
  nprobes(nprobes: number): Query {
    this._nprobes = nprobes;
    return this;
  }

  /**
   * Set the refine factor for the query.
   */
  refine_factor(refine_factor: number): Query {
    this._refine_factor = refine_factor;
    return this;
  }

  [Symbol.asyncIterator](): AsyncIterator<RecordBatch<any>, any, undefined> {
    throw new RecordBatchIterator();
  }
}
