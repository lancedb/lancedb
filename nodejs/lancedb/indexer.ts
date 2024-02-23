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
  MetricType,
  IndexBuilder as NativeBuilder,
  Table as NativeTable,
} from "./native";

/** Options to create `IVF_PQ` index */
export interface IvfPQOptions {
  /** Number of IVF partitions. */
  num_partitions?: number;

  /** Number of sub-vectors in PQ coding. */
  num_sub_vectors?: number;

  /** Number of bits used for each PQ code.
   */
  num_bits?: number;

  /** Metric type to calculate the distance between vectors.
   *
   * Supported metrics: `L2`, `Cosine` and `Dot`.
   */
  metric_type?: MetricType;

  /** Number of iterations to train K-means.
   *
   * Default is 50. The more iterations it usually yield better results,
   * but it takes longer to train.
   */
  max_iterations?: number;

  sample_rate?: number;
}

/**
 * Building an index on LanceDB {@link Table}
 *
 * @see {@link Table.createIndex} for detailed usage.
 */
export class IndexBuilder {
  private inner: NativeBuilder;

  constructor(tbl: NativeTable) {
    this.inner = tbl.createIndex();
  }

  /** Instruct the builder to build an `IVF_PQ` index */
  ivf_pq(options?: IvfPQOptions): IndexBuilder {
    this.inner.ivfPq(
      options?.metric_type,
      options?.num_partitions,
      options?.num_sub_vectors,
      options?.num_bits,
      options?.max_iterations,
      options?.sample_rate
    );
    return this;
  }

  /** Instruct the builder to build a Scalar index. */
  scalar(): IndexBuilder {
    this.scalar();
    return this;
  }

  /** Set the column(s) to create index on top of. */
  column(col: string): IndexBuilder {
    this.inner.column(col);
    return this;
  }

  /** Set to true to replace existing index. */
  replace(val: boolean): IndexBuilder {
    this.inner.replace(val);
    return this;
  }

  /** Specify the name of the index. Optional */
  name(n: string): IndexBuilder {
    this.inner.name(n);
    return this;
  }

  /** Building the index. */
  async build() {
    await this.inner.build();
  }
}
