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
import { fromBufferToRecordBatch, fromRecordBatchToBuffer } from "../arrow";
import { RrfReranker as NativeRRFReranker } from "../native";

/**
 * Reranks the results using the Reciprocal Rank Fusion (RRF) algorithm.
 *
 * Internally this uses the Rust implementation
 */
export class RRFReranker {
  private inner: NativeRRFReranker;

  constructor(inner: NativeRRFReranker) {
    this.inner = inner;
  }

  public static async create(k: number = 60) {
    return new RRFReranker(
      await NativeRRFReranker.tryNew(new Float32Array([k])),
    );
  }

  async rerankHybrid(
    query: string,
    vecResults: RecordBatch,
    ftsResults: RecordBatch,
  ): Promise<RecordBatch> {
    const buffer = await this.inner.rerankHybrid(
      query,
      await fromRecordBatchToBuffer(vecResults),
      await fromRecordBatchToBuffer(ftsResults),
    );
    const recordBatch = await fromBufferToRecordBatch(buffer);

    return recordBatch as RecordBatch;
  }
}
