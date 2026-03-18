// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import { RecordBatch } from "apache-arrow";
import { fromBufferToRecordBatch, fromRecordBatchToBuffer } from "../arrow";
import { RrfReranker as NativeRRFReranker } from "../native";

/**
 * Reranks the results using the Reciprocal Rank Fusion (RRF) algorithm.
 *
 * @param k - Constant used in the RRF formula (default `60`). Experiments
 *   indicate that `k = 60` was near-optimal, but the choice is not critical.
 *   See paper: https://plg.uwaterloo.ca/~gvcormac/cormacksigir09-rrf.pdf
 * @param returnScore - Controls which score columns appear in the output.
 *   - `"relevance"` (default): Only the `_relevance_score` column is kept;
 *     the raw `_distance` and `_score` columns are dropped.
 *   - `"all"`: All score columns are retained alongside `_relevance_score`,
 *     which is useful for debugging.
 *
 * @hideconstructor
 */
export class RRFReranker {
  private inner: NativeRRFReranker;

  /** @ignore */
  constructor(inner: NativeRRFReranker) {
    this.inner = inner;
  }

  public static async create(
    k: number = 60,
    returnScore: "relevance" | "all" = "relevance",
  ) {
    return new RRFReranker(
      await NativeRRFReranker.tryNew(new Float32Array([k]), returnScore),
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
