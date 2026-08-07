// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import { Field, Float32, RecordBatch, Schema } from "apache-arrow";
import { fromBufferToRecordBatch, fromRecordBatchToBuffer } from "../arrow";
import { RrfReranker as NativeRRFReranker } from "../native";

/**
 * Reranks the results using the Reciprocal Rank Fusion (RRF) algorithm.
 *
 * @hideconstructor
 */
export class RRFReranker {
  private inner: NativeRRFReranker;

  /** @ignore */
  constructor(inner: NativeRRFReranker) {
    this.inner = inner;
  }

  public static async create(k: number = 60) {
    return new RRFReranker(
      await NativeRRFReranker.tryNew(new Float32Array([k])),
    );
  }

  /** Declare the RRF output schema for vector-only query execution. */
  async outputSchema(inputSchema: Schema): Promise<Schema> {
    return new Schema(
      [
        ...inputSchema.fields,
        new Field("_relevance_score", new Float32(), false),
      ],
      inputSchema.metadata,
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
