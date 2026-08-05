// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import { RecordBatch } from "apache-arrow";

export * from "./rrf";

// Interface for a reranker. A reranker is used to rerank vector and hybrid
// search results. For vector-only searches, query is empty and ftsResults is an
// empty batch with the same schema as vecResults.
export interface Reranker {
  rerankHybrid(
    query: string,
    vecResults: RecordBatch,
    ftsResults: RecordBatch,
  ): Promise<RecordBatch>;
}
