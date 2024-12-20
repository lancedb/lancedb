// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import { RecordBatch } from "apache-arrow";

export * from "./rrf";

// Interface for a reranker. A reranker is used to rerank the results from a
// vector and FTS search. This is useful for combining the results from both
// search methods.
export interface Reranker {
  rerankHybrid(
    query: string,
    vecResults: RecordBatch,
    ftsResults: RecordBatch,
  ): Promise<RecordBatch>;
}
