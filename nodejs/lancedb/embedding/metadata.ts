// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import type { EmbeddingFunction } from "./embedding_function";

interface SerializedEmbeddingFunction {
  name: string;
  model: EmbeddingFunction["TOptions"];
  sourceColumn?: string;
  vectorColumn?: string;
  [key: string]: unknown;
}

export interface EmbeddingFunctionMetadata {
  name: string;
  model: EmbeddingFunction["TOptions"];
  sourceColumn: string;
  vectorColumn?: string;
}

export function parseEmbeddingFunctionMetadata(
  metadata: Map<string, string>,
): EmbeddingFunctionMetadata[] {
  const serialized = metadata.get("embedding_functions");
  if (serialized === undefined) {
    return [];
  }

  return (JSON.parse(serialized) as SerializedEmbeddingFunction[]).map(
    (functionMetadata) => {
      const sourceColumn =
        functionMetadata.sourceColumn ??
        (functionMetadata["source_column"] as string | undefined);
      if (sourceColumn === undefined) {
        throw new Error(
          "Embedding function metadata is missing a source column",
        );
      }

      return {
        name: functionMetadata.name,
        model: functionMetadata.model,
        sourceColumn,
        vectorColumn:
          functionMetadata.vectorColumn ??
          (functionMetadata["vector_column"] as string | undefined),
      };
    },
  );
}
