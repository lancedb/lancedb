// Copyright 2023 Lance Developers.
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

import { type Float } from "apache-arrow";

/**
 * An embedding function that automatically creates vector representation for a given column.
 */
export interface EmbeddingFunction<T> {
  /**
   * The name of the column that will be used as input for the Embedding Function.
   */
  sourceColumn: string;

  /**
   * The data type of the embedding
   *
   * The embedding function should return `number`.  This will be converted into
   * an Arrow float array.  By default this will be Float32 but this property can
   * be used to control the conversion.
   */
  embeddingDataType?: Float;

  /**
   * The dimension of the embedding
   *
   * This is optional, normally this can be determined by looking at the results of
   * `embed`.  If this is not specified, and there is an attempt to apply the embedding
   * to an empty table, then that process will fail.
   */
  embeddingDimension?: number;

  /**
   * The name of the column that will contain the embedding
   *
   * By default this is "vector"
   */
  destColumn?: string;

  /**
   * Should the source column be excluded from the resulting table
   *
   * By default the source column is included.  Set this to true and
   * only the embedding will be stored.
   */
  excludeSource?: boolean;

  /**
   * Creates a vector representation for the given values.
   */
  embed: (data: T[]) => Promise<number[][]>;
}

export function isEmbeddingFunction<T>(
  value: unknown,
): value is EmbeddingFunction<T> {
  if (typeof value !== "object" || value === null) {
    return false;
  }
  if (!("sourceColumn" in value) || !("embed" in value)) {
    return false;
  }
  return (
    typeof value.sourceColumn === "string" && typeof value.embed === "function"
  );
}
