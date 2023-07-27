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

/**
 * An embedding function that automatically creates vector representation for a given column.
 */
export interface EmbeddingFunction<T> {
  /**
     * The name of the column that will be used as input for the Embedding Function.
     */
  sourceColumn: string

  /**
     * Creates a vector representation for the given values.
     */
  embed: (data: T[]) => Promise<number[][]>
}

export function isEmbeddingFunction<T> (value: any): value is EmbeddingFunction<T> {
  return typeof value.sourceColumn === 'string' &&
      typeof value.embed === 'function'
}
