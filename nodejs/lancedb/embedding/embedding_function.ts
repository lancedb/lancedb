import { DataType, Field, FixedSizeList, Float32 } from "apache-arrow";
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
export abstract class EmbeddingFunction<T = any> {
  /**
   * Convert the embedding function to a JSON object
   */
  // biome-ignore lint/suspicious/noExplicitAny: `toJSON` typically can return any object
  toJSON(): Record<string, any> {
    return {
      name: this.constructor.name,
    };
  }

  modelDump(): string {
    return JSON.stringify({
      name: this.constructor.name,
      ...this.toJSON(),
    });
  }

  sourceField(
    options: Partial<FieldOptions> | DataType,
  ): [DataType, Map<string, string>] {
    const datatype = options instanceof DataType ? options : options?.datatype;
    if (!datatype) {
      throw new Error("Datatype is required");
    }
    const metadata = new Map<string, string>();
    metadata.set("model", this.modelDump());
    metadata.set("source_column", "true");
    if (options instanceof DataType) {
      return [options, metadata];
    } else {
      return [datatype, metadata];
    }
  }

  vectorField(
    options?: Partial<FieldOptions>,
  ): [DataType, Map<string, string>] {
    let dtype: DataType;
    if (!options?.datatype) {
      dtype = new FixedSizeList(this.ndims(), new Field("item", new Float32()));
    } else {
      dtype = options.datatype;
    }
    const metadata = new Map<string, string>();
    metadata.set("model", this.modelDump());
    metadata.set("vector_column", "true");
    return [dtype, metadata];
  }
  abstract ndims(): number;
  /**
  Compute the embeddings for the source column in the database
 */
  abstract computeQueryEmbeddings(
    data: T,
  ): Promise<number[] | Float32Array | Float64Array>;
  /**
   * Creates a vector representation for the given values.
   */
  abstract computeSourceEmbeddings(
    data: T[],
  ): Promise<number[][] | Float32Array[] | Float64Array[]>;
}

export interface FieldOptions<T extends DataType = DataType> {
  datatype: T;
}
