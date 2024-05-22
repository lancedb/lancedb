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

import { DataType, Field, FixedSizeList, Float, Float32 } from "apache-arrow";
import "reflect-metadata";
import { newVectorType } from "../arrow";

/**
 * Options for a given embedding function
 */
export interface FunctionOptions {
  // biome-ignore lint/suspicious/noExplicitAny: options can be anything
  [key: string]: any;
}

/**
 * An embedding function that automatically creates vector representation for a given column.
 */
export abstract class EmbeddingFunction<
  // biome-ignore lint/suspicious/noExplicitAny: we don't know what the implementor will do
  T = any,
  M extends FunctionOptions = FunctionOptions,
> {
  /**
   * Convert the embedding function to a JSON object
   * It is used to serialize the embedding function to the schema
   * It's important that any object returned by this method contains all the necessary
   * information to recreate the embedding function
   *
   * It should return the same object that was passed to the constructor
   * If it does not, the embedding function will not be able to be recreated, or could be recreated incorrectly
   *
   * @example
   * ```ts
   * class MyEmbeddingFunction extends EmbeddingFunction {
   *   constructor(options: {model: string, timeout: number}) {
   *     super();
   *     this.model = options.model;
   *     this.timeout = options.timeout;
   *   }
   *   toJSON() {
   *     return {
   *       model: this.model,
   *       timeout: this.timeout,
   *     };
   * }
   * ```
   */
  abstract toJSON(): Partial<M>;

  /**
   * sourceField is used in combination with `LanceSchema` to provide a declarative data model
   *
   * @param optionsOrDatatype - The options for the field or the datatype
   *
   * @see {@link lancedb.LanceSchema}
   */
  sourceField(
    options: Partial<FieldOptions> | DataType,
  ): [DataType, Map<string, EmbeddingFunction>] {
    const datatype = options instanceof DataType ? options : options?.datatype;
    if (!datatype) {
      throw new Error("Datatype is required");
    }
    const metadata = new Map<string, EmbeddingFunction>();
    metadata.set("source_column_for", this);

    if (options instanceof DataType) {
      return [options, metadata];
    }
    return [datatype, metadata];
  }

  /**
   * vectorField is used in combination with `LanceSchema` to provide a declarative data model
   *
   * @param options - The options for the field
   *
   * @see {@link lancedb.LanceSchema}
   */
  vectorField(
    options?: Partial<FieldOptions>,
  ): [DataType, Map<string, EmbeddingFunction>] {
    let dtype: DataType;
    const dims = this.ndims() ?? options?.dims;
    if (!options?.datatype) {
      if (dims === undefined) {
        throw new Error("ndims is required for vector field");
      }
      dtype = new FixedSizeList(dims, new Field("item", new Float32()));
    } else {
      if (options.datatype instanceof FixedSizeList) {
        dtype = options.datatype;
      } else if (options.datatype instanceof Float) {
        if (dims === undefined) {
          throw new Error("ndims is required for vector field");
        }
        dtype = newVectorType(dims, options.datatype);
      } else {
        throw new Error(
          "Expected FixedSizeList or Float as datatype for vector field",
        );
      }
    }
    const metadata = new Map<string, EmbeddingFunction>();
    metadata.set("vector_column_for", this);

    return [dtype, metadata];
  }

  /** The number of dimensions of the embeddings */
  ndims(): number | undefined {
    return undefined;
  }

  /** The datatype of the embeddings */
  abstract embeddingDataType(): Float;

  /**
   * Creates a vector representation for the given values.
   */
  abstract computeSourceEmbeddings(
    data: T[],
  ): Promise<number[][] | Float32Array[] | Float64Array[]>;

  /**
  Compute the embeddings for a single query
 */
  computeQueryEmbeddings(
    data: T,
  ): Promise<number[] | Float32Array | Float64Array> {
    return this.computeSourceEmbeddings([data]).then(
      (embeddings) => embeddings[0],
    );
  }
}

export interface FieldOptions<T extends DataType = DataType> {
  datatype: T;
  dims?: number;
}
