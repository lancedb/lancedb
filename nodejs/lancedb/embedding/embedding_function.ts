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

import "reflect-metadata";
import {
  DataType,
  Field,
  FixedSizeList,
  Float,
  Float32,
  type IntoVector,
  Utf8,
  isFixedSizeList,
  isFloat,
  newVectorType,
} from "../arrow";
import { sanitizeType } from "../sanitize";

/**
 * Options for a given embedding function
 */
export interface FunctionOptions {
  // biome-ignore lint/suspicious/noExplicitAny: options can be anything
  [key: string]: any;
}

export interface EmbeddingFunctionConstructor<
  T extends EmbeddingFunction = EmbeddingFunction,
> {
  new (modelOptions?: T["TOptions"]): T;
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
   * @ignore
   *  This is only used for associating the options type with the class for type checking
   */
  // biome-ignore lint/style/useNamingConvention: we want to keep the name as it is
  readonly TOptions!: M;
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

  async init?(): Promise<void>;

  /**
   * sourceField is used in combination with `LanceSchema` to provide a declarative data model
   *
   * @param optionsOrDatatype - The options for the field or the datatype
   *
   * @see {@link lancedb.LanceSchema}
   */
  sourceField(
    optionsOrDatatype: Partial<FieldOptions> | DataType,
  ): [DataType, Map<string, EmbeddingFunction>] {
    let datatype =
      "datatype" in optionsOrDatatype
        ? optionsOrDatatype.datatype
        : optionsOrDatatype;
    if (!datatype) {
      throw new Error("Datatype is required");
    }
    datatype = sanitizeType(datatype);
    const metadata = new Map<string, EmbeddingFunction>();
    metadata.set("source_column_for", this);

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
    optionsOrDatatype?: Partial<FieldOptions> | DataType,
  ): [DataType, Map<string, EmbeddingFunction>] {
    let dtype: DataType | undefined;
    let vectorType: DataType;
    let dims: number | undefined = this.ndims();

    // `func.vectorField(new Float32())`
    if (optionsOrDatatype === undefined) {
      dtype = new Float32();
    } else if (!("datatype" in optionsOrDatatype)) {
      dtype = sanitizeType(optionsOrDatatype);
    } else {
      // `func.vectorField({
      //  datatype: new Float32(),
      //  dims: 10
      // })`
      dims = dims ?? optionsOrDatatype?.dims;
      dtype = sanitizeType(optionsOrDatatype?.datatype);
    }

    if (dtype !== undefined) {
      // `func.vectorField(new FixedSizeList(dims, new Field("item", new Float32(), true)))`
      // or `func.vectorField({datatype: new FixedSizeList(dims, new Field("item", new Float32(), true))})`
      if (isFixedSizeList(dtype)) {
        vectorType = dtype;
        // `func.vectorField(new Float32())`
        // or `func.vectorField({datatype: new Float32()})`
      } else if (isFloat(dtype)) {
        // No `ndims` impl and no `{dims: n}` provided;
        if (dims === undefined) {
          throw new Error("ndims is required for vector field");
        }
        vectorType = newVectorType(dims, dtype);
      } else {
        throw new Error(
          "Expected FixedSizeList or Float as datatype for vector field",
        );
      }
    } else {
      if (dims === undefined) {
        throw new Error("ndims is required for vector field");
      }
      vectorType = new FixedSizeList(
        dims,
        new Field("item", new Float32(), true),
      );
    }
    const metadata = new Map<string, EmbeddingFunction>();
    metadata.set("vector_column_for", this);

    return [vectorType, metadata];
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
  async computeQueryEmbeddings(data: T): Promise<Awaited<IntoVector>> {
    return this.computeSourceEmbeddings([data]).then(
      (embeddings) => embeddings[0],
    );
  }
}

/**
 * an abstract class for implementing embedding functions that take text as input
 */
export abstract class TextEmbeddingFunction<
  M extends FunctionOptions = FunctionOptions,
> extends EmbeddingFunction<string, M> {
  //** Generate the embeddings for the given texts */
  abstract generateEmbeddings(
    texts: string[],
    // biome-ignore lint/suspicious/noExplicitAny: we don't know what the implementor will do
    ...args: any[]
  ): Promise<number[][] | Float32Array[] | Float64Array[]>;

  async computeQueryEmbeddings(data: string): Promise<Awaited<IntoVector>> {
    return this.generateEmbeddings([data]).then((data) => data[0]);
  }

  embeddingDataType(): Float {
    return new Float32();
  }

  override sourceField(): [DataType, Map<string, EmbeddingFunction>] {
    return super.sourceField(new Utf8());
  }

  computeSourceEmbeddings(
    data: string[],
  ): Promise<number[][] | Float32Array[] | Float64Array[]> {
    return this.generateEmbeddings(data);
  }
}

export interface FieldOptions<T extends DataType = DataType> {
  datatype: T;
  dims?: number;
}
