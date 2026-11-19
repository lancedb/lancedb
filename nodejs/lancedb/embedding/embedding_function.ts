// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

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
import { getRegistry } from "./registry";

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
  /**
   * The models these options make the function load, other than its default
   * model, as `[name, revision]` pairs. A function restored from table
   * metadata only loads them once they are approved with
   * `EmbeddingFunctionRegistry.allowModel`.
   *
   * It is called before the constructor, with the non-sensitive options.
   */
  loadedModels?(options: FunctionOptions): [string, string | undefined][];
  /**
   * The custom endpoints these options make the function send data to. A
   * function restored from table metadata only uses them once they are
   * approved with `EmbeddingFunctionRegistry.allowEndpoint`.
   *
   * It is called before the constructor, with the non-sensitive options.
   */
  endpoints?(options: FunctionOptions): string[];
}

/**
 * Raised when an embedding function restored from table metadata is used
 * before its options are approved.
 */
export class UntrustedEmbeddingConfigError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "UntrustedEmbeddingConfigError";
  }
}

/**
 * An embedding function that automatically creates vector representation for a given column.
 *
 * It's important subclasses pass the **original** options to the super constructor
 * and then pass those options to `resolveVariables` to resolve any variables before
 * using them.
 *
 * @example
 * ```ts
 * class MyEmbeddingFunction extends EmbeddingFunction {
 *   constructor(options: {model: string, timeout: number}) {
 *     super(optionsRaw);
 *     const options = this.resolveVariables(optionsRaw);
 *     this.model = options.model;
 *     this.timeout = options.timeout;
 *   }
 * }
 * ```
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

  #config: Partial<M>;

  /**
   * Get the original arguments to the constructor, to serialize them so they
   * can be used to recreate the embedding function later.
   */
  // biome-ignore lint/suspicious/noExplicitAny :
  toJSON(): Record<string, any> {
    return JSON.parse(JSON.stringify(this.#config));
  }

  constructor() {
    this.#config = {};
  }

  /**
   * Provide a list of keys in the function options that should be treated as
   * sensitive. If users pass raw values for these keys, they will be rejected.
   */
  protected getSensitiveKeys(): string[] {
    return [];
  }

  /**
   * Apply variables to the config.
   *
   * Secret variables, the default for
   * `EmbeddingFunctionRegistry.setVar`, only resolve in the keys
   * returned by {@link getSensitiveKeys}.
   */
  protected resolveVariables(config: Partial<M>): Partial<M> {
    this.#config = config;
    return getRegistry().resolveVariables(
      config,
      this.getSensitiveKeys(),
    ) as Partial<M>;
  }

  /**
   * Optionally load any resources needed for the embedding function.
   *
   * This method is called after the embedding function has been initialized
   * but before any embeddings are computed. It is useful for loading local models
   * or other resources that are needed for the embedding function to work.
   */
  async init?(): Promise<void>;

  /**
   * sourceField is used in combination with `LanceSchema` to provide a declarative data model
   *
   * @param optionsOrDatatype - The options for the field or the datatype
   *
   * @see {@link LanceSchema}
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
   * @param optionsOrDatatype - The options for the field
   *
   * @see {@link LanceSchema}
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

/**
 * Stands in for an embedding function restored from table metadata whose
 * options aren't approved yet. Computing embeddings throws
 * {@link UntrustedEmbeddingConfigError}, which says how to approve them.
 * Writing the table's metadata again keeps the original options.
 */
export class UntrustedEmbeddingFunction extends EmbeddingFunction {
  readonly functionName: string;
  readonly reasons: string[];
  #options: FunctionOptions;

  constructor(
    functionName: string,
    options: FunctionOptions,
    reasons: string[],
  ) {
    super();
    this.functionName = functionName;
    this.#options = options;
    this.reasons = reasons;
  }

  // biome-ignore lint/suspicious/noExplicitAny :
  override toJSON(): Record<string, any> {
    return JSON.parse(JSON.stringify(this.#options));
  }

  embeddingDataType(): Float {
    return new Float32();
  }

  computeSourceEmbeddings(): Promise<number[][]> {
    return Promise.reject(this.#error());
  }

  override computeQueryEmbeddings(): Promise<Awaited<IntoVector>> {
    return Promise.reject(this.#error());
  }

  #error(): UntrustedEmbeddingConfigError {
    const reasons = this.reasons.map((reason) => `\n  - ${reason}`).join("");
    return new UntrustedEmbeddingConfigError(
      `Embedding function "${this.functionName}" was restored from table ` +
        `metadata, which anyone who can write the table controls. It won't ` +
        `compute embeddings until you approve its options. It${reasons}`,
    );
  }
}

export interface FieldOptions<T extends DataType = DataType> {
  datatype: T;
  dims?: number;
}
