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
import { RateLimiter, retryWithExponentialBackoff } from "./utils";

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
  private rateLimiterOptions?: { maxCalls: number; period: number };
  private retryOptions?: {
    initialDelay: number;
    exponentialBase: number;
    jitter: boolean;
    maxRetries: number;
  };

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
   * Add rate limiting to the embedding function
   * 
   * @param maxCalls Maximum number of calls allowed in the period
   * @param period Period in seconds
   * @returns This embedding function for chaining
   */
  rateLimit(maxCalls: number = 0.9, period: number = 1.0): this {
    this.rateLimiterOptions = { maxCalls, period };
    return this;
  }

  /**
   * Add retry with exponential backoff to the embedding function
   * 
   * @param initialDelay Initial delay in seconds (default is 1)
   * @param exponentialBase The base for exponential backoff (default is 2)
   * @param jitter Whether to add jitter to the delay (default is true)
   * @param maxRetries Maximum number of retries (default is 7)
   * @returns This embedding function for chaining
   */
  retry(
    initialDelay: number = 1,
    exponentialBase: number = 2,
    jitter: boolean = true,
    maxRetries: number = 7
  ): this {
    this.retryOptions = { initialDelay, exponentialBase, jitter, maxRetries };
    return this;
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
   */
  protected resolveVariables(config: Partial<M>): Partial<M> {
    this.#config = config;
    const registry = getRegistry();
    const newConfig = { ...config };
    for (const [key_, value] of Object.entries(newConfig)) {
      if (
        this.getSensitiveKeys().includes(key_) &&
        !value.startsWith("$var:")
      ) {
        throw new Error(
          `The key "${key_}" is sensitive and cannot be set directly. Please use the $var: syntax to set it.`,
        );
      }
      // Makes TS happy (https://stackoverflow.com/a/78391854)
      const key = key_ as keyof M;
      if (typeof value === "string" && value.startsWith("$var:")) {
        const [name, defaultValue] = value.slice(5).split(":", 2);
        const variableValue = registry.getVar(name);
        if (!variableValue) {
          if (defaultValue) {
            // biome-ignore lint/suspicious/noExplicitAny:
            newConfig[key] = defaultValue as any;
          } else {
            throw new Error(`Variable "${name}" not found`);
          }
        } else {
          // biome-ignore lint/suspicious/noExplicitAny:
          newConfig[key] = variableValue as any;
        }
      }
    }
    return newConfig;
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
   * Compute source embeddings with retry and rate limiting.
   * 
   * @param data The data to create embeddings for
   * @returns The embeddings
   */
  async computeSourceEmbeddingsWithRetry(
    data: T[],
  ): Promise<number[][] | Float32Array[] | Float64Array[]> {
    let embedFunc = () => this.computeSourceEmbeddings(data);
    
    // Apply rate limiting if configured
    if (this.rateLimiterOptions) {
      const limiter = new RateLimiter(
        this.rateLimiterOptions.maxCalls,
        this.rateLimiterOptions.period
      );
      embedFunc = limiter.wrap(embedFunc);
    }
    
    // Apply retry if configured
    if (this.retryOptions) {
      return retryWithExponentialBackoff(
        embedFunc,
        this.retryOptions.initialDelay,
        this.retryOptions.exponentialBase,
        this.retryOptions.jitter,
        this.retryOptions.maxRetries
      );
    }
    
    return embedFunc();
  }

  /**
  Compute the embeddings for a single query
 */
  async computeQueryEmbeddings(data: T): Promise<Awaited<IntoVector>> {
    return this.computeSourceEmbeddings([data]).then(
      (embeddings) => embeddings[0],
    );
  }

  /**
   * Compute query embeddings with retry and rate limiting.
   * 
   * @param data The query to create embeddings for
   * @returns The embeddings
   */
  async computeQueryEmbeddingsWithRetry(data: T): Promise<Awaited<IntoVector>> {
    return this.computeSourceEmbeddingsWithRetry([data]).then(
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
