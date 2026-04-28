import type { Schema, Table as ArrowTable } from "apache-arrow";
import {
  openTable,
  type OpenTableOptions,
  type RemoteSearchTable,
  type SearchRequest,
} from "./index.js";

export type PoolingStrategy = "mean" | "cls" | "last_token";

export interface QueryTransformContext {
  /** The Hugging Face model id being used to embed. */
  model: string;
}

// ---------------------------------------------------------------------------
// EmbeddingModel — the first-class abstraction
// ---------------------------------------------------------------------------

/**
 * A model that can embed text into vectors.
 *
 * Pass an `EmbeddingModel` as the second argument to `searchTable`, or to the
 * standalone `embed`/`embedMany` functions.  Use `transformersEmbedder()` to
 * create one backed by `@huggingface/transformers`, or bring your own
 * implementation.
 */
export interface EmbeddingModel {
  /** Embed a single text value. */
  embed(value: string, options?: { signal?: AbortSignal }): Promise<EmbedResult>;
  /** Embed multiple text values. */
  embedMany(values: string[], options?: { signal?: AbortSignal }): Promise<EmbedManyResult>;
  /**
   * Eagerly load the model weights and tokenizer so the first `embed()` call
   * doesn't pay the full download + init cost.  No-op if already loaded or if
   * the implementation doesn't support preloading.
   */
  preload?(): Promise<void>;
  /**
   * Release the loaded model and tokenizer from memory.  After calling
   * `dispose()` the model can still be used — it will simply re-download on
   * the next `embed()` call.
   */
  dispose?(): void;
}

/**
 * Result of a single embedding operation.  Mirrors the shape returned by
 * the Vercel AI SDK `embed()` function.
 */
export interface EmbedResult {
  /** The embedding vector for the input value. */
  embedding: number[];
}

/**
 * Result of a batch embedding operation.  Mirrors the shape returned by
 * the Vercel AI SDK `embedMany()` function.
 */
export interface EmbedManyResult {
  /** One embedding vector per input value, in the same order. */
  embeddings: number[][];
}

// ---------------------------------------------------------------------------
// transformersEmbedder — factory for HuggingFace-backed EmbeddingModel
// ---------------------------------------------------------------------------

export interface TransformersEmbedderOptions {
  /**
   * Hugging Face model id to load with `@huggingface/transformers`.
   *
   * Defaults to `Xenova/all-MiniLM-L6-v2`.
   */
  model?: string;
  /**
   * Optional tokenizer override. If omitted, the model id is reused.
   */
  tokenizer?: string;
  /**
   * Pooling strategy applied to the model hidden states.
   *
   * Defaults are inferred for known model families.
   */
  pooling?: PoolingStrategy;
  /**
   * Normalize the final embedding.
   *
   * Defaults to `true`.
   */
  normalize?: boolean;
  /**
   * Optional hook to rewrite query text before embedding.
   */
  prepareQuery?: (query: string, context: QueryTransformContext) => string;
  /**
   * Options passed to `AutoModel.from_pretrained`.
   */
  modelOptions?: Record<string, unknown>;
  /**
   * Options passed to the tokenizer call.
   */
  tokenizerOptions?: {
    textPair?: string | string[];
    padding?: boolean | "max_length";
    addSpecialTokens?: boolean;
    truncation?: boolean;
    maxLength?: number;
  };
}

/**
 * Create an `EmbeddingModel` backed by `@huggingface/transformers`.
 *
 * ```ts
 * import { transformersEmbedder } from "@lancedb/lancedb-web/transformers";
 *
 * const model = transformersEmbedder("BAAI/bge-small-en-v1.5");
 * const { embedding } = await model.embed("best hikes in colorado");
 * ```
 *
 * The underlying ONNX session is cached at the module level so multiple
 * embedders that reference the same `model` (and `tokenizer`/`modelOptions`)
 * share the loaded weights.
 */
export function transformersEmbedder(
  model?: string,
  options?: Omit<TransformersEmbedderOptions, "model">,
): EmbeddingModel;
export function transformersEmbedder(
  options?: TransformersEmbedderOptions,
): EmbeddingModel;
export function transformersEmbedder(
  modelOrOptions?: string | TransformersEmbedderOptions,
  options?: Omit<TransformersEmbedderOptions, "model">,
): EmbeddingModel {
  const resolved =
    typeof modelOrOptions === "string"
      ? { ...options, model: modelOrOptions }
      : modelOrOptions ?? {};
  return buildEmbeddingModel(resolved);
}

// ---------------------------------------------------------------------------
// Standalone embed / embedMany
// ---------------------------------------------------------------------------

export interface EmbedRequest {
  /** The text value to embed. */
  value: string;
  /**
   * An `EmbeddingModel`, or a Hugging Face model id string that will be
   * auto-promoted to one via `transformersEmbedder()`.
   */
  model?: string | EmbeddingModel;
  /** Abort signal for cancellation (e.g. typeahead debouncing). */
  signal?: AbortSignal;
}

export interface EmbedManyRequest {
  /** The text values to embed. */
  values: string[];
  /**
   * An `EmbeddingModel`, or a Hugging Face model id string that will be
   * auto-promoted to one via `transformersEmbedder()`.
   */
  model?: string | EmbeddingModel;
  /** Abort signal for cancellation. */
  signal?: AbortSignal;
}

/**
 * Embed a single text value.
 *
 * ```ts
 * import { embed } from "@lancedb/lancedb-web/transformers";
 *
 * const { embedding } = await embed({
 *   model: "BAAI/bge-small-en-v1.5",
 *   value: "best places to hike in colorado",
 * });
 * ```
 */
export async function embed(request: EmbedRequest): Promise<EmbedResult> {
  const model = resolveModel(request.model);
  return model.embed(request.value, { signal: request.signal });
}

/**
 * Embed multiple text values.
 *
 * ```ts
 * import { embedMany } from "@lancedb/lancedb-web/transformers";
 *
 * const { embeddings } = await embedMany({
 *   model: "BAAI/bge-small-en-v1.5",
 *   values: ["hiking trails", "mountain biking"],
 * });
 * ```
 */
export async function embedMany(
  request: EmbedManyRequest,
): Promise<EmbedManyResult> {
  const model = resolveModel(request.model);
  return model.embedMany(request.values, { signal: request.signal });
}

// ---------------------------------------------------------------------------
// searchTable
// ---------------------------------------------------------------------------

export interface TransformersSearchTableOptions extends OpenTableOptions {
  /**
   * An `EmbeddingModel`, or a Hugging Face model id to auto-promote.
   *
   * Defaults to `Xenova/all-MiniLM-L6-v2`.
   */
  model?: string | EmbeddingModel;
  /**
   * Optional tokenizer override (only used when `model` is a string).
   */
  tokenizer?: string;
  /**
   * Pooling strategy (only used when `model` is a string).
   */
  pooling?: PoolingStrategy;
  /**
   * Normalize the final embedding (only used when `model` is a string).
   *
   * Defaults to `true`.
   */
  normalize?: boolean;
  /**
   * Optional hook to rewrite query text before embedding (only used when
   * `model` is a string).
   */
  prepareQuery?: (query: string, context: QueryTransformContext) => string;
  /**
   * Options passed to `AutoModel.from_pretrained` (only used when `model` is
   * a string).
   */
  modelOptions?: Record<string, unknown>;
  /**
   * Options passed to the tokenizer (only used when `model` is a string).
   */
  tokenizerOptions?: {
    textPair?: string | string[];
    padding?: boolean | "max_length";
    addSpecialTokens?: boolean;
    truncation?: boolean;
    maxLength?: number;
  };
}

export interface TextEmbeddingSearchRequest
  extends Omit<SearchRequest, "text" | "vector"> {
  /**
   * Natural language query text to embed on the client.
   */
  text: string;
  /**
   * When true, logs the resolved embedding configuration to `console.debug`.
   */
  debug?: boolean;
  /** Abort signal for cancellation (e.g. typeahead debouncing). */
  signal?: AbortSignal;
}

export interface TextEmbeddingSearchTable {
  /** The `EmbeddingModel` powering this table's search. */
  readonly model: EmbeddingModel;
  schema(): Promise<Schema>;
  search(request: TextEmbeddingSearchRequest): Promise<ArrowTable>;
  refresh(): Promise<boolean>;
  close(): void;
}

/**
 * Open a published Lance table and wrap it with client-side query embedding
 * generation.
 *
 * The second argument is an `EmbeddingModel`, a Hugging Face model id string
 * (auto-promoted via `transformersEmbedder()`), or an options object.
 *
 * ```ts
 * // String shorthand — auto-promoted to EmbeddingModel
 * const t = await searchTable(url, "BAAI/bge-small-en-v1.5");
 *
 * // Bring your own EmbeddingModel
 * const t = await searchTable(url, myEmbeddingModel);
 *
 * // Options object
 * const t = await searchTable(url, { model: "BAAI/bge-small-en-v1.5", normalize: false });
 * ```
 */
export async function searchTable(
  tableUrl: string,
  model?: string | EmbeddingModel,
  options?: OpenTableOptions,
): Promise<TextEmbeddingSearchTable>;
export async function searchTable(
  tableUrl: string,
  options?: TransformersSearchTableOptions,
): Promise<TextEmbeddingSearchTable>;
export async function searchTable(
  tableUrl: string,
  modelOrOptions:
    | string
    | EmbeddingModel
    | TransformersSearchTableOptions = {},
  options?: OpenTableOptions,
): Promise<TextEmbeddingSearchTable> {
  let embeddingModel: EmbeddingModel;
  let openOptions: OpenTableOptions;

  if (typeof modelOrOptions === "string") {
    embeddingModel = transformersEmbedder(modelOrOptions);
    openOptions = options ?? {};
  } else if (isEmbeddingModel(modelOrOptions)) {
    embeddingModel = modelOrOptions;
    openOptions = options ?? {};
  } else {
    const {
      model,
      tokenizer,
      pooling,
      normalize,
      prepareQuery,
      modelOptions,
      tokenizerOptions,
      ...rest
    } = modelOrOptions;
    openOptions = rest;

    if (isEmbeddingModel(model)) {
      embeddingModel = model;
    } else {
      embeddingModel = buildEmbeddingModel({
        model,
        tokenizer,
        pooling,
        normalize,
        prepareQuery,
        modelOptions,
        tokenizerOptions,
      });
    }
  }

  const table = await openTable(tableUrl, openOptions);
  return new TextEmbeddingSearchTableImpl(table, embeddingModel);
}

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

type TokenizerOptions = {
  textPair?: string | string[];
  padding?: boolean | "max_length";
  addSpecialTokens?: boolean;
  truncation?: boolean;
  maxLength?: number;
};

type TransformersModule = {
  AutoModel: {
    from_pretrained(
      model: string,
      options?: Record<string, unknown>,
    ): Promise<ModelLike>;
  };
  AutoTokenizer: {
    from_pretrained(model: string): Promise<TokenizerLike>;
  };
};

type TokenizerLike = (
  input: string | string[],
  options?: TokenizerOptions,
) => Record<string, unknown> | Promise<Record<string, unknown>>;

type ModelLike = {
  forward(
    inputs: Record<string, unknown>,
  ): Promise<Record<string, unknown>>;
};

type TensorLike = {
  dims: number[];
  data: ArrayLike<number>;
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const DEFAULT_MODEL = "Xenova/all-MiniLM-L6-v2";
const ENGLISH_BGE_RETRIEVAL_PREFIX =
  "Represent this sentence for searching relevant passages: ";
const CHINESE_BGE_RETRIEVAL_PREFIX =
  "为这个句子生成表示以用于检索相关文章：";
const MULTILINGUAL_GEMMA2_INSTRUCTION =
  "Given a web search query, retrieve relevant passages that answer the query.";
const DEFAULT_TOKENIZER_OPTIONS: TokenizerOptions = {
  padding: true,
};

// ---------------------------------------------------------------------------
// Module-level model/tokenizer cache
// ---------------------------------------------------------------------------

let transformersModuleLoader: () => Promise<TransformersModule> =
  defaultTransformersModuleLoader;

const resourcesCache = new Map<
  string,
  Promise<{ model: ModelLike; tokenizer: TokenizerLike }>
>();

function resourcesCacheKey(
  model: string,
  tokenizer: string,
  modelOptions?: Record<string, unknown>,
): string {
  return modelOptions
    ? `${model}|${tokenizer}|${JSON.stringify(modelOptions)}`
    : `${model}|${tokenizer}`;
}

async function loadSharedResources(
  modelId: string,
  tokenizerId: string,
  modelOptions?: Record<string, unknown>,
): Promise<{ model: ModelLike; tokenizer: TokenizerLike }> {
  const key = resourcesCacheKey(modelId, tokenizerId, modelOptions);
  let promise = resourcesCache.get(key);
  if (!promise) {
    promise = initializeResources(modelId, tokenizerId, modelOptions);
    resourcesCache.set(key, promise);
  }
  return promise;
}

async function initializeResources(
  modelId: string,
  tokenizerId: string,
  modelOptions?: Record<string, unknown>,
): Promise<{ model: ModelLike; tokenizer: TokenizerLike }> {
  const transformers = await transformersModuleLoader();
  try {
    const [model, tokenizer] = await Promise.all([
      transformers.AutoModel.from_pretrained(modelId, modelOptions),
      transformers.AutoTokenizer.from_pretrained(tokenizerId),
    ]);
    return { model, tokenizer };
  } catch (error) {
    resourcesCache.delete(
      resourcesCacheKey(modelId, tokenizerId, modelOptions),
    );
    throw new Error(
      `Failed to initialize transformers model "${modelId}": ${(error as Error).message}`,
    );
  }
}

/**
 * Override the Transformers.js module loader for tests.
 */
export function __setTransformersModuleLoaderForTests(
  loader?: () => Promise<TransformersModule>,
): void {
  transformersModuleLoader = loader ?? defaultTransformersModuleLoader;
  resourcesCache.clear();
}

// ---------------------------------------------------------------------------
// Helpers: resolve model arg, build EmbeddingModel
// ---------------------------------------------------------------------------

function isEmbeddingModel(value: unknown): value is EmbeddingModel {
  return (
    typeof value === "object" &&
    value !== null &&
    typeof (value as EmbeddingModel).embed === "function" &&
    typeof (value as EmbeddingModel).embedMany === "function"
  );
}

function resolveModel(model: string | EmbeddingModel | undefined): EmbeddingModel {
  if (model === undefined || typeof model === "string") {
    return buildEmbeddingModel({ model });
  }
  return model;
}

function buildEmbeddingModel(
  options: TransformersEmbedderOptions,
): EmbeddingModel {
  const modelId = options.model ?? DEFAULT_MODEL;
  const tokenizerId = options.tokenizer ?? modelId;
  const pooling = options.pooling ?? inferDefaultPooling(modelId);
  const normalize = options.normalize ?? true;
  const prepareQuery = options.prepareQuery ?? defaultPrepareQuery;
  const modelOptions = options.modelOptions;
  const tokenizerOptions: TokenizerOptions = {
    ...DEFAULT_TOKENIZER_OPTIONS,
    ...options.tokenizerOptions,
  };

  async function embedOne(
    value: string,
    signal?: AbortSignal,
  ): Promise<number[]> {
    signal?.throwIfAborted();
    const prepared = prepareQuery(value, { model: modelId });
    const { tokenizer, model } = await loadSharedResources(
      modelId,
      tokenizerId,
      modelOptions,
    );
    signal?.throwIfAborted();
    const inputs = await tokenizer([prepared], tokenizerOptions);
    signal?.throwIfAborted();
    const attentionMask = extractAttentionMask(inputs);
    const outputs = await model.forward(inputs);
    signal?.throwIfAborted();
    let vector = poolTensor(firstTensor(outputs), pooling, attentionMask);
    if (normalize) {
      vector = normalizeVector(vector);
    }
    return vector;
  }

  const cacheKey = resourcesCacheKey(modelId, tokenizerId, modelOptions);

  return {
    async embed(
      value: string,
      opts?: { signal?: AbortSignal },
    ): Promise<EmbedResult> {
      return { embedding: await embedOne(value, opts?.signal) };
    },
    async embedMany(
      values: string[],
      opts?: { signal?: AbortSignal },
    ): Promise<EmbedManyResult> {
      if (values.length === 0) {
        return { embeddings: [] };
      }
      if (values.length === 1) {
        return { embeddings: [await embedOne(values[0], opts?.signal)] };
      }

      const signal = opts?.signal;
      signal?.throwIfAborted();

      const prepared = values.map((v) =>
        prepareQuery(v, { model: modelId }),
      );
      const { tokenizer, model } = await loadSharedResources(
        modelId,
        tokenizerId,
        modelOptions,
      );
      signal?.throwIfAborted();

      const inputs = await tokenizer(prepared, tokenizerOptions);
      signal?.throwIfAborted();

      const attentionMask = extractAttentionMask(inputs);
      const outputs = await model.forward(inputs);
      signal?.throwIfAborted();

      const tensor = firstTensor(outputs);
      const embeddings = poolBatchTensor(
        tensor,
        pooling,
        values.length,
        attentionMask,
      );
      if (normalize) {
        return { embeddings: embeddings.map(normalizeVector) };
      }
      return { embeddings };
    },
    async preload(): Promise<void> {
      await loadSharedResources(modelId, tokenizerId, modelOptions);
    },
    dispose(): void {
      resourcesCache.delete(cacheKey);
    },
  };
}

// ---------------------------------------------------------------------------
// TextEmbeddingSearchTableImpl
// ---------------------------------------------------------------------------

class TextEmbeddingSearchTableImpl implements TextEmbeddingSearchTable {
  readonly #table: RemoteSearchTable;
  readonly model: EmbeddingModel;

  constructor(table: RemoteSearchTable, model: EmbeddingModel) {
    this.#table = table;
    this.model = model;
  }

  schema(): Promise<Schema> {
    return this.#table.schema();
  }

  async search(request: TextEmbeddingSearchRequest): Promise<ArrowTable> {
    const { text, debug, signal, ...vectorSearchRequest } = request;
    const { embedding } = await this.model.embed(text, { signal });

    if (debug && typeof console.debug === "function") {
      console.debug("[@lancedb/lancedb-web/transformers]", {
        query: text,
        dimensions: embedding.length,
      });
    }

    return await this.#table.search({
      ...vectorSearchRequest,
      vector: embedding,
    });
  }

  refresh(): Promise<boolean> {
    return this.#table.refresh();
  }

  close(): void {
    this.#table.close();
  }
}

// ---------------------------------------------------------------------------
// Transformers.js loader
// ---------------------------------------------------------------------------

async function defaultTransformersModuleLoader(): Promise<TransformersModule> {
  // The variable indirection plus bundler-specific comments prevent bundlers
  // from statically resolving the import, keeping @huggingface/transformers
  const specifier = "@huggingface/transformers";
  try {
    return await import(/* webpackIgnore: true */ /* @vite-ignore */ specifier);
  } catch (error) {
    throw new Error(
      "Failed to load @huggingface/transformers. Install it to use `@lancedb/lancedb-web/transformers`.",
    );
  }
}

// ---------------------------------------------------------------------------
// Model-family defaults
// ---------------------------------------------------------------------------

function inferDefaultPooling(model: string): PoolingStrategy {
  if (model === "BAAI/bge-multilingual-gemma2") {
    return "last_token";
  }
  if (model.startsWith("BAAI/bge-")) {
    return "cls";
  }
  return "mean";
}

function defaultPrepareQuery(
  query: string,
  context: QueryTransformContext,
): string {
  if (context.model === "BAAI/bge-multilingual-gemma2") {
    return `<instruct>${MULTILINGUAL_GEMMA2_INSTRUCTION}\n<query>${query}`;
  }
  if (isEnglishBgeModel(context.model)) {
    return `${ENGLISH_BGE_RETRIEVAL_PREFIX}${query}`;
  }
  if (isChineseBgeModel(context.model)) {
    return `${CHINESE_BGE_RETRIEVAL_PREFIX}${query}`;
  }
  return query;
}

function isEnglishBgeModel(model: string): boolean {
  return /^BAAI\/bge-.*-en(?:-v1\.5)?$/.test(model);
}

function isChineseBgeModel(model: string): boolean {
  return /^BAAI\/bge-.*-zh(?:-v1\.5)?$/.test(model);
}

// ---------------------------------------------------------------------------
// Tensor utilities
// ---------------------------------------------------------------------------

/** Known output keys in priority order. */
const PREFERRED_OUTPUT_KEYS = ["last_hidden_state", "hidden_states", "embeddings"];

function firstTensor(outputs: Record<string, unknown>): TensorLike {
  // Try well-known keys first so we don't accidentally grab pooler_output or attentions.
  for (const key of PREFERRED_OUTPUT_KEYS) {
    const value = outputs[key];
    if (value !== undefined && isTensorLike(value)) {
      return value;
    }
  }
  // Fallback: first tensor-like value.
  for (const value of Object.values(outputs)) {
    if (isTensorLike(value)) {
      return value;
    }
  }
  throw new Error("Transformers model output did not contain an embedding tensor.");
}

function isTensorLike(value: unknown): value is TensorLike {
  if (typeof value !== "object" || value === null) {
    return false;
  }

  const candidate = value as Partial<TensorLike>;
  return Array.isArray(candidate.dims) && candidate.data !== undefined;
}

function extractAttentionMask(
  inputs: Record<string, unknown>,
): TensorLike | undefined {
  const mask = inputs.attention_mask;
  if (mask !== undefined && isTensorLike(mask)) {
    return mask;
  }
  return undefined;
}

function poolTensor(
  tensor: TensorLike,
  pooling: PoolingStrategy,
  attentionMask?: TensorLike,
): number[] {
  if (tensor.dims.length === 1) {
    return Array.from(tensor.data);
  }

  if (tensor.dims.length === 2) {
    const [tokenCount, hiddenSize] = tensor.dims;
    const data = tensor.data;
    // For 2D tensors the mask (if present) has shape [1, tokenCount] or [tokenCount].
    const mask1d = flattenMaskForBatchElement(attentionMask, 0, tokenCount);
    switch (pooling) {
      case "cls":
        return sliceToken(data, 0, hiddenSize);
      case "last_token":
        return sliceToken(data, tokenCount - 1, hiddenSize);
      case "mean":
        return meanPool(data, tokenCount, hiddenSize, mask1d);
    }
  }

  if (tensor.dims.length !== 3) {
    throw new Error(
      `Unsupported embedding tensor shape [${tensor.dims.join(", ")}]. Expected 1D, 2D, or 3D output.`,
    );
  }

  const [batchSize, tokenCount, hiddenSize] = tensor.dims;
  if (batchSize < 1) {
    throw new Error("Embedding tensor batch dimension must be at least 1.");
  }

  const data = tensor.data;
  const mask1d = flattenMaskForBatchElement(attentionMask, 0, tokenCount);
  switch (pooling) {
    case "cls":
      return sliceToken(data, 0, hiddenSize);
    case "last_token":
      return sliceToken(data, tokenCount - 1, hiddenSize);
    case "mean":
      return meanPool(data, tokenCount, hiddenSize, mask1d);
  }
}

/**
 * Convert a batched tensor into one embedding per batch element.
 *
 * Supports already pooled 2D tensors `[batchSize, hiddenSize]` and token-level
 * 3D tensors `[batchSize, tokenCount, hiddenSize]`.
 */
function poolBatchTensor(
  tensor: TensorLike,
  pooling: PoolingStrategy,
  batchSize: number,
  attentionMask?: TensorLike,
): number[][] {
  // Some models return already pooled embeddings shaped as [batchSize, hiddenSize].
  if (tensor.dims.length === 2) {
    const [rows, hiddenSize] = tensor.dims;
    if (rows !== batchSize) {
      throw new Error(
        `Unsupported batched 2D tensor shape [${tensor.dims.join(", ")}]. Expected first dimension to match batch size ${batchSize}.`,
      );
    }
    const data = tensor.data;
    const results: number[][] = [];
    for (let rowIndex = 0; rowIndex < rows; rowIndex += 1) {
      results.push(sliceTokenAt(data, rowIndex * hiddenSize, 0, hiddenSize));
    }
    return results;
  }

  if (tensor.dims.length < 2) {
    throw new Error(
      `Unsupported batched embedding tensor shape [${tensor.dims.join(", ")}]. Expected 2D or 3D output.`,
    );
  }

  const tokenCount = tensor.dims[1];
  const hiddenSize = tensor.dims[2];
  const batchStride = tokenCount * hiddenSize;
  const data = tensor.data;
  const results: number[][] = [];

  for (let b = 0; b < batchSize; b += 1) {
    const offset = b * batchStride;
    const mask1d = flattenMaskForBatchElement(attentionMask, b, tokenCount);

    switch (pooling) {
      case "cls":
        results.push(sliceTokenAt(data, offset, 0, hiddenSize));
        break;
      case "last_token":
        results.push(sliceTokenAt(data, offset, tokenCount - 1, hiddenSize));
        break;
      case "mean":
        results.push(meanPoolAt(data, offset, tokenCount, hiddenSize, mask1d));
        break;
    }
  }

  return results;
}

function sliceToken(
  data: ArrayLike<number>,
  tokenIndex: number,
  hiddenSize: number,
): number[] {
  return sliceTokenAt(data, 0, tokenIndex, hiddenSize);
}

function sliceTokenAt(
  data: ArrayLike<number>,
  baseOffset: number,
  tokenIndex: number,
  hiddenSize: number,
): number[] {
  const start = baseOffset + tokenIndex * hiddenSize;
  const result = new Array<number>(hiddenSize);
  for (let i = 0; i < hiddenSize; i += 1) {
    result[i] = data[start + i];
  }
  return result;
}

/**
 * Extract a 1D mask slice for a given batch element from the attention mask
 * tensor.  Returns `undefined` when no mask is available (all tokens are
 * treated as real).
 */
function flattenMaskForBatchElement(
  mask: TensorLike | undefined,
  batchIndex: number,
  tokenCount: number,
): ArrayLike<number> | undefined {
  if (mask === undefined) {
    return undefined;
  }
  // 1D mask — applies directly.
  if (mask.dims.length === 1) {
    return mask.data;
  }
  // 2D mask [batchSize, seqLen] — slice the row for this batch element.
  if (mask.dims.length === 2) {
    const seqLen = mask.dims[1];
    const start = batchIndex * seqLen;
    const out = new Array<number>(tokenCount);
    for (let i = 0; i < tokenCount; i += 1) {
      out[i] = mask.data[start + i];
    }
    return out;
  }
  return undefined;
}

function meanPool(
  data: ArrayLike<number>,
  tokenCount: number,
  hiddenSize: number,
  mask?: ArrayLike<number>,
): number[] {
  return meanPoolAt(data, 0, tokenCount, hiddenSize, mask);
}

function meanPoolAt(
  data: ArrayLike<number>,
  baseOffset: number,
  tokenCount: number,
  hiddenSize: number,
  mask?: ArrayLike<number>,
): number[] {
  const result = new Array<number>(hiddenSize).fill(0);
  let maskSum = 0;
  for (let tokenIndex = 0; tokenIndex < tokenCount; tokenIndex += 1) {
    const weight = mask ? mask[tokenIndex] : 1;
    if (weight === 0) continue;
    maskSum += weight;
    const offset = baseOffset + tokenIndex * hiddenSize;
    for (let i = 0; i < hiddenSize; i += 1) {
      result[i] += data[offset + i] * weight;
    }
  }
  const divisor = maskSum > 0 ? maskSum : 1;
  for (let i = 0; i < hiddenSize; i += 1) {
    result[i] /= divisor;
  }
  return result;
}

function normalizeVector(vector: number[]): number[] {
  let magnitudeSquared = 0;
  for (const value of vector) {
    magnitudeSquared += value * value;
  }

  if (magnitudeSquared === 0) {
    return vector;
  }

  const magnitude = Math.sqrt(magnitudeSquared);
  return vector.map((value) => value / magnitude);
}
