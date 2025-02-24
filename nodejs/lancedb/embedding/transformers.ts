// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import { Float, Float32 } from "../arrow";
import { EmbeddingFunction } from "./embedding_function";
import { register } from "./registry";

export type XenovaTransformerOptions = {
  /** The wasm compatible model to use */
  model: string;
  /**
   * The wasm compatible tokenizer to use
   * If not provided, it will use the default tokenizer for the model
   */
  tokenizer?: string;
  /**
   * The number of dimensions of the embeddings
   *
   * We will attempt to infer this from the model config if not provided.
   * Since there isn't a standard way to get this information from the model,
   * you may need to manually specify this if using a model that doesn't have a 'hidden_size' in the config.
   * */
  ndims?: number;
  /** Options for the tokenizer */
  tokenizerOptions?: {
    textPair?: string | string[];
    padding?: boolean | "max_length";
    addSpecialTokens?: boolean;
    truncation?: boolean;
    maxLength?: number;
  };
};

@register("huggingface")
export class TransformersEmbeddingFunction extends EmbeddingFunction<
  string,
  Partial<XenovaTransformerOptions>
> {
  #model?: import("@huggingface/transformers").PreTrainedModel;
  #tokenizer?: import("@huggingface/transformers").PreTrainedTokenizer;
  #modelName: XenovaTransformerOptions["model"];
  #initialized = false;
  #tokenizerOptions: XenovaTransformerOptions["tokenizerOptions"];
  #ndims?: number;

  constructor(
    optionsRaw: Partial<XenovaTransformerOptions> = {
      model: "Xenova/all-MiniLM-L6-v2",
    },
  ) {
    super();
    const options = this.resolveVariables(optionsRaw);

    const modelName = options?.model ?? "Xenova/all-MiniLM-L6-v2";
    this.#tokenizerOptions = {
      padding: true,
      ...options.tokenizerOptions,
    };

    this.#ndims = options.ndims;
    this.#modelName = modelName;
  }

  async init() {
    let transformers;
    try {
      // SAFETY:
      // since typescript transpiles `import` to `require`, we need to do this in an unsafe way
      // We can't use `require` because `@huggingface/transformers` is an ESM module
      // and we can't use `import` directly because typescript will transpile it to `require`.
      // and we want to remain compatible with both ESM and CJS modules
      // so we use `eval` to bypass typescript for this specific import.
      transformers = await eval('import("@huggingface/transformers")');
    } catch (e) {
      throw new Error(`error loading @huggingface/transformers\nReason: ${e}`);
    }

    try {
      this.#model = await transformers.AutoModel.from_pretrained(
        this.#modelName,
        { dtype: "fp32" },
      );
    } catch (e) {
      throw new Error(
        `error loading model ${this.#modelName}. Make sure you are using a wasm compatible model.\nReason: ${e}`,
      );
    }
    try {
      this.#tokenizer = await transformers.AutoTokenizer.from_pretrained(
        this.#modelName,
      );
    } catch (e) {
      throw new Error(
        `error loading tokenizer for ${this.#modelName}. Make sure you are using a wasm compatible model:\nReason: ${e}`,
      );
    }
    this.#initialized = true;
  }

  ndims(): number {
    if (this.#ndims) {
      return this.#ndims;
    } else {
      const config = this.#model!.config;

      // biome-ignore lint/style/useNamingConvention: we don't control this name.
      const ndims = (config as unknown as { hidden_size: number }).hidden_size;
      if (!ndims) {
        throw new Error(
          "hidden_size not found in model config, you may need to manually specify the embedding dimensions. ",
        );
      }
      return ndims;
    }
  }

  embeddingDataType(): Float {
    return new Float32();
  }

  async computeSourceEmbeddings(data: string[]): Promise<number[][]> {
    // this should only happen if the user is trying to use the function directly.
    // Anything going through the registry should already be initialized.
    if (!this.#initialized) {
      return Promise.reject(
        new Error(
          "something went wrong: embedding function not initialized. Please call init()",
        ),
      );
    }
    const tokenizer = this.#tokenizer!;
    const model = this.#model!;

    const inputs = await tokenizer(data, this.#tokenizerOptions);
    let tokens = await model.forward(inputs);
    tokens = tokens[Object.keys(tokens)[0]];

    const [nItems, nTokens] = tokens.dims;

    tokens = tensorDiv(tokens.sum(1), nTokens);

    // TODO: support other data types
    const tokenData = tokens.data;
    const stride = this.ndims();

    const embeddings = [];
    for (let i = 0; i < nItems; i++) {
      const start = i * stride;
      const end = start + stride;
      const slice = tokenData.slice(start, end);
      embeddings.push(Array.from(slice) as number[]); // TODO: Avoid copy here
    }
    return embeddings;
  }

  async computeQueryEmbeddings(data: string): Promise<number[]> {
    return (await this.computeSourceEmbeddings([data]))[0];
  }
}

const tensorDiv = (
  src: import("@huggingface/transformers").Tensor,
  divBy: number,
) => {
  for (let i = 0; i < src.data.length; ++i) {
    src.data[i] /= divBy;
  }
  return src;
};
