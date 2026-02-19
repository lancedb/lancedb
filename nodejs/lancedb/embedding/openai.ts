// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import type OpenAI from "openai";
import type { EmbeddingCreateParams } from "openai/resources/index";
import { Float, Float32 } from "../arrow";
import { TextEmbeddingFunction } from "./embedding_function";
import { register } from "./registry";

export type OpenAIOptions = {
  apiKey: string;
  model: EmbeddingCreateParams["model"];
  maxRetries?: number;
  maxRequestsPerMinute?: number;
};

/**
 * OpenAI embedding function with built-in rate limiting and retry support.
 * 
 * @example
 * ```ts
 * // Create with default rate limiting (0.9 requests per second) and retries (7 attempts)
 * const embed = new OpenAIEmbeddingFunction({ 
 *   model: "text-embedding-3-small",
 *   apiKey: "your-api-key" 
 * });
 * 
 * // Or customize rate limiting and retries
 * const embed = new OpenAIEmbeddingFunction({ 
 *   model: "text-embedding-3-small",
 *   apiKey: "your-api-key",
 *   maxRequestsPerMinute: 60, // 60 requests per minute
 *   maxRetries: 5 // 5 retries with exponential backoff
 * });
 * 
 * // Then use with retry and rate limiting
 * const embeddings = await embed.computeSourceEmbeddingsWithRetry(["text1", "text2"]);
 * ```
 */
@register("openai")
export class OpenAIEmbeddingFunction extends TextEmbeddingFunction<
  Partial<OpenAIOptions>
> {
  #openai: OpenAI;
  #modelName: OpenAIOptions["model"];

  constructor(
    optionsRaw: Partial<OpenAIOptions> = {
      model: "text-embedding-ada-002",
    },
  ) {
    super();
    const options = this.resolveVariables(optionsRaw);

    const openAIKey = options?.apiKey ?? process.env.OPENAI_API_KEY;
    if (!openAIKey) {
      throw new Error("OpenAI API key is required");
    }
    const modelName = options?.model ?? "text-embedding-ada-002";

    /**
     * @type {import("openai").default}
     */
    // eslint-disable-next-line @typescript-eslint/naming-convention
    let Openai;
    try {
      // eslint-disable-next-line @typescript-eslint/no-var-requires
      Openai = require("openai");
    } catch {
      throw new Error("please install openai@^4.24.1 using npm install openai");
    }

    const configuration = {
      apiKey: openAIKey,
    };

    this.#openai = new Openai(configuration);
    this.#modelName = modelName;

    // Configure rate limiting based on options or use default
    if (options.maxRequestsPerMinute) {
      this.rateLimit(options.maxRequestsPerMinute / 60, 1.0);
    } else {
      // Default is 0.9 requests per second (54 per minute)
      this.rateLimit();
    }

    // Configure retry with exponential backoff
    if (options.maxRetries !== undefined) {
      this.retry(1, 2, true, options.maxRetries);
    } else {
      // Default is 7 retries with exponential backoff
      this.retry();
    }
  }

  protected getSensitiveKeys(): string[] {
    return ["apiKey"];
  }

  ndims(): number {
    switch (this.#modelName) {
      case "text-embedding-ada-002":
        return 1536;
      case "text-embedding-3-large":
        return 3072;
      case "text-embedding-3-small":
        return 1536;
      default:
        throw new Error(`Unknown model: ${this.#modelName}`);
    }
  }

  embeddingDataType(): Float {
    return new Float32();
  }

  async computeSourceEmbeddings(data: string[]): Promise<number[][]> {
    const response = await this.#openai.embeddings.create({
      model: this.#modelName,
      input: data,
    });

    const embeddings: number[][] = [];
    for (let i = 0; i < response.data.length; i++) {
      embeddings.push(response.data[i].embedding);
    }
    return embeddings;
  }

  async computeQueryEmbeddings(data: string): Promise<number[]> {
    if (typeof data !== "string") {
      throw new Error("Data must be a string");
    }
    const response = await this.#openai.embeddings.create({
      model: this.#modelName,
      input: data,
    });

    return response.data[0].embedding;
  }
}
