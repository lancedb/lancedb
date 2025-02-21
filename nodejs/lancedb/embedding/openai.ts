// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import type OpenAI from "openai";
import type { EmbeddingCreateParams } from "openai/resources/index";
import { Float, Float32 } from "../arrow";
import { EmbeddingFunction } from "./embedding_function";
import { register } from "./registry";

export type OpenAIOptions = {
  apiKey: string;
  model: EmbeddingCreateParams["model"];
  dimensions: number;
  user: EmbeddingCreateParams["user"];
  /**
   * The format of the embedding vector. Defaults to "base64".
   */
  encodingFormat: EmbeddingCreateParams["encoding_format"];
};

@register("openai")
export class OpenAIEmbeddingFunction extends EmbeddingFunction<
  string,
  Partial<OpenAIOptions>
> {
  #openai: OpenAI;
  #modelName: OpenAIOptions["model"];
  #dimensions?: OpenAIOptions["dimensions"];
  #user?: OpenAIOptions["user"];
  #encodingFormat: OpenAIOptions["encodingFormat"];

  constructor(
    options: Partial<OpenAIOptions> = {
      model: "text-embedding-ada-002",
    },
  ) {
    super();
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
    this.#dimensions = options?.dimensions;
    this.#user = options?.user;
    // Default to base64 for efficiency.
    this.#encodingFormat = options?.encodingFormat ?? "base64";
  }

  toJSON() {
    return {
      model: this.#modelName,
      dimensions: this.#dimensions,
      user: this.#user,
      encodingFormat: this.#encodingFormat,
    };
  }

  ndims(): number {
    if (this.#dimensions) {
      return this.#dimensions;
    }
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

  async computeSourceEmbeddings(data: string[]): Promise<number[][] | Float32Array[]> {
    const response = await this.#openai.embeddings.create({
      model: this.#modelName,
      input: data,
      dimensions: this.#dimensions,
      user: this.#user,
    });

    const embeddings = [];
    for (let i = 0; i < response.data.length; i++) {
      embeddings.push(this.parseEmbedding(response.data[i].embedding));
    }
    return embeddings;
  }

  private parseEmbedding(embedding: string ): Float32Array;
  private parseEmbedding(embedding: number[]): number[];
  private parseEmbedding(embedding: string | number[]): number[] | Float32Array {
    if (this.#encodingFormat === "float" && Array.isArray(embedding)) {
      return embedding
    } else if (this.#encodingFormat === "base64" && typeof embedding === "string") {
      return new Float32Array(
        Buffer.from(embedding, 'base64').buffer
      );
    } else {
      throw new Error(`Unexpected embedding format: ${typeof embedding}`);
    }
  }

  async computeQueryEmbeddings(data: string): Promise<number[]> {
    if (typeof data !== "string") {
      throw new Error("Data must be a string");
    }
    const response = await this.#openai.embeddings.create({
      model: this.#modelName,
      input: data,
      dimensions: this.#dimensions,
      user: this.#user,
    });
    const embedding_raw = response.data[0].embedding;
    return this.parseEmbedding(embedding_raw);
  }
}
