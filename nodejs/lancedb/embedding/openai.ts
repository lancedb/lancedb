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

import { EmbeddingFunction } from "./embedding_function";
import type OpenAI from "openai";
import { register } from "./registry";

export type OpenAIOptions =
  | {
      apiKey?: string;
      model?: string;
    }
  | string;

@register("openai")
export class OpenAIEmbeddingFunction extends EmbeddingFunction<string> {
  #openai: OpenAI;
  #modelName: string;

  constructor(options: OpenAIOptions = { model: "text-embedding-ada-002" }) {
    super();
    let openAIKey: string;
    let modelName = "text-embedding-ada-002";

    if (typeof options === "string") {
      openAIKey = options;
    } else {
      console.log({ options });
      openAIKey = options?.apiKey ?? "";
      modelName = options?.model ?? modelName;
    }
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
  }
  toJSON(): Record<string, any> {
    return {
      modelName: this.#modelName,
    };
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
        return null as never;
    }
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
