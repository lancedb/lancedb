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

import { type EmbeddingFunction } from "./embedding_function";
import type OpenAI from "openai";

export class OpenAIEmbeddingFunction implements EmbeddingFunction<string> {
  private readonly _openai: OpenAI;
  private readonly _modelName: string;

  constructor(
    sourceColumn: string,
    openAIKey: string,
    modelName: string = "text-embedding-ada-002",
  ) {
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

    this.sourceColumn = sourceColumn;
    const configuration = {
      apiKey: openAIKey,
    };

    this._openai = new Openai(configuration);
    this._modelName = modelName;
  }

  async embed(data: string[]): Promise<number[][]> {
    const response = await this._openai.embeddings.create({
      model: this._modelName,
      input: data,
    });

    const embeddings: number[][] = [];
    for (let i = 0; i < response.data.length; i++) {
      embeddings.push(response.data[i].embedding);
    }
    return embeddings;
  }

  sourceColumn: string;
}
