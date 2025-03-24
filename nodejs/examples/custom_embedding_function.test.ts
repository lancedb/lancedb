import {
  type FeatureExtractionPipeline,
  pipeline,
} from "@huggingface/transformers";
// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors
import { expect, test } from "@jest/globals";
// --8<-- [start:imports]
import * as lancedb from "@lancedb/lancedb";
import {
  LanceSchema,
  TextEmbeddingFunction,
  getRegistry,
  register,
} from "@lancedb/lancedb/embedding";
// --8<-- [end:imports]
import { withTempDirectory } from "./util.ts";

// --8<-- [start:embedding_impl]
@register("sentence-transformers")
class SentenceTransformersEmbeddings extends TextEmbeddingFunction {
  name = "Xenova/all-miniLM-L6-v2";
  #ndims!: number;
  extractor!: FeatureExtractionPipeline;

  async init() {
    this.extractor = await pipeline("feature-extraction", this.name, {
      dtype: "fp32",
    });
    this.#ndims = await this.generateEmbeddings(["hello"]).then(
      (e) => e[0].length,
    );
  }

  ndims() {
    return this.#ndims;
  }

  toJSON() {
    return {
      name: this.name,
    };
  }
  async generateEmbeddings(texts: string[]) {
    const output = await this.extractor(texts, {
      pooling: "mean",
      normalize: true,
    });
    return output.tolist();
  }
}
// -8<-- [end:embedding_impl]

test("Registry examples", async () => {
  await withTempDirectory(async (databaseDir) => {
    // --8<-- [start:call_custom_function]
    const registry = getRegistry();

    const sentenceTransformer = await registry
      .get<SentenceTransformersEmbeddings>("sentence-transformers")!
      .create();

    const schema = LanceSchema({
      vector: sentenceTransformer.vectorField(),
      text: sentenceTransformer.sourceField(),
    });

    const db = await lancedb.connect(databaseDir);
    const table = await db.createEmptyTable("table", schema, {
      mode: "overwrite",
    });

    await table.add([{ text: "hello" }, { text: "world" }]);

    const results = await table.search("greeting").limit(1).toArray();
    // -8<-- [end:call_custom_function]
    expect(results.length).toBe(1);
  });
}, 100_000);
