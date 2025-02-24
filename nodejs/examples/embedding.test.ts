// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors
import { expect, test } from "@jest/globals";
// --8<-- [start:imports]
import * as lancedb from "@lancedb/lancedb";
import "@lancedb/lancedb/embedding/openai";
import { LanceSchema, getRegistry, register } from "@lancedb/lancedb/embedding";
import { EmbeddingFunction } from "@lancedb/lancedb/embedding";
import { type Float, Float32, Utf8 } from "apache-arrow";
// --8<-- [end:imports]
import { withTempDirectory } from "./util.ts";

const openAiTest = process.env.OPENAI_API_KEY == null ? test.skip : test;

openAiTest("openai embeddings", async () => {
  await withTempDirectory(async (databaseDir) => {
    // --8<-- [start:openai_embeddings]
    const db = await lancedb.connect(databaseDir);
    const func = getRegistry()
      .get("openai")
      ?.create({ model: "text-embedding-ada-002" }) as EmbeddingFunction;

    const wordsSchema = LanceSchema({
      text: func.sourceField(new Utf8()),
      vector: func.vectorField(),
    });
    const tbl = await db.createEmptyTable("words", wordsSchema, {
      mode: "overwrite",
    });
    await tbl.add([{ text: "hello world" }, { text: "goodbye world" }]);

    const query = "greetings";
    const actual = (await tbl.search(query).limit(1).toArray())[0];
    // --8<-- [end:openai_embeddings]
    expect(actual).toHaveProperty("text");
  });
});

test("custom embedding function", async () => {
  await withTempDirectory(async (databaseDir) => {
    // --8<-- [start:embedding_function]
    const db = await lancedb.connect(databaseDir);

    @register("my_embedding")
    class MyEmbeddingFunction extends EmbeddingFunction<string> {
      constructor(optionsRaw = {}) {
        super();
        const options = this.resolveVariables(optionsRaw);
        // Initialize using options
      }
      ndims() {
        return 3;
      }
      protected getSensitiveKeys(): string[] {
        return [];
      }
      embeddingDataType(): Float {
        return new Float32();
      }
      async computeQueryEmbeddings(_data: string) {
        // This is a placeholder for a real embedding function
        return [1, 2, 3];
      }
      async computeSourceEmbeddings(data: string[]) {
        // This is a placeholder for a real embedding function
        return Array.from({ length: data.length }).fill([
          1, 2, 3,
        ]) as number[][];
      }
    }

    const func = new MyEmbeddingFunction();

    const data = [{ text: "pepperoni" }, { text: "pineapple" }];

    // Option 1: manually specify the embedding function
    const table = await db.createTable("vectors", data, {
      embeddingFunction: {
        function: func,
        sourceColumn: "text",
        vectorColumn: "vector",
      },
      mode: "overwrite",
    });

    // Option 2: provide the embedding function through a schema

    const schema = LanceSchema({
      text: func.sourceField(new Utf8()),
      vector: func.vectorField(),
    });

    const table2 = await db.createTable("vectors2", data, {
      schema,
      mode: "overwrite",
    });
    // --8<-- [end:embedding_function]
    expect(await table.countRows()).toBe(2);
    expect(await table2.countRows()).toBe(2);
  });
});

test("embedding function api_key", async () => {
  // --8<-- [start:register_secret]
  const registry = getRegistry();
  registry.setVar("api_key", "sk-...");

  const func = registry.get("openai")!.create({
    apiKey: "$var:api_key",
  });
  // --8<-- [end:register_secret]
});
