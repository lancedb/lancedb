// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors
import { expect, test } from "@jest/globals";
import { withTempDirectory } from "./util.ts";

// --8<-- [start:quickstart_imports]
import * as lancedb from "@lancedb/lancedb";
import "@lancedb/lancedb/embedding/transformers";
import { Utf8 } from "apache-arrow";
// --8<-- [end:quickstart_imports]

test("full text search", async () => {
  await withTempDirectory(async (databaseDir) => {
    const db = await lancedb.connect(databaseDir);
    const func = (await lancedb.embedding
      .getRegistry()
      .get("huggingface")
      ?.create()) as lancedb.embedding.EmbeddingFunction;

    const facts = [
      "Albert Einstein was a theoretical physicist.",
      "The capital of France is Paris.",
      "The Great Wall of China is one of the Seven Wonders of the World.",
      "Python is a popular programming language.",
      "Mount Everest is the highest mountain in the world.",
      "Leonardo da Vinci painted the Mona Lisa.",
      "Shakespeare wrote Hamlet.",
      "The human body has 206 bones.",
      "The speed of light is approximately 299,792 kilometers per second.",
      "Water boils at 100 degrees Celsius.",
      "The Earth orbits the Sun.",
      "The Pyramids of Giza are located in Egypt.",
      "Coffee is one of the most popular beverages in the world.",
      "Tokyo is the capital city of Japan.",
      "Photosynthesis is the process by which plants make their food.",
      "The Pacific Ocean is the largest ocean on Earth.",
      "Mozart was a prolific composer of classical music.",
      "The Internet is a global network of computers.",
      "Basketball is a sport played with a ball and a hoop.",
      "The first computer virus was created in 1983.",
      "Artificial neural networks are inspired by the human brain.",
      "Deep learning is a subset of machine learning.",
      "IBM's Watson won Jeopardy! in 2011.",
      "The first computer programmer was Ada Lovelace.",
      "The first chatbot was ELIZA, created in the 1960s.",
    ].map((text) => ({ text }));

    const factsSchema = lancedb.embedding.LanceSchema({
      text: func.sourceField(new Utf8()),
      vector: func.vectorField(),
    });

    const tbl = await db.createTable("facts", facts, {
      mode: "overwrite",
      schema: factsSchema,
    });

    const query = "How many bones are in the human body?";
    const actual = await tbl.search(query).limit(1).toArray();

    expect(actual[0].text).toBe("The human body has 206 bones.");
  });
}, 100_000);

test.skip("embedding quickstart snippets", async () => {
  // --8<-- [start:quickstart_connect]
  const db = await lancedb.connect("data/sample-lancedb");
  // --8<-- [end:quickstart_connect]

  // --8<-- [start:quickstart_init_model]
  const model = (await lancedb.embedding
    .getRegistry()
    .get("huggingface")
    ?.create()) as lancedb.embedding.EmbeddingFunction;
  // --8<-- [end:quickstart_init_model]

  // --8<-- [start:quickstart_schema]
  const wordsSchema = lancedb.embedding.LanceSchema({
    text: model.sourceField(new Utf8()),
    vector: model.vectorField(),
  });
  // --8<-- [end:quickstart_schema]

  // --8<-- [start:quickstart_create_table]
  const table = await db.createEmptyTable("words", wordsSchema, {
    mode: "overwrite",
  });
  await table.add([{ text: "hello world" }, { text: "goodbye world" }]);
  // --8<-- [end:quickstart_create_table]

  // --8<-- [start:quickstart_query]
  const query = "greetings";
  const actual = (await table.search(query).limit(1).toArray())[0];
  console.log(actual.text);
  // --8<-- [end:quickstart_query]
});
