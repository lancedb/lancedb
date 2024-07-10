// --8<-- [start:imports]
import * as lancedb from "@lancedb/lancedb";
import { LanceSchema, getRegistry, register } from "@lancedb/lancedb/embedding";
import { EmbeddingFunction } from "@lancedb/lancedb/embedding";
import { type Float, Float32, Utf8 } from "apache-arrow";
// --8<-- [end:imports]

{
  // --8<-- [start:openai_embeddings]

  const db = await lancedb.connect("/tmp/db");
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
  const actual = (await (await tbl.search(query)).limit(1).toArray())[0];

  // --8<-- [end:openai_embeddings]
  console.log("result = ", actual.text);
}

{
  // --8<-- [start:embedding_function]
  const db = await lancedb.connect("/tmp/db");

  @register("my_embedding")
  class MyEmbeddingFunction extends EmbeddingFunction<string> {
    toJSON(): object {
      return {};
    }
    ndims() {
      return 3;
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
      return Array.from({ length: data.length }).fill([1, 2, 3]) as number[][];
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
}
