import { Utf8 } from "apache-arrow";
import * as lancedb from "../../";
import { LanceSchema, getRegistry } from "../../lancedb/embedding";

describe("Embedding", () => {
  test("basic", async () => {
    // --8<-- [start:openai_embeddings]
    const db = await lancedb.connect("/tmp/db");
    const func = getRegistry()
      .get("openai")!
      .create({ model: "text-embedding-ada-002" });

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
    expect(actual.text).toBe("hello world");
  });
});
