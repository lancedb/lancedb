// --8<-- [start:imports]
import * as lancedb from "@lancedb/lancedb";
import { LanceSchema, getRegistry } from "@lancedb/lancedb/embedding";
import type { EmbeddingFunction } from "@lancedb/lancedb/embedding";
import { Utf8 } from "apache-arrow";
// --8<-- [end:imports]

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
const _actual = (await (await tbl.search(query)).limit(1).toArray())[0];
// --8<-- [end:openai_embeddings]
