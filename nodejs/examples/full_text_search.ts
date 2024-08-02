import * as lancedb from "@lancedb/lancedb";

const db = await lancedb.connect("data/sample-lancedb");

const words = ["apple", "banana", "cherry", "date", "elderberry", "fig", "grape"];

const data = Array.from({ length: 10_000 }, (_, i) => ({
  vector: Array(1536).fill(i),
  id: i,
  item: `item ${i}`,
  strId: `${i}`,
  doc: words[i % words.length],
}));

const tbl = await db.createTable("myVectors", data, { mode: "overwrite" });

// --8<-- [start:full_text_search]
await tbl.query().fullTextSearch("apple").limit(10).toArray();
// --8<-- [end:full_text_search]

console.log("SQL search: done");
