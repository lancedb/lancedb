import * as lancedb from "@lancedb/lancedb";

const db = await lancedb.connect("data/sample-lancedb");

const words = [
  "apple",
  "banana",
  "cherry",
  "date",
  "elderberry",
  "fig",
  "grape",
];

const data = Array.from({ length: 10_000 }, (_, i) => ({
  vector: Array(1536).fill(i),
  id: i,
  item: `item ${i}`,
  strId: `${i}`,
  doc: words[i % words.length],
}));

const tbl = await db.createTable("myVectors", data, { mode: "overwrite" });

await tbl.createIndex("doc", {
  config: lancedb.Index.fts(),
});

// --8<-- [start:full_text_search]
let result = await tbl
  .search("apple")
  .select(["id", "doc"])
  .limit(10)
  .toArray();
console.log(result);
// --8<-- [end:full_text_search]

console.log("SQL search: done");
