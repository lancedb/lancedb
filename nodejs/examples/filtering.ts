import * as lancedb from "@lancedb/lancedb";

const db = await lancedb.connect("data/sample-lancedb");

const data = Array.from({ length: 10_000 }, (_, i) => ({
  vector: Array(1536).fill(i),
  id: i,
  item: `item ${i}`,
  strId: `${i}`,
}));

const tbl = await db.createTable("myVectors", data, { mode: "overwrite" });

// --8<-- [start:search]
const _result = await tbl
  .search(Array(1536).fill(0.5))
  .limit(1)
  .where("id = 10")
  .toArray();
// --8<-- [end:search]

// --8<-- [start:vec_search]
await tbl
  .search(Array(1536).fill(0))
  .where("(item IN ('item 0', 'item 2')) AND (id > 10)")
  .postfilter()
  .toArray();
// --8<-- [end:vec_search]

// --8<-- [start:sql_search]
await tbl.query().where("id = 10").limit(10).toArray();
// --8<-- [end:sql_search]

console.log("SQL search: done");
