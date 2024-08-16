// --8<-- [end:import]
import * as fs from "node:fs";
// --8<-- [start:import]
import * as lancedb from "@lancedb/lancedb";

async function setup() {
  fs.rmSync("data/sample-lancedb", { recursive: true, force: true });
  const db = await lancedb.connect("data/sample-lancedb");

  const data = Array.from({ length: 10_000 }, (_, i) => ({
    vector: Array(1536).fill(i),
    id: `${i}`,
    content: "",
    longId: `${i}`,
  }));

  await db.createTable("my_vectors", data);
}

await setup();

// --8<-- [start:search1]
const db = await lancedb.connect("data/sample-lancedb");
const tbl = await db.openTable("my_vectors");

const _results1 = await tbl.search(Array(1536).fill(1.2)).limit(10).toArray();
// --8<-- [end:search1]

// --8<-- [start:search2]
const _results2 = await tbl
  .search(Array(1536).fill(1.2))
  .distanceType("cosine")
  .limit(10)
  .toArray();
console.log(_results2);
// --8<-- [end:search2]

console.log("search: done");
