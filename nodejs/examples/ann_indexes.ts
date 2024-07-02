// --8<-- [start:import]
import * as lancedb from "@lancedb/lancedb";
// --8<-- [end:import]

// --8<-- [start:ingest]
const db = await lancedb.connect("/tmp/lancedb/");

const data = Array.from({ length: 10_000 }, (_, i) => ({
  vector: Array(1536).fill(i),
  id: `${i}`,
  content: "",
  longId: `${i}`,
}));

const table = await db.createTable("my_vectors", data, { mode: "overwrite" });
await table.createIndex("vector", {
  config: lancedb.Index.ivfPq({
    numPartitions: 16,
    numSubVectors: 48,
  }),
});
// --8<-- [end:ingest]

// --8<-- [start:search1]
const _results1 = await table
  .search(Array(1536).fill(1.2))
  .limit(2)
  .nprobes(20)
  .refineFactor(10)
  .toArray();
// --8<-- [end:search1]

// --8<-- [start:search2]
const _results2 = await table
  .search(Array(1536).fill(1.2))
  .where("id != '1141'")
  .limit(2)
  .toArray();
// --8<-- [end:search2]

// --8<-- [start:search3]
const _results3 = await table
  .search(Array(1536).fill(1.2))
  .select(["id"])
  .limit(2)
  .toArray();
// --8<-- [end:search3]

console.log("Ann indexes: done");
