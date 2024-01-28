// --8<-- [start:import]
import * as lancedb from "vectordb";
// --8<-- [end:import]
import * as fs from "fs";

async function setup() {
  fs.rmSync("data/sample-lancedb", { recursive: true, force: true });
  const db = await lancedb.connect("data/sample-lancedb");

  let data = [];
  for (let i = 0; i < 10_000; i++) {
    data.push({
      vector: Array(1536).fill(i),
      id: `${i}`,
      content: "",
      longId: `${i}`,
    });
  }
  await db.createTable("my_vectors", data);
}

async () => {
  await setup();

  // --8<-- [start:search1]
  const db = await lancedb.connect("data/sample-lancedb");
  const tbl = await db.openTable("my_vectors");

  const results_1 = await tbl.search(Array(1536).fill(1.2)).limit(10).execute();
  // --8<-- [end:search1]

  // --8<-- [start:search2]
  const results_2 = await tbl
    .search(Array(1536).fill(1.2))
    .metricType(lancedb.MetricType.Cosine)
    .limit(10)
    .execute();
  // --8<-- [end:search2]

  console.log("search: done");
};
