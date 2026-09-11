// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors
import { expect, test } from "@jest/globals";
import * as lancedb from "@lancedb/lancedb";
import { withTempDirectory } from "./util.ts";

test("filtering examples", async () => {
  await withTempDirectory(async (databaseDir) => {
    const db = await lancedb.connect(databaseDir);

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
    const result = await (
      tbl.search(Array(1536).fill(0)) as lancedb.VectorQuery
    )
      .where("(item IN ('item 0', 'item 2')) AND (id > 10)")
      .postfilter()
      .toArray();
    // --8<-- [end:vec_search]
    expect(result.length).toBe(0);

    // --8<-- [start:sql_search]
    await tbl.query().where("id = 10").limit(10).toArray();
    // --8<-- [end:sql_search]

    // --8<-- [start:orderby_search]
    await tbl
      .query()
      .where("id > 10")
      .orderBy({ columnName: "id", ascending: false })
      .limit(5)
      .toArray();
    // --8<-- [end:orderby_search]

    const ftsTable = await db.createTable("myFts", [
      { text: "Frodo was a happy puppy", category: "pet" },
      { text: "A puppy training guide", category: "guide" },
      { text: "There are several kittens playing", category: "pet" },
    ]);
    await ftsTable.createIndex("text", { config: lancedb.Index.fts() });

    // --8<-- [start:fts_prefilter]
    const ftsResults = await ftsTable
      .search("puppy", "fts")
      .where("category = 'pet'")
      .limit(10)
      .toArray();
    // --8<-- [end:fts_prefilter]
    expect(ftsResults).toHaveLength(1);
    expect(ftsResults[0].category).toBe("pet");
  });
});
