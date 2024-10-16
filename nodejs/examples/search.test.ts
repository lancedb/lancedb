// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors
import { expect, test } from "@jest/globals";
// --8<-- [start:import]
import * as lancedb from "@lancedb/lancedb";
// --8<-- [end:import]
import { withTempDirectory } from "./util.ts";

test("full text search", async () => {
  await withTempDirectory(async (databaseDir) => {
    {
      const db = await lancedb.connect(databaseDir);

      const data = Array.from({ length: 10_000 }, (_, i) => ({
        vector: Array(128).fill(i),
        id: `${i}`,
        content: "",
        longId: `${i}`,
      }));

      await db.createTable("my_vectors", data);
    }

    // --8<-- [start:search1]
    const db = await lancedb.connect(databaseDir);
    const tbl = await db.openTable("my_vectors");

    const results1 = await tbl.search(Array(128).fill(1.2)).limit(10).toArray();
    // --8<-- [end:search1]
    expect(results1.length).toBe(10);

    // --8<-- [start:search2]
    const results2 = await (
      tbl.search(Array(128).fill(1.2)) as lancedb.VectorQuery
    )
      .distanceType("cosine")
      .limit(10)
      .toArray();
    // --8<-- [end:search2]
    expect(results2.length).toBe(10);
  });
});
