// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors
import { expect, test } from "@jest/globals";
// --8<-- [start:import]
import * as lancedb from "@lancedb/lancedb";
import type { VectorQuery } from "@lancedb/lancedb";
// --8<-- [end:import]
import { withTempDirectory } from "./util.ts";

test("ann index examples", async () => {
  await withTempDirectory(async (databaseDir) => {
    // --8<-- [start:ingest]
    const db = await lancedb.connect(databaseDir);

    const data = Array.from({ length: 5_000 }, (_, i) => ({
      vector: Array(128).fill(i),
      id: `${i}`,
      content: "",
      longId: `${i}`,
    }));

    const table = await db.createTable("my_vectors", data, {
      mode: "overwrite",
    });
    await table.createIndex("vector", {
      config: lancedb.Index.ivfPq({
        numPartitions: 10,
        numSubVectors: 16,
      }),
    });
    // --8<-- [end:ingest]

    // --8<-- [start:search1]
    const search = table.search(Array(128).fill(1.2)).limit(2) as VectorQuery;
    const results1 = await search.nprobes(20).refineFactor(10).toArray();
    // --8<-- [end:search1]
    expect(results1.length).toBe(2);

    // --8<-- [start:search2]
    const results2 = await table
      .search(Array(128).fill(1.2))
      .where("id != '1141'")
      .limit(2)
      .toArray();
    // --8<-- [end:search2]
    expect(results2.length).toBe(2);

    // --8<-- [start:search3]
    const results3 = await table
      .search(Array(128).fill(1.2))
      .select(["id"])
      .limit(2)
      .toArray();
    // --8<-- [end:search3]
    expect(results3.length).toBe(2);
  });
}, 100_000);
