// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors
import { expect, test } from "@jest/globals";
// --8<-- [start:import]
import * as lancedb from "@lancedb/lancedb";
// --8<-- [end:import]
// --8<-- [start:import_bin_util]
import { packBits } from "@lancedb/lancedb/util";
// --8<-- [end:import_bin_util]
import { withTempDirectory } from "./util.ts";

test("vector search", async () => {
  await withTempDirectory(async (databaseDir) => {
    {
      const db = await lancedb.connect(databaseDir);

      const data = Array.from({ length: 10_000 }, (_, i) => ({
        vector: Array(128).fill(i),
        id: `${i}`,
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

    // --8<-- [start:distance_range]
    const results3 = await (
      tbl.search(Array(128).fill(1.2)) as lancedb.VectorQuery
    )
      .distanceType("cosine")
      .distanceRange(0.1, 0.2)
      .limit(10)
      .toArray();
    // --8<-- [end:distance_range]
    for (const r of results3) {
      expect(r.distance).toBeGreaterThanOrEqual(0.1);
      expect(r.distance).toBeLessThan(0.2);
    }

    {
      // --8<-- [start:ingest_binary_data]
      const data = Array.from({ length: 10_000 }, (_, i) => ({
        // the 256 bits would be store in 32 bytes,
        // if your data is already in this format, you can skip the packBits step
        vector: packBits(new Uint8Array(256).fill(i % 2)),
        id: `${i}`,
      }));

      await db.createTable("my_vectors", data);
      // --8<-- [end:ingest_binary_data]

      // --8<-- [start:search_binary_data]
      const query = Array(32)
        .fill(1)
        .map(() => Math.floor(Math.random() * 255));
      const results = await tbl.search(query).limit(10).toArrow();
      // --8<-- [end:search_binary_data
      expect(results.numRows).toBe(10);
    }
  });
});
