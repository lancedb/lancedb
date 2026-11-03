// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors
import { expect, test } from "@jest/globals";
import * as lancedb from "@lancedb/lancedb";
import { withTempDirectory } from "./util.ts";

test("full text search", async () => {
  await withTempDirectory(async (databaseDir) => {
    const db = await lancedb.connect(databaseDir);

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
    const result = await tbl
      .query()
      .nearestToText("apple")
      .select(["id", "doc"])
      .limit(10)
      .toArray();
    expect(result.length).toBe(10);
    // --8<-- [end:full_text_search]
  });
}, 10_000);
