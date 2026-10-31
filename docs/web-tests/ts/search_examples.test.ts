// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import { expect, test } from "@jest/globals";

test("complete vector_search", async () => {
  // --8<-- [start:vector_search_body]
  const lancedb = await import("@lancedb/lancedb");
  const { mkdtemp, rm } = await import("node:fs/promises");
  const { tmpdir } = await import("node:os");
  const { join } = await import("node:path");
  const uri = await mkdtemp(join(tmpdir(), "lancedb-search-"));
  try {
    const db = await lancedb.connect(uri);
    const table = await db.createTable("documents", [
      {
        id: 1,
        text: "A puppy playing in the park",
        category: "animals",
        vector: [1.0, 0.0],
      },
      {
        id: 2,
        text: "A dog resting at home",
        category: "animals",
        vector: [0.9, 0.1],
      },
      {
        id: 3,
        text: "A guide to growing tomatoes",
        category: "gardening",
        vector: [0.0, 1.0],
      },
    ]);
    const rows = await table
      .vectorSearch([1.0, 0.0])
      .distanceType("l2")
      .select(["id", "text", "_distance"])
      .limit(2)
      .toArray();
    console.log(rows);
    // --8<-- [end:vector_search_body]
    expect(rows.map((row) => row.id)).toEqual([1, 2]);
    expect(rows[0]._distance).toBe(0);
    // --8<-- [start:vector_search_tail]
  } finally {
    await rm(uri, { recursive: true, force: true });
  }
  // --8<-- [end:vector_search_tail]
});

test("complete text_search", async () => {
  // --8<-- [start:text_search_body]
  const lancedb = await import("@lancedb/lancedb");
  const { mkdtemp, rm } = await import("node:fs/promises");
  const { tmpdir } = await import("node:os");
  const { join } = await import("node:path");
  const uri = await mkdtemp(join(tmpdir(), "lancedb-search-"));
  try {
    const db = await lancedb.connect(uri);
    const table = await db.createTable("documents", [
      { id: 1, text: "A puppy playing in the park", category: "animals" },
      { id: 2, text: "A dog resting at home", category: "animals" },
      { id: 3, text: "A guide to growing tomatoes", category: "gardening" },
    ]);
    await table.createIndex("text", { config: lancedb.Index.fts() });
    const rows = await table
      .query()
      .fullTextSearch("puppy", { columns: ["text"] })
      .select(["id", "text", "_score"])
      .limit(2)
      .toArray();
    console.log(rows);
    // --8<-- [end:text_search_body]
    expect(rows.map((row) => row.id)).toEqual([1]);
    expect(rows[0]._score).toBeGreaterThan(0);
    // --8<-- [start:text_search_tail]
  } finally {
    await rm(uri, { recursive: true, force: true });
  }
  // --8<-- [end:text_search_tail]
});

test("complete hybrid_search", async () => {
  // --8<-- [start:hybrid_search_body]
  const lancedb = await import("@lancedb/lancedb");
  const { mkdtemp, rm } = await import("node:fs/promises");
  const { tmpdir } = await import("node:os");
  const { join } = await import("node:path");
  const uri = await mkdtemp(join(tmpdir(), "lancedb-search-"));
  try {
    const db = await lancedb.connect(uri);
    const table = await db.createTable("documents", [
      {
        id: 1,
        text: "A puppy playing in the park",
        category: "animals",
        vector: [1.0, 0.0],
      },
      {
        id: 2,
        text: "A dog resting at home",
        category: "animals",
        vector: [0.9, 0.1],
      },
      {
        id: 3,
        text: "A guide to growing tomatoes",
        category: "gardening",
        vector: [0.0, 1.0],
      },
    ]);
    await table.createIndex("text", { config: lancedb.Index.fts() });
    const reranker = await lancedb.rerankers.RRFReranker.create();
    const rows = await table
      .vectorSearch([1.0, 0.0])
      .fullTextSearch("puppy", { columns: ["text"] })
      .rerank(reranker)
      .limit(2)
      .toArray();
    console.log(rows);
    // --8<-- [end:hybrid_search_body]
    expect(rows.map((row) => row.id)).toEqual([1, 2]);
    expect(rows[0]._relevance_score).toBeGreaterThan(rows[1]._relevance_score);
    // --8<-- [start:hybrid_search_tail]
  } finally {
    await rm(uri, { recursive: true, force: true });
  }
  // --8<-- [end:hybrid_search_tail]
});

test("complete filter_search", async () => {
  // --8<-- [start:filter_search_body]
  const lancedb = await import("@lancedb/lancedb");
  const { mkdtemp, rm } = await import("node:fs/promises");
  const { tmpdir } = await import("node:os");
  const { join } = await import("node:path");
  const uri = await mkdtemp(join(tmpdir(), "lancedb-search-"));
  try {
    const db = await lancedb.connect(uri);
    const table = await db.createTable("documents", [
      {
        id: 1,
        text: "A puppy playing in the park",
        category: "animals",
        vector: [1.0, 0.0],
      },
      {
        id: 2,
        text: "A dog resting at home",
        category: "animals",
        vector: [0.9, 0.1],
      },
      {
        id: 3,
        text: "A guide to growing tomatoes",
        category: "gardening",
        vector: [0.0, 1.0],
      },
    ]);
    const rows = await table
      .vectorSearch([0.0, 1.0])
      .where("category = 'animals'")
      .select(["id", "text", "category", "_distance"])
      .limit(2)
      .toArray();
    console.log(rows);
    // --8<-- [end:filter_search_body]
    expect(rows.map((row) => row.id).sort()).toEqual([1, 2]);
    expect(rows.every((row) => row.category === "animals")).toBe(true);
    // --8<-- [start:filter_search_tail]
  } finally {
    await rm(uri, { recursive: true, force: true });
  }
  // --8<-- [end:filter_search_tail]
});

test("complete query_plan", async () => {
  // --8<-- [start:query_plan_body]
  const lancedb = await import("@lancedb/lancedb");
  const { mkdtemp, rm } = await import("node:fs/promises");
  const { tmpdir } = await import("node:os");
  const { join } = await import("node:path");
  const uri = await mkdtemp(join(tmpdir(), "lancedb-search-"));
  try {
    const db = await lancedb.connect(uri);
    const table = await db.createTable("documents", [
      {
        id: 1,
        text: "A puppy playing in the park",
        category: "animals",
        vector: [1.0, 0.0],
      },
      {
        id: 2,
        text: "A dog resting at home",
        category: "animals",
        vector: [0.9, 0.1],
      },
      {
        id: 3,
        text: "A guide to growing tomatoes",
        category: "gardening",
        vector: [0.0, 1.0],
      },
    ]);
    const query = table
      .vectorSearch([1.0, 0.0])
      .select(["id", "text", "_distance"])
      .limit(2);
    console.log("Plan:", await query.explainPlan());
    console.log("Measured execution:", await query.analyzePlan());
    const rows = await query.toArray();
    console.log(rows);
    // --8<-- [end:query_plan_body]
    expect(rows.map((row) => row.id)).toEqual([1, 2]);
    // --8<-- [start:query_plan_tail]
  } finally {
    await rm(uri, { recursive: true, force: true });
  }
  // --8<-- [end:query_plan_tail]
});
