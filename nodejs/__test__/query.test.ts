// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import * as tmp from "tmp";

import { type Table, connect } from "../lancedb";
import {
  Field,
  FixedSizeList,
  Float32,
  Int64,
  Schema,
  Utf8,
  makeArrowTable,
} from "../lancedb/arrow";
import { Index } from "../lancedb/indices";

describe("Query outputSchema", () => {
  let tmpDir: tmp.DirResult;
  let table: Table;

  beforeEach(async () => {
    tmpDir = tmp.dirSync({ unsafeCleanup: true });
    const db = await connect(tmpDir.name);

    // Create table with explicit schema to ensure proper types
    const schema = new Schema([
      new Field("a", new Int64(), true),
      new Field("text", new Utf8(), true),
      new Field(
        "vec",
        new FixedSizeList(2, new Field("item", new Float32())),
        true,
      ),
    ]);

    const data = makeArrowTable(
      [
        { a: 1n, text: "foo", vec: [1, 2] },
        { a: 2n, text: "bar", vec: [3, 4] },
        { a: 3n, text: "baz", vec: [5, 6] },
      ],
      { schema },
    );
    table = await db.createTable("test", data);
  });

  afterEach(() => {
    tmpDir.removeCallback();
  });

  it("should return schema for plain query", async () => {
    const schema = await table.query().outputSchema();

    expect(schema.fields.length).toBe(3);
    expect(schema.fields.map((f) => f.name)).toEqual(["a", "text", "vec"]);
    expect(schema.fields[0].type.toString()).toBe("Int64");
    expect(schema.fields[1].type.toString()).toBe("Utf8");
  });

  it("should return schema with dynamic projection", async () => {
    const schema = await table.query().select({ bl: "a * 2" }).outputSchema();

    expect(schema.fields.length).toBe(1);
    expect(schema.fields[0].name).toBe("bl");
    expect(schema.fields[0].type.toString()).toBe("Int64");
  });

  it("should return schema for vector search with _distance column", async () => {
    const schema = await table
      .vectorSearch([1, 2])
      .select(["a"])
      .outputSchema();

    expect(schema.fields.length).toBe(2);
    expect(schema.fields.map((f) => f.name)).toEqual(["a", "_distance"]);
    expect(schema.fields[0].type.toString()).toBe("Int64");
    expect(schema.fields[1].type.toString()).toBe("Float32");
  });

  it("should return schema for FTS search", async () => {
    await table.createIndex("text", { config: Index.fts() });

    const schema = await table
      .search("foo", "fts")
      .select(["a"])
      .outputSchema();

    // FTS search includes _score column in addition to selected columns
    expect(schema.fields.length).toBe(2);
    expect(schema.fields.map((f) => f.name)).toContain("a");
    expect(schema.fields.map((f) => f.name)).toContain("_score");
    const aField = schema.fields.find((f) => f.name === "a");
    expect(aField?.type.toString()).toBe("Int64");
  });

  it("should return schema for take query", async () => {
    const schema = await table.takeOffsets([0]).select(["text"]).outputSchema();

    expect(schema.fields.length).toBe(1);
    expect(schema.fields[0].name).toBe("text");
    expect(schema.fields[0].type.toString()).toBe("Utf8");
  });

  it("should return full schema when no select is specified", async () => {
    const schema = await table.query().outputSchema();

    // Should return all columns
    expect(schema.fields.length).toBe(3);
  });
});

describe("Query orderBy", () => {
  let tmpDir: tmp.DirResult;
  let table: Table;

  beforeEach(async () => {
    tmpDir = tmp.dirSync({ unsafeCleanup: true });
    const db = await connect(tmpDir.name);

    // Create table with numeric data for sorting
    const schema = new Schema([
      new Field("id", new Int64(), true),
      new Field("score", new Float32(), true),
      new Field("name", new Utf8(), true),
    ]);

    const data = makeArrowTable(
      [
        { id: 1n, score: 3.5, name: "charlie" },
        { id: 2n, score: 1.2, name: "alice" },
        { id: 3n, score: 2.8, name: "bob" },
        { id: 4n, score: 0.5, name: "david" },
        { id: 5n, score: 4.1, name: "eve" },
      ],
      { schema },
    );
    table = await db.createTable("test", data);
  });

  afterEach(() => {
    tmpDir.removeCallback();
  });

  it("should sort by single column ascending", async () => {
    const results = await table
      .query()
      .orderBy({ columnName: "score", ascending: true, nullsFirst: false })
      .toArray();

    expect(results.length).toBe(5);
    // Verify ascending order
    expect(results[0].score).toBeCloseTo(0.5, 0.001);
    expect(results[1].score).toBeCloseTo(1.2, 0.001);
    expect(results[2].score).toBeCloseTo(2.8, 0.001);
    expect(results[3].score).toBeCloseTo(3.5, 0.001);
    expect(results[4].score).toBeCloseTo(4.1, 0.001);
  });

  it("should sort by single column descending", async () => {
    const results = await table
      .query()
      .orderBy({ columnName: "score", ascending: false, nullsFirst: false })
      .toArray();

    expect(results.length).toBe(5);
    // Verify descending order
    expect(results[0].score).toBeCloseTo(4.1, 0.001);
    expect(results[1].score).toBeCloseTo(3.5, 0.001);
    expect(results[2].score).toBeCloseTo(2.8, 0.001);
    expect(results[3].score).toBeCloseTo(1.2, 0.001);
    expect(results[4].score).toBeCloseTo(0.5, 0.001);
  });

  it("should use ascending as default direction", async () => {
    const results = await table
      .query()
      .orderBy({ columnName: "score" })
      .toArray();

    expect(results.length).toBe(5);
    // Verify ascending order (default)
    expect(results[0].score).toBeCloseTo(0.5, 0.001);
    expect(results[1].score).toBeCloseTo(1.2, 0.001);
    expect(results[2].score).toBeCloseTo(2.8, 0.001);
    expect(results[3].score).toBeCloseTo(3.5, 0.001);
    expect(results[4].score).toBeCloseTo(4.1, 0.001);
  });

  it("should sort by string column", async () => {
    const results = await table
      .query()
      .orderBy({ columnName: "name" })
      .toArray();

    expect(results.length).toBe(5);
    // Verify alphabetical order
    expect(results[0].name).toBe("alice");
    expect(results[1].name).toBe("bob");
    expect(results[2].name).toBe("charlie");
    expect(results[3].name).toBe("david");
    expect(results[4].name).toBe("eve");
  });

  it("should support method chaining with where", async () => {
    const results = await table
      .query()
      .where("score > 2.0")
      .orderBy({ columnName: "score" })
      .toArray();
    expect(results.length).toBe(3);
    // Verify filtered and sorted
    expect(results[0].score).toBeCloseTo(2.8, 0.001);
    expect(results[1].score).toBeCloseTo(3.5, 0.001);
    expect(results[2].score).toBeCloseTo(4.1, 0.001);
  });

  it("should support method chaining with limit", async () => {
    const results = await table
      .query()
      .orderBy({ columnName: "score", ascending: false })
      .limit(3)
      .toArray();

    expect(results.length).toBe(3);
    // Verify top 3 in descending order
    expect(results[0].score).toBeCloseTo(4.1, 0.001);
    expect(results[1].score).toBeCloseTo(3.5, 0.001);
    expect(results[2].score).toBeCloseTo(2.8, 0.001);
  });

  it("should support method chaining with offset", async () => {
    const results = await table
      .query()
      .orderBy({ columnName: "score" })
      .offset(2)
      .limit(2)
      .toArray();

    expect(results.length).toBe(2);
    // Verify results skip first 2 and take next 2
    expect(results[0].score).toBeCloseTo(2.8, 0.001);
    expect(results[1].score).toBeCloseTo(3.5, 0.001);
  });

  it("should support method chaining with select", async () => {
    const results = await table
      .query()
      .orderBy({ columnName: "name" })
      .select(["name", "score"])
      .toArray();

    expect(results.length).toBe(5);
    // Verify only selected columns are present
    expect(Object.keys(results[0])).toEqual(["name", "score"]);
    expect(Object.keys(results[4])).toEqual(["name", "score"]);
    // Verify sorted by name
    expect(results[0].name).toBe("alice");
    expect(results[4].name).toBe("eve");
  });

  it("should support complex method chaining", async () => {
    const results = await table
      .query()
      .where("score > 1.0")
      .orderBy({ columnName: "score", ascending: false })
      .limit(3)
      .select(["id", "score", "name"])
      .toArray();

    expect(results.length).toBe(3);
    // Verify filtered, sorted, limited, and projected
    expect(results[0].score).toBeCloseTo(4.1, 0.001);
    expect(results[1].score).toBeCloseTo(3.5, 0.001);
    expect(results[2].score).toBeCloseTo(2.8, 0.001);
    expect(Object.keys(results[0])).toEqual(["id", "score", "name"]);
  });

  it("should support multi-column ordering and null placement", async () => {
    const schema = new Schema([
      new Field("group", new Int64(), true),
      new Field("score", new Float32(), true),
      new Field("name", new Utf8(), true),
    ]);

    const data = makeArrowTable(
      [
        { group: 1n, score: null, name: "z" },
        { group: 1n, score: 1.0, name: "b" },
        { group: 1n, score: 1.0, name: "a" },
        { group: 2n, score: 0.5, name: "c" },
      ],
      { schema },
    );
    const nullTable = await (await connect(tmpDir.name)).createTable(
      "test_multi_order",
      data,
      { mode: "overwrite" },
    );

    const results = await nullTable
      .query()
      .orderBy([
        { columnName: "group", ascending: true, nullsFirst: false },
        { columnName: "score", ascending: true, nullsFirst: true },
        { columnName: "name", ascending: true, nullsFirst: false },
      ])
      .toArray();

    expect(results.map((r) => [r.group, r.score, r.name])).toEqual([
      [1n, null, "z"],
      [1n, 1.0, "a"],
      [1n, 1.0, "b"],
      [2n, 0.5, "c"],
    ]);
  });
});
