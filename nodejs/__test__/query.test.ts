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
