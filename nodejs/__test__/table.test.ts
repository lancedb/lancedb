// Copyright 2024 Lance Developers.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import * as fs from "fs";
import * as path from "path";
import * as tmp from "tmp";

import { Table, connect } from "../dist";
import {
  Schema,
  Field,
  Float32,
  Int32,
  FixedSizeList,
  Int64,
  Float64,
} from "apache-arrow";
import { makeArrowTable } from "../dist/arrow";
import { Index } from "../dist/indices";

describe("Given a table", () => {
  let tmpDir: tmp.DirResult;
  let table: Table;
  const schema = new Schema([new Field("id", new Float64(), true)]);
  beforeEach(async () => {
    tmpDir = tmp.dirSync({ unsafeCleanup: true });
    const conn = await connect(tmpDir.name);
    table = await conn.createEmptyTable("some_table", schema);
  });
  afterEach(() => tmpDir.removeCallback());

  it("be displayable", async () => {
    expect(table.display()).toMatch(
      /NativeTable\(some_table, uri=.*, read_consistency_interval=None\)/,
    );
    table.close();
    expect(table.display()).toBe("ClosedTable(some_table)");
  });

  it("should let me add data", async () => {
    await table.add([{ id: 1 }, { id: 2 }]);
    await table.add([{ id: 1 }]);
    await expect(table.countRows()).resolves.toBe(3);
  });

  it("should overwrite data if asked", async () => {
    await table.add([{ id: 1 }, { id: 2 }]);
    await table.add([{ id: 1 }], { mode: "overwrite" });
    await expect(table.countRows()).resolves.toBe(1);
  });

  it("should let me close the table", async () => {
    expect(table.isOpen()).toBe(true);
    table.close();
    expect(table.isOpen()).toBe(false);
    expect(table.countRows()).rejects.toThrow("Table some_table is closed");
  });

  it("should let me update values", async () => {
    await table.add([{ id: 1 }]);
    expect(await table.countRows("id == 1")).toBe(1);
    expect(await table.countRows("id == 7")).toBe(0);
    await table.update({ id: "7" });
    expect(await table.countRows("id == 1")).toBe(0);
    expect(await table.countRows("id == 7")).toBe(1);
    await table.add([{ id: 2 }]);
    // Test Map as input
    await table.update(new Map(Object.entries({ id: "10" })), {
      where: "id % 2 == 0",
    });
    expect(await table.countRows("id == 2")).toBe(0);
    expect(await table.countRows("id == 7")).toBe(1);
    expect(await table.countRows("id == 10")).toBe(1);
  });
});

describe("When creating an index", () => {
  let tmpDir: tmp.DirResult;
  const schema = new Schema([
    new Field("id", new Int32(), true),
    new Field("vec", new FixedSizeList(32, new Field("item", new Float32()))),
  ]);
  let tbl: Table;
  let queryVec: number[];

  beforeEach(async () => {
    tmpDir = tmp.dirSync({ unsafeCleanup: true });
    const db = await connect(tmpDir.name);
    const data = makeArrowTable(
      Array(300)
        .fill(1)
        .map((_, i) => ({
          id: i,
          vec: Array(32)
            .fill(1)
            .map(() => Math.random()),
        })),
      {
        schema,
      },
    );
    queryVec = data.toArray()[5].vec.toJSON();
    tbl = await db.createTable("test", data);
  });
  afterEach(() => tmpDir.removeCallback());

  it("should create a vector index on vector columns", async () => {
    await tbl.createIndex("vec");

    // check index directory
    const indexDir = path.join(tmpDir.name, "test.lance", "_indices");
    expect(fs.readdirSync(indexDir)).toHaveLength(1);
    const indices = await tbl.listIndices();
    expect(indices.length).toBe(1);
    expect(indices[0]).toEqual({
      indexType: "IvfPq",
      columns: ["vec"],
    });

    // Search without specifying the column
    const rst = await tbl.query().nearestTo(queryVec).limit(2).toArrow();
    expect(rst.numRows).toBe(2);

    // Search with specifying the column
    const rst2 = await tbl.search(queryVec, "vec").limit(2).toArrow();
    expect(rst2.numRows).toBe(2);
    expect(rst.toString()).toEqual(rst2.toString());
  });

  it("should allow parameters to be specified", async () => {
    await tbl.createIndex("vec", {
      config: Index.ivfPq({
        numPartitions: 10,
      }),
    });

    // TODO: Verify parameters when we can load index config as part of list indices
  });

  it("should allow me to replace (or not) an existing index", async () => {
    await tbl.createIndex("id");
    // Default is replace=true
    await tbl.createIndex("id");
    await expect(tbl.createIndex("id", { replace: false })).rejects.toThrow(
      "already exists",
    );
    await tbl.createIndex("id", { replace: true });
  });

  test("should create a scalar index on scalar columns", async () => {
    await tbl.createIndex("id");
    const indexDir = path.join(tmpDir.name, "test.lance", "_indices");
    expect(fs.readdirSync(indexDir)).toHaveLength(1);

    for await (const r of tbl.query().filter("id > 1").select(["id"])) {
      expect(r.numRows).toBe(298);
    }
  });

  // TODO: Move this test to the query API test (making sure we can reject queries
  // when the dimension is incorrect)
  test("two columns with different dimensions", async () => {
    const db = await connect(tmpDir.name);
    const schema = new Schema([
      new Field("id", new Int32(), true),
      new Field("vec", new FixedSizeList(32, new Field("item", new Float32()))),
      new Field(
        "vec2",
        new FixedSizeList(64, new Field("item", new Float32())),
      ),
    ]);
    const tbl = await db.createTable(
      "two_vectors",
      makeArrowTable(
        Array(300)
          .fill(1)
          .map((_, i) => ({
            id: i,
            vec: Array(32)
              .fill(1)
              .map(() => Math.random()),
            vec2: Array(64) // different dimension
              .fill(1)
              .map(() => Math.random()),
          })),
        { schema },
      ),
    );

    // Only build index over v1
    await tbl.createIndex("vec", {
      config: Index.ivfPq({ numPartitions: 2, numSubVectors: 2 }),
    });

    const rst = await tbl
      .query()
      .nearestTo(
        Array(32)
          .fill(1)
          .map(() => Math.random()),
      )
      .limit(2)
      .toArrow();
    expect(rst.numRows).toBe(2);

    // Search with specifying the column
    await expect(
      tbl
        .search(
          Array(64)
            .fill(1)
            .map(() => Math.random()),
          "vec",
        )
        .limit(2)
        .toArrow(),
    ).rejects.toThrow(/.*does not match the dimension.*/);

    const query64 = Array(64)
      .fill(1)
      .map(() => Math.random());
    const rst64Query = await tbl.query().nearestTo(query64).limit(2).toArrow();
    const rst64Search = await tbl.search(query64, "vec2").limit(2).toArrow();
    expect(rst64Query.toString()).toEqual(rst64Search.toString());
    expect(rst64Query.numRows).toBe(2);
  });
});

describe("Read consistency interval", () => {
  let tmpDir: tmp.DirResult;
  beforeEach(() => {
    tmpDir = tmp.dirSync({ unsafeCleanup: true });
  });
  afterEach(() => tmpDir.removeCallback());

  // const intervals = [undefined, 0, 0.1];
  const intervals = [0];
  test.each(intervals)("read consistency interval %p", async (interval) => {
    const db = await connect(tmpDir.name);
    const table = await db.createTable("my_table", [{ id: 1 }]);

    const db2 = await connect(tmpDir.name, {
      readConsistencyInterval: interval,
    });
    const table2 = await db2.openTable("my_table");
    expect(await table2.countRows()).toEqual(await table.countRows());

    await table.add([{ id: 2 }]);

    if (interval === undefined) {
      expect(await table2.countRows()).toEqual(1);
      // TODO: once we implement time travel we can uncomment this part of the test.
      // await table2.checkout_latest();
      // expect(await table2.countRows()).toEqual(2);
    } else if (interval === 0) {
      expect(await table2.countRows()).toEqual(2);
    } else {
      // interval == 0.1
      expect(await table2.countRows()).toEqual(1);
      await new Promise((r) => setTimeout(r, 100));
      expect(await table2.countRows()).toEqual(2);
    }
  });
});

describe("schema evolution", function () {
  let tmpDir: tmp.DirResult;
  beforeEach(() => {
    tmpDir = tmp.dirSync({ unsafeCleanup: true });
  });
  afterEach(() => {
    tmpDir.removeCallback();
  });

  // Create a new sample table
  it("can add a new column to the schema", async function () {
    const con = await connect(tmpDir.name);
    const table = await con.createTable("vectors", [
      { id: 1n, vector: [0.1, 0.2] },
    ]);

    await table.addColumns([
      { name: "price", valueSql: "cast(10.0 as float)" },
    ]);

    const expectedSchema = new Schema([
      new Field("id", new Int64(), true),
      new Field(
        "vector",
        new FixedSizeList(2, new Field("item", new Float32(), true)),
        true,
      ),
      new Field("price", new Float32(), false),
    ]);
    expect(await table.schema()).toEqual(expectedSchema);
  });

  it("can alter the columns in the schema", async function () {
    const con = await connect(tmpDir.name);
    const schema = new Schema([
      new Field("id", new Int64(), true),
      new Field(
        "vector",
        new FixedSizeList(2, new Field("item", new Float32(), true)),
        true,
      ),
      new Field("price", new Float64(), false),
    ]);
    const table = await con.createTable("vectors", [
      { id: 1n, vector: [0.1, 0.2] },
    ]);
    // Can create a non-nullable column only through addColumns at the moment.
    await table.addColumns([
      { name: "price", valueSql: "cast(10.0 as double)" },
    ]);
    expect(await table.schema()).toEqual(schema);

    await table.alterColumns([
      { path: "id", rename: "new_id" },
      { path: "price", nullable: true },
    ]);

    const expectedSchema = new Schema([
      new Field("new_id", new Int64(), true),
      new Field(
        "vector",
        new FixedSizeList(2, new Field("item", new Float32(), true)),
        true,
      ),
      new Field("price", new Float64(), true),
    ]);
    expect(await table.schema()).toEqual(expectedSchema);
  });

  it("can drop a column from the schema", async function () {
    const con = await connect(tmpDir.name);
    const table = await con.createTable("vectors", [
      { id: 1n, vector: [0.1, 0.2] },
    ]);
    await table.dropColumns(["vector"]);

    const expectedSchema = new Schema([new Field("id", new Int64(), true)]);
    expect(await table.schema()).toEqual(expectedSchema);
  });
});

describe("when dealing with versioning", () => {
  let tmpDir: tmp.DirResult;
  beforeEach(() => {
    tmpDir = tmp.dirSync({ unsafeCleanup: true });
  });
  afterEach(() => {
    tmpDir.removeCallback();
  });

  it("can travel in time", async () => {
    // Setup
    const con = await connect(tmpDir.name);
    const table = await con.createTable("vectors", [
      { id: 1n, vector: [0.1, 0.2] },
    ]);
    const version = await table.version();
    await table.add([{ id: 2n, vector: [0.1, 0.2] }]);
    expect(await table.countRows()).toBe(2);
    // Make sure we can rewind
    await table.checkout(version);
    expect(await table.countRows()).toBe(1);
    // Can't add data in time travel mode
    await expect(table.add([{ id: 3n, vector: [0.1, 0.2] }])).rejects.toThrow(
      "table cannot be modified when a specific version is checked out",
    );
    // Can go back to normal mode
    await table.checkoutLatest();
    expect(await table.countRows()).toBe(2);
    // Should be able to add data again
    await table.add([{ id: 2n, vector: [0.1, 0.2] }]);
    expect(await table.countRows()).toBe(3);
    // Now checkout and restore
    await table.checkout(version);
    await table.restore();
    expect(await table.countRows()).toBe(1);
    // Should be able to add data
    await table.add([{ id: 2n, vector: [0.1, 0.2] }]);
    expect(await table.countRows()).toBe(2);
    // Can't use restore if not checked out
    await expect(table.restore()).rejects.toThrow(
      "checkout before running restore",
    );
  });
});
