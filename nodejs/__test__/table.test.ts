// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import * as fs from "fs";
import * as path from "path";
import * as tmp from "tmp";

import * as arrow15 from "apache-arrow-15";
import * as arrow16 from "apache-arrow-16";
import * as arrow17 from "apache-arrow-17";
import * as arrow18 from "apache-arrow-18";

import { MatchQuery, PhraseQuery, Table, connect } from "../lancedb";
import {
  Table as ArrowTable,
  Field,
  FixedSizeList,
  Float32,
  Float64,
  Int32,
  Int64,
  List,
  Schema,
  Uint8,
  Utf8,
  makeArrowTable,
} from "../lancedb/arrow";
import * as arrow from "../lancedb/arrow";
import {
  EmbeddingFunction,
  LanceSchema,
  getRegistry,
  register,
} from "../lancedb/embedding";
import { Index } from "../lancedb/indices";
import {
  BooleanQuery,
  Occur,
  Operator,
  instanceOfFullTextQuery,
} from "../lancedb/query";
import exp = require("constants");

describe.each([arrow15, arrow16, arrow17, arrow18])(
  "Given a table",
  // biome-ignore lint/suspicious/noExplicitAny: <explanation>
  (arrow: any) => {
    let tmpDir: tmp.DirResult;
    let table: Table;

    const schema:
      | import("apache-arrow-15").Schema
      | import("apache-arrow-16").Schema
      | import("apache-arrow-17").Schema
      | import("apache-arrow-18").Schema = new arrow.Schema([
      new arrow.Field("id", new arrow.Float64(), true),
    ]);

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

    it("should show table stats", async () => {
      await table.add([{ id: 1 }, { id: 2 }]);
      await table.add([{ id: 1 }]);
      await expect(table.stats()).resolves.toEqual({
        fragmentStats: {
          lengths: {
            max: 2,
            mean: 1,
            min: 1,
            p25: 1,
            p50: 2,
            p75: 2,
            p99: 2,
          },
          numFragments: 2,
          numSmallFragments: 2,
        },
        numIndices: 0,
        numRows: 3,
        totalBytes: 24,
      });
    });

    it("should overwrite data if asked", async () => {
      const addRes = await table.add([{ id: 1 }, { id: 2 }]);
      expect(addRes).toHaveProperty("version");
      expect(addRes.version).toBe(2);
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
      const updateRes = await table.update({ id: "7" });
      expect(updateRes).toHaveProperty("version");
      expect(updateRes.version).toBe(3);
      expect(updateRes).toHaveProperty("rowsUpdated");
      expect(updateRes.rowsUpdated).toBe(1);
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

    it("should let me update values with `values`", async () => {
      await table.add([{ id: 1 }]);
      expect(await table.countRows("id == 1")).toBe(1);
      expect(await table.countRows("id == 7")).toBe(0);
      await table.update({ values: { id: 7 } });
      expect(await table.countRows("id == 1")).toBe(0);
      expect(await table.countRows("id == 7")).toBe(1);
      await table.add([{ id: 2 }]);
      // Test Map as input
      await table.update({
        values: {
          id: "10",
        },
        where: "id % 2 == 0",
      });
      expect(await table.countRows("id == 2")).toBe(0);
      expect(await table.countRows("id == 7")).toBe(1);
      expect(await table.countRows("id == 10")).toBe(1);
    });

    it("should let me update values with `valuesSql`", async () => {
      await table.add([{ id: 1 }]);
      expect(await table.countRows("id == 1")).toBe(1);
      expect(await table.countRows("id == 7")).toBe(0);
      await table.update({
        valuesSql: {
          id: "7",
        },
      });
      expect(await table.countRows("id == 1")).toBe(0);
      expect(await table.countRows("id == 7")).toBe(1);
      await table.add([{ id: 2 }]);
      // Test Map as input
      await table.update({
        valuesSql: {
          id: "10",
        },
        where: "id % 2 == 0",
      });
      expect(await table.countRows("id == 2")).toBe(0);
      expect(await table.countRows("id == 7")).toBe(1);
      expect(await table.countRows("id == 10")).toBe(1);
    });

    // https://github.com/lancedb/lancedb/issues/1293
    test.each([new arrow.Float16(), new arrow.Float32(), new arrow.Float64()])(
      "can create empty table with non default float type: %s",
      async (floatType) => {
        const db = await connect(tmpDir.name);

        const data = [
          { text: "hello", vector: Array(512).fill(1.0) },
          { text: "hello world", vector: Array(512).fill(1.0) },
        ];
        const f64Schema = new arrow.Schema([
          new arrow.Field("text", new arrow.Utf8(), true),
          new arrow.Field(
            "vector",
            new arrow.FixedSizeList(512, new arrow.Field("item", floatType)),
            true,
          ),
        ]);

        const f64Table = await db.createEmptyTable("f64", f64Schema, {
          mode: "overwrite",
        });
        try {
          await f64Table.add(data);
          const res = await f64Table.query().toArray();
          expect(res.length).toBe(2);
        } catch (e) {
          expect(e).toBeUndefined();
        }
      },
    );

    // TODO: https://github.com/lancedb/lancedb/issues/1832
    it.skip("should be able to omit nullable fields", async () => {
      const db = await connect(tmpDir.name);
      const schema = new arrow.Schema([
        new arrow.Field(
          "vector",
          new arrow.FixedSizeList(
            2,
            new arrow.Field("item", new arrow.Float64()),
          ),
          true,
        ),
        new arrow.Field("item", new arrow.Utf8(), true),
        new arrow.Field("price", new arrow.Float64(), false),
      ]);
      const table = await db.createEmptyTable("test", schema);

      const data1 = { item: "foo", price: 10.0 };
      await table.add([data1]);
      const data2 = { vector: [3.1, 4.1], price: 2.0 };
      await table.add([data2]);
      const data3 = { vector: [5.9, 26.5], item: "bar", price: 3.0 };
      await table.add([data3]);

      let res = await table.query().limit(10).toArray();
      const resVector = res.map((r) => r.get("vector").toArray());
      expect(resVector).toEqual([null, data2.vector, data3.vector]);
      const resItem = res.map((r) => r.get("item").toArray());
      expect(resItem).toEqual(["foo", null, "bar"]);
      const resPrice = res.map((r) => r.get("price").toArray());
      expect(resPrice).toEqual([10.0, 2.0, 3.0]);

      const data4 = { item: "foo" };
      // We can't omit a column if it's not nullable
      await expect(table.add([data4])).rejects.toThrow("Invalid user input");

      // But we can alter columns to make them nullable
      await table.alterColumns([{ path: "price", nullable: true }]);
      await table.add([data4]);

      res = (await table.query().limit(10).toArray()).map((r) => r.toJSON());
      expect(res).toEqual([data1, data2, data3, data4]);
    });

    it("should be able to insert nullable data for non-nullable fields", async () => {
      const db = await connect(tmpDir.name);
      const schema = new arrow.Schema([
        new arrow.Field("x", new arrow.Float64(), false),
        new arrow.Field("id", new arrow.Utf8(), false),
      ]);
      const table = await db.createEmptyTable("test", schema);

      const data1 = { x: 4.1, id: "foo" };
      await table.add([data1]);
      const res = (await table.query().toArray())[0];
      expect(res.x).toEqual(data1.x);
      expect(res.id).toEqual(data1.id);

      const data2 = { x: null, id: "bar" };
      await expect(table.add([data2])).rejects.toThrow(
        "declared as non-nullable but contains null values",
      );

      // But we can alter columns to make them nullable
      await table.alterColumns([{ path: "x", nullable: true }]);
      await table.add([data2]);

      const res2 = await table.query().toArray();
      expect(res2.length).toBe(2);
      expect(res2[0].x).toEqual(data1.x);
      expect(res2[0].id).toEqual(data1.id);
      expect(res2[1].x).toBeNull();
      expect(res2[1].id).toEqual(data2.id);
    });

    it("should return the table as an instance of an arrow table", async () => {
      const arrowTbl = await table.toArrow();
      expect(arrowTbl).toBeInstanceOf(ArrowTable);
    });

    it("should be able to handle missing fields", async () => {
      const schema = new arrow.Schema([
        new arrow.Field("id", new arrow.Int32(), true),
        new arrow.Field("y", new arrow.Int32(), true),
        new arrow.Field("z", new arrow.Int64(), true),
      ]);
      const db = await connect(tmpDir.name);
      const table = await db.createEmptyTable("testNull", schema);
      await table.add([{ id: 1, y: 2 }]);
      await table.add([{ id: 2 }]);

      await table
        .mergeInsert("id")
        .whenNotMatchedInsertAll()
        .execute([
          { id: 3, z: 3 },
          { id: 4, z: 5 },
        ]);

      const res = await table.query().toArrow();
      expect(res.getChild("id")?.toJSON()).toEqual([1, 2, 3, 4]);
      expect(res.getChild("y")?.toJSON()).toEqual([2, null, null, null]);
      expect(res.getChild("z")?.toJSON()).toEqual([null, null, 3n, 5n]);
    });

    it("should handle null vectors at end of data", async () => {
      // https://github.com/lancedb/lancedb/issues/2240
      const data = [{ vector: [1, 2, 3] }, { vector: null }];
      const db = await connect("memory://");

      const table = await db.createTable("my_table", data);
      expect(await table.countRows()).toEqual(2);
    });
  },
);

describe("merge insert", () => {
  let tmpDir: tmp.DirResult;
  let table: Table;

  beforeEach(async () => {
    tmpDir = tmp.dirSync({ unsafeCleanup: true });
    const conn = await connect(tmpDir.name);

    table = await conn.createTable("some_table", [
      { a: 1, b: "a" },
      { a: 2, b: "b" },
      { a: 3, b: "c" },
    ]);
  });
  afterEach(() => tmpDir.removeCallback());

  test("upsert", async () => {
    const newData = [
      { a: 2, b: "x" },
      { a: 3, b: "y" },
      { a: 4, b: "z" },
    ];
    const mergeInsertRes = await table
      .mergeInsert("a")
      .whenMatchedUpdateAll()
      .whenNotMatchedInsertAll()
      .execute(newData, { timeoutMs: 10_000 });
    expect(mergeInsertRes).toHaveProperty("version");
    expect(mergeInsertRes.version).toBe(2);
    expect(mergeInsertRes.numInsertedRows).toBe(1);
    expect(mergeInsertRes.numUpdatedRows).toBe(2);
    expect(mergeInsertRes.numDeletedRows).toBe(0);

    const expected = [
      { a: 1, b: "a" },
      { a: 2, b: "x" },
      { a: 3, b: "y" },
      { a: 4, b: "z" },
    ];

    const result = (await table.toArrow()).toArray().sort((a, b) => a.a - b.a);

    expect(result.map((row) => ({ ...row }))).toEqual(expected);
  });
  test("conditional update", async () => {
    const newData = [
      { a: 2, b: "x" },
      { a: 3, b: "y" },
      { a: 4, b: "z" },
    ];
    const mergeInsertRes = await table
      .mergeInsert("a")
      .whenMatchedUpdateAll({ where: "target.b = 'b'" })
      .execute(newData);
    expect(mergeInsertRes).toHaveProperty("version");
    expect(mergeInsertRes.version).toBe(2);

    const expected = [
      { a: 1, b: "a" },
      { a: 2, b: "x" },
      { a: 3, b: "c" },
    ];
    // round trip to arrow and back to json to avoid comparing arrow objects to js object
    // biome-ignore lint/suspicious/noExplicitAny: test
    let res: any[] = JSON.parse(
      JSON.stringify((await table.toArrow()).toArray()),
    );
    res = res.sort((a, b) => a.a - b.a);

    expect(res).toEqual(expected);
  });

  test("insert if not exists", async () => {
    const newData = [
      { a: 2, b: "x" },
      { a: 3, b: "y" },
      { a: 4, b: "z" },
    ];
    await table.mergeInsert("a").whenNotMatchedInsertAll().execute(newData);
    const expected = [
      { a: 1, b: "a" },
      { a: 2, b: "b" },
      { a: 3, b: "c" },
      { a: 4, b: "z" },
    ];
    // biome-ignore lint/suspicious/noExplicitAny: <explanation>
    let res: any[] = JSON.parse(
      JSON.stringify((await table.toArrow()).toArray()),
    );
    res = res.sort((a, b) => a.a - b.a);
    expect(res).toEqual(expected);
  });
  test("replace range", async () => {
    const newData = [
      { a: 2, b: "x" },
      { a: 4, b: "z" },
    ];
    await table
      .mergeInsert("a")
      .whenMatchedUpdateAll()
      .whenNotMatchedInsertAll()
      .whenNotMatchedBySourceDelete({ where: "a > 2" })
      .execute(newData);

    const expected = [
      { a: 1, b: "a" },
      { a: 2, b: "x" },
      { a: 4, b: "z" },
    ];
    // biome-ignore lint/suspicious/noExplicitAny: <explanation>
    let res: any[] = JSON.parse(
      JSON.stringify((await table.toArrow()).toArray()),
    );
    res = res.sort((a, b) => a.a - b.a);
    expect(res).toEqual(expected);
  });
  test("replace range no condition", async () => {
    const newData = [
      { a: 2, b: "x" },
      { a: 4, b: "z" },
    ];
    await table
      .mergeInsert("a")
      .whenMatchedUpdateAll()
      .whenNotMatchedInsertAll()
      .whenNotMatchedBySourceDelete()
      .execute(newData);

    const expected = [
      { a: 2, b: "x" },
      { a: 4, b: "z" },
    ];

    // biome-ignore lint/suspicious/noExplicitAny: test
    let res: any[] = JSON.parse(
      JSON.stringify((await table.toArrow()).toArray()),
    );
    res = res.sort((a, b) => a.a - b.a);
    expect(res).toEqual(expected);
  });

  test("timeout", async () => {
    const newData = [
      { a: 2, b: "x" },
      { a: 4, b: "z" },
    ];
    await expect(
      table
        .mergeInsert("a")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute(newData, { timeoutMs: 0 }),
    ).rejects.toThrow("merge insert timed out");
  });
});

describe("When creating an index", () => {
  let tmpDir: tmp.DirResult;
  const schema = new Schema([
    new Field("id", new Int32(), true),
    new Field("vec", new FixedSizeList(32, new Field("item", new Float32()))),
    new Field("tags", new List(new Field("item", new Utf8(), true))),
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
          tags: ["tag1", "tag2", "tag3"],
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
      name: "vec_idx",
      indexType: "IvfPq",
      columns: ["vec"],
    });
    const stats = await tbl.indexStats("vec_idx");
    expect(stats?.loss).toBeDefined();

    // Search without specifying the column
    let rst = await tbl
      .query()
      .limit(2)
      .nearestTo(queryVec)
      .distanceType("dot")
      .toArrow();
    expect(rst.numRows).toBe(2);

    // Search using `vectorSearch`
    rst = await tbl.vectorSearch(queryVec).limit(2).toArrow();
    expect(rst.numRows).toBe(2);

    // Search with specifying the column
    const rst2 = await tbl
      .query()
      .limit(2)
      .nearestTo(queryVec)
      .column("vec")
      .toArrow();
    expect(rst2.numRows).toBe(2);
    expect(rst.toString()).toEqual(rst2.toString());

    // test offset
    rst = await tbl.query().limit(2).offset(1).nearestTo(queryVec).toArrow();
    expect(rst.numRows).toBe(1);

    // test nprobes
    rst = await tbl.query().nearestTo(queryVec).limit(2).nprobes(50).toArrow();
    expect(rst.numRows).toBe(2);
    rst = await tbl
      .query()
      .nearestTo(queryVec)
      .limit(2)
      .minimumNprobes(15)
      .toArrow();
    expect(rst.numRows).toBe(2);
    rst = await tbl
      .query()
      .nearestTo(queryVec)
      .limit(2)
      .minimumNprobes(10)
      .maximumNprobes(20)
      .toArrow();
    expect(rst.numRows).toBe(2);

    expect(() => tbl.query().nearestTo(queryVec).minimumNprobes(0)).toThrow(
      "Invalid input, minimum_nprobes must be greater than 0",
    );
    expect(() => tbl.query().nearestTo(queryVec).maximumNprobes(5)).toThrow(
      "Invalid input, maximum_nprobes must be greater than minimum_nprobes",
    );

    await tbl.dropIndex("vec_idx");
    const indices2 = await tbl.listIndices();
    expect(indices2.length).toBe(0);
  });

  it("should wait for index readiness", async () => {
    // Create an index and then wait for it to be ready
    await tbl.createIndex("vec");
    const indices = await tbl.listIndices();
    expect(indices.length).toBeGreaterThan(0);
    const idxName = indices[0].name;
    await expect(tbl.waitForIndex([idxName], 5)).resolves.toBeUndefined();
  });

  it("should search with distance range", async () => {
    await tbl.createIndex("vec");

    const rst = await tbl.query().limit(10).nearestTo(queryVec).toArrow();
    const distanceColumn = rst.getChild("_distance");
    let minDist = undefined;
    let maxDist = undefined;
    if (distanceColumn) {
      minDist = distanceColumn.get(0);
      maxDist = distanceColumn.get(9);
    }

    const rst2 = await tbl
      .query()
      .limit(10)
      .nearestTo(queryVec)
      .distanceRange(minDist, maxDist)
      .toArrow();
    const distanceColumn2 = rst2.getChild("_distance");
    expect(distanceColumn2).toBeDefined();
    if (distanceColumn2) {
      for await (const d of distanceColumn2) {
        expect(d).toBeGreaterThanOrEqual(minDist);
        expect(d).toBeLessThan(maxDist);
      }
    }

    const rst3 = await tbl
      .query()
      .limit(10)
      .nearestTo(queryVec)
      .distanceRange(maxDist, undefined)
      .toArrow();
    const distanceColumn3 = rst3.getChild("_distance");
    expect(distanceColumn3).toBeDefined();
    if (distanceColumn3) {
      for await (const d of distanceColumn3) {
        expect(d).toBeGreaterThanOrEqual(maxDist);
      }
    }

    const rst4 = await tbl
      .query()
      .limit(10)
      .nearestTo(queryVec)
      .distanceRange(undefined, minDist)
      .toArrow();
    const distanceColumn4 = rst4.getChild("_distance");
    expect(distanceColumn4).toBeDefined();
    if (distanceColumn4) {
      for await (const d of distanceColumn4) {
        expect(d).toBeLessThan(minDist);
      }
    }
  });

  it("should create and search IVF_HNSW indices", async () => {
    await tbl.createIndex("vec", {
      config: Index.hnswSq(),
    });

    // check index directory
    const indexDir = path.join(tmpDir.name, "test.lance", "_indices");
    expect(fs.readdirSync(indexDir)).toHaveLength(1);
    const indices = await tbl.listIndices();
    expect(indices.length).toBe(1);
    expect(indices[0]).toEqual({
      name: "vec_idx",
      indexType: "IvfHnswSq",
      columns: ["vec"],
    });

    // Search without specifying the column
    let rst = await tbl
      .query()
      .limit(2)
      .nearestTo(queryVec)
      .distanceType("dot")
      .toArrow();
    expect(rst.numRows).toBe(2);

    // Search using `vectorSearch`
    rst = await tbl.vectorSearch(queryVec).limit(2).toArrow();
    expect(rst.numRows).toBe(2);

    // Search with specifying the column
    const rst2 = await tbl
      .query()
      .limit(2)
      .nearestTo(queryVec)
      .column("vec")
      .toArrow();
    expect(rst2.numRows).toBe(2);
    expect(rst.toString()).toEqual(rst2.toString());

    // test offset
    rst = await tbl.query().limit(2).offset(1).nearestTo(queryVec).toArrow();
    expect(rst.numRows).toBe(1);

    // test ef
    rst = await tbl.query().limit(2).nearestTo(queryVec).ef(100).toArrow();
    expect(rst.numRows).toBe(2);
  });

  it("should be able to query unindexed data", async () => {
    await tbl.createIndex("vec");
    await tbl.add([
      {
        id: 300,
        vec: Array(32)
          .fill(1)
          .map(() => Math.random()),
        tags: [],
      },
    ]);

    const plan1 = await tbl.query().nearestTo(queryVec).explainPlan(true);
    expect(plan1).toMatch("LanceScan");

    const plan2 = await tbl
      .query()
      .nearestTo(queryVec)
      .fastSearch()
      .explainPlan(true);
    expect(plan2).not.toMatch("LanceScan");
  });

  it("should be able to run analyze plan", async () => {
    await tbl.createIndex("vec");
    await tbl.add([
      {
        id: 300,
        vec: Array(32)
          .fill(1)
          .map(() => Math.random()),
        tags: [],
      },
    ]);

    const plan = await tbl.query().nearestTo(queryVec).analyzePlan();
    expect(plan).toMatch("AnalyzeExec");
    expect(plan).toMatch("metrics=");
  });

  it("should be able to query with row id", async () => {
    const results = await tbl
      .query()
      .nearestTo(queryVec)
      .withRowId()
      .limit(1)
      .toArray();
    expect(results.length).toBe(1);
    expect(results[0]).toHaveProperty("_rowid");
  });

  it("should allow parameters to be specified", async () => {
    await tbl.createIndex("vec", {
      config: Index.ivfPq({
        numPartitions: 10,
      }),
    });

    // TODO: Verify parameters when we can load index config as part of list indices
  });

  it("should be able to create 4bit IVF_PQ", async () => {
    await tbl.createIndex("vec", {
      config: Index.ivfPq({
        numPartitions: 10,
        numBits: 4,
      }),
    });
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

    for await (const r of tbl.query().where("id > 1").select(["id"])) {
      expect(r.numRows).toBe(298);
    }
    // should also work with 'filter' alias
    for await (const r of tbl.query().filter("id > 1").select(["id"])) {
      expect(r.numRows).toBe(298);
    }
  });

  test("create a bitmap index", async () => {
    await tbl.createIndex("id", {
      config: Index.bitmap(),
    });
    const indexDir = path.join(tmpDir.name, "test.lance", "_indices");
    expect(fs.readdirSync(indexDir)).toHaveLength(1);
  });

  test("create a hnswPq index", async () => {
    await tbl.createIndex("vec", {
      config: Index.hnswPq({
        numPartitions: 10,
      }),
    });
    const indexDir = path.join(tmpDir.name, "test.lance", "_indices");
    expect(fs.readdirSync(indexDir)).toHaveLength(1);
  });

  test("create a HnswSq index", async () => {
    await tbl.createIndex("vec", {
      config: Index.hnswSq({
        numPartitions: 10,
      }),
    });
    const indexDir = path.join(tmpDir.name, "test.lance", "_indices");
    expect(fs.readdirSync(indexDir)).toHaveLength(1);
  });

  test("create a label list index", async () => {
    await tbl.createIndex("tags", {
      config: Index.labelList(),
    });
    const indexDir = path.join(tmpDir.name, "test.lance", "_indices");
    expect(fs.readdirSync(indexDir)).toHaveLength(1);
  });

  test("should be able to get index stats", async () => {
    await tbl.createIndex("id");

    const stats = await tbl.indexStats("id_idx");
    expect(stats).toBeDefined();
    expect(stats?.numIndexedRows).toEqual(300);
    expect(stats?.numUnindexedRows).toEqual(0);
    expect(stats?.distanceType).toBeUndefined();
    expect(stats?.indexType).toEqual("BTREE");
    expect(stats?.numIndices).toEqual(1);
    expect(stats?.loss).toBeUndefined();
  });

  test("when getting stats on non-existent index", async () => {
    const stats = await tbl.indexStats("some non-existent index");
    expect(stats).toBeUndefined();
  });

  test("create ivf_flat with binary vectors", async () => {
    const db = await connect(tmpDir.name);
    const binarySchema = new Schema([
      new Field("id", new Int32(), true),
      new Field("vec", new FixedSizeList(32, new Field("item", new Uint8()))),
    ]);
    const tbl = await db.createTable(
      "binary",
      makeArrowTable(
        Array(300)
          .fill(1)
          .map((_, i) => ({
            id: i,
            vec: Array(32)
              .fill(1)
              .map(() => Math.floor(Math.random() * 255)),
          })),
        { schema: binarySchema },
      ),
    );
    await tbl.createIndex("vec", {
      config: Index.ivfFlat({ numPartitions: 10, distanceType: "hamming" }),
    });

    // query with binary vectors
    const queryVec = Array(32)
      .fill(1)
      .map(() => Math.floor(Math.random() * 255));
    const rst = await tbl.query().limit(5).nearestTo(queryVec).toArrow();
    expect(rst.numRows).toBe(5);
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
      waitTimeoutSeconds: 30,
    });

    const rst = await tbl
      .query()
      .limit(2)
      .nearestTo(
        Array(32)
          .fill(1)
          .map(() => Math.random()),
      )
      .toArrow();
    expect(rst.numRows).toBe(2);

    // Search with specifying the column
    await expect(
      tbl
        .query()
        .limit(2)
        .nearestTo(
          Array(64)
            .fill(1)
            .map(() => Math.random()),
        )
        .column("vec")
        .toArrow(),
    ).rejects.toThrow(
      /.* query dim\(64\) doesn't match the column vec vector dim\(32\).*/,
    );

    const query64 = Array(64)
      .fill(1)
      .map(() => Math.random());
    const rst64Query = await tbl.query().limit(2).nearestTo(query64).toArrow();
    const rst64Search = await tbl
      .query()
      .limit(2)
      .nearestTo(query64)
      .column("vec2")
      .toArrow();
    expect(rst64Query.toString()).toEqual(rst64Search.toString());
    expect(rst64Query.numRows).toBe(2);
  });
});

describe("When querying a table", () => {
  let tmpDir: tmp.DirResult;
  beforeEach(() => {
    tmpDir = tmp.dirSync({ unsafeCleanup: true });
  });
  afterEach(() => tmpDir.removeCallback());

  it("should throw an error when timeout is reached", async () => {
    const db = await connect(tmpDir.name);
    const data = makeArrowTable([
      { text: "a", vector: [0.1, 0.2] },
      { text: "b", vector: [0.3, 0.4] },
    ]);
    const table = await db.createTable("test", data);
    await table.createIndex("text", { config: Index.fts() });

    await expect(
      table.query().where("text != 'a'").toArray({ timeoutMs: 0 }),
    ).rejects.toThrow("Query timeout");

    await expect(
      table.query().nearestTo([0.0, 0.0]).toArrow({ timeoutMs: 0 }),
    ).rejects.toThrow("Query timeout");

    await expect(
      table.search("a", "fts").toArray({ timeoutMs: 0 }),
    ).rejects.toThrow("Query timeout");

    await expect(
      table
        .query()
        .nearestToText("a")
        .nearestTo([0.0, 0.0])
        .toArrow({ timeoutMs: 0 }),
    ).rejects.toThrow("Query timeout");
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
    const addColumnsRes = await table.addColumns([
      { name: "price", valueSql: "cast(10.0 as double)" },
    ]);
    expect(addColumnsRes).toHaveProperty("version");
    expect(addColumnsRes.version).toBe(2);
    expect(await table.schema()).toEqual(schema);

    const alterColumnsRes = await table.alterColumns([
      { path: "id", rename: "new_id" },
      { path: "price", nullable: true },
    ]);
    expect(alterColumnsRes).toHaveProperty("version");
    expect(alterColumnsRes.version).toBe(3);

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

    await table.alterColumns([{ path: "new_id", dataType: "int32" }]);
    const expectedSchema2 = new Schema([
      new Field("new_id", new Int32(), true),
      new Field(
        "vector",
        new FixedSizeList(2, new Field("item", new Float32(), true)),
        true,
      ),
      new Field("price", new Float64(), true),
    ]);
    expect(await table.schema()).toEqual(expectedSchema2);

    await table.alterColumns([
      {
        path: "vector",
        dataType: new FixedSizeList(2, new Field("item", new Float64(), true)),
      },
    ]);
    const expectedSchema3 = new Schema([
      new Field("new_id", new Int32(), true),
      new Field(
        "vector",
        new FixedSizeList(2, new Field("item", new Float64(), true)),
        true,
      ),
      new Field("price", new Float64(), true),
    ]);
    expect(await table.schema()).toEqual(expectedSchema3);
  });

  it("can cast to various types", async function () {
    const con = await connect(tmpDir.name);

    // integers
    const intTypes = [
      new arrow.Int8(),
      new arrow.Int16(),
      new arrow.Int32(),
      new arrow.Int64(),
      new arrow.Uint8(),
      new arrow.Uint16(),
      new arrow.Uint32(),
      new arrow.Uint64(),
    ];
    const tableInts = await con.createTable("ints", [{ id: 1n }], {
      schema: new Schema([new Field("id", new Int64(), true)]),
    });
    for (const intType of intTypes) {
      await tableInts.alterColumns([{ path: "id", dataType: intType }]);
      const schema = new Schema([new Field("id", intType, true)]);
      expect(await tableInts.schema()).toEqual(schema);
    }

    // floats
    const floatTypes = [
      new arrow.Float16(),
      new arrow.Float32(),
      new arrow.Float64(),
    ];
    const tableFloats = await con.createTable("floats", [{ val: 2.1 }], {
      schema: new Schema([new Field("val", new Float32(), true)]),
    });
    for (const floatType of floatTypes) {
      await tableFloats.alterColumns([{ path: "val", dataType: floatType }]);
      const schema = new Schema([new Field("val", floatType, true)]);
      expect(await tableFloats.schema()).toEqual(schema);
    }

    // Lists of floats
    const listTypes = [
      new arrow.List(new arrow.Field("item", new arrow.Float32(), true)),
      new arrow.FixedSizeList(
        2,
        new arrow.Field("item", new arrow.Float64(), true),
      ),
      new arrow.FixedSizeList(
        2,
        new arrow.Field("item", new arrow.Float16(), true),
      ),
      new arrow.FixedSizeList(
        2,
        new arrow.Field("item", new arrow.Float32(), true),
      ),
    ];
    const tableLists = await con.createTable("lists", [{ val: [2.1, 3.2] }], {
      schema: new Schema([
        new Field(
          "val",
          new FixedSizeList(2, new arrow.Field("item", new Float32())),
          true,
        ),
      ]),
    });
    for (const listType of listTypes) {
      await tableLists.alterColumns([{ path: "val", dataType: listType }]);
      const schema = new Schema([new Field("val", listType, true)]);
      expect(await tableLists.schema()).toEqual(schema);
    }
  });

  it("can drop a column from the schema", async function () {
    const con = await connect(tmpDir.name);
    const table = await con.createTable("vectors", [
      { id: 1n, vector: [0.1, 0.2] },
    ]);
    const dropColumnsRes = await table.dropColumns(["vector"]);
    expect(dropColumnsRes).toHaveProperty("version");
    expect(dropColumnsRes.version).toBe(2);

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

describe("when dealing with tags", () => {
  let tmpDir: tmp.DirResult;
  beforeEach(() => {
    tmpDir = tmp.dirSync({ unsafeCleanup: true });
  });
  afterEach(() => {
    tmpDir.removeCallback();
  });

  it("can manage tags", async () => {
    const conn = await connect(tmpDir.name, {
      readConsistencyInterval: 0,
    });

    const table = await conn.createTable("my_table", [
      { id: 1n, vector: [0.1, 0.2] },
    ]);
    expect(await table.version()).toBe(1);

    await table.add([{ id: 2n, vector: [0.3, 0.4] }]);
    expect(await table.version()).toBe(2);

    const tagsManager = await table.tags();

    const initialTags = await tagsManager.list();
    expect(Object.keys(initialTags).length).toBe(0);

    const tag1 = "tag1";
    await tagsManager.create(tag1, 1);
    expect(await tagsManager.getVersion(tag1)).toBe(1);

    const tagsAfterFirst = await tagsManager.list();
    expect(Object.keys(tagsAfterFirst).length).toBe(1);
    expect(tagsAfterFirst).toHaveProperty(tag1);
    expect(tagsAfterFirst[tag1].version).toBe(1);

    await tagsManager.create("tag2", 2);
    expect(await tagsManager.getVersion("tag2")).toBe(2);

    const tagsAfterSecond = await tagsManager.list();
    expect(Object.keys(tagsAfterSecond).length).toBe(2);
    expect(tagsAfterSecond).toHaveProperty(tag1);
    expect(tagsAfterSecond[tag1].version).toBe(1);
    expect(tagsAfterSecond).toHaveProperty("tag2");
    expect(tagsAfterSecond["tag2"].version).toBe(2);

    await table.add([{ id: 3n, vector: [0.5, 0.6] }]);
    await tagsManager.update(tag1, 3);
    expect(await tagsManager.getVersion(tag1)).toBe(3);

    await tagsManager.delete("tag2");
    const tagsAfterDelete = await tagsManager.list();
    expect(Object.keys(tagsAfterDelete).length).toBe(1);
    expect(tagsAfterDelete).toHaveProperty(tag1);
    expect(tagsAfterDelete[tag1].version).toBe(3);

    await table.add([{ id: 4n, vector: [0.7, 0.8] }]);
    expect(await table.version()).toBe(4);

    await table.checkout(tag1);
    expect(await table.version()).toBe(3);

    await table.checkoutLatest();
    expect(await table.version()).toBe(4);
  });

  it("can checkout and restore tags", async () => {
    const conn = await connect(tmpDir.name, {
      readConsistencyInterval: 0,
    });

    const table = await conn.createTable("my_table", [
      { id: 1n, vector: [0.1, 0.2] },
    ]);
    expect(await table.version()).toBe(1);
    expect(await table.countRows()).toBe(1);
    const tagsManager = await table.tags();
    const tag1 = "tag1";
    await tagsManager.create(tag1, 1);
    await table.add([{ id: 2n, vector: [0.3, 0.4] }]);
    const tag2 = "tag2";
    await tagsManager.create(tag2, 2);
    expect(await table.version()).toBe(2);
    await table.checkout(tag1);
    expect(await table.version()).toBe(1);
    await table.restore();
    expect(await table.version()).toBe(3);
    expect(await table.countRows()).toBe(1);
    await table.add([{ id: 3n, vector: [0.5, 0.6] }]);
    expect(await table.countRows()).toBe(2);
  });
});

describe("when optimizing a dataset", () => {
  let tmpDir: tmp.DirResult;
  let table: Table;
  beforeEach(async () => {
    tmpDir = tmp.dirSync({ unsafeCleanup: true });
    const con = await connect(tmpDir.name);
    table = await con.createTable("vectors", [{ id: 1 }]);
    await table.add([{ id: 2 }]);
  });
  afterEach(() => {
    tmpDir.removeCallback();
  });

  it("compacts files", async () => {
    const stats = await table.optimize();
    expect(stats.compaction.filesAdded).toBe(1);
    expect(stats.compaction.filesRemoved).toBe(2);
    expect(stats.compaction.fragmentsAdded).toBe(1);
    expect(stats.compaction.fragmentsRemoved).toBe(2);
  });

  it("cleanups old versions", async () => {
    const stats = await table.optimize({ cleanupOlderThan: new Date() });
    expect(stats.prune.bytesRemoved).toBeGreaterThan(0);
    expect(stats.prune.oldVersionsRemoved).toBe(3);
  });

  it("delete unverified", async () => {
    const version = await table.version();
    const versionFile = `${tmpDir.name}/${table.name}.lance/_versions/${version - 1}.manifest`;
    fs.rmSync(versionFile);

    let stats = await table.optimize({ deleteUnverified: false });
    expect(stats.prune.oldVersionsRemoved).toBe(0);

    stats = await table.optimize({
      cleanupOlderThan: new Date(),
      deleteUnverified: true,
    });
    expect(stats.prune.oldVersionsRemoved).toBeGreaterThan(1);
  });
});

describe.each([arrow15, arrow16, arrow17, arrow18])(
  "when optimizing a dataset",
  // biome-ignore lint/suspicious/noExplicitAny: <explanation>
  (arrow: any) => {
    let tmpDir: tmp.DirResult;
    beforeEach(() => {
      getRegistry().reset();
      tmpDir = tmp.dirSync({ unsafeCleanup: true });
    });
    afterEach(() => {
      tmpDir.removeCallback();
    });

    test("can search using a string", async () => {
      @register()
      class MockEmbeddingFunction extends EmbeddingFunction<string> {
        ndims() {
          return 1;
        }
        embeddingDataType() {
          return new Float32();
        }

        // Hardcoded embeddings for the sake of testing
        async computeQueryEmbeddings(_data: string) {
          switch (_data) {
            case "greetings":
              return [0.1];
            case "farewell":
              return [0.2];
            default:
              return null as never;
          }
        }

        // Hardcoded embeddings for the sake of testing
        async computeSourceEmbeddings(data: string[]) {
          return data.map((s) => {
            switch (s) {
              case "hello world":
                return [0.1];
              case "goodbye world":
                return [0.2];
              default:
                return null as never;
            }
          });
        }
      }

      const func = new MockEmbeddingFunction();
      const schema = LanceSchema({
        text: func.sourceField(new arrow.Utf8()),
        vector: func.vectorField(),
      });
      const db = await connect(tmpDir.name);
      const data = [{ text: "hello world" }, { text: "goodbye world" }];
      const table = await db.createTable("test", data, { schema });

      const results = await table.search("greetings").toArray();
      expect(results[0].text).toBe(data[0].text);

      const results2 = await table.search("farewell").toArray();
      expect(results2[0].text).toBe(data[1].text);
    });

    test("rejects if no embedding function provided", async () => {
      const db = await connect(tmpDir.name);
      const data = [
        { text: "hello world", vector: [0.1, 0.2, 0.3] },
        { text: "goodbye world", vector: [0.4, 0.5, 0.6] },
      ];
      const table = await db.createTable("test", data);

      expect(table.search("hello", "vector").toArray()).rejects.toThrow(
        "No embedding functions are defined in the table",
      );
    });

    test("full text search if no embedding function provided", async () => {
      const db = await connect(tmpDir.name);
      const data = [
        { text: "hello world", vector: [0.1, 0.2, 0.3] },
        { text: "goodbye world", vector: [0.4, 0.5, 0.6] },
      ];
      const table = await db.createTable("test", data);
      await table.createIndex("text", {
        config: Index.fts(),
      });

      const results = await table.search("hello").toArray();
      expect(results[0].text).toBe(data[0].text);

      const query = new MatchQuery("goodbye", "text");
      expect(instanceOfFullTextQuery(query)).toBe(true);
      const results2 = await table
        .search(new MatchQuery("goodbye", "text"))
        .toArray();
      expect(results2[0].text).toBe(data[1].text);
    });

    test("prewarm full text search index", async () => {
      const db = await connect(tmpDir.name);
      const data = [
        { text: ["lance database", "the", "search"], vector: [0.1, 0.2, 0.3] },
        { text: ["lance database"], vector: [0.4, 0.5, 0.6] },
        { text: ["lance", "search"], vector: [0.7, 0.8, 0.9] },
        { text: ["database", "search"], vector: [1.0, 1.1, 1.2] },
        { text: ["unrelated", "doc"], vector: [1.3, 1.4, 1.5] },
      ];
      const table = await db.createTable("test", data);
      await table.createIndex("text", {
        config: Index.fts(),
      });

      // For the moment, we just confirm we can call prewarmIndex without error
      // and still search it afterwards
      await table.prewarmIndex("text_idx");

      const results = await table.search("lance").toArray();
      expect(results.length).toBe(3);
    });

    test("full text index on list", async () => {
      const db = await connect(tmpDir.name);
      const data = [
        { text: ["lance database", "the", "search"], vector: [0.1, 0.2, 0.3] },
        { text: ["lance database"], vector: [0.4, 0.5, 0.6] },
        { text: ["lance", "search"], vector: [0.7, 0.8, 0.9] },
        { text: ["database", "search"], vector: [1.0, 1.1, 1.2] },
        { text: ["unrelated", "doc"], vector: [1.3, 1.4, 1.5] },
      ];
      const table = await db.createTable("test", data);
      await table.createIndex("text", {
        config: Index.fts({
          withPosition: true,
        }),
      });

      const results = await table.search("lance").toArray();
      expect(results.length).toBe(3);

      const results2 = await table.search('"lance database"').toArray();
      expect(results2.length).toBe(2);
    });

    test("full text search without positions", async () => {
      const db = await connect(tmpDir.name);
      const data = [
        { text: "hello world", vector: [0.1, 0.2, 0.3] },
        { text: "goodbye world", vector: [0.4, 0.5, 0.6] },
      ];
      const table = await db.createTable("test", data);
      await table.createIndex("text", {
        config: Index.fts({ withPosition: false }),
      });

      const results = await table.search("hello").toArray();
      expect(results[0].text).toBe(data[0].text);

      const results2 = await table
        .search(new MatchQuery("hello world", "text"))
        .toArray();
      expect(results2.length).toBe(2);

      const results3 = await table
        .search(
          new MatchQuery("hello world", "text", { operator: Operator.And }),
        )
        .toArray();
      expect(results3.length).toBe(1);
    });

    test("full text search without lowercase", async () => {
      const db = await connect(tmpDir.name);
      const data = [
        { text: "hello world", vector: [0.1, 0.2, 0.3] },
        { text: "Hello World", vector: [0.4, 0.5, 0.6] },
      ];
      const table = await db.createTable("test", data);
      await table.createIndex("text", {
        config: Index.fts({ withPosition: false }),
      });
      const results = await table.search("hello").toArray();
      expect(results.length).toBe(2);

      await table.createIndex("text", {
        config: Index.fts({ withPosition: false, lowercase: false }),
      });
      const results2 = await table.search("hello").toArray();
      expect(results2.length).toBe(1);
    });

    test("full text search phrase query", async () => {
      const db = await connect(tmpDir.name);
      const data = [
        { text: "hello world", vector: [0.1, 0.2, 0.3] },
        { text: "goodbye world", vector: [0.4, 0.5, 0.6] },
      ];
      const table = await db.createTable("test", data);
      await table.createIndex("text", {
        config: Index.fts({
          withPosition: true,
        }),
      });

      const results = await table.search("world").toArray();
      expect(results.length).toBe(2);
      const phraseResults = await table.search('"hello world"').toArray();
      expect(phraseResults.length).toBe(1);
      const phraseResults2 = await table
        .search(new PhraseQuery("hello world", "text"))
        .toArray();
      expect(phraseResults2.length).toBe(1);
    });

    test("full text search fuzzy query", async () => {
      const db = await connect(tmpDir.name);
      const data = [
        { text: "fa", vector: [0.1, 0.2, 0.3] },
        { text: "fo", vector: [0.4, 0.5, 0.6] },
        { text: "fob", vector: [0.4, 0.5, 0.6] },
        { text: "focus", vector: [0.4, 0.5, 0.6] },
        { text: "foo", vector: [0.4, 0.5, 0.6] },
        { text: "food", vector: [0.4, 0.5, 0.6] },
        { text: "foul", vector: [0.4, 0.5, 0.6] },
      ];
      const table = await db.createTable("test", data);
      await table.createIndex("text", {
        config: Index.fts(),
      });

      const results = await table
        .search(new MatchQuery("foo", "text"))
        .toArray();
      expect(results.length).toBe(1);
      expect(results[0].text).toBe("foo");

      const fuzzyResults = await table
        .search(new MatchQuery("foo", "text", { fuzziness: 1 }))
        .toArray();
      expect(fuzzyResults.length).toBe(4);
      const resultSet = new Set(fuzzyResults.map((r) => r.text));
      expect(resultSet.has("foo")).toBe(true);
      expect(resultSet.has("fob")).toBe(true);
      expect(resultSet.has("fo")).toBe(true);
      expect(resultSet.has("food")).toBe(true);

      const prefixResults = await table
        .search(
          new MatchQuery("foo", "text", { fuzziness: 3, prefixLength: 3 }),
        )
        .toArray();
      expect(prefixResults.length).toBe(2);
      const resultSet2 = new Set(prefixResults.map((r) => r.text));
      expect(resultSet2.has("foo")).toBe(true);
      expect(resultSet2.has("food")).toBe(true);
    });

    test("full text search boolean query", async () => {
      const db = await connect(tmpDir.name);
      const data = [
        { text: "The cat and dog are playing" },
        { text: "The cat is sleeping" },
        { text: "The dog is barking" },
        { text: "The dog chases the cat" },
      ];
      const table = await db.createTable("test", data);
      await table.createIndex("text", {
        config: Index.fts({ withPosition: false }),
      });

      const shouldResults = await table
        .search(
          new BooleanQuery([
            [Occur.Should, new MatchQuery("cat", "text")],
            [Occur.Should, new MatchQuery("dog", "text")],
          ]),
        )
        .toArray();
      expect(shouldResults.length).toBe(4);

      const mustResults = await table
        .search(
          new BooleanQuery([
            [Occur.Must, new MatchQuery("cat", "text")],
            [Occur.Must, new MatchQuery("dog", "text")],
          ]),
        )
        .toArray();
      expect(mustResults.length).toBe(2);

      const mustNotResults = await table
        .search(
          new BooleanQuery([
            [Occur.Must, new MatchQuery("cat", "text")],
            [Occur.MustNot, new MatchQuery("dog", "text")],
          ]),
        )
        .toArray();
      expect(mustNotResults.length).toBe(1);
    });

    test("full text search ngram", async () => {
      const db = await connect(tmpDir.name);
      const data = [
        { text: "hello world", vector: [0.1, 0.2, 0.3] },
        { text: "lance database", vector: [0.4, 0.5, 0.6] },
        { text: "lance is cool", vector: [0.7, 0.8, 0.9] },
      ];
      const table = await db.createTable("test", data);
      await table.createIndex("text", {
        config: Index.fts({ baseTokenizer: "ngram" }),
      });

      const results = await table.search("lan").toArray();
      expect(results.length).toBe(2);
      const resultSet = new Set(results.map((r) => r.text));
      expect(resultSet.has("lance database")).toBe(true);
      expect(resultSet.has("lance is cool")).toBe(true);

      const results2 = await table.search("nce").toArray(); // spellchecker:disable-line
      expect(results2.length).toBe(2);
      const resultSet2 = new Set(results2.map((r) => r.text));
      expect(resultSet2.has("lance database")).toBe(true);
      expect(resultSet2.has("lance is cool")).toBe(true);

      // the default min_ngram_length is 3, so "la" should not match
      const results3 = await table.search("la").toArray();
      expect(results3.length).toBe(0);

      // test setting min_ngram_length and prefix_only
      await table.createIndex("text", {
        config: Index.fts({
          baseTokenizer: "ngram",
          ngramMinLength: 2,
          prefixOnly: true,
        }),
        replace: true,
      });

      const results4 = await table.search("lan").toArray();
      expect(results4.length).toBe(2);
      const resultSet4 = new Set(results4.map((r) => r.text));
      expect(resultSet4.has("lance database")).toBe(true);
      expect(resultSet4.has("lance is cool")).toBe(true);

      const results5 = await table.search("nce").toArray(); // spellchecker:disable-line
      expect(results5.length).toBe(0);

      const results6 = await table.search("la").toArray();
      expect(results6.length).toBe(2);
      const resultSet6 = new Set(results6.map((r) => r.text));
      expect(resultSet6.has("lance database")).toBe(true);
      expect(resultSet6.has("lance is cool")).toBe(true);
    });

    test.each([
      [0.4, 0.5, 0.599], // number[]
      Float32Array.of(0.4, 0.5, 0.599), // Float32Array
      Float64Array.of(0.4, 0.5, 0.599), // Float64Array
    ])("can search using vectorlike datatypes", async (vectorlike) => {
      const db = await connect(tmpDir.name);
      const data = [
        { text: "hello world", vector: [0.1, 0.2, 0.3] },
        { text: "goodbye world", vector: [0.4, 0.5, 0.6] },
      ];
      const table = await db.createTable("test", data);

      // biome-ignore lint/suspicious/noExplicitAny: test
      const results: any[] = await table.search(vectorlike).toArray();

      expect(results.length).toBe(2);
      expect(results[0].text).toBe(data[1].text);
    });
  },
);

describe("when calling explainPlan", () => {
  let tmpDir: tmp.DirResult;
  let table: Table;
  let queryVec: number[];
  beforeEach(async () => {
    tmpDir = tmp.dirSync({ unsafeCleanup: true });
    const con = await connect(tmpDir.name);
    table = await con.createTable("vectors", [{ id: 1, vector: [0.1, 0.2] }]);
  });

  afterEach(() => {
    tmpDir.removeCallback();
  });

  it("retrieves query plan", async () => {
    queryVec = Array(2)
      .fill(1)
      .map(() => Math.random());
    const plan = await table.query().nearestTo(queryVec).explainPlan(true);

    expect(plan).toMatch("KNN");
  });
});

describe("when calling analyzePlan", () => {
  let tmpDir: tmp.DirResult;
  let table: Table;
  let queryVec: number[];
  beforeEach(async () => {
    tmpDir = tmp.dirSync({ unsafeCleanup: true });
    const con = await connect(tmpDir.name);
    table = await con.createTable("vectors", [{ id: 1, vector: [1.1, 0.9] }]);
  });

  afterEach(() => {
    tmpDir.removeCallback();
  });

  it("retrieves runtime metrics", async () => {
    queryVec = Array(2)
      .fill(1)
      .map(() => Math.random());
    const plan = await table.query().nearestTo(queryVec).analyzePlan();
    console.log("Query Plan:\n", plan); // <--- Print the plan
    expect(plan).toMatch("AnalyzeExec");
  });
});

describe("column name options", () => {
  let tmpDir: tmp.DirResult;
  let table: Table;
  beforeEach(async () => {
    tmpDir = tmp.dirSync({ unsafeCleanup: true });
    const con = await connect(tmpDir.name);
    table = await con.createTable("vectors", [
      { camelCase: 1, vector: [0.1, 0.2] },
    ]);
  });

  test("can select columns with different names", async () => {
    const results = await table.query().select(["camelCase"]).toArray();
    expect(results[0].camelCase).toBe(1);
  });

  test("can filter on columns with different names", async () => {
    const results = await table.query().where("`camelCase` = 1").toArray();
    expect(results[0].camelCase).toBe(1);
  });

  test("can make multiple vector queries in one go", async () => {
    const results = await table
      .query()
      .nearestTo([0.1, 0.2])
      .addQueryVector([0.1, 0.2])
      .limit(1)
      .toArray();
    console.log(results);
    expect(results.length).toBe(2);
    results.sort((a, b) => a.query_index - b.query_index);
    expect(results[0].query_index).toBe(0);
    expect(results[1].query_index).toBe(1);
  });

  test("index and search multivectors", async () => {
    const db = await connect(tmpDir.name);
    const data = [];
    // generate 512 random multivectors
    for (let i = 0; i < 256; i++) {
      data.push({
        multivector: Array.from({ length: 10 }, () =>
          Array(2).fill(Math.random()),
        ),
      });
    }
    const table = await db.createTable("multivectors", data, {
      schema: new Schema([
        new Field(
          "multivector",
          new List(
            new Field(
              "item",
              new FixedSizeList(2, new Field("item", new Float32())),
            ),
          ),
        ),
      ]),
    });

    const results = await table.search(data[0].multivector).limit(10).toArray();
    expect(results.length).toBe(10);

    await table.createIndex("multivector", {
      config: Index.ivfPq({ numPartitions: 2, distanceType: "cosine" }),
    });

    const results2 = await table
      .search(data[0].multivector)
      .limit(10)
      .toArray();
    expect(results2.length).toBe(10);
  });

  test("replaceSchemaMetadata", async () => {
    const data = [
      { vector: [3.1, 4.1], item: "foo", price: 10.0 },
      { vector: [5.9, 26.5], item: "bar", price: 20.0 },
    ];

    const conn = await connect(tmpDir.name);
    table = await conn.createTable("test_metadata", data);

    // Get initial schema
    const initialSchema = await table.schema();
    expect(initialSchema).toBeDefined();

    // Replace schema metadata
    const newMetadata = {
      description: "Test table for schema metadata",
      version: "1.0",
      createdBy: "jest",
    };

    await table.replaceSchemaMetadata(newMetadata);

    // Verify metadata was updated
    const updatedSchema = await table.schema();
    expect(updatedSchema).toBeDefined();
    expect(updatedSchema.metadata).toBeDefined();

    // Check that our new metadata is present
    const metadata = updatedSchema.metadata;
    for (const [key, value] of Object.entries(newMetadata)) {
      expect(metadata.get(key)).toBe(value);
    }
  });
});
