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

import * as os from "os";
import * as path from "path";
import * as fs from "fs";

import { connect } from "../dist";
import { Schema, Field, Float32, Int32, FixedSizeList, Int64, Float64 } from "apache-arrow";
import { makeArrowTable } from "../dist/arrow";

describe("Test creating index", () => {
  let tmpDir: string;
  const schema = new Schema([
    new Field("id", new Int32(), true),
    new Field("vec", new FixedSizeList(32, new Field("item", new Float32()))),
  ]);

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "index-"));
  });

  test("create vector index with no column", async () => {
    const db = await connect(tmpDir);
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
      }
    );
    const tbl = await db.createTable("test", data);
    await tbl.createIndex().build();

    // check index directory
    const indexDir = path.join(tmpDir, "test.lance", "_indices");
    expect(fs.readdirSync(indexDir)).toHaveLength(1);
    // TODO: check index type.

    // Search without specifying the column
    let query_vector = data.toArray()[5].vec.toJSON();
    let rst = await tbl.query().nearestTo(query_vector).limit(2).toArrow();
    expect(rst.numRows).toBe(2);

    // Search with specifying the column
    let rst2 = await tbl.search(query_vector, "vec").limit(2).toArrow();
    expect(rst2.numRows).toBe(2);
    expect(rst.toString()).toEqual(rst2.toString());
  });

  test("no vector column available", async () => {
    const db = await connect(tmpDir);
    const tbl = await db.createTable(
      "no_vec",
      makeArrowTable([
        { id: 1, val: 2 },
        { id: 2, val: 3 },
      ])
    );
    await expect(tbl.createIndex().build()).rejects.toThrow(
      "No vector column found"
    );

    await tbl.createIndex("val").build();
    const indexDir = path.join(tmpDir, "no_vec.lance", "_indices");
    expect(fs.readdirSync(indexDir)).toHaveLength(1);

    for await (const r of tbl.query().filter("id > 1").select(["id"])) {
      expect(r.numRows).toBe(1);
    }
  });

  test("two columns with different dimensions", async () => {
    const db = await connect(tmpDir);
    const schema = new Schema([
      new Field("id", new Int32(), true),
      new Field("vec", new FixedSizeList(32, new Field("item", new Float32()))),
      new Field(
        "vec2",
        new FixedSizeList(64, new Field("item", new Float32()))
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
        { schema }
      )
    );

    // Only build index over v1
    await expect(tbl.createIndex().build()).rejects.toThrow(
      /.*More than one vector columns found.*/
    );
    tbl
      .createIndex("vec")
      .ivf_pq({ num_partitions: 2, num_sub_vectors: 2 })
      .build();

    const rst = await tbl
      .query()
      .nearestTo(
        Array(32)
          .fill(1)
          .map(() => Math.random())
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
          "vec"
        )
        .limit(2)
        .toArrow()
    ).rejects.toThrow(/.*does not match the dimension.*/);

    const query64 = Array(64)
      .fill(1)
      .map(() => Math.random());
    const rst64_1 = await tbl.query().nearestTo(query64).limit(2).toArrow();
    const rst64_2 = await tbl.search(query64, "vec2").limit(2).toArrow();
    expect(rst64_1.toString()).toEqual(rst64_2.toString());
    expect(rst64_1.numRows).toBe(2);
  });

  test("create scalar index", async () => {
    const db = await connect(tmpDir);
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
      }
    );
    const tbl = await db.createTable("test", data);
    await tbl.createIndex("id").build();

    // check index directory
    const indexDir = path.join(tmpDir, "test.lance", "_indices");
    expect(fs.readdirSync(indexDir)).toHaveLength(1);
    // TODO: check index type.
  });
});

describe("Read consistency interval", () => {
  let tmpDir: string;
  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "read-consistency-"));
  });

  // const intervals = [undefined, 0, 0.1];
  const intervals = [0];
  test.each(intervals)("read consistency interval %p", async (interval) => {
    const db = await connect({ uri: tmpDir });
    const table = await db.createTable("my_table", [{ id: 1 }]);

    const db2 = await connect({ uri: tmpDir, readConsistencyInterval: interval });
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
      await new Promise(r => setTimeout(r, 100));
      expect(await table2.countRows()).toEqual(2);
    }
  });
});


describe('schema evolution', function () {
  let tmpDir: string;
  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "schema-evolution-"));
  });

  // Create a new sample table
  it('can add a new column to the schema', async function () {
    const con = await connect(tmpDir)
    const table = await con.createTable('vectors', [
      { id: 1n, vector: [0.1, 0.2] }
    ])

    await table.addColumns([{ name: 'price', valueSql: 'cast(10.0 as float)' }])

    const expectedSchema = new Schema([
      new Field('id', new Int64(), true),
      new Field('vector', new FixedSizeList(2, new Field('item', new Float32(), true)), true),
      new Field('price', new Float32(), false)
    ])
    expect(await table.schema()).toEqual(expectedSchema)
  });

  it('can alter the columns in the schema', async function () {
    const con = await connect(tmpDir)
    const schema = new Schema([
      new Field('id', new Int64(), true),
      new Field('vector', new FixedSizeList(2, new Field('item', new Float32(), true)), true),
      new Field('price', new Float64(), false)
    ])
    const table = await con.createTable('vectors', [
      { id: 1n, vector: [0.1, 0.2] }
    ])
    // Can create a non-nullable column only through addColumns at the moment.
    await table.addColumns([{ name: 'price', valueSql: 'cast(10.0 as double)' }])
    expect(await table.schema()).toEqual(schema)

    await table.alterColumns([
      { path: 'id', rename: 'new_id' },
      { path: 'price', nullable: true }
    ])

    const expectedSchema = new Schema([
      new Field('new_id', new Int64(), true),
      new Field('vector', new FixedSizeList(2, new Field('item', new Float32(), true)), true),
      new Field('price', new Float64(), true)
    ])
    expect(await table.schema()).toEqual(expectedSchema)
  });

  it('can drop a column from the schema', async function () {
    const con = await connect(tmpDir)
    const table = await con.createTable('vectors', [
      { id: 1n, vector: [0.1, 0.2] }
    ])
    await table.dropColumns(['vector'])

    const expectedSchema = new Schema([
      new Field('id', new Int64(), true)
    ])
    expect(await table.schema()).toEqual(expectedSchema)
  });
});