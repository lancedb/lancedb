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
import { Schema, Field, Float32, Int32, FixedSizeList } from "apache-arrow";
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
