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
