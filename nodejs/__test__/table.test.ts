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

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "test-open"));
  });

  test("create vector index with no column", async () => {
    const db = await connect(tmpDir);
    const start = Date.now();
    const data = makeArrowTable(
      Array(200)
        .fill(1)
        .map((_, i) => ({
          id: i,
          vec: Array(32)
            .fill(1)
            .map(() => Math.random()),
        })),
      {
        schema: new Schema([
          new Field("id", new Int32(), true),
          new Field(
            "vec",
            new FixedSizeList(32, new Field("item", new Float32()))
          ),
        ]),
      }
    );
    const tbl = await db.createTable("test", data);
    await tbl.createIndex().ivf_pq({ num_partitions: 2, num_sub_vectors: 2 });
  });

  test("create vector index", async () => {});
});
