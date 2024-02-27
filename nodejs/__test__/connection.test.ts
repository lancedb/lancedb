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

import { connect } from "../dist/index.js";

describe("when working with a connection", () => {

  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "test-connection"));

  it("should fail if creating table twice, unless overwrite is true", async() => {
    const db = await connect(tmpDir);
    let tbl = await db.createTable("test", [{ id: 1 }, { id: 2 }]);
    await expect(tbl.countRows()).resolves.toBe(2);
    await expect(db.createTable("test", [{ id: 1 }, { id: 2 }])).rejects.toThrow();
    tbl = await db.createTable("test", [{ id: 3 }], { mode: "overwrite" });
    await expect(tbl.countRows()).resolves.toBe(1);
  })

});
