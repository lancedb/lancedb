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

import * as tmp from "tmp";

import { Connection, connect } from "../dist/index.js";

describe("when connecting", () => {
  let tmpDir: tmp.DirResult;
  beforeEach(() => (tmpDir = tmp.dirSync({ unsafeCleanup: true })));
  afterEach(() => tmpDir.removeCallback());

  it("should connect", async () => {
    const db = await connect(tmpDir.name);
    expect(db.display()).toBe(
      `NativeDatabase(uri=${tmpDir.name}, read_consistency_interval=None)`,
    );
  });

  it("should allow read consistency interval to be specified", async () => {
    const db = await connect(tmpDir.name, { readConsistencyInterval: 5 });
    expect(db.display()).toBe(
      `NativeDatabase(uri=${tmpDir.name}, read_consistency_interval=5s)`,
    );
  });
});

describe("given a connection", () => {
  let tmpDir: tmp.DirResult;
  let db: Connection;
  beforeEach(async () => {
    tmpDir = tmp.dirSync({ unsafeCleanup: true });
    db = await connect(tmpDir.name);
  });
  afterEach(() => tmpDir.removeCallback());

  it("should raise an error if opening a non-existent table", async () => {
    await expect(db.openTable("non-existent")).rejects.toThrow("was not found");
  });

  it("should raise an error if any operation is tried after it is closed", async () => {
    expect(db.isOpen()).toBe(true);
    await db.close();
    expect(db.isOpen()).toBe(false);
    await expect(db.tableNames()).rejects.toThrow("Connection is closed");
  });

  it("should fail if creating table twice, unless overwrite is true", async () => {
    let tbl = await db.createTable("test", [{ id: 1 }, { id: 2 }]);
    await expect(tbl.countRows()).resolves.toBe(2);
    await expect(
      db.createTable("test", [{ id: 1 }, { id: 2 }]),
    ).rejects.toThrow();
    tbl = await db.createTable("test", [{ id: 3 }], { mode: "overwrite" });
    await expect(tbl.countRows()).resolves.toBe(1);
  });

  it("should list tables", async () => {
    await db.createTable("test2", [{ id: 1 }, { id: 2 }]);
    await db.createTable("test1", [{ id: 1 }, { id: 2 }]);
    expect(await db.tableNames()).toEqual(["test1", "test2"]);
  });
});
