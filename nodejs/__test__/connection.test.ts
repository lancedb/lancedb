// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import { readdirSync } from "fs";
import { Field, Float64, Schema } from "apache-arrow";
import * as tmp from "tmp";
import { Connection, Table, connect } from "../lancedb";
import { LocalTable } from "../lancedb/table";

describe("when connecting", () => {
  let tmpDir: tmp.DirResult;
  beforeEach(() => {
    tmpDir = tmp.dirSync({ unsafeCleanup: true });
  });
  afterEach(() => tmpDir.removeCallback());

  it("should connect", async () => {
    const db = await connect(tmpDir.name);
    expect(db.display()).toBe(
      `ListingDatabase(uri=${tmpDir.name}, read_consistency_interval=5s)`,
    );
  });

  it("should allow read consistency interval to be specified", async () => {
    const db = await connect(tmpDir.name, { readConsistencyInterval: 5 });
    expect(db.display()).toBe(
      `ListingDatabase(uri=${tmpDir.name}, read_consistency_interval=5s)`,
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
  it("should be able to create a table from an object arg `createTable(options)`, or args `createTable(name, data, options)`", async () => {
    let tbl = await db.createTable("test", [{ id: 1 }, { id: 2 }]);
    await expect(tbl.countRows()).resolves.toBe(2);

    tbl = await db.createTable({
      name: "test",
      data: [{ id: 3 }],
      mode: "overwrite",
    });

    await expect(tbl.countRows()).resolves.toBe(1);
  });

  it("should be able to drop tables`", async () => {
    await db.createTable("test", [{ id: 1 }, { id: 2 }]);
    await db.createTable("test2", [{ id: 1 }, { id: 2 }]);
    await db.createTable("test3", [{ id: 1 }, { id: 2 }]);

    await expect(db.tableNames()).resolves.toEqual(["test", "test2", "test3"]);

    await db.dropTable("test2");

    await expect(db.tableNames()).resolves.toEqual(["test", "test3"]);

    await db.dropAllTables();

    await expect(db.tableNames()).resolves.toEqual([]);

    // Make sure we can still create more tables after dropping all

    await db.createTable("test4", [{ id: 1 }, { id: 2 }]);
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

  it("should respect limit and page token when listing tables", async () => {
    const db = await connect(tmpDir.name);

    await db.createTable("b", [{ id: 1 }]);
    await db.createTable("a", [{ id: 1 }]);
    await db.createTable("c", [{ id: 1 }]);

    let tables = await db.tableNames();
    expect(tables).toEqual(["a", "b", "c"]);

    tables = await db.tableNames({ limit: 1 });
    expect(tables).toEqual(["a"]);

    tables = await db.tableNames({ limit: 1, startAfter: "a" });
    expect(tables).toEqual(["b"]);

    tables = await db.tableNames({ startAfter: "a" });
    expect(tables).toEqual(["b", "c"]);
  });

  it("should create tables in v2 mode", async () => {
    const db = await connect(tmpDir.name);
    const data = [...Array(10000).keys()].map((i) => ({ id: i }));

    // Create in v1 mode
    let table = await db.createTable("test", data, {
      storageOptions: { newTableDataStorageVersion: "legacy" },
    });

    const isV2 = async (table: Table) => {
      const data = await table
        .query()
        .limit(10000)
        .toArrow({ maxBatchLength: 100000 });
      return data.batches.length < 5;
    };

    await expect(isV2(table)).resolves.toBe(false);

    // Create in v2 mode
    table = await db.createTable("test_v2", data);

    await expect(isV2(table)).resolves.toBe(true);

    await table.add(data);

    await expect(isV2(table)).resolves.toBe(true);

    // Create empty in v2 mode
    const schema = new Schema([new Field("id", new Float64(), true)]);

    table = await db.createEmptyTable("test_v2_empty", schema, {
      storageOptions: { newTableDataStorageVersion: "stable" },
    });

    await table.add(data);
    await expect(isV2(table)).resolves.toBe(true);
  });

  it("should be able to create tables with V2 manifest paths", async () => {
    const db = await connect(tmpDir.name);
    let table = (await db.createEmptyTable(
      "test_manifest_paths_v2_empty",
      new Schema([new Field("id", new Float64(), true)]),
      {
        enableV2ManifestPaths: true,
      },
    )) as LocalTable;
    expect(await table.usesV2ManifestPaths()).toBe(true);

    let manifestDir =
      tmpDir.name + "/test_manifest_paths_v2_empty.lance/_versions";
    readdirSync(manifestDir).forEach((file) => {
      expect(file).toMatch(/^\d{20}\.manifest$/);
    });

    table = (await db.createTable("test_manifest_paths_v2", [{ id: 1 }], {
      enableV2ManifestPaths: true,
    })) as LocalTable;
    expect(await table.usesV2ManifestPaths()).toBe(true);
    manifestDir = tmpDir.name + "/test_manifest_paths_v2.lance/_versions";
    readdirSync(manifestDir).forEach((file) => {
      expect(file).toMatch(/^\d{20}\.manifest$/);
    });
  });

  it("should be able to migrate tables to the V2 manifest paths", async () => {
    const db = await connect(tmpDir.name);
    const table = (await db.createEmptyTable(
      "test_manifest_path_migration",
      new Schema([new Field("id", new Float64(), true)]),
      {
        enableV2ManifestPaths: false,
      },
    )) as LocalTable;

    expect(await table.usesV2ManifestPaths()).toBe(false);

    const manifestDir =
      tmpDir.name + "/test_manifest_path_migration.lance/_versions";
    readdirSync(manifestDir).forEach((file) => {
      expect(file).toMatch(/^\d\.manifest$/);
    });

    await table.migrateManifestPathsV2();
    expect(await table.usesV2ManifestPaths()).toBe(true);

    readdirSync(manifestDir).forEach((file) => {
      expect(file).toMatch(/^\d{20}\.manifest$/);
    });
  });
});
