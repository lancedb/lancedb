// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import { readdirSync } from "fs";
import { Field, Float64, Schema } from "apache-arrow";
import * as tmp from "tmp";
import { Connection, Table, connect, connectNamespace } from "../lancedb";
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
      `ListingDatabase(uri=${tmpDir.name}, read_consistency_interval=None)`,
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

  it("should expose renameTable and reject on OSS listing DB", async () => {
    await db.createTable("old_name", [{ id: 1 }]);

    await expect(db.renameTable("old_name", "new_name")).rejects.toThrow(
      "rename_table is not supported in LanceDB OSS",
    );

    await expect(db.tableNames()).resolves.toEqual(["old_name"]);
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
    readdirSync(manifestDir)
      .filter((f) => f.endsWith(".manifest"))
      .forEach((file) => {
        expect(file).toMatch(/^\d{20}\.manifest$/);
      });

    table = (await db.createTable("test_manifest_paths_v2", [{ id: 1 }], {
      enableV2ManifestPaths: true,
    })) as LocalTable;
    expect(await table.usesV2ManifestPaths()).toBe(true);
    manifestDir = tmpDir.name + "/test_manifest_paths_v2.lance/_versions";
    readdirSync(manifestDir)
      .filter((f) => f.endsWith(".manifest"))
      .forEach((file) => {
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
    readdirSync(manifestDir)
      .filter((f) => f.endsWith(".manifest"))
      .forEach((file) => {
        expect(file).toMatch(/^\d\.manifest$/);
      });

    await table.migrateManifestPathsV2();
    expect(await table.usesV2ManifestPaths()).toBe(true);

    readdirSync(manifestDir)
      .filter((f) => f.endsWith(".manifest"))
      .forEach((file) => {
        expect(file).toMatch(/^\d{20}\.manifest$/);
      });
  });
});

describe("clone table functionality", () => {
  let tmpDir: tmp.DirResult;
  let db: Connection;
  beforeEach(async () => {
    tmpDir = tmp.dirSync({ unsafeCleanup: true });
    db = await connect(tmpDir.name);
  });
  afterEach(() => tmpDir.removeCallback());

  it("should clone a table with latest version (default behavior)", async () => {
    // Create source table with some data
    const data = [
      { id: 1, text: "hello", vector: [1.0, 2.0] },
      { id: 2, text: "world", vector: [3.0, 4.0] },
    ];
    const sourceTable = await db.createTable("source", data);

    // Add more data to create a new version
    const moreData = [{ id: 3, text: "test", vector: [5.0, 6.0] }];
    await sourceTable.add(moreData);

    // Clone the table (should get latest version with 3 rows)
    const sourceUri = `${tmpDir.name}/source.lance`;
    const clonedTable = await db.cloneTable("cloned", sourceUri);

    // Verify cloned table has all 3 rows
    expect(await clonedTable.countRows()).toBe(3);
    expect((await db.tableNames()).includes("cloned")).toBe(true);
  });

  it("should clone a table from a specific version", async () => {
    // Create source table with initial data
    const data = [
      { id: 1, text: "hello", vector: [1.0, 2.0] },
      { id: 2, text: "world", vector: [3.0, 4.0] },
    ];
    const sourceTable = await db.createTable("source", data);

    // Get the initial version
    const initialVersion = await sourceTable.version();

    // Add more data to create a new version
    const moreData = [{ id: 3, text: "test", vector: [5.0, 6.0] }];
    await sourceTable.add(moreData);

    // Verify source now has 3 rows
    expect(await sourceTable.countRows()).toBe(3);

    // Clone from the initial version (should have only 2 rows)
    const sourceUri = `${tmpDir.name}/source.lance`;
    const clonedTable = await db.cloneTable("cloned", sourceUri, {
      sourceVersion: initialVersion,
    });

    // Verify cloned table has only the initial 2 rows
    expect(await clonedTable.countRows()).toBe(2);
  });

  it("should clone a table from a tagged version", async () => {
    // Create source table with initial data
    const data = [
      { id: 1, text: "hello", vector: [1.0, 2.0] },
      { id: 2, text: "world", vector: [3.0, 4.0] },
    ];
    const sourceTable = await db.createTable("source", data);

    // Create a tag for the current version
    const tags = await sourceTable.tags();
    await tags.create("v1.0", await sourceTable.version());

    // Add more data after the tag
    const moreData = [{ id: 3, text: "test", vector: [5.0, 6.0] }];
    await sourceTable.add(moreData);

    // Verify source now has 3 rows
    expect(await sourceTable.countRows()).toBe(3);

    // Clone from the tagged version (should have only 2 rows)
    const sourceUri = `${tmpDir.name}/source.lance`;
    const clonedTable = await db.cloneTable("cloned", sourceUri, {
      sourceTag: "v1.0",
    });

    // Verify cloned table has only the tagged version's 2 rows
    expect(await clonedTable.countRows()).toBe(2);
  });

  it("should fail when attempting deep clone", async () => {
    // Create source table with some data
    const data = [
      { id: 1, text: "hello", vector: [1.0, 2.0] },
      { id: 2, text: "world", vector: [3.0, 4.0] },
    ];
    await db.createTable("source", data);

    // Try to create a deep clone (should fail)
    const sourceUri = `${tmpDir.name}/source.lance`;
    await expect(
      db.cloneTable("cloned", sourceUri, { isShallow: false }),
    ).rejects.toThrow("Deep clone is not yet implemented");
  });
});

describe("namespaces", () => {
  let tmpDir: tmp.DirResult;
  let db: Connection;

  beforeEach(async () => {
    tmpDir = tmp.dirSync({ unsafeCleanup: true });
    // The local DirectoryNamespace backend only supports child namespaces
    // when manifest mode is enabled (see lance-namespace-impls/src/dir.rs).
    db = await connect(tmpDir.name, {
      // biome-ignore lint/style/useNamingConvention: opaque backend property key, must match Rust
      namespaceClientProperties: { manifest_enabled: "true" },
    });
  });
  afterEach(() => tmpDir.removeCallback());

  it("should create and describe a namespace", async () => {
    await db.createNamespace(["myns"]);
    const desc = await db.describeNamespace(["myns"]);
    expect(desc).toBeDefined();
  });

  it("should list namespaces created at the root", async () => {
    await db.createNamespace(["alpha"]);
    await db.createNamespace(["beta"]);
    const list = await db.listNamespaces();
    expect(list.namespaces).toEqual(expect.arrayContaining(["alpha", "beta"]));
  });

  it("should list child namespaces under a parent", async () => {
    await db.createNamespace(["parent"]);
    await db.createNamespace(["parent", "child"]);
    const list = await db.listNamespaces(["parent"]);
    expect(list.namespaces).toContain("child");
  });

  it("should drop a namespace", async () => {
    await db.createNamespace(["ephemeral"]);
    await db.dropNamespace(["ephemeral"]);
    const list = await db.listNamespaces();
    expect(list.namespaces).not.toContain("ephemeral");
  });

  it("should raise an error on any namespace op after close", async () => {
    await db.close();
    await expect(db.describeNamespace(["foo"])).rejects.toThrow(
      "Connection is closed",
    );
    await expect(db.listNamespaces()).rejects.toThrow("Connection is closed");
    await expect(db.createNamespace(["foo"])).rejects.toThrow(
      "Connection is closed",
    );
    await expect(db.dropNamespace(["foo"])).rejects.toThrow(
      "Connection is closed",
    );
  });

  it("should raise an understandable error when describing a non-existent namespace", async () => {
    await expect(db.describeNamespace(["does-not-exist"])).rejects.toThrow(
      /not found/i,
    );
  });

  it("should raise an error when creating a namespace that already exists", async () => {
    await db.createNamespace(["dup"]);
    await expect(db.createNamespace(["dup"])).rejects.toThrow();
  });

  it("should reject an unrecognized createNamespace mode with a clear error", async () => {
    await expect(
      // biome-ignore lint/suspicious/noExplicitAny: deliberately bypass TS to test runtime validation
      db.createNamespace(["x"], { mode: "frobnicate" as any }),
    ).rejects.toThrow(/Invalid mode 'frobnicate'/);
  });

  it("should reject an unrecognized dropNamespace mode with a clear error", async () => {
    await db.createNamespace(["x"]);
    await expect(
      // biome-ignore lint/suspicious/noExplicitAny: deliberately bypass TS to test runtime validation
      db.dropNamespace(["x"], { mode: "frobnicate" as any }),
    ).rejects.toThrow(/Invalid mode 'frobnicate'/);
  });

  it("should reject an unrecognized dropNamespace behavior with a clear error", async () => {
    await db.createNamespace(["x"]);
    await expect(
      // biome-ignore lint/suspicious/noExplicitAny: deliberately bypass TS to test runtime validation
      db.dropNamespace(["x"], { behavior: "frobnicate" as any }),
    ).rejects.toThrow(/Invalid behavior 'frobnicate'/);
  });
});

describe("connectNamespace", () => {
  let tmpDir: tmp.DirResult;
  beforeEach(() => {
    tmpDir = tmp.dirSync({ unsafeCleanup: true });
  });
  afterEach(() => tmpDir.removeCallback());

  it("connects via the dir implementation and supports table ops", async () => {
    const db = await connectNamespace("dir", { root: tmpDir.name });
    await db.createTable("users", [{ id: 1 }, { id: 2 }]);
    await expect(db.tableNames()).resolves.toContain("users");
  });

  it("throws a clear error when implName is empty", async () => {
    await expect(connectNamespace("", {})).rejects.toThrow(
      "implName must be a non-empty string",
    );
  });

  it("throws when the namespace implementation is unknown", async () => {
    await expect(connectNamespace("not-a-real-impl", {})).rejects.toThrow();
  });

  it("passes storage options through to the namespace", async () => {
    const db = await connectNamespace(
      "dir",
      { root: tmpDir.name },
      { storageOptions: { newTableDataStorageVersion: "stable" } },
    );
    await db.createTable("plumbing", [{ id: 1 }]);
    await expect(db.tableNames()).resolves.toContain("plumbing");
  });

  it("supports child namespaces when manifestEnabled is true on the dir config", async () => {
    const writer = await connectNamespace("dir", {
      root: tmpDir.name,
      manifestEnabled: true,
    });
    await writer.createNamespace(["analytics"]);
    await writer.createTable("orders", [{ id: 1 }, { id: 2 }], ["analytics"]);
    await writer.close();

    const reader = await connectNamespace("dir", {
      root: tmpDir.name,
      manifestEnabled: true,
    });
    await expect(reader.tableNames(["analytics"])).resolves.toContain("orders");
    const orders = await reader.openTable("orders", ["analytics"]);
    await expect(orders.countRows()).resolves.toBe(2);
  });

  it("merges extraProperties into the dir config and is overridden by typed fields", async () => {
    // Two observable assertions:
    // - Typed `root` overrides extraProperties.root: createTable would fail
    //   under the bogus path if the override didn't happen.
    // - extraProperties.manifest_enabled="false" is honored end-to-end. Child
    //   namespaces require manifest mode (default true), so explicitly
    //   disabling it via extraProperties must make createNamespace reject. If
    //   extraProperties pass-through were silently broken, the default would
    //   let createNamespace succeed.
    const db = await connectNamespace("dir", {
      root: tmpDir.name,
      extraProperties: {
        root: "/should/be/overridden",
        // biome-ignore lint/style/useNamingConvention: backend property key
        manifest_enabled: "false",
      },
    });
    await db.createTable("base", [{ id: 1 }]);
    await expect(db.tableNames()).resolves.toContain("base");
    await expect(db.createNamespace(["analytics"])).rejects.toThrow();
  });

  it("flows unknown top-level keys through when implName is dynamic (no silent drop)", async () => {
    // Routes via the third overload because `impl` is `string`, not the
    // literal `"dir"`. The dispatcher still notices the runtime value is
    // "dir", but unknown keys like `manifest_enabled` must not be silently
    // dropped during the conversion.
    //
    // Asserting a *negative* outcome (manifest disabled -> createNamespace
    // rejects) is required for observability, since the backend default for
    // `manifest_enabled` is true.
    const impl: string = "dir";
    const db = await connectNamespace(impl, {
      root: tmpDir.name,
      // biome-ignore lint/style/useNamingConvention: backend property key
      manifest_enabled: "false",
    });
    await expect(db.createNamespace(["mixed"])).rejects.toThrow();
  });
});
