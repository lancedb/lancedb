// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import { connect } from "../lancedb";
import { makeArrowTable } from "../lancedb/arrow";
import * as tmp from "tmp";

describe("metadata tests", () => {
  let tmpDir: tmp.DirResult;

  beforeEach(() => {
    tmpDir = tmp.dirSync({ unsafeCleanup: true });
  });

  afterEach(() => {
    tmpDir.removeCallback();
  });

  test("metadata accessors exist", async () => {
    const db = await connect(tmpDir.name);
    const data = makeArrowTable([
      { vector: [1.0, 2.0], text: "hello", id: 1 },
      { vector: [3.0, 4.0], text: "world", id: 2 },
      { vector: [5.0, 6.0], text: "test", id: 3 },
    ]);

    const table = await db.createTable("test_table", data);

    // Test that accessor objects are created correctly
    const tableMetadata = table.metadata;
    const schemaMetadata = table.schemaMetadata;
    const config = table.config;

    // Check that the objects have the expected methods
    expect(typeof tableMetadata.get).toBe("function");
    expect(typeof tableMetadata.update).toBe("function");
    expect(typeof schemaMetadata.get).toBe("function");
    expect(typeof schemaMetadata.update).toBe("function");
    expect(typeof config.get).toBe("function");
    expect(typeof config.update).toBe("function");
  });

  test("metadata get panics with todo", async () => {
    const db = await connect(tmpDir.name);
    const data = makeArrowTable([
      { vector: [1.0, 2.0], text: "hello", id: 1 },
      { vector: [3.0, 4.0], text: "world", id: 2 },
      { vector: [5.0, 6.0], text: "test", id: 3 },
    ]);

    const table = await db.createTable("test_table", data);

    // Test that calling get() on local tables panics with todo! message
    // Since we're using todo!() in the local implementation, these should fail

    await expect(table.metadata.get()).rejects.toThrow();
    await expect(table.schemaMetadata.get()).rejects.toThrow();
    await expect(table.config.get()).rejects.toThrow();
  });

  test("metadata update panics with todo", async () => {
    const db = await connect(tmpDir.name);
    const data = makeArrowTable([
      { vector: [1.0, 2.0], text: "hello", id: 1 },
      { vector: [3.0, 4.0], text: "world", id: 2 },
      { vector: [5.0, 6.0], text: "test", id: 3 },
    ]);

    const table = await db.createTable("test_table", data);

    // Test that calling update() on local tables panics with todo! message
    await expect(
      table.metadata.update({ key: "value" })
    ).rejects.toThrow();

    await expect(
      table.schemaMetadata.update({ key: "value" })
    ).rejects.toThrow();

    await expect(table.config.update({ key: "value" })).rejects.toThrow();
  });

  test("metadata update replace parameter", async () => {
    const db = await connect(tmpDir.name);
    const data = makeArrowTable([
      { vector: [1.0, 2.0], text: "hello", id: 1 },
      { vector: [3.0, 4.0], text: "world", id: 2 },
      { vector: [5.0, 6.0], text: "test", id: 3 },
    ]);

    const table = await db.createTable("test_table", data);

    // Test that calling update() with replace=true also panics (with todo!)
    await expect(
      table.metadata.update({ key: "value" }, true)
    ).rejects.toThrow();

    // Test with replace=false (default)
    await expect(
      table.metadata.update({ key: "value" }, false)
    ).rejects.toThrow();
  });

  test("metadata update with null values", async () => {
    const db = await connect(tmpDir.name);
    const data = makeArrowTable([
      { vector: [1.0, 2.0], text: "hello", id: 1 },
      { vector: [3.0, 4.0], text: "world", id: 2 },
      { vector: [5.0, 6.0], text: "test", id: 3 },
    ]);

    const table = await db.createTable("test_table", data);

    // Test that calling update() with null values (for deletion) panics with todo!
    await expect(
      table.metadata.update({ key_to_delete: null, key_to_set: "value" })
    ).rejects.toThrow();
  });
});