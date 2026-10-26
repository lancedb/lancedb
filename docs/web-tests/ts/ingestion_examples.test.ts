// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors
import { expect, test } from "@jest/globals";

test("complete ingestion workflow", async () => {
  // --8<-- [start:write_workflow_body]
  const lancedb = await import("@lancedb/lancedb");
  const { mkdtemp, rm } = await import("node:fs/promises");
  const { tmpdir } = await import("node:os");
  const { join } = await import("node:path");

  const uri = await mkdtemp(join(tmpdir(), "lancedb-ingestion-"));
  try {
    const db = await lancedb.connect(uri);
    const table = await db.createTable("users", [
      { id: 1, name: "Alice", login_count: 10 },
      { id: 2, name: "Bob", login_count: 20 },
    ]);
    await table.add([{ id: 3, name: "Charlie", login_count: 5 }]);
    await table.update({ where: "id = 2", values: { name: "Bobby" } });
    await table.update({
      where: "id = 2",
      valuesSql: { login_count: "login_count + 1" },
    });
    await table
      .mergeInsert("id")
      .whenMatchedUpdateAll()
      .whenNotMatchedInsertAll()
      .execute([
        { id: 3, name: "Charlie", login_count: 6 },
        { id: 4, name: "Dana", login_count: 1 },
      ]);
    console.log(
      "Rows selected for deletion:",
      await table.query().where("id = 4").toArray(),
    );
    await table.delete("id = 4");
    const rows = (await table.query().toArray()).sort((a, b) => a.id - b.id);
    console.log(rows);
    // --8<-- [end:write_workflow_body]
    expect(rows.map((row) => [row.id, row.name, row.login_count])).toEqual([
      [1, "Alice", 10],
      [2, "Bobby", 21],
      [3, "Charlie", 6],
    ]);
    // --8<-- [start:write_workflow_tail]
  } finally {
    await rm(uri, { recursive: true, force: true });
  }
  // --8<-- [end:write_workflow_tail]
});

test("complete delete_rows", async () => {
  // --8<-- [start:delete_rows_body]
  const lancedb = await import("@lancedb/lancedb");
  const { mkdtemp, rm } = await import("node:fs/promises");
  const { tmpdir } = await import("node:os");
  const { join } = await import("node:path");
  const uri = await mkdtemp(join(tmpdir(), "lancedb-ingestion-"));
  try {
    const db = await lancedb.connect(uri);
    const table = await db.createTable("users", [
      { id: 1, name: "Alice" },
      { id: 2, name: "Bob" },
      { id: 3, name: "Charlie" },
    ]);
    const predicate = "id = 3";
    console.log("Selected:", await table.query().where(predicate).toArray());
    await table.delete(predicate);
    const rows = (await table.query().toArray()).sort((a, b) => a.id - b.id);
    console.log("Remaining:", rows);
    // --8<-- [end:delete_rows_body]
    expect(rows.map((row) => [row.id, row.name])).toEqual([
      [1, "Alice"],
      [2, "Bob"],
    ]);
    // --8<-- [start:delete_rows_tail]
  } finally {
    await rm(uri, { recursive: true, force: true });
  }
  // --8<-- [end:delete_rows_tail]
});

test("complete add_progress", async () => {
  // --8<-- [start:add_progress_body]
  const lancedb = await import("@lancedb/lancedb");
  const { mkdtemp, rm } = await import("node:fs/promises");
  const { tmpdir } = await import("node:os");
  const { join } = await import("node:path");
  const uri = await mkdtemp(join(tmpdir(), "lancedb-ingestion-"));
  try {
    const db = await lancedb.connect(uri);
    const table = await db.createTable("users", [{ id: 1, name: "Alice" }]);
    await table.add(
      [
        { id: 2, name: "Bob" },
        { id: 3, name: "Charlie" },
      ],
      {
        progress: (progress) =>
          console.log(`Rows written: ${progress.outputRows}`),
      },
    );
    const rows = (await table.query().toArray()).sort((a, b) => a.id - b.id);
    console.log("After insertion:", rows);
    // --8<-- [end:add_progress_body]
    expect(rows.map((row) => [row.id, row.name])).toEqual([
      [1, "Alice"],
      [2, "Bob"],
      [3, "Charlie"],
    ]);
    // --8<-- [start:add_progress_tail]
  } finally {
    await rm(uri, { recursive: true, force: true });
  }
  // --8<-- [end:add_progress_tail]
});
