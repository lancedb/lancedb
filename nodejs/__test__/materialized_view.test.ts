// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import * as tmp from "tmp";

import { Connection, connect } from "../lancedb";
import {
  DEFINITION_META_KEY,
  definitionFromMetadata,
} from "../lancedb/materialized_view";

describe("materialized views", () => {
  let tmpDir: tmp.DirResult;
  let db: Connection;

  beforeEach(async () => {
    tmpDir = tmp.dirSync({ unsafeCleanup: true });
    db = await connect(tmpDir.name);
    await db.createTable(
      "people",
      [
        { name: "ada", age: 36 },
        { name: "kid", age: 7 },
        { name: "grace", age: 85 },
      ],
      { storageOptions: { newTableEnableStableRowIds: "true" } },
    );
  });
  afterEach(() => tmpDir.removeCallback());

  it("rejects a stored limit a number cannot carry", () => {
    const big = new Map([
      [
        DEFINITION_META_KEY,
        '{"kind":"select","source_table":"people","limit":9007199254740993}',
      ],
    ]);
    expect(() => definitionFromMetadata(big, "v")).toThrow(
      /too large to represent exactly/,
    );

    const safe = new Map([
      [
        DEFINITION_META_KEY,
        '{"kind":"select","source_table":"people","limit":42}',
      ],
    ]);
    expect(definitionFromMetadata(safe, "v").limit).toBe(42);
  });

  it("creates, refreshes and queries a view", async () => {
    const view = await db.createMaterializedView("adults", "people", {
      select: ["name", ["shout", "upper(name)"]],
      where: "age >= 18",
    });
    expect(view.name).toBe("adults");
    expect(await view.table().countRows()).toBe(0);

    const result = await view.refresh();
    expect(result.mode).toBe("rebuild");
    expect(Number(result.rowsWritten)).toBe(2);

    const rows = await view.table().query().toArray();
    expect(rows.map((r) => r.shout).sort()).toEqual(["ADA", "GRACE"]);
  });

  it("round-trips the definition", async () => {
    await db.createMaterializedView("adults", "people", {
      where: "age >= 18",
    });
    const view = await db.openMaterializedView("adults");
    const definition = await view.definition();
    expect(definition.sourceTable).toBe("people");
    expect(definition.filter).toBe("age >= 18");
    expect(definition.projections).toEqual([
      ["name", "`name`"],
      ["age", "`age`"],
    ]);
    expect(definition.inputs).toEqual(["age", "name"]);
  });

  it("refreshes incrementally after an append", async () => {
    const view = await db.createMaterializedView("copy", "people");
    await view.refresh();

    const people = await db.openTable("people");
    await people.add([{ name: "alan", age: 41 }]);
    const result = await view.refresh();
    expect(result.mode).toBe("incremental");
    expect(Number(result.rowsWritten)).toBe(1);
    expect(await view.table().countRows()).toBe(4);

    expect((await view.refresh()).mode).toBe("no_op");
  });

  it("lists views and rejects non-views", async () => {
    await db.createMaterializedView("adults", "people", {
      where: "age >= 18",
    });
    expect(await db.listMaterializedViews()).toEqual(["adults"]);
    await expect(db.openMaterializedView("people")).rejects.toThrow(
      "not a materialized view",
    );
  });

  it("rejects an invalid expression at create time", async () => {
    await expect(
      db.createMaterializedView("bad", "people", {
        select: [["x", "missing + 1"]],
      }),
    ).rejects.toThrow("missing");
  });

  it("rejects invalid numeric options before creating anything", async () => {
    for (const limit of [-5, 1.5, Infinity, NaN]) {
      await expect(
        db.createMaterializedView("bad", "people", { limit }),
      ).rejects.toThrow("non-negative integer");
    }
    expect(await db.listMaterializedViews()).toEqual([]);

    const view = await db.createMaterializedView("copy", "people");
    for (const sourceVersion of [-1, 1.5, Infinity, NaN]) {
      await expect(view.refresh({ sourceVersion })).rejects.toThrow(
        "non-negative integer",
      );
    }
  });

  it("quotes bare select names", async () => {
    await db.createTable("odd_names", [{ "order item": "widget" }], {
      storageOptions: { newTableEnableStableRowIds: "true" },
    });
    const view = await db.createMaterializedView("quoted", "odd_names", {
      select: ["order item"],
    });
    const result = await view.refresh();
    expect(Number(result.rowsWritten)).toBe(1);
  });

  it("requires stable row ids on the source", async () => {
    await db.createTable("plain", [{ x: 1 }]);
    await expect(db.createMaterializedView("v", "plain")).rejects.toThrow(
      "stable row ids",
    );
  });
});
