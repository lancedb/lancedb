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

  it("reads stored queries and legacy layouts", () => {
    const read = (stored: string) =>
      definitionFromMetadata(new Map([[DEFINITION_META_KEY, stored]]), "v");
    const query =
      "SELECT id, c.chunk FROM ns.docs, UNNEST(chunks) AS c WHERE id > 1";
    expect(read(`{"format":1,"query":${JSON.stringify(query)}}`).query).toBe(
      query,
    );

    // The structured layout written before the format number reads as the
    // query it described, under either of its kind tags.
    expect(
      read(
        '{"kind":"namespaced_select","source_table":"people","source_namespace":["ns"],' +
          '"projections":[{"output":"name","expression":"`name`"},' +
          '{"output":"Shout","expression":"upper(name)"}],"filter":"age >= 18","limit":42}',
      ).query,
    ).toBe(
      "SELECT `name`, upper(name) AS `Shout` FROM ns.people WHERE age >= 18 LIMIT 42",
    );
    expect(read('{"kind":"select","source_table":"people"}').query).toBe(
      "SELECT * FROM people",
    );

    // A newer writer's layout is reported, never guessed at.
    for (const newer of [
      `{"format":2,"query":${JSON.stringify(query)}}`,
      '{"kind":"select_v3","source_table":"people"}',
    ]) {
      expect(() => read(newer)).toThrow(/cannot refresh/);
    }
  });

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
    expect(definition.query).toBe(
      "SELECT name, age FROM people WHERE age >= 18",
    );
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
