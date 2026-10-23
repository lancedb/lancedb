// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors
import { expect, test } from "@jest/globals";
import * as fs from "node:fs";
import * as path from "node:path";
// --8<-- [start:basic_imports]
import * as lancedb from "@lancedb/lancedb";
import * as arrow from "apache-arrow";
// --8<-- [end:basic_imports]
import { withTempDirectory } from "./util.ts";

const dataPath = path.join(__dirname, "..", "camelot.json");

test("basic usage examples (async)", async () => {
  await withTempDirectory(async (databaseDir) => {
    const db = await lancedb.connect(databaseDir);

    // --8<-- [start:data_load]
    const data = JSON.parse(fs.readFileSync(dataPath, "utf-8"));
    // --8<-- [end:data_load]

    // --8<-- [start:basic_create_table]
    let table = await db.createTable("camelot", data, {
       mode: "overwrite",
    });
    // --8<-- [end:basic_create_table]
    expect(await table.countRows()).toBe(8);

    // --8<-- [start:basic_open_table]
    table = await db.openTable("camelot");
    // --8<-- [end:basic_open_table]

    // --8<-- [start:basic_create_empty_table]
    const schema = new arrow.Schema([
      new arrow.Field("id", new arrow.Int16()),
      new arrow.Field("name", new arrow.Utf8()),
      new arrow.Field("role", new arrow.Utf8()),
      new arrow.Field("description", new arrow.Utf8()),
      new arrow.Field(
        "vector",
        new arrow.FixedSizeList(
          4,
          new arrow.Field("item", new arrow.Float32(), true),
        ),
      ),
      new arrow.Field(
        "stats",
        new arrow.Struct([
          new arrow.Field("strength", new arrow.Int8()),
          new arrow.Field("courage", new arrow.Int8()),
          new arrow.Field("magic", new arrow.Int8()),
          new arrow.Field("wisdom", new arrow.Int8()),
        ]),
      ),
    ]);
    await db.createEmptyTable("camelot_empty", schema, { mode: "overwrite" });
    // --8<-- [end:basic_create_empty_table]
    expect(await db.tableNames()).toContain("camelot_empty");
    await db.dropTable("camelot_empty");

    // --8<-- [start:basic_add_data]
    const magicalCharacters = [
      {
        id: 9,
        name: "Morgan le Fay",
        role: "Sorceress",
        description:
          "A powerful enchantress, Arthur's half-sister, and a complex figure who oscillates between aiding and opposing Camelot.",
        vector: [0.1, 0.84, 0.25, 0.7],
        stats: { strength: 2, courage: 3, magic: 5, wisdom: 4 },
      },
      {
        id: 10,
        name: "The Lady of the Lake",
        role: "Mystical Guardian",
        description:
          "A mysterious supernatural figure associated with Avalon, known for giving Arthur the sword Excalibur.",
        vector: [0.0, 0.9, 0.58, 0.88],
        stats: { strength: 2, courage: 3, magic: 5, wisdom: 5 },
      },
    ];
    await table.add(magicalCharacters);
    // --8<-- [end:basic_add_data]
    expect(await table.countRows()).toBe(10);

    // --8<-- [start:basic_vector_search]
    const queryVector = [0.03, 0.85, 0.61, 0.9];
    const result = await table.search(queryVector).limit(5).toArray();
    console.log(result);
    // --8<-- [end:basic_vector_search]

    // --8<-- [start:basic_add_columns]
    await table.addColumns([
      {
        name: "power",
        valueSql:
          "cast(((stats.strength + stats.courage + stats.magic + stats.wisdom) / 4.0) as float)",
      },
    ]);
    // --8<-- [end:basic_add_columns]
    const schemaWithPower = await table.schema();
    expect(schemaWithPower.fields.some((f) => f.name === "power")).toBe(true);

    // --8<-- [start:basic_vector_search_q1]
    // Who are the characters similar to  "wizard"?
    const queryVector1 = [0.03, 0.85, 0.61, 0.9];
    const r1 = await table
      .search(queryVector1)
      .limit(5)
      .select(["name", "role", "description"])
      .toArray();
    console.log(r1);
    // --8<-- [end:basic_vector_search_q1]

    // --8<-- [start:basic_vector_search_q2]
    // Who are the characters similar to "wizard" with high magic stats?
    const queryVector2 = [0.03, 0.85, 0.61, 0.9];
    const r2 = await table
      .search(queryVector2)
      .where("stats.magic > 3")
      .select(["name", "role", "description"])
      .limit(5)
      .toArray();
    console.log(r2);
    // --8<-- [end:basic_vector_search_q2]

    // --8<-- [start:basic_vector_search_q3]
    // Who are the strongest characters?
    const r3 = await table
      .query()
      .where("stats.strength > 3")
      .select(["name", "role", "description"])
      .limit(5)
      .toArray();
    console.log(r3);
    // --8<-- [end:basic_vector_search_q3]

    // --8<-- [start:basic_vector_search_q4]
    // Who are the strongest characters?
    const r4 = await table
      .query()
      .select(["name", "role", "description", "power"])
      .toArray();
    console.log(r4);
    // --8<-- [end:basic_vector_search_q4]

    // --8<-- [start:basic_drop_columns]
    await table.dropColumns(["power"]);
    // --8<-- [end:basic_drop_columns]

    // --8<-- [start:basic_delete_rows]
    await table.delete('role = "Traitor Knight"');
    // --8<-- [end:basic_delete_rows]
    expect(await table.countRows()).toBe(9);

    // --8<-- [start:basic_drop_table]
    await db.dropTable("camelot");
    // --8<-- [end:basic_drop_table]
    expect(await db.tableNames()).not.toContain("camelot");
  });
});
