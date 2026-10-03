// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors
import { expect, test } from "@jest/globals";
import * as lancedb from "@lancedb/lancedb";
import { withTempDirectory } from "./util.ts";

test("quickstart example (async)", async () => {
  await withTempDirectory(async (databaseDir) => {
    const db = await lancedb.connect(databaseDir);

    // --8<-- [start:quickstart_data]
    const data = [
      {
        id: "1",
        name: "King Arthur",
        role: "King",
        description: "Leader of Camelot and wielder of Excalibur.",
        stats: { strength: 4, magic: 1, leadership: 5, wisdom: 4 },
        vector: [0.7, 0.1, 0.9, 0.7],
      },
      {
        id: "2",
        name: "Merlin",
        role: "Wizard",
        description: "Advisor and prophet with deep magical knowledge.",
        stats: { strength: 2, magic: 5, leadership: 4, wisdom: 5 },
        vector: [0.2, 0.9, 0.4, 0.9],
      },
      {
        id: "3",
        name: "Sir Lancelot",
        role: "Knight",
        description: "Legendary knight known for courage and combat skill.",
        stats: { strength: 5, magic: 1, leadership: 3, wisdom: 3 },
        vector: [0.9, 0.1, 0.5, 0.4],
      },
    ];
    // --8<-- [end:quickstart_data]

    // --8<-- [start:quickstart_create_table]
    let table = await db.createTable("characters", data, { mode: "overwrite" });
    // --8<-- [end:quickstart_create_table]
    expect(await table.countRows()).toBe(3);
    await db.dropTable("characters");

    // --8<-- [start:quickstart_create_table_no_overwrite]
    table = await db.createTable("characters", data);
    // --8<-- [end:quickstart_create_table_no_overwrite]
    expect(await table.countRows()).toBe(3);

    // --8<-- [start:quickstart_vector_search_1]
    // Search for examples similar to a "wise magical advisor"
    let queryVector = [0.2, 0.8, 0.4, 0.9];

    let result = await table
      .search(queryVector)
      .select(["name", "role", "description", "_distance"])
      .limit(2)
      .toArray();
    console.table(result);
    // --8<-- [end:quickstart_vector_search_1]
    expect(result[0].name).toBe("Merlin");

    // --8<-- [start:quickstart_curate_with_metadata]
    const curated = await table
      .search(queryVector)
      .where("stats.magic >= 4")
      .select(["name", "role", "description", "_distance"])
      .limit(2)
      .toArray();
    console.table(curated);
    // --8<-- [end:quickstart_curate_with_metadata]
    expect(curated[0].name).toBe("Merlin");

    // --8<-- [start:quickstart_output_array]
    result = await table.search(queryVector).limit(2).toArray();
    console.table(result);
    // --8<-- [end:quickstart_output_array]
    expect(result[0].name).toBe("Merlin");

    // --8<-- [start:quickstart_add_feature]
    await table.addColumns([
      {
        name: "power_score",
        valueSql:
          "cast(((stats.strength + stats.magic + stats.leadership + stats.wisdom) / 4.0) as float)",
      },
    ]);
    // --8<-- [end:quickstart_add_feature]
    const schemaWithFeature = await table.schema();
    expect(schemaWithFeature.fields.some((f) => f.name === "power_score")).toBe(
      true,
    );

    // --8<-- [start:quickstart_query_feature]
    const features = await table
      .query()
      .select(["name", "role", "power_score"])
      .toArray();
    console.table(features);
    // --8<-- [end:quickstart_query_feature]
    expect(features[0]).toHaveProperty("power_score");

    // --8<-- [start:quickstart_multimodal_bytes]
    const arrow = await import("apache-arrow");
    const path = await import("node:path");
    const { readFile } = await import("node:fs/promises");

    const imagePath = path.resolve(
      "../../docs/static/assets/images/quickstart/sir-lancelot.jpg",
    );
    const imageBytes = await readFile(imagePath);
    const imageSchema = new arrow.Schema([
      new arrow.Field("id", new arrow.Utf8()),
      new arrow.Field("description", new arrow.Utf8()),
      new arrow.Field("image", new arrow.Binary()),
      new arrow.Field(
        "vector",
        new arrow.FixedSizeList(
          4,
          new arrow.Field("item", new arrow.Float32(), true),
        ),
      ),
    ]);
    const imageData = lancedb.makeArrowTable(
      [
        {
          id: "lancelot",
          description: "Portrait of Sir Lancelot",
          image: imageBytes,
          vector: [0.9, 0.1, 0.5, 0.4],
        },
      ],
      { schema: imageSchema },
    );
    const multimodalTable = await db.createTable(
      "character_images",
      imageData,
      { mode: "overwrite" },
    );
    // --8<-- [end:quickstart_multimodal_bytes]
    expect(await multimodalTable.countRows()).toBe(1);

    // --8<-- [start:quickstart_open_table]
    table = await db.openTable("characters");
    // --8<-- [end:quickstart_open_table]

    // --8<-- [start:quickstart_add_data]
    const moreData = [
      {
        id: "4",
        name: "Morgana",
        role: "Sorceress",
        description: "Powerful sorceress of Avalon.",
        stats: { strength: 2, magic: 5, leadership: 4, wisdom: 4 },
        vector: [0.3, 0.9, 0.6, 0.8],
        power_score: 3.75,
      },
    ];

    // Add data to table
    await table.add(moreData);
    // --8<-- [end:quickstart_add_data]
    expect(await table.countRows()).toBe(4);

    // --8<-- [start:quickstart_vector_search_2]
    // Search for examples similar to a "powerful sorceress"
    queryVector = [0.3, 0.9, 0.6, 0.8];

    const results = await table.search(queryVector).limit(2).toArray();
    console.table(results);
    // --8<-- [end:quickstart_vector_search_2]
    expect(results[0].name).toBe("Morgana");
  });
});
