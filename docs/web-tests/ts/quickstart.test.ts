// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors
import { describe, expect, test } from "@jest/globals";
import { randomUUID } from "node:crypto";
import * as fs from "node:fs";
import * as path from "node:path";
import * as lancedb from "@lancedb/lancedb";
import { withTempDirectory } from "./util.ts";

// What the quickstart's download step leaves in an application's directory.
const IMAGE = path.join(
  __dirname,
  "../../web/static/assets/images/quickstart/sir-lancelot.jpg",
);
const deployment = ["LANCEDB_URI", "LANCEDB_HOST_OVERRIDE", "LANCEDB_API_KEY"];
// The connection settings alone never send these tests to a deployment.
const optIn = "LANCEDB_DOCS_TEST_DEPLOYMENT";
const connections =
  process.env[optIn] === "1" ? ["local", "enterprise"] : ["local"];

async function tableNames(db: lancedb.Connection) {
  const names: string[] = [];
  for await (const name of db.listTables()) names.push(name);
  return names;
}

// A deployment connection on which a test creates only tables of its own:
// each name gets a suffix unique to the test and is claimed only while no
// table has it; the test drops what it claimed, whether or not it passed.
async function withOwnedTables(
  db: lancedb.Connection,
  fn: (db: lancedb.Connection) => Promise<void>,
) {
  const suffix = randomUUID().replaceAll("-", "").slice(0, 12);
  const claimed: string[] = [];
  const createTable = async (
    name: string,
    data: Parameters<lancedb.Connection["createTable"]>[1],
    options: Partial<lancedb.CreateTableOptions> = {},
  ) => {
    const owned = `${name}_${suffix}`;
    let mode = options.mode;
    if (!claimed.includes(owned)) {
      if ((await tableNames(db)).includes(owned)) {
        throw new Error(`${owned} already exists and is not this test's`);
      }
      claimed.push(owned);
      mode = "create";
    }
    return db.createTable(owned, data, { ...options, mode });
  };
  try {
    await fn(
      new Proxy(db, {
        get: (target, property, receiver) =>
          property === "createTable"
            ? createTable
            : Reflect.get(target, property, receiver),
      }),
    );
  } finally {
    const existing = await tableNames(db);
    for (const name of claimed) {
      if (existing.includes(name)) {
        await db.dropTable(name);
      }
    }
  }
}

async function withDatabase(
  connection: string,
  fn: (db: lancedb.Connection) => Promise<void>,
) {
  if (connection === "enterprise") {
    const missing = deployment.filter((name) => !process.env[name]);
    if (missing.length) {
      throw new Error(`${optIn}=1 needs ${missing.join(", ")}`);
    }
    const db = await lancedb.connect(process.env.LANCEDB_URI!, {
      apiKey: process.env.LANCEDB_API_KEY!,
      region: process.env.LANCEDB_REGION ?? "us-east-1",
      hostOverride: process.env.LANCEDB_HOST_OVERRIDE!,
    });
    await withOwnedTables(db, fn);
    return;
  }
  await withTempDirectory(async (databaseDir) => {
    await fn(await lancedb.connect(databaseDir));
  });
}

async function createCharacters(db: lancedb.Connection) {
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
  const table = await db.createTable("characters", data, { mode: "overwrite" });
  // --8<-- [end:quickstart_create_table]
  expect(await table.countRows()).toBe(3);

  // --8<-- [start:quickstart_add_data]
  await table.add([
    {
      id: "4",
      name: "Morgana",
      role: "Sorceress",
      description: "Powerful sorceress of Avalon.",
      stats: { strength: 2, magic: 5, leadership: 4, wisdom: 4 },
      vector: [0.3, 0.9, 0.6, 0.8],
    },
  ]);
  // --8<-- [end:quickstart_add_data]
  expect(await table.countRows()).toBe(4);
  return table;
}

describe.each(connections)("quickstart (%s)", (connection) => {
  test("create, add and search", async () => {
    await withDatabase(connection, async (db) => {
      const table = await createCharacters(db);

      // --8<-- [start:quickstart_vector_search_1]
      // Search for examples similar to a "wise magical advisor"
      const queryVector = [0.2, 0.8, 0.4, 0.9];

      const result = await table
        .search(queryVector)
        .select(["name", "role", "description", "_distance"])
        .limit(2)
        .toArray();
      console.table(result);
      // --8<-- [end:quickstart_vector_search_1]
      expect(result.map((row) => row.name)).toEqual(["Merlin", "Morgana"]);

      // --8<-- [start:quickstart_curate_with_metadata]
      const curated = await table
        .search(queryVector)
        .where("stats.magic >= 4")
        .select(["name", "role", "description", "_distance"])
        .limit(2)
        .toArray();
      console.table(curated);
      // --8<-- [end:quickstart_curate_with_metadata]
      expect(curated.map((row) => row.name)).toEqual(["Merlin", "Morgana"]);
    });
  });

  test("a derived feature", async () => {
    await withDatabase(connection, async (db) => {
      const table = await createCharacters(db);

      // --8<-- [start:quickstart_add_feature]
      await table.addColumns([
        {
          name: "power_score",
          valueSql:
            "cast(((stats.strength + stats.magic + stats.leadership + stats.wisdom) / 4.0) as float)",
        },
      ]);
      // --8<-- [end:quickstart_add_feature]

      // --8<-- [start:quickstart_query_feature]
      const features = await table
        .query()
        .select(["name", "role", "power_score"])
        .toArray();
      console.table(features);
      // --8<-- [end:quickstart_query_feature]
      expect(
        features.map((row) => [row.name, row.power_score]).sort(),
      ).toEqual([
        ["King Arthur", 3.5],
        ["Merlin", 4],
        ["Morgana", 3.75],
        ["Sir Lancelot", 3],
      ]);
    });
  });

  test("multimodal data", async () => {
    await withDatabase(connection, async (db) => {
      await withTempDirectory(async (app) => {
        fs.mkdirSync(path.join(app, "data"));
        fs.copyFileSync(IMAGE, path.join(app, "data/sir-lancelot.jpg"));
        const cwd = process.cwd();
        process.chdir(app);
        try {
          // --8<-- [start:quickstart_multimodal_bytes]
          const arrow = await import("apache-arrow");
          const { readFile } = await import("node:fs/promises");

          const imageBytes = await readFile("data/sir-lancelot.jpg");
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
          const [row] = await multimodalTable.query().select(["image"]).toArray();
          expect(Buffer.from(row.image)).toEqual(fs.readFileSync(IMAGE));
        } finally {
          process.chdir(cwd);
        }
      });
    });
  });
});
