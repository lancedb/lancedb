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

async function createFrames(db: lancedb.Connection) {
  // --8<-- [start:quickstart_data]
  const data = [
    {
      frame_id: "1",
      video_id: "clip-1",
      caption: "a robot arm reaching toward a cup",
      stats: { motion: 3, sharpness: 4, exposure: 4, framing: 3 },
      embedding: [0.7, 0.1, 0.9, 0.7],
    },
    {
      frame_id: "2",
      video_id: "clip-1",
      caption: "a two-finger gripper closing on a cup",
      stats: { motion: 5, sharpness: 4, exposure: 5, framing: 2 },
      embedding: [0.2, 0.9, 0.4, 0.9],
    },
    {
      frame_id: "3",
      video_id: "clip-2",
      caption: "a robot arm holding a cup steady",
      stats: { motion: 2, sharpness: 3, exposure: 3, framing: 4 },
      embedding: [0.9, 0.1, 0.5, 0.4],
    },
  ];
  // --8<-- [end:quickstart_data]

  // --8<-- [start:quickstart_create_table]
  const table = await db.createTable("frames", data, { mode: "overwrite" });
  // --8<-- [end:quickstart_create_table]
  expect(await table.countRows()).toBe(3);

  // --8<-- [start:quickstart_create_indexes]
  await table.createIndex("embedding", { config: lancedb.Index.ivfPq() });
  await table.createIndex("stats.motion", { config: lancedb.Index.btree() });
  // --8<-- [end:quickstart_create_indexes]
  expect((await table.listIndices()).map((index) => index.name).sort()).toEqual(
    ["embedding_idx", "stats.motion_idx"],
  );

  // --8<-- [start:quickstart_add_data]
  await table.add([
    {
      frame_id: "4",
      video_id: "clip-2",
      caption: "a gripper lowering a cup onto the table",
      stats: { motion: 5, sharpness: 4, exposure: 4, framing: 2 },
      embedding: [0.3, 0.9, 0.6, 0.8],
    },
  ]);
  // --8<-- [end:quickstart_add_data]
  expect(await table.countRows()).toBe(4);
  return table;
}

describe.each(connections)("quickstart (%s)", (connection) => {
  test("create, add and search", async () => {
    await withDatabase(connection, async (db) => {
      const table = await createFrames(db);

      // --8<-- [start:quickstart_vector_search_1]
      // Search for frames similar to "a gripper closing on an object"
      const queryVector = [0.2, 0.8, 0.4, 0.9];

      const result = await table
        .search(queryVector)
        .select(["frame_id", "video_id", "caption", "_distance"])
        .limit(2)
        .toArray();
      console.table(result);
      // --8<-- [end:quickstart_vector_search_1]
      expect(result.map((row) => row.frame_id)).toEqual(["2", "4"]);

      // --8<-- [start:quickstart_curate_with_metadata]
      const curated = await table
        .search(queryVector)
        .where("stats.motion >= 4")
        .select(["frame_id", "video_id", "caption", "_distance"])
        .limit(2)
        .toArray();
      console.table(curated);
      // --8<-- [end:quickstart_curate_with_metadata]
      expect(curated.map((row) => row.frame_id)).toEqual(["2", "4"]);
    });
  });

  test("a derived feature", async () => {
    await withDatabase(connection, async (db) => {
      const table = await createFrames(db);

      // --8<-- [start:quickstart_add_feature]
      await table.addColumns([
        {
          name: "quality_score",
          valueSql:
            "cast(((stats.motion + stats.sharpness + stats.exposure + stats.framing) / 4.0) as float)",
        },
      ]);
      // --8<-- [end:quickstart_add_feature]

      // --8<-- [start:quickstart_query_feature]
      const features = await table
        .query()
        .select(["frame_id", "video_id", "quality_score"])
        .toArray();
      console.table(features);
      // --8<-- [end:quickstart_query_feature]
      expect(
        features.map((row) => [row.frame_id, row.quality_score]).sort(),
      ).toEqual([
        ["1", 3.5],
        ["2", 4],
        ["3", 3],
        ["4", 3.75],
      ]);
    });
  });

  test("multimodal data", async () => {
    await withDatabase(connection, async (db) => {
      await withTempDirectory(async (app) => {
        fs.mkdirSync(path.join(app, "data"));
        fs.copyFileSync(IMAGE, path.join(app, "data/sample-frame.jpg"));
        const cwd = process.cwd();
        process.chdir(app);
        try {
          // --8<-- [start:quickstart_multimodal_bytes]
          const arrow = await import("apache-arrow");
          const { readFile } = await import("node:fs/promises");

          const imageBytes = await readFile("data/sample-frame.jpg");
          const imageSchema = new arrow.Schema([
            new arrow.Field("frame_id", new arrow.Utf8()),
            new arrow.Field("caption", new arrow.Utf8()),
            new arrow.Field("image", new arrow.Binary()),
            new arrow.Field(
              "embedding",
              new arrow.FixedSizeList(
                4,
                new arrow.Field("item", new arrow.Float32(), true),
              ),
            ),
          ]);
          const imageData = lancedb.makeArrowTable(
            [
              {
                frame_id: "thumb-1",
                caption: "Sample frame thumbnail",
                image: imageBytes,
                embedding: [0.9, 0.1, 0.5, 0.4],
              },
            ],
            { schema: imageSchema },
          );
          const multimodalTable = await db.createTable(
            "frame_images",
            imageData,
            { mode: "overwrite" },
          );
          // --8<-- [end:quickstart_multimodal_bytes]
          expect(await multimodalTable.countRows()).toBe(1);
          const [row] = await multimodalTable
            .query()
            .select(["image"])
            .toArray();
          expect(Buffer.from(row.image)).toEqual(fs.readFileSync(IMAGE));
        } finally {
          process.chdir(cwd);
        }
      });
    });
  });
});
