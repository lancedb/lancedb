// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors
import { expect, test } from "@jest/globals";
// --8<-- [start:multimodal_imports]
import * as arrow from "apache-arrow";
import { Buffer } from "node:buffer";
import * as lancedb from "@lancedb/lancedb";
// --8<-- [end:multimodal_imports]
import { chdir, cwd } from "node:process";
import { withTempDirectory } from "./util.ts";

test("multimodal snippets (async)", async () => {
  await withTempDirectory(async (databaseDir) => {
    const db = await lancedb.connect(databaseDir);

    // --8<-- [start:create_dummy_data]
    const createDummyImage = (color: string): Uint8Array => {
      const pngHeader = Uint8Array.from([137, 80, 78, 71, 13, 10, 26, 10]);
      return Buffer.concat([Buffer.from(pngHeader), Buffer.from(color, "utf8")]);
    };

    const data = [
      {
        id: 1,
        filename: "red_square.png",
        vector: Array.from({ length: 128 }, (_, i) => (i % 16) / 16),
        image_blob: createDummyImage("red"),
        label: "red",
      },
      {
        id: 2,
        filename: "blue_square.png",
        vector: Array.from({ length: 128 }, (_, i) => ((i + 8) % 16) / 16),
        image_blob: createDummyImage("blue"),
        label: "blue",
      },
    ];
    // --8<-- [end:create_dummy_data]

    // --8<-- [start:define_schema]
    const schema = new arrow.Schema([
      new arrow.Field("id", new arrow.Int32()),
      new arrow.Field("filename", new arrow.Utf8()),
      new arrow.Field(
        "vector",
        new arrow.FixedSizeList(
          128,
          new arrow.Field("item", new arrow.Float32(), true),
        ),
      ),
      new arrow.Field("image_blob", new arrow.Binary()),
      new arrow.Field("label", new arrow.Utf8()),
    ]);
    // --8<-- [end:define_schema]

    // --8<-- [start:ingest_data]
    const multimodalData = lancedb.makeArrowTable(data, { schema });
    const tbl = await db.createTable("images", multimodalData, {
      mode: "overwrite",
    });
    // --8<-- [end:ingest_data]
    expect(await tbl.countRows()).toBe(2);

    // --8<-- [start:search_data]
    const queryVector = Array.from({ length: 128 }, (_, i) => (i % 16) / 16);
    const results = await tbl.search(queryVector).limit(1).toArray();
    // --8<-- [end:search_data]

    // --8<-- [start:process_results]
    for (const row of results) {
      const imageBytes = row.image_blob as Uint8Array;
      console.log(
        `Retrieved image: ${row.filename}, Byte length: ${imageBytes.length}`,
      );
    }
    // --8<-- [end:process_results]
    expect(results).toHaveLength(1);

    const previousCwd = cwd();
    chdir(databaseDir);
    try {
      // --8<-- [start:blob_api_setup]
      const db = await lancedb.connect("./blob_db");
      // --8<-- [end:blob_api_setup]

      // --8<-- [start:blob_api_schema]
      const blobSchema = new arrow.Schema([
        new arrow.Field("id", new arrow.Int64()),
        lancedb.blob("video", {
          inlineSizeThreshold: 64 * 1024,
          dedicatedSizeThreshold: 4 * 1024 * 1024,
        }),
      ]);
      // --8<-- [end:blob_api_schema]

      // --8<-- [start:blob_api_ingest]
      const blobTable = await db.createEmptyTable("videos", blobSchema, {
        mode: "overwrite",
      });

      await blobTable.add(
        [
          { id: 1n, video: Buffer.from("inline_video_bytes") },
          { id: 2n, video: Buffer.from("second_video_bytes") },
        ],
      );
      // --8<-- [end:blob_api_ingest]
      expect(await blobTable.countRows()).toBe(2);

      // --8<-- [start:blob_api_fetch]
      const rows = await blobTable
        .query()
        .select(["id", "video"])
        .withRowId()
        .toArray();
      const rowIds = rows.map((row) => row._rowid as bigint);

      const payloads = await blobTable.fetchBlobs("video", rowIds);

      const [clip] = await blobTable.fetchBlobFiles("video", [rowIds[0]!]);
      const header = await clip!.readRange(0n, 6n);
      await clip!.close();
      // --8<-- [end:blob_api_fetch]

      expect(payloads).toHaveLength(2);
      expect(header.toString("utf8")).toBe("inline");

      // --8<-- [start:blob_api_ranges]
      const [secondClip] = await blobTable.fetchBlobFiles("video", [rowIds[1]!]);
      const parts = await secondClip!.readRanges([
        { start: 0n, end: 6n },
        { start: 7n, end: 12n },
      ]);
      await secondClip!.close();
      // --8<-- [end:blob_api_ranges]
      expect(parts.map((part) => part.toString("utf8"))).toEqual([
        "second",
        "video",
      ]);

      // --8<-- [start:blob_api_bytes_mode]
      const withBytes = await blobTable
        .query()
        .select(["id", "video"])
        .toArray({ blobMode: "bytes" });
      const firstClip: Uint8Array | null = withBytes[0].video;
      // --8<-- [end:blob_api_bytes_mode]
      expect(Buffer.from(firstClip!).toString("utf8")).toBe(
        "inline_video_bytes",
      );

      // --8<-- [start:legacy_blob_api_schema]
      const legacyBlobSchema = new arrow.Schema([
        new arrow.Field("id", new arrow.Int64()),
        new arrow.Field(
          "video",
          new arrow.LargeBinary(),
          true,
          new Map([["lance-encoding:blob", "true"]]),
        ),
      ]);
      // --8<-- [end:legacy_blob_api_schema]

      // --8<-- [start:legacy_blob_api_ingest]
      const legacyBlobData = lancedb.makeArrowTable(
        [
          { id: 1, video: Buffer.from("fake_video_bytes_1") },
          { id: 2, video: Buffer.from("fake_video_bytes_2") },
        ],
        { schema: legacyBlobSchema },
      );
      const legacyBlobTable = await db.createTable(
        "legacy_videos",
        legacyBlobData,
        {
          mode: "overwrite",
        },
      );
      // --8<-- [end:legacy_blob_api_ingest]
      expect(await legacyBlobTable.countRows()).toBe(2);
    } finally {
      chdir(previousCwd);
    }
  });
});
