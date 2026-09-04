// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors
import { expect, test } from "@jest/globals";
// --8<-- [start:multimodal_imports]
import * as arrow from "apache-arrow";
import { Buffer } from "node:buffer";
import * as lancedb from "@lancedb/lancedb";
// --8<-- [end:multimodal_imports]
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

    // --8<-- [start:blob_api_schema]
    const blobSchema = new arrow.Schema([
      new arrow.Field("id", new arrow.Int64()),
      new arrow.Field(
        "video",
        new arrow.LargeBinary(),
        true,
        new Map([["lance-encoding:blob", "true"]]),
      ),
    ]);
    // --8<-- [end:blob_api_schema]

    // --8<-- [start:blob_api_ingest]
    const blobData = lancedb.makeArrowTable(
      [
        { id: 1, video: Buffer.from("fake_video_bytes_1") },
        { id: 2, video: Buffer.from("fake_video_bytes_2") },
      ],
      { schema: blobSchema },
    );
    const blobTable = await db.createTable("videos", blobData, {
      mode: "overwrite",
    });
    // --8<-- [end:blob_api_ingest]
    expect(await blobTable.countRows()).toBe(2);
  });
});
