// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { chdir, cwd } from "node:process";
import { test } from "node:test";
import * as arrow from "apache-arrow";
import { Buffer } from "node:buffer";
import * as lancedb from "@lancedb/lancedb";

const INLINE_MAX = 64 * 1024;
const DEDICATED_MIN = 4 * 1024 * 1024;

function marked(size: number, marker: number): Buffer {
  const payload = Buffer.alloc(size);
  payload[0] = marker;
  payload[payload.length - 1] = marker;
  return payload;
}

function kindOf(value: unknown): number {
  assert.equal(typeof value, "object");
  assert.ok(value !== null);
  const kind = (value as { kind?: unknown }).kind;
  assert.equal(typeof kind, "number");
  return kind;
}

test("blob column tutorial", async () => {
  const directory = mkdtempSync(join(tmpdir(), "blob-v2-"));
  const previous = cwd();
  chdir(directory);
  try {
    // --8<-- [start:blob_api_setup]
    const db = await lancedb.connect("./blob_db");
    // --8<-- [end:blob_api_setup]

    // --8<-- [start:blob_api_schema]
    const blobSchema = new arrow.Schema([
      new arrow.Field("id", new arrow.Int64()),
      new arrow.Field("label", new arrow.Utf8()),
      lancedb.blob("image"),
    ]);
    // --8<-- [end:blob_api_schema]

    // --8<-- [start:blob_api_ingest]
    const blobTable = await db.createEmptyTable("photos", blobSchema, {
      mode: "overwrite",
    });

    await blobTable.add([
      { id: 1n, label: "door", image: Buffer.from("inline_image_bytes") },
      { id: 2n, label: "window", image: Buffer.from("second_image_bytes") },
    ]);
    // --8<-- [end:blob_api_ingest]
    assert.equal(await blobTable.countRows(), 2);

    // --8<-- [start:blob_api_files]
    const rows = await blobTable
      .query()
      .select(["id", "label"])
      .withRowId()
      .toArray();
    const rowIds = rows.map((row) => row._rowid as bigint);

    const [image] = await blobTable.fetchBlobFiles("image", [rowIds[0]!]);
    const header = await image!.readRange(0n, 6n);
    await image!.close();
    // --8<-- [end:blob_api_files]
    assert.equal(header.toString("utf8"), "inline");

    // --8<-- [start:blob_api_fetch]
    const payloads = await blobTable.fetchBlobs("image", rowIds);
    // --8<-- [end:blob_api_fetch]
    assert.equal(payloads.length, 2);

    // --8<-- [start:blob_api_ranges]
    const [secondImage] = await blobTable.fetchBlobFiles("image", [rowIds[1]!]);
    const parts = await secondImage!.readRanges([
      { start: 0n, end: 6n },
      { start: 7n, end: 12n },
    ]);
    await secondImage!.close();
    // --8<-- [end:blob_api_ranges]
    assert.deepEqual(
      parts.map((part) => part.toString("utf8")),
      ["second", "image"],
    );

    // --8<-- [start:blob_api_bytes_mode]
    const withBytes = await blobTable
      .query()
      .select(["id", "image"])
      .toArray({ blobMode: "bytes" });
    const firstImage: Uint8Array | null = withBytes[0].image;
    // --8<-- [end:blob_api_bytes_mode]
    assert.equal(Buffer.from(firstImage!).toString("utf8"), "inline_image_bytes");
    assert.equal(Object.hasOwn(withBytes[0], "_rowid"), false);
  } finally {
    chdir(previous);
    rmSync(directory, { recursive: true, force: true });
  }
});

test("eager query returns blob bytes without a row id", async () => {
  const directory = mkdtempSync(join(tmpdir(), "blob-v2-eager-"));
  try {
    const db = await lancedb.connect(directory);
    const schema = new arrow.Schema([
      new arrow.Field("id", new arrow.Int64()),
      lancedb.blob("image"),
    ]);
    const table = await db.createEmptyTable("photos", schema);
    const payload = Buffer.from("inline-bytes");
    await table.add([{ id: 1n, image: payload }]);

    const rows = await table
      .query()
      .where("id = 1")
      .select(["id", "image"])
      .toArray({ blobMode: "bytes" });

    assert.equal(rows.length, 1);
    assert.equal(Object.hasOwn(rows[0], "_rowid"), false);
    assert.equal(Buffer.from(rows[0].image).toString("utf8"), "inline-bytes");
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("deferred fetch reads a blob by row id", async () => {
  const directory = mkdtempSync(join(tmpdir(), "blob-v2-deferred-"));
  try {
    const db = await lancedb.connect(directory);
    const schema = new arrow.Schema([
      new arrow.Field("id", new arrow.Int64()),
      lancedb.blob("image"),
    ]);
    const table = await db.createEmptyTable("photos", schema);
    await table.add([{ id: 1n, image: Buffer.from("deferred-bytes") }]);

    const described = await table
      .query()
      .where("id = 1")
      .select(["id", "image"])
      .toArray();
    assert.equal(kindOf(described[0].image), 0);
    assert.equal(Object.hasOwn(described[0], "_rowid"), false);

    const found = await table.query().where("id = 1").select(["id"]).withRowId().toArray();
    const rowId = found[0]._rowid as bigint;
    const payloads = await table.fetchBlobs("image", [rowId]);
    const [handle] = await table.fetchBlobFiles("image", [rowId]);
    const streamed = await handle!.readRange(0n, handle!.size());
    await handle!.close();

    assert.equal(Buffer.from(payloads[0]).toString("utf8"), "deferred-bytes");
    assert.equal(Buffer.from(streamed).toString("utf8"), "deferred-bytes");
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("storage tiers round-trip", async () => {
  const cases = [
    { id: 1n, image: marked(INLINE_MAX, 11), kind: 0 },
    { id: 2n, image: marked(INLINE_MAX + 1, 22), kind: 1 },
    { id: 3n, image: marked(DEDICATED_MIN, 33), kind: 1 },
    { id: 4n, image: marked(DEDICATED_MIN + 1, 44), kind: 2 },
  ];
  const directory = mkdtempSync(join(tmpdir(), "blob-v2-tiers-"));
  try {
    const db = await lancedb.connect(directory);
    const schema = new arrow.Schema([
      new arrow.Field("id", new arrow.Int64()),
      lancedb.blob("image"),
    ]);
    const table = await db.createEmptyTable("tiers", schema);
    await table.add(cases.map(({ id, image }) => ({ id, image })));

    const described = await table.query().select(["id", "image"]).toArray();
    const located = await table.query().select(["id"]).withRowId().toArray();
    const rowIdByValue = new Map(located.map((row) => [row.id as bigint, row._rowid as bigint]));

    for (const item of cases) {
      const descriptor = described.find((row) => row.id === item.id);
      assert.ok(descriptor);
      assert.equal(kindOf(descriptor.image), item.kind);
      const rowId = rowIdByValue.get(item.id);
      assert.ok(rowId !== undefined);
      const [payload] = await table.fetchBlobs("image", [rowId]);
      assert.deepEqual(Buffer.from(payload), item.image);
      const [handle] = await table.fetchBlobFiles("image", [rowId]);
      const streamed = await handle!.read();
      await handle!.close();
      assert.deepEqual(streamed, item.image);
    }
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});
