// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import { Field, Int64, Schema } from "apache-arrow";
import { makeArrowTable } from "../lancedb/arrow";
import {
  BLOB_V2_EXTENSION_NAME,
  blob,
  coerceBlobValue,
  isBlobField,
} from "../lancedb/blob";

describe("blob()", () => {
  it("marks the field as lance.blob.v2", () => {
    const field = blob("image", { nullable: false });
    expect(field.nullable).toBe(false);
    expect(isBlobField(field)).toBe(true);
    expect(field.metadata.get("ARROW:extension:name")).toBe(
      BLOB_V2_EXTENSION_NAME,
    );
  });

  it("writes encoding thresholds as field metadata", () => {
    const field = blob("video", {
      inlineSizeThreshold: 1024,
      dedicatedSizeThreshold: 2 * 1024 * 1024,
      packFileSizeThreshold: 64 * 1024 * 1024,
    });
    expect(
      field.metadata.get("lance-encoding:blob-inline-size-threshold"),
    ).toBe("1024");
    expect(
      field.metadata.get("lance-encoding:blob-dedicated-size-threshold"),
    ).toBe(String(2 * 1024 * 1024));
    expect(
      field.metadata.get("lance-encoding:blob-pack-file-size-threshold"),
    ).toBe(String(64 * 1024 * 1024));
  });

  it("rejects invalid thresholds", () => {
    expect(() => blob("image", { inlineSizeThreshold: -1 })).toThrow(
      /inlineSizeThreshold must be non-negative/,
    );
    expect(() => blob("image", { dedicatedSizeThreshold: 0 })).toThrow(
      /dedicatedSizeThreshold must be positive/,
    );
    expect(() => blob("image", { packFileSizeThreshold: 1.5 })).toThrow(
      /packFileSizeThreshold must be a safe integer/,
    );
    expect(() =>
      blob("image", { dedicatedSizeThreshold: Number.MAX_SAFE_INTEGER + 1 }),
    ).toThrow(/dedicatedSizeThreshold must be a safe integer/);
  });
});

describe("coerceBlobValue", () => {
  it.each([
    ["Buffer", Buffer.from("x"), { data: Buffer.from("x"), uri: null }],
    [
      "Uint8Array",
      new Uint8Array([120]),
      { data: new Uint8Array([120]), uri: null },
    ],
    ["URI string", "s3://bucket/key", { data: null, uri: "s3://bucket/key" }],
    [
      "data struct",
      { data: Buffer.from("y") },
      { data: Buffer.from("y"), uri: null },
    ],
    [
      "uri struct",
      { uri: "s3://bucket/key" },
      { data: null, uri: "s3://bucket/key" },
    ],
    ["null", null, null],
  ])("accepts %s", (_name, input, expected) => {
    expect(coerceBlobValue(input)).toEqual(expected);
  });

  it.each([
    ["empty URI", "", /uri cannot be empty/],
    ["object without data or uri", { position: 0 }, /data' or 'uri/],
    [
      "Int16Array",
      new Int16Array([1]),
      /Blob data must be Buffer or Uint8Array/,
    ],
    [
      "both data and uri",
      { data: Buffer.from("y"), uri: "s3://bucket/key" },
      /exactly one of 'data' or 'uri'/,
    ],
    [
      "neither data nor uri",
      { data: null, uri: null },
      /exactly one of 'data' or 'uri'/,
    ],
  ])("rejects %s", (_name, input, message) => {
    expect(() => coerceBlobValue(input)).toThrow(message);
  });
});

describe("makeArrowTable blob columns", () => {
  it("coerces Buffer input onto a blob field", () => {
    const schema = new Schema([
      new Field("id", new Int64(), true),
      blob("image"),
    ]);
    const table = makeArrowTable([{ id: 1n, image: Buffer.from("hello") }], {
      schema,
    });
    expect(isBlobField(table.schema.fields[1])).toBe(true);
    const image = table.getChild("image")!;
    expect(image.nullCount).toBe(0);
    expect(image.getChild("uri")!.get(0)).toBeNull();
    expect(image.getChild("data")!.nullCount).toBe(0);
  });
});
