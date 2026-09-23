// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import { Field, Int64, List, Schema, Struct, Utf8 } from "apache-arrow";
import { makeArrowTable, resolveBlobInputs } from "../lancedb/arrow";
import { BlobFile, blob, coerceBlobValue, isBlobField } from "../lancedb/blob";

describe("blob()", () => {
  it("marks the field as lance.blob.v2", () => {
    const field = blob("image", { nullable: false });
    expect(field.nullable).toBe(false);
    expect(isBlobField(field)).toBe(true);
    expect(field.metadata.get("ARROW:extension:name")).toBe("lance.blob.v2");
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
    [
      "ArrayBuffer",
      new Uint8Array([120]).buffer,
      { data: new Uint8Array([120]), uri: null },
    ],
    [
      "URL",
      new URL("https://example.com/blob"),
      { data: null, uri: "https://example.com/blob" },
    ],
    [
      "data struct with ArrayBuffer",
      { data: new Uint8Array([121]).buffer },
      { data: new Uint8Array([121]), uri: null },
    ],
    [
      "uri struct with URL",
      { uri: new URL("file:///tmp/a.png") },
      { data: null, uri: "file:///tmp/a.png" },
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
      /Blob data must be Buffer, Uint8Array, ArrayBuffer, or Blob/,
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
    ["Blob", new Blob(["x"]), /must be read asynchronously/],
    ["data struct with Blob", { data: new Blob(["x"]) }, /asynchronously/],
  ])("rejects %s", (_name, input, message) => {
    expect(() => coerceBlobValue(input)).toThrow(message);
  });
});

describe("resolveBlobInputs", () => {
  it("reads Blob and File values in blob columns only", async () => {
    const schema = new Schema([
      new Field("name", new Utf8(), true),
      blob("image"),
      new Field("images", new List(blob("image")), true),
    ]);
    const shared = new Blob(["same"]);
    const notABlobColumn = new Blob(["other"]);
    const data = [
      {
        name: notABlobColumn,
        image: new File(["file"], "a.txt"),
        images: [shared, { data: shared }, null],
      },
      { name: "x", image: Buffer.from("buf"), images: null },
    ];
    const resolved = await resolveBlobInputs(data, schema);

    expect(resolved[0].name).toBe(notABlobColumn);
    expect(Buffer.from(resolved[0].image as Uint8Array).toString()).toBe(
      "file",
    );
    const images = resolved[0].images as [Uint8Array, { data: Uint8Array }];
    expect(Buffer.from(images[0]).toString()).toBe("same");
    expect(Buffer.from(images[1].data).toString()).toBe("same");
    // Rows without Blob values and the caller's records are left alone.
    expect(resolved[1]).toBe(data[1]);
    expect(data[0].images[0]).toBe(shared);
  });

  it("returns the input when there is nothing to read", async () => {
    const data = [{ image: Buffer.from("x") }];
    const schema = new Schema([blob("image")]);
    await expect(resolveBlobInputs(data, schema)).resolves.toBe(data);
    await expect(resolveBlobInputs(data)).resolves.toBe(data);
  });
});

describe("BlobFile", () => {
  it("rejects constructing BlobFile without a native handle", () => {
    expect(() => new (BlobFile as unknown as { new (): BlobFile })()).toThrow(
      /fetchBlobFiles/,
    );
  });
});

describe("makeArrowTable blob columns", () => {
  it.each([
    ["ReadableStream", new ReadableStream()],
    ["Int16Array", new Int16Array([1])],
  ])("rejects %s input rather than writing a null blob", (_name, image) => {
    const schema = new Schema([blob("image")]);
    expect(() => makeArrowTable([{ image }], { schema })).toThrow(/image/);
  });

  it.each([
    ["Blob", new Blob(["hello"])],
    ["File", new File(["hello"], "hello.txt")],
    ["data struct with Blob", { data: new Blob(["hello"]) }],
    [
      "Blob with enumerable fields",
      Object.assign(new Blob(["hello"]), { position: 0 }),
    ],
  ])("points %s input at the async APIs", (_name, image) => {
    const schema = new Schema([blob("image")]);
    expect(() => makeArrowTable([{ id: 1, image }], { schema })).toThrow(
      /blob field image at row 0: Blob and File values must be read asynchronously/,
    );
  });

  it.each([
    ["ArrayBuffer", new Uint8Array([104, 105]).buffer, "hi", null],
    [
      "data struct with ArrayBuffer",
      { data: Buffer.from("hi").buffer },
      "hi",
      null,
    ],
    ["URL", new URL("https://example.com/a"), null, "https://example.com/a"],
    [
      "uri struct with URL",
      { uri: new URL("file:///tmp/a.png") },
      null,
      "file:///tmp/a.png",
    ],
  ])("accepts %s input", (_name, image, data, uri) => {
    const schema = new Schema([blob("image")]);
    const input = { image };
    const table = makeArrowTable([input], { schema });
    const column = table.getChild("image")!;
    expect(column.nullCount).toBe(0);
    const bytes = column.getChild("data")!.get(0);
    expect(bytes == null ? null : Buffer.from(bytes).toString()).toBe(data);
    expect(column.getChild("uri")!.get(0)).toBe(uri);
    // The caller's record is left untouched.
    expect(input.image).toBe(image);
  });

  it("accepts ArrayBuffer and URL inside lists and structs", () => {
    const schema = new Schema([
      new Field("images", new List(blob("image")), true),
      new Field("info", new Struct([blob("image")]), true),
    ]);
    const table = makeArrowTable(
      [
        {
          images: [new Uint8Array([97]).buffer, new URL("s3://b/k")],
          info: { image: new Uint8Array([98]).buffer },
        },
      ],
      { schema },
    );
    const images = Array.from(
      table.getChild("images")!.get(0) as Iterable<{
        data: Uint8Array | null;
        uri: string | null;
      }>,
    );
    expect(Buffer.from(images[0].data!).toString()).toBe("a");
    expect(images[1].uri).toBe("s3://b/k");
    const info = table.getChild("info")!.get(0) as {
      image: { data: Uint8Array };
    };
    expect(Buffer.from(info.image.data).toString()).toBe("b");
  });

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
    expect(Buffer.from(image.getChild("data")!.get(0)!).toString()).toBe(
      "hello",
    );
  });

  it("coerces Buffer elements inside a list and keeps null slots", () => {
    const schema = new Schema([
      new Field("id", new Int64(), true),
      new Field("images", new List(blob("image")), true),
    ]);
    const table = makeArrowTable(
      [
        { id: 1n, images: [Buffer.from("a"), Buffer.from("bb")] },
        { id: 2n, images: null },
        { id: 3n, images: [Buffer.from("c"), null] },
        { id: 4n, images: [] },
      ],
      { schema },
    );
    const images = table.getChild("images")!;
    expect(images.nullCount).toBe(1);
    const rows = images.toArray();
    expect(rows[1]).toBeNull();
    expect(Array.from(rows[3] as Iterable<unknown>)).toHaveLength(0);
    const first = Array.from(rows[0] as Iterable<{ data: Uint8Array | null }>);
    expect(Buffer.from(first[0].data!).toString()).toBe("a");
    expect(Buffer.from(first[1].data!).toString()).toBe("bb");
    const third = Array.from(
      rows[2] as Iterable<{ data: Uint8Array | null } | null>,
    );
    expect(Buffer.from(third[0]!.data!).toString()).toBe("c");
    expect(third[1]).toBeNull();
  });

  it("coerces Buffer fields inside list structs", () => {
    const schema = new Schema([
      new Field("id", new Int64(), true),
      new Field(
        "items",
        new List(
          new Field(
            "item",
            new Struct([new Field("name", new Utf8(), true), blob("image")]),
            true,
          ),
        ),
        true,
      ),
    ]);
    const table = makeArrowTable(
      [
        {
          id: 1n,
          items: [{ name: "one", image: Buffer.from("alpha") }],
        },
      ],
      { schema },
    );
    const items = Array.from(
      table.getChild("items")!.toArray()[0] as Iterable<{
        name: string;
        image: { data: Uint8Array | null };
      }>,
    );
    expect(items[0].name).toBe("one");
    expect(Buffer.from(items[0].image.data!).toString()).toBe("alpha");
  });
});
