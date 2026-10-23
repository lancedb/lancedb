// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import * as tmp from "tmp";
import { type Table, blob, connect, isBlobField } from "../lancedb";
import {
  DataType,
  Field,
  Int64,
  List,
  Schema,
  Struct,
  Utf8,
} from "../lancedb/arrow";

describe('nested blobs with blobMode "bytes"', () => {
  let tmpDir: tmp.DirResult;
  let table: Table;

  const caption = new Field(
    "caption",
    new Utf8(),
    false,
    new Map([["label", "caption"]]),
  );
  const info = new Field(
    "info",
    new Struct([
      blob("image"),
      caption,
      new Field("preview", new Struct([blob("image")]), true),
    ]),
    true,
    new Map([["label", "info"]]),
  );
  const schema = new Schema(
    [new Field("id", new Int64(), false), info, blob("cover")],
    new Map([["source", "nested-blobs"]]),
  );

  beforeEach(async () => {
    tmpDir = tmp.dirSync({ unsafeCleanup: true });
    const db = await connect(tmpDir.name);
    table = await db.createTable(
      "nested_blobs",
      [
        {
          id: 1n,
          info: {
            image: Buffer.from("hello"),
            caption: "first",
            preview: { image: Buffer.from("preview") },
          },
          cover: Buffer.from("cover"),
        },
        { id: 2n, info: null, cover: null },
        {
          id: 3n,
          info: { image: null, caption: "third", preview: null },
          cover: null,
        },
        {
          id: 4n,
          info: {
            image: Buffer.alloc(0),
            caption: "fourth",
            preview: { image: null },
          },
          cover: Buffer.alloc(0),
        },
      ],
      { schema },
    );
  });

  afterEach(() => tmpDir.removeCallback());

  it.each([false, true])(
    "reads struct leaves across batches (select parent: %s)",
    async (selectParent) => {
      const query = table
        .query()
        .orderBy({ columnName: "id", ascending: true });
      if (selectParent) {
        query.select(["info"]);
      }
      const result = await query.toArrow({
        blobMode: "bytes",
        maxBatchLength: 1,
      });
      expect(result.batches.length).toBeGreaterThan(1);
      expect(result.schema.fields.map((field) => field.name)).toEqual(
        selectParent ? ["info"] : ["id", "info", "cover"],
      );
      expect(result.schema.metadata).toEqual(schema.metadata);
      const infoField = result.schema.fields.find(
        (field) => field.name === "info",
      )!;
      expect(infoField.nullable).toBe(true);
      expect(infoField.metadata).toEqual(info.metadata);
      const fields = (infoField.type as Struct).children;
      expect(DataType.isLargeBinary(fields[0].type)).toBe(true);
      expect(isBlobField(fields[0])).toBe(false);
      expect(fields[0].metadata.size).toBe(0);
      expect(fields[1]).toEqual(caption);
      expect(
        DataType.isLargeBinary((fields[2].type as Struct).children[0].type),
      ).toBe(true);

      const rows = result.toArray();
      expect(Buffer.from(rows[0].info.image).toString()).toBe("hello");
      expect(rows[0].info.caption).toBe("first");
      expect(Buffer.from(rows[0].info.preview.image).toString()).toBe(
        "preview",
      );
      expect(rows[1].info).toBeNull();
      expect(rows[2].info.image).toBeNull();
      expect(rows[2].info.caption).toBe("third");
      expect(rows[2].info.preview).toBeNull();
      expect(Buffer.from(rows[3].info.image)).toEqual(Buffer.alloc(0));
      expect(rows[3].info.preview.image).toBeNull();
      if (!selectParent) {
        expect(Buffer.from(rows[0].cover).toString()).toBe("cover");
        expect(rows[1].cover).toBeNull();
      }
    },
  );

  it("reads direct leaf and struct aliases", async () => {
    const rows = await table
      .query()
      .orderBy({ columnName: "id", ascending: true })
      .select({
        image: "info.image",
        details: "info",
        preview: "info.preview",
        caption: "info.caption",
      })
      .toArray({ blobMode: "bytes", maxBatchLength: 1 });
    expect(Object.keys(rows[0])).toEqual([
      "image",
      "details",
      "preview",
      "caption",
    ]);
    expect(Buffer.from(rows[0].image).toString()).toBe("hello");
    expect(Buffer.from(rows[0].details.image).toString()).toBe("hello");
    expect(Buffer.from(rows[0].details.preview.image).toString()).toBe(
      "preview",
    );
    expect(Buffer.from(rows[0].preview.image).toString()).toBe("preview");
    expect(rows[0].caption).toBe("first");
    expect(rows[1].details).toBeNull();
    expect(rows[1].image).toBeNull();
    expect(rows[2].image).toBeNull();
    expect(Buffer.from(rows[3].image)).toEqual(Buffer.alloc(0));
  });

  it.each([false, true])(
    "preserves a literal scalar alias matching a nested blob path (reverse: %s)",
    async (reverse) => {
      const projection: [string, string][] = [
        ["info", "info"],
        ["info.image", "id"],
      ];
      if (reverse) {
        projection.reverse();
      }
      const [row] = await table
        .query()
        .where("id = 1")
        .select(new Map(projection))
        .toArray({ blobMode: "bytes" });
      expect(row["info.image"]).toBe(1n);
      expect(Buffer.from(row.info.image).toString()).toBe("hello");
    },
  );

  it.each([false, true])(
    "materializes every overlapping blob projection (reverse: %s)",
    async (reverse) => {
      const columns = ["info.image", "info"];
      if (reverse) {
        columns.reverse();
      }
      const result = await table
        .query()
        .orderBy({ columnName: "id", ascending: true })
        .select(columns)
        .toArrow({ blobMode: "bytes", maxBatchLength: 1 });
      expect(result.schema.fields.map((field) => field.name)).toEqual(columns);
      expect(DataType.isLargeBinary(result.getChild("info.image")!.type)).toBe(
        true,
      );
      expect(
        DataType.isLargeBinary(
          result.getChild("info")!.getChild("image")!.type,
        ),
      ).toBe(true);
      const rows = result.toArray();
      expect(Buffer.from(rows[0]["info.image"]).toString()).toBe("hello");
      expect(Buffer.from(rows[0].info.image).toString()).toBe("hello");
      expect(rows[0].info.caption).toBe("first");
      expect(rows[1]["info.image"]).toBeNull();
      expect(rows[1].info).toBeNull();
      expect(rows[2]["info.image"]).toBeNull();
      expect(rows[2].info.image).toBeNull();
      expect(Buffer.from(rows[3]["info.image"])).toEqual(Buffer.alloc(0));
      expect(Buffer.from(rows[3].info.image)).toEqual(Buffer.alloc(0));
    },
  );

  it("materializes dotted aliases independently of nested fields", async () => {
    const [row] = await table
      .query()
      .where("id = 1")
      .select({
        details: "info",
        "details.image": "info.image",
        "details.preview": "info.preview",
        "details.preview.image": "info.preview.image",
      })
      .toArray({ blobMode: "bytes" });
    expect(Buffer.from(row.details.image).toString()).toBe("hello");
    expect(Buffer.from(row["details.image"]).toString()).toBe("hello");
    expect(Buffer.from(row.details.preview.image).toString()).toBe("preview");
    expect(Buffer.from(row["details.preview"].image).toString()).toBe(
      "preview",
    );
    expect(Buffer.from(row["details.preview.image"]).toString()).toBe(
      "preview",
    );
  });

  it.each([false, true])(
    "converts the empty result schema (alias: %s)",
    async (alias) => {
      const query = table.query().where("id < 0");
      if (alias) {
        query.select({ image: "info.image", details: "info" });
      }
      const result = await query.toArrow({ blobMode: "bytes" });
      expect(result.numRows).toBe(0);
      expect(result.schema.fields.map((field) => field.name)).toEqual(
        alias ? ["image", "details"] : ["id", "info", "cover"],
      );
      const fields = (
        result.schema.fields.find(
          (field) => field.name === (alias ? "details" : "info"),
        )!.type as Struct
      ).children;
      expect(DataType.isLargeBinary(fields[0].type)).toBe(true);
      expect(isBlobField(fields[0])).toBe(false);
      expect(fields[1]).toEqual(caption);
      expect(
        DataType.isLargeBinary((fields[2].type as Struct).children[0].type),
      ).toBe(true);
      if (alias) {
        expect(DataType.isLargeBinary(result.schema.fields[0].type)).toBe(true);
      }
    },
  );

  it("preserves requested row IDs and leaves subsequent descriptors intact", async () => {
    const query = table.query().where("id = 1").select(["info"]);
    const [bytes] = await query.toArray({ blobMode: "bytes" });
    expect(Object.keys(bytes)).toEqual(["info"]);
    const [description] = await query.toArray();
    expect(Object.keys(description)).toEqual(["info"]);
    expect(Number(description.info.image.size)).toBe(5);
    const [withRowId] = await query.withRowId().toArray({ blobMode: "bytes" });
    expect(typeof withRowId._rowid).toBe("bigint");
    expect(await table.fetchBlobs("info.image", [withRowId._rowid])).toEqual([
      Buffer.from(withRowId.info.image),
    ]);
  });

  it("keeps descriptors in description mode", async () => {
    const [description] = await table
      .query()
      .where("id = 1")
      .toArray({ blobMode: "descriptions" });
    expect(Number(description.info.image.size)).toBe(5);
  });

  it("keeps list elements as descriptors alongside struct blob bytes", async () => {
    const db = await connect(tmpDir.name);
    const table = await db.createTable(
      "list_blobs",
      [
        {
          info: { image: Buffer.from("hello"), images: [Buffer.from("list")] },
        },
      ],
      {
        schema: new Schema([
          new Field(
            "info",
            new Struct([
              blob("image"),
              new Field("images", new List(blob("item")), true),
            ]),
            true,
          ),
        ]),
      },
    );
    const [bytes] = await table.query().toArray({ blobMode: "bytes" });
    expect(Buffer.from(bytes.info.image).toString()).toBe("hello");
    expect(Number(bytes.info.images.get(0).size)).toBe(4);
    const [row] = await table
      .query()
      .select(["info.images"])
      .toArray({ blobMode: "bytes" });
    expect(Number(row["info.images"].get(0).size)).toBe(4);
    expect(Object.keys(row)).toEqual(["info.images"]);
    const result = await table.query().toArrow({ blobMode: "bytes" });
    const fields = (result.schema.fields[0].type as Struct).children;
    const descriptions = await table.query().toArrow();
    expect(fields[1]).toEqual(
      (descriptions.schema.fields[0].type as Struct).children[1],
    );
    expect(DataType.isStruct((fields[1].type as List).children[0].type)).toBe(
      true,
    );
  });
});
