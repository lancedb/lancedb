// Copyright 2024 Lance Developers.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import {
  convertToTable,
  fromTableToBuffer,
  makeArrowTable,
  makeEmptyTable,
} from "../dist/arrow";
import {
  Field,
  FixedSizeList,
  Float16,
  Float32,
  Int32,
  tableFromIPC,
  Schema,
  Float64,
  type Table,
  Binary,
  Bool,
  Utf8,
  Struct,
  List,
  DataType,
  Dictionary,
  Int64,
  Float,
  Precision,
} from "apache-arrow";
import { type EmbeddingFunction } from "../dist/embedding/embedding_function";

function sampleRecords(): Array<Record<string, any>> {
  return [
    {
      binary: Buffer.alloc(5),
      boolean: false,
      number: 7,
      string: "hello",
      struct: { x: 0, y: 0 },
      list: ["anime", "action", "comedy"],
    },
  ];
}

// Helper method to verify various ways to create a table
async function checkTableCreation(
  tableCreationMethod: (
    records: any,
    recordsReversed: any,
    schema: Schema,
  ) => Promise<Table>,
  infersTypes: boolean,
): Promise<void> {
  const records = sampleRecords();
  const recordsReversed = [
    {
      list: ["anime", "action", "comedy"],
      struct: { x: 0, y: 0 },
      string: "hello",
      number: 7,
      boolean: false,
      binary: Buffer.alloc(5),
    },
  ];
  const schema = new Schema([
    new Field("binary", new Binary(), false),
    new Field("boolean", new Bool(), false),
    new Field("number", new Float64(), false),
    new Field("string", new Utf8(), false),
    new Field(
      "struct",
      new Struct([
        new Field("x", new Float64(), false),
        new Field("y", new Float64(), false),
      ]),
    ),
    new Field("list", new List(new Field("item", new Utf8(), false)), false),
  ]);

  const table = await tableCreationMethod(records, recordsReversed, schema);
  schema.fields.forEach((field, idx) => {
    const actualField = table.schema.fields[idx];
    // Type inference always assumes nullable=true
    if (infersTypes) {
      expect(actualField.nullable).toBe(true);
    } else {
      expect(actualField.nullable).toBe(false);
    }
    expect(table.getChild(field.name)?.type.toString()).toEqual(
      field.type.toString(),
    );
    expect(table.getChildAt(idx)?.type.toString()).toEqual(
      field.type.toString(),
    );
  });
}

describe("The function makeArrowTable", function () {
  it("will use data types from a provided schema instead of inference", async function () {
    const schema = new Schema([
      new Field("a", new Int32()),
      new Field("b", new Float32()),
      new Field("c", new FixedSizeList(3, new Field("item", new Float16()))),
      new Field("d", new Int64()),
    ]);
    const table = makeArrowTable(
      [
        { a: 1, b: 2, c: [1, 2, 3], d: 9 },
        { a: 4, b: 5, c: [4, 5, 6], d: 10 },
        { a: 7, b: 8, c: [7, 8, 9], d: null },
      ],
      { schema },
    );

    const buf = await fromTableToBuffer(table);
    expect(buf.byteLength).toBeGreaterThan(0);

    const actual = tableFromIPC(buf);
    expect(actual.numRows).toBe(3);
    const actualSchema = actual.schema;
    expect(actualSchema).toEqual(schema);
  });

  it("will assume the column `vector` is FixedSizeList<Float32> by default", async function () {
    const schema = new Schema([
      new Field("a", new Float(Precision.DOUBLE), true),
      new Field("b", new Float(Precision.DOUBLE), true),
      new Field(
        "vector",
        new FixedSizeList(
          3,
          new Field("item", new Float(Precision.SINGLE), true),
        ),
        true,
      ),
    ]);
    const table = makeArrowTable([
      { a: 1, b: 2, vector: [1, 2, 3] },
      { a: 4, b: 5, vector: [4, 5, 6] },
      { a: 7, b: 8, vector: [7, 8, 9] },
    ]);

    const buf = await fromTableToBuffer(table);
    expect(buf.byteLength).toBeGreaterThan(0);

    const actual = tableFromIPC(buf);
    expect(actual.numRows).toBe(3);
    const actualSchema = actual.schema;
    expect(actualSchema).toEqual(schema);
  });

  it("can support multiple vector columns", async function () {
    const schema = new Schema([
      new Field("a", new Float(Precision.DOUBLE), true),
      new Field("b", new Float(Precision.DOUBLE), true),
      new Field(
        "vec1",
        new FixedSizeList(3, new Field("item", new Float16(), true)),
        true,
      ),
      new Field(
        "vec2",
        new FixedSizeList(3, new Field("item", new Float16(), true)),
        true,
      ),
    ]);
    const table = makeArrowTable(
      [
        { a: 1, b: 2, vec1: [1, 2, 3], vec2: [2, 4, 6] },
        { a: 4, b: 5, vec1: [4, 5, 6], vec2: [8, 10, 12] },
        { a: 7, b: 8, vec1: [7, 8, 9], vec2: [14, 16, 18] },
      ],
      {
        vectorColumns: {
          vec1: { type: new Float16() },
          vec2: { type: new Float16() },
        },
      },
    );

    const buf = await fromTableToBuffer(table);
    expect(buf.byteLength).toBeGreaterThan(0);

    const actual = tableFromIPC(buf);
    expect(actual.numRows).toBe(3);
    const actualSchema = actual.schema;
    expect(actualSchema).toEqual(schema);
  });

  it("will allow different vector column types", async function () {
    const table = makeArrowTable([{ fp16: [1], fp32: [1], fp64: [1] }], {
      vectorColumns: {
        fp16: { type: new Float16() },
        fp32: { type: new Float32() },
        fp64: { type: new Float64() },
      },
    });

    expect(table.getChild("fp16")?.type.children[0].type.toString()).toEqual(
      new Float16().toString(),
    );
    expect(table.getChild("fp32")?.type.children[0].type.toString()).toEqual(
      new Float32().toString(),
    );
    expect(table.getChild("fp64")?.type.children[0].type.toString()).toEqual(
      new Float64().toString(),
    );
  });

  it("will use dictionary encoded strings if asked", async function () {
    const table = makeArrowTable([{ str: "hello" }]);
    expect(DataType.isUtf8(table.getChild("str")?.type)).toBe(true);

    const tableWithDict = makeArrowTable([{ str: "hello" }], {
      dictionaryEncodeStrings: true,
    });
    expect(DataType.isDictionary(tableWithDict.getChild("str")?.type)).toBe(
      true,
    );

    const schema = new Schema([
      new Field("str", new Dictionary(new Utf8(), new Int32())),
    ]);

    const tableWithDict2 = makeArrowTable([{ str: "hello" }], { schema });
    expect(DataType.isDictionary(tableWithDict2.getChild("str")?.type)).toBe(
      true,
    );
  });

  it("will infer data types correctly", async function () {
    await checkTableCreation(async (records) => makeArrowTable(records), true);
  });

  it("will allow a schema to be provided", async function () {
    await checkTableCreation(
      async (records, _, schema) => makeArrowTable(records, { schema }),
      false,
    );
  });

  it("will use the field order of any provided schema", async function () {
    await checkTableCreation(
      async (_, recordsReversed, schema) =>
        makeArrowTable(recordsReversed, { schema }),
      false,
    );
  });

  it("will make an empty table", async function () {
    await checkTableCreation(
      async (_, __, schema) => makeArrowTable([], { schema }),
      false,
    );
  });
});

class DummyEmbedding implements EmbeddingFunction<string> {
  public readonly sourceColumn = "string";
  public readonly embeddingDimension = 2;
  public readonly embeddingDataType = new Float16();

  async embed(data: string[]): Promise<number[][]> {
    return data.map(() => [0.0, 0.0]);
  }
}

class DummyEmbeddingWithNoDimension implements EmbeddingFunction<string> {
  public readonly sourceColumn = "string";

  async embed(data: string[]): Promise<number[][]> {
    return data.map(() => [0.0, 0.0]);
  }
}

describe("convertToTable", function () {
  it("will infer data types correctly", async function () {
    await checkTableCreation(
      async (records) => await convertToTable(records),
      true,
    );
  });

  it("will allow a schema to be provided", async function () {
    await checkTableCreation(
      async (records, _, schema) =>
        await convertToTable(records, undefined, { schema }),
      false,
    );
  });

  it("will use the field order of any provided schema", async function () {
    await checkTableCreation(
      async (_, recordsReversed, schema) =>
        await convertToTable(recordsReversed, undefined, { schema }),
      false,
    );
  });

  it("will make an empty table", async function () {
    await checkTableCreation(
      async (_, __, schema) => await convertToTable([], undefined, { schema }),
      false,
    );
  });

  it("will apply embeddings", async function () {
    const records = sampleRecords();
    const table = await convertToTable(records, new DummyEmbedding());
    expect(DataType.isFixedSizeList(table.getChild("vector")?.type)).toBe(true);
    expect(table.getChild("vector")?.type.children[0].type.toString()).toEqual(
      new Float16().toString(),
    );
  });

  it("will fail if missing the embedding source column", async function () {
    await expect(
      convertToTable([{ id: 1 }], new DummyEmbedding()),
    ).rejects.toThrow("'string' was not present");
  });

  it("use embeddingDimension if embedding missing from table", async function () {
    const schema = new Schema([new Field("string", new Utf8(), false)]);
    // Simulate getting an empty Arrow table (minus embedding) from some other source
    // In other words, we aren't starting with records
    const table = makeEmptyTable(schema);

    // If the embedding specifies the dimension we are fine
    await fromTableToBuffer(table, new DummyEmbedding());

    // We can also supply a schema and should be ok
    const schemaWithEmbedding = new Schema([
      new Field("string", new Utf8(), false),
      new Field(
        "vector",
        new FixedSizeList(2, new Field("item", new Float16(), false)),
        false,
      ),
    ]);
    await fromTableToBuffer(
      table,
      new DummyEmbeddingWithNoDimension(),
      schemaWithEmbedding,
    );

    // Otherwise we will get an error
    await expect(
      fromTableToBuffer(table, new DummyEmbeddingWithNoDimension()),
    ).rejects.toThrow("does not specify `embeddingDimension`");
  });

  it("will apply embeddings to an empty table", async function () {
    const schema = new Schema([
      new Field("string", new Utf8(), false),
      new Field(
        "vector",
        new FixedSizeList(2, new Field("item", new Float16(), false)),
        false,
      ),
    ]);
    const table = await convertToTable([], new DummyEmbedding(), { schema });
    expect(DataType.isFixedSizeList(table.getChild("vector")?.type)).toBe(true);
    expect(table.getChild("vector")?.type.children[0].type.toString()).toEqual(
      new Float16().toString(),
    );
  });

  it("will complain if embeddings present but schema missing embedding column", async function () {
    const schema = new Schema([new Field("string", new Utf8(), false)]);
    await expect(
      convertToTable([], new DummyEmbedding(), { schema }),
    ).rejects.toThrow("column vector was missing");
  });

  it("will provide a nice error if run twice", async function () {
    const records = sampleRecords();
    const table = await convertToTable(records, new DummyEmbedding());
    // fromTableToBuffer will try and apply the embeddings again
    await expect(
      fromTableToBuffer(table, new DummyEmbedding()),
    ).rejects.toThrow("already existed");
  });
});

describe("makeEmptyTable", function () {
  it("will make an empty table", async function () {
    await checkTableCreation(
      async (_, __, schema) => makeEmptyTable(schema),
      false,
    );
  });
});
