// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import * as tmp from "tmp";

import { connect } from "../lancedb";
import {
  Field,
  FixedSizeList,
  Float,
  Float16,
  Float32,
  Float64,
  Schema,
  Utf8,
} from "../lancedb/arrow";
import { EmbeddingFunction, LanceSchema } from "../lancedb/embedding";
import { getRegistry, register } from "../lancedb/embedding/registry";

const testOpenAIInteg = process.env.OPENAI_API_KEY == null ? test.skip : test;

describe("embedding functions", () => {
  let tmpDir: tmp.DirResult;
  beforeEach(() => {
    tmpDir = tmp.dirSync({ unsafeCleanup: true });
  });
  afterEach(() => {
    tmpDir.removeCallback();
    getRegistry().reset();
  });

  it("should be able to create a table with an embedding function", async () => {
    class MockEmbeddingFunction extends EmbeddingFunction<string> {
      ndims() {
        return 3;
      }
      embeddingDataType(): Float {
        return new Float32();
      }
      async computeQueryEmbeddings(_data: string) {
        return [1, 2, 3];
      }
      async computeSourceEmbeddings(data: string[]) {
        return Array.from({ length: data.length }).fill([
          1, 2, 3,
        ]) as number[][];
      }
    }
    const func = new MockEmbeddingFunction();
    const db = await connect(tmpDir.name);
    const table = await db.createTable(
      "test",
      [
        { id: 1, text: "hello" },
        { id: 2, text: "world" },
      ],
      {
        embeddingFunction: {
          function: func,
          sourceColumn: "text",
        },
      },
    );
    // biome-ignore lint/suspicious/noExplicitAny: test
    const arr = (await table.query().toArray()) as any;
    expect(arr[0].vector).toBeDefined();

    // we round trip through JSON to make sure the vector properly gets converted to an array
    // otherwise it'll be a TypedArray or Vector
    const vector0 = JSON.parse(JSON.stringify(arr[0].vector));
    expect(vector0).toEqual([1, 2, 3]);
  });

  it("should be able to append and upsert using embedding function", async () => {
    @register()
    class MockEmbeddingFunction extends EmbeddingFunction<string> {
      ndims() {
        return 3;
      }
      embeddingDataType(): Float {
        return new Float32();
      }
      async computeQueryEmbeddings(_data: string) {
        return [1, 2, 3];
      }
      async computeSourceEmbeddings(data: string[]) {
        return Array.from({ length: data.length }).fill([
          1, 2, 3,
        ]) as number[][];
      }
    }
    const func = new MockEmbeddingFunction();
    const db = await connect(tmpDir.name);
    const table = await db.createTable(
      "test",
      [
        { id: 1, text: "hello" },
        { id: 2, text: "world" },
      ],
      {
        embeddingFunction: {
          function: func,
          sourceColumn: "text",
        },
      },
    );

    const schema = await table.schema();
    expect(schema.metadata.get("embedding_functions")).toBeDefined();

    // Append some new data
    const data1 = [
      { id: 3, text: "forest" },
      { id: 4, text: "mountain" },
    ];
    await table.add(data1);

    // Upsert some data
    const data2 = [
      { id: 5, text: "river" },
      { id: 2, text: "canyon" },
    ];
    await table
      .mergeInsert("id")
      .whenMatchedUpdateAll()
      .whenNotMatchedInsertAll()
      .execute(data2);

    const rows = await table.query().toArray();
    rows.sort((a, b) => a.id - b.id);
    const texts = rows.map((row) => row.text);
    expect(texts).toEqual(["hello", "canyon", "forest", "mountain", "river"]);
    const vectorsDefined = rows.map(
      (row) => row.vector !== undefined && row.vector !== null,
    );
    expect(vectorsDefined).toEqual(new Array(5).fill(true));
  });

  it("should be able to create an empty table with an embedding function", async () => {
    @register()
    class MockEmbeddingFunction extends EmbeddingFunction<string> {
      ndims() {
        return 3;
      }
      embeddingDataType(): Float {
        return new Float32();
      }
      async computeQueryEmbeddings(_data: string) {
        return [1, 2, 3];
      }
      async computeSourceEmbeddings(data: string[]) {
        return Array.from({ length: data.length }).fill([
          1, 2, 3,
        ]) as number[][];
      }
    }
    const schema = new Schema([
      new Field("text", new Utf8(), true),
      new Field(
        "vector",
        new FixedSizeList(3, new Field("item", new Float32(), true)),
        true,
      ),
    ]);

    const func = new MockEmbeddingFunction();
    const db = await connect(tmpDir.name);
    const table = await db.createEmptyTable("test", schema, {
      embeddingFunction: {
        function: func,
        sourceColumn: "text",
      },
    });
    const outSchema = await table.schema();
    expect(outSchema.metadata.get("embedding_functions")).toBeDefined();
    await table.add([{ text: "hello world" }]);

    // biome-ignore lint/suspicious/noExplicitAny: test
    const arr = (await table.query().toArray()) as any;
    expect(arr[0].vector).toBeDefined();

    // we round trip through JSON to make sure the vector properly gets converted to an array
    // otherwise it'll be a TypedArray or Vector
    const vector0 = JSON.parse(JSON.stringify(arr[0].vector));
    expect(vector0).toEqual([1, 2, 3]);
  });
  it("should error when appending to a table with an unregistered embedding function", async () => {
    @register("mock")
    class MockEmbeddingFunction extends EmbeddingFunction<string> {
      ndims() {
        return 3;
      }
      embeddingDataType(): Float {
        return new Float32();
      }
      async computeQueryEmbeddings(_data: string) {
        return [1, 2, 3];
      }
      async computeSourceEmbeddings(data: string[]) {
        return Array.from({ length: data.length }).fill([
          1, 2, 3,
        ]) as number[][];
      }
    }
    const func = getRegistry().get<MockEmbeddingFunction>("mock")!.create();

    const schema = LanceSchema({
      id: new Float64(),
      text: func.sourceField(new Utf8()),
      vector: func.vectorField(),
    });

    const db = await connect(tmpDir.name);
    await db.createTable(
      "test",
      [
        { id: 1, text: "hello" },
        { id: 2, text: "world" },
      ],
      {
        schema,
      },
    );

    getRegistry().reset();
    const db2 = await connect(tmpDir.name);

    const tbl = await db2.openTable("test");

    expect(tbl.add([{ id: 3, text: "hello" }])).rejects.toThrow(
      `Function "mock" not found in registry`,
    );
  });

  testOpenAIInteg("propagates variables through all methods", async () => {
    delete process.env.OPENAI_API_KEY;
    const registry = getRegistry();
    registry.setVar("openai_api_key", "sk-...");
    const func = registry.get("openai")?.create({
      model: "text-embedding-ada-002",
      apiKey: "$var:openai_api_key",
    }) as EmbeddingFunction;

    const db = await connect("memory://");
    const wordsSchema = LanceSchema({
      text: func.sourceField(new Utf8()),
      vector: func.vectorField(),
    });
    const tbl = await db.createEmptyTable("words", wordsSchema, {
      mode: "overwrite",
    });
    await tbl.add([{ text: "hello world" }, { text: "goodbye world" }]);

    const query = "greetings";
    const actual = (await tbl.search(query).limit(1).toArray())[0];
    expect(actual).toHaveProperty("text");
  });

  it("should handle undefined vector field with embedding function correctly", async () => {
    @register("undefined_test")
    class MockEmbeddingFunction extends EmbeddingFunction<string> {
      ndims() {
        return 3;
      }
      embeddingDataType(): Float {
        return new Float32();
      }
      async computeQueryEmbeddings(_data: string) {
        return [1, 2, 3];
      }
      async computeSourceEmbeddings(data: string[]) {
        return Array.from({ length: data.length }).fill([
          1, 2, 3,
        ]) as number[][];
      }
    }
    const func = getRegistry()
      .get<MockEmbeddingFunction>("undefined_test")!
      .create();
    const schema = new Schema([
      new Field("text", new Utf8(), true),
      new Field(
        "vector",
        new FixedSizeList(3, new Field("item", new Float32(), true)),
        true,
      ),
    ]);

    const db = await connect(tmpDir.name);
    const table = await db.createEmptyTable("test_undefined", schema, {
      embeddingFunction: {
        function: func,
        sourceColumn: "text",
        vectorColumn: "vector",
      },
    });

    // Test that undefined, null, and omitted vector fields all work
    await table.add([{ text: "test1", vector: undefined }]);
    await table.add([{ text: "test2", vector: null }]);
    await table.add([{ text: "test3" }]);

    const rows = await table.query().toArray();
    expect(rows.length).toBe(3);

    // All rows should have vectors computed by the embedding function
    for (const row of rows) {
      expect(row.vector).toBeDefined();
      expect(JSON.parse(JSON.stringify(row.vector))).toEqual([1, 2, 3]);
    }
  });

  test.each([new Float16(), new Float32(), new Float64()])(
    "should be able to provide manual embeddings with multiple float datatype",
    async (floatType) => {
      class MockEmbeddingFunction extends EmbeddingFunction<string> {
        ndims() {
          return 3;
        }
        embeddingDataType(): Float {
          return floatType;
        }
        async computeQueryEmbeddings(_data: string) {
          return [1, 2, 3];
        }
        async computeSourceEmbeddings(data: string[]) {
          return Array.from({ length: data.length }).fill([
            1, 2, 3,
          ]) as number[][];
        }
      }
      const data = [{ text: "hello" }, { text: "hello world" }];

      const schema = new Schema([
        new Field("vector", new FixedSizeList(3, new Field("item", floatType))),
        new Field("text", new Utf8()),
      ]);
      const func = new MockEmbeddingFunction();

      const name = "test";
      const db = await connect(tmpDir.name);

      const table = await db.createTable(name, data, {
        schema,
        embeddingFunction: {
          sourceColumn: "text",
          function: func,
        },
      });
      const res = await table.query().toArray();

      expect([...res[0].vector]).toEqual([1, 2, 3]);
    },
  );

  test.each([new Float16(), new Float32(), new Float64()])(
    "should be able to provide auto embeddings with multiple float datatypes",
    async (floatType) => {
      @register("test1")
      class MockEmbeddingFunctionWithoutNDims extends EmbeddingFunction<string> {
        embeddingDataType(): Float {
          return floatType;
        }
        async computeQueryEmbeddings(_data: string) {
          return [1, 2, 3];
        }
        async computeSourceEmbeddings(data: string[]) {
          return Array.from({ length: data.length }).fill([
            1, 2, 3,
          ]) as number[][];
        }
      }
      @register("test")
      class MockEmbeddingFunction extends EmbeddingFunction<string> {
        ndims() {
          return 3;
        }
        embeddingDataType(): Float {
          return floatType;
        }
        async computeQueryEmbeddings(_data: string) {
          return [1, 2, 3];
        }
        async computeSourceEmbeddings(data: string[]) {
          return Array.from({ length: data.length }).fill([
            1, 2, 3,
          ]) as number[][];
        }
      }
      const func = getRegistry().get<MockEmbeddingFunction>("test")!.create();
      const func2 = getRegistry()
        .get<MockEmbeddingFunctionWithoutNDims>("test1")!
        .create();

      const schema = LanceSchema({
        text: func.sourceField(new Utf8()),
        vector: func.vectorField(floatType),
      });

      const schema2 = LanceSchema({
        text: func2.sourceField(new Utf8()),
        vector: func2.vectorField({ datatype: floatType, dims: 3 }),
      });
      const schema3 = LanceSchema({
        text: func2.sourceField(new Utf8()),
        vector: func.vectorField({
          datatype: new FixedSizeList(3, new Field("item", floatType, true)),
          dims: 3,
        }),
      });

      const expectedSchema = new Schema([
        new Field("text", new Utf8(), true),
        new Field(
          "vector",
          new FixedSizeList(3, new Field("item", floatType, true)),
          true,
        ),
      ]);
      const stringSchema = JSON.stringify(schema, null, 2);
      const stringSchema2 = JSON.stringify(schema2, null, 2);
      const stringSchema3 = JSON.stringify(schema3, null, 2);
      const stringExpectedSchema = JSON.stringify(expectedSchema, null, 2);

      expect(stringSchema).toEqual(stringExpectedSchema);
      expect(stringSchema2).toEqual(stringExpectedSchema);
      expect(stringSchema3).toEqual(stringExpectedSchema);
    },
  );
});
