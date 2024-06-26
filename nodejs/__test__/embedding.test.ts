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
      toJSON(): object {
        return {};
      }
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

  it("should be able to create an empty table with an embedding function", async () => {
    @register()
    class MockEmbeddingFunction extends EmbeddingFunction<string> {
      toJSON(): object {
        return {};
      }
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
      toJSON(): object {
        return {};
      }
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
  test.each([new Float16(), new Float32(), new Float64()])(
    "should be able to provide manual embeddings with multiple float datatype",
    async (floatType) => {
      class MockEmbeddingFunction extends EmbeddingFunction<string> {
        toJSON(): object {
          return {};
        }
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
        toJSON(): object {
          return {};
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
      @register("test")
      class MockEmbeddingFunction extends EmbeddingFunction<string> {
        toJSON(): object {
          return {};
        }
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
