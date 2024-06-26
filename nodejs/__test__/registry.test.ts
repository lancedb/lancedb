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
import * as arrow from "apache-arrow";
import * as arrowOld from "apache-arrow-old";

import * as tmp from "tmp";

import { connect } from "../lancedb";
import { EmbeddingFunction, LanceSchema } from "../lancedb/embedding";
import { getRegistry, register } from "../lancedb/embedding/registry";

describe.each([arrow, arrowOld])("LanceSchema", (arrow) => {
  test("should preserve input order", async () => {
    const schema = LanceSchema({
      id: new arrow.Int32(),
      text: new arrow.Utf8(),
      vector: new arrow.Float32(),
    });
    expect(schema.fields.map((x) => x.name)).toEqual(["id", "text", "vector"]);
  });
});

describe("Registry", () => {
  let tmpDir: tmp.DirResult;
  beforeEach(() => {
    tmpDir = tmp.dirSync({ unsafeCleanup: true });
  });

  afterEach(() => {
    tmpDir.removeCallback();
    getRegistry().reset();
  });

  it("should register a new item to the registry", async () => {
    @register("mock-embedding")
    class MockEmbeddingFunction extends EmbeddingFunction<string> {
      toJSON(): object {
        return {
          someText: "hello",
        };
      }
      constructor() {
        super();
      }
      ndims() {
        return 3;
      }
      embeddingDataType(): arrow.Float {
        return new arrow.Float32();
      }
      async computeSourceEmbeddings(data: string[]) {
        return data.map(() => [1, 2, 3]);
      }
    }

    const func = getRegistry()
      .get<MockEmbeddingFunction>("mock-embedding")!
      .create();

    const schema = LanceSchema({
      id: new arrow.Int32(),
      text: func.sourceField(new arrow.Utf8()),
      vector: func.vectorField(),
    });

    const db = await connect(tmpDir.name);
    const table = await db.createTable(
      "test",
      [
        { id: 1, text: "hello" },
        { id: 2, text: "world" },
      ],
      { schema },
    );
    const expected = [
      [1, 2, 3],
      [1, 2, 3],
    ];
    const actual = await table.query().toArrow();
    const vectors = actual
      .getChild("vector")
      ?.toArray()
      .map((x: unknown) => {
        if (x instanceof arrow.Vector) {
          return [...x];
        } else {
          return x;
        }
      });
    expect(vectors).toEqual(expected);
  });
  test("should error if registering with the same name", async () => {
    class MockEmbeddingFunction extends EmbeddingFunction<string> {
      toJSON(): object {
        return {
          someText: "hello",
        };
      }
      constructor() {
        super();
      }
      ndims() {
        return 3;
      }
      embeddingDataType(): arrow.Float {
        return new arrow.Float32();
      }
      async computeSourceEmbeddings(data: string[]) {
        return data.map(() => [1, 2, 3]);
      }
    }
    register("mock-embedding")(MockEmbeddingFunction);
    expect(() => register("mock-embedding")(MockEmbeddingFunction)).toThrow(
      'Embedding function with alias "mock-embedding" already exists',
    );
  });
  test("schema should contain correct metadata", async () => {
    class MockEmbeddingFunction extends EmbeddingFunction<string> {
      toJSON(): object {
        return {
          someText: "hello",
        };
      }
      constructor() {
        super();
      }
      ndims() {
        return 3;
      }
      embeddingDataType(): arrow.Float {
        return new arrow.Float32();
      }
      async computeSourceEmbeddings(data: string[]) {
        return data.map(() => [1, 2, 3]);
      }
    }
    const func = new MockEmbeddingFunction();

    const schema = LanceSchema({
      id: new arrow.Int32(),
      text: func.sourceField(new arrow.Utf8()),
      vector: func.vectorField(),
    });
    const expectedMetadata = new Map<string, string>([
      [
        "embedding_functions",
        JSON.stringify([
          {
            sourceColumn: "text",
            vectorColumn: "vector",
            name: "MockEmbeddingFunction",
            model: { someText: "hello" },
          },
        ]),
      ],
    ]);
    expect(schema.metadata).toEqual(expectedMetadata);
  });
});
