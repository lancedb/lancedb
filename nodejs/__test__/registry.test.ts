import { getRegistry, register } from "../lancedb/embedding/registry";
import { EmbeddingFunction, LanceSchema } from "../lancedb/embedding";
import { Float, Float32, Int32, Utf8, Vector } from "apache-arrow";
import * as tmp from "tmp";
import { connect } from "../lancedb";

describe("Registry", () => {
  let tmpDir: tmp.DirResult;
  beforeEach(() => {
    tmpDir = tmp.dirSync({ unsafeCleanup: true });
  });

  afterEach(() => {
    tmpDir.removeCallback();
    getRegistry().reset();
  });

  it.only("should register a new item to the registry", async () => {
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
      embeddingDataType(): Float {
        return new Float32();
      }
      async computeSourceEmbeddings(data: string[]) {
        return data.map(() => [1, 2, 3]);
      }
    }
    const func = getRegistry()
      .get<MockEmbeddingFunction>("mock-embedding")!
      .create();

    const schema = LanceSchema({
      id: new Int32(),
      text: func.sourceField(new Utf8()),
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
        if (x instanceof Vector) {
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
      embeddingDataType(): Float {
        return new Float32();
      }
      async computeSourceEmbeddings(data: string[]) {
        return data.map(() => [1, 2, 3]);
      }
    }
    register("mock-embedding")(MockEmbeddingFunction);
    expect(() => register("mock-embedding")(MockEmbeddingFunction)).toThrow();
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
      embeddingDataType(): Float {
        return new Float32();
      }
      async computeSourceEmbeddings(data: string[]) {
        return data.map(() => [1, 2, 3]);
      }
    }
    const func = new MockEmbeddingFunction();

    const schema = LanceSchema({
      id: new Int32(),
      text: func.sourceField(new Utf8()),
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
