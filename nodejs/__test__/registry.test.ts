import { getRegistry, register } from "../lancedb/embedding/registry";
import { EmbeddingFunction, LanceSchema } from "../lancedb/embedding";
import { Int32, Utf8 } from "apache-arrow";
import * as tmp from "tmp";
import { connect } from "../lancedb";

describe("Registry", () => {
  let tmpDir: tmp.DirResult;
  beforeEach(() => {
    tmpDir = tmp.dirSync({ unsafeCleanup: true });
  });
  afterEach(() => tmpDir.removeCallback());

  describe("register decorator", () => {
    it("should register a new item to the registry", async () => {
      @register("mock-embedding")
      class MockEmbeddingFunction extends EmbeddingFunction<string> {
        toJSON(): object {
          return {};
        }
        constructor() {
          super();
        }
        ndims() {
          return 3;
        }
        async computeQueryEmbeddings(_data: string) {
          return [1, 2, 3];
        }
        async computeSourceEmbeddings(_data: string[]) {
          return [[1, 2, 3]];
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
    });
  });
});
