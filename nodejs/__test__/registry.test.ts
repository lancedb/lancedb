// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import * as apiArrow from "apache-arrow";

import * as tmp from "tmp";

import { arrowVersions } from "./arrow_versions";

import { connect } from "../lancedb";
import {
  EmbeddingFunction,
  FunctionOptions,
  LanceSchema,
  UntrustedEmbeddingConfigError,
  UntrustedEmbeddingFunction,
} from "../lancedb/embedding";
import { getRegistry as getRegistryWithBuiltIns } from "../lancedb/embedding";
import { getRegistry, register } from "../lancedb/embedding/registry";
import { TransformersEmbeddingFunction } from "../lancedb/embedding/transformers";

describe.each(arrowVersions)("LanceSchema", (arrow) => {
  test("should preserve input order", async () => {
    const schema = LanceSchema({
      id: new arrow.Int32(),
      text: new arrow.Utf8(),
      vector: new arrow.Float32(),
    });
    expect(schema.fields.map((x) => x.name)).toEqual(["id", "text", "vector"]);
  });
});

describe.each(arrowVersions)("Registry", (arrow) => {
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
      constructor() {
        super();
      }
      ndims() {
        return 3;
      }
      embeddingDataType() {
        return new arrow.Float32() as apiArrow.Float;
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
      text: func.sourceField(new arrow.Utf8() as apiArrow.DataType),
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
    const vectors = actual.getChild("vector")!.toArray();
    expect(JSON.parse(JSON.stringify(vectors))).toEqual(
      JSON.parse(JSON.stringify(expected)),
    );
  });
  test("should error if registering with the same name", async () => {
    class MockEmbeddingFunction extends EmbeddingFunction<string> {
      constructor() {
        super();
      }
      ndims() {
        return 3;
      }
      embeddingDataType() {
        return new arrow.Float32() as apiArrow.Float;
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
  test("parseFunctions keeps entries sharing a function name", async () => {
    class MockEmbeddingFunction extends EmbeddingFunction<string> {
      ndims() {
        return 3;
      }
      embeddingDataType() {
        return new arrow.Float32() as apiArrow.Float;
      }
      async computeSourceEmbeddings(data: string[]) {
        return data.map(() => [1, 2, 3]);
      }
    }
    register("mock-embedding")(MockEmbeddingFunction);
    const parsed = await getRegistry().parseFunctions(
      new Map([
        [
          "embedding_functions",
          JSON.stringify([
            {
              name: "mock-embedding",
              sourceColumn: "text",
              vectorColumn: "vector_a",
              model: {},
            },
            {
              name: "mock-embedding",
              sourceColumn: "text",
              vectorColumn: "vector_b",
              model: {},
            },
          ]),
        ],
      ]),
    );
    expect([...parsed.values()].map((f) => f.vectorColumn)).toEqual([
      "vector_a",
      "vector_b",
    ]);

    // The Python bindings write snake_case keys.
    const snake = await getRegistry().parseFunctions(
      new Map([
        [
          "embedding_functions",
          JSON.stringify([
            {
              name: "mock-embedding",
              // biome-ignore lint/style/useNamingConvention: the Python wire spelling
              source_column: "text",
              // biome-ignore lint/style/useNamingConvention: the Python wire spelling
              vector_column: "vector_a",
              model: {},
            },
            {
              name: "mock-embedding",
              // biome-ignore lint/style/useNamingConvention: the Python wire spelling
              source_column: "text",
              // biome-ignore lint/style/useNamingConvention: the Python wire spelling
              vector_column: "vector_b",
              model: {},
            },
          ]),
        ],
      ]),
    );
    expect([...snake.keys()]).toEqual(["vector_a", "vector_b"]);
    expect([...snake.values()].map((f) => f.sourceColumn)).toEqual([
      "text",
      "text",
    ]);
  });
  test("schema should contain correct metadata", async () => {
    class MockEmbeddingFunction extends EmbeddingFunction<string> {
      constructor(args: FunctionOptions = {}) {
        super();
        this.resolveVariables(args);
      }
      ndims() {
        return 3;
      }
      embeddingDataType() {
        return new arrow.Float32() as apiArrow.Float;
      }
      async computeSourceEmbeddings(data: string[]) {
        return data.map(() => [1, 2, 3]);
      }
    }
    const func = new MockEmbeddingFunction({ someText: "hello" });

    const schema = LanceSchema({
      id: new arrow.Int32(),
      text: func.sourceField(new arrow.Utf8() as apiArrow.DataType),
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

describe("Registry.setVar", () => {
  const registry = getRegistry();

  beforeEach(() => {
    @register("mock-embedding")
    // biome-ignore lint/correctness/noUnusedVariables :
    class MockEmbeddingFunction extends EmbeddingFunction<string> {
      constructor(optionsRaw: FunctionOptions = {}) {
        super();
        const options = this.resolveVariables(optionsRaw);

        expect(optionsRaw["someKey"].startsWith("$var:someName")).toBe(true);
        expect(options["someKey"]).toBe("someValue");

        if (options["secretKey"]) {
          expect(optionsRaw["secretKey"]).toBe("$var:secretKey");
          expect(options["secretKey"]).toBe("mySecret");
        }
      }
      async computeSourceEmbeddings(data: string[]) {
        return data.map(() => [1, 2, 3]);
      }
      embeddingDataType() {
        return new apiArrow.Float32();
      }
      protected getSensitiveKeys() {
        return ["secretKey"];
      }
    }
  });
  afterEach(() => {
    registry.reset();
  });

  it("Should error if the variable is not set", () => {
    console.log(registry.get("mock-embedding"));
    expect(() =>
      registry.get("mock-embedding")!.create({ someKey: "$var:someName" }),
    ).toThrow('Variable "someName" not found');
  });

  it("should use default values if not set", () => {
    registry
      .get("mock-embedding")!
      .create({ someKey: "$var:someName:someValue" });
  });

  it("should set a variable that the embedding function understand", () => {
    registry.setVar("someName", "someValue", { secret: false });
    registry.get("mock-embedding")!.create({ someKey: "$var:someName" });
  });

  it("should reject secrets that aren't passed as variables", () => {
    registry.setVar("someName", "someValue", { secret: false });
    expect(() =>
      registry
        .get("mock-embedding")!
        .create({ secretKey: "someValue", someKey: "$var:someName" }),
    ).toThrow(
      'The key "secretKey" is sensitive and cannot be set directly. Please use the $var: syntax to set it.',
    );
  });

  it("should not serialize secrets", () => {
    registry.setVar("someName", "someValue", { secret: false });
    registry.setVar("secretKey", "mySecret");
    const func = registry
      .get("mock-embedding")!
      .create({ secretKey: "$var:secretKey", someKey: "$var:someName" });
    expect(func.toJSON()).toEqual({
      secretKey: "$var:secretKey",
      someKey: "$var:someName",
    });
  });
});

describe("Restoring embedding functions from table metadata", () => {
  const registry = getRegistry();
  const secret = "sk-very-secret";
  let constructed: FunctionOptions[];

  class MockEmbeddingFunction extends EmbeddingFunction<string> {
    options: FunctionOptions;
    constructor(optionsRaw: FunctionOptions = {}) {
      super();
      this.options = this.resolveVariables(optionsRaw);
      constructed.push(this.options);
      if (this.options.model === "unknown") {
        throw new Error(`Unknown model: ${this.options.model}`);
      }
    }
    static loadedModels(options: FunctionOptions): [string, undefined][] {
      return options.model && options.model !== "default"
        ? [[options.model, undefined]]
        : [];
    }
    static endpoints(options: FunctionOptions): string[] {
      return options.baseUrl ? [options.baseUrl] : [];
    }
    ndims() {
      return 3;
    }
    embeddingDataType() {
      return new apiArrow.Float32();
    }
    async computeSourceEmbeddings(data: string[]) {
      return data.map(() => [1, 2, 3]);
    }
    protected getSensitiveKeys() {
      return ["apiKey"];
    }
  }

  let nameCounter = 0;
  function registerMock(
    options: { safeToRestore?: boolean } = { safeToRestore: true },
    ctor: typeof MockEmbeddingFunction = MockEmbeddingFunction,
  ): string {
    // allow* approvals outlive reset(), so each test uses its own names.
    const name = `restore-mock-${nameCounter++}`;
    registry.register(name, options)(ctor);
    return name;
  }

  async function restore(
    name: string,
    model: FunctionOptions,
  ): Promise<EmbeddingFunction> {
    const parsed = await registry.parseFunctions(
      new Map([
        [
          "embedding_functions",
          JSON.stringify([
            { name, sourceColumn: "text", vectorColumn: "vector", model },
          ]),
        ],
      ]),
    );
    return parsed.get("vector")!.function;
  }

  async function restoreError(name: string, model: FunctionOptions) {
    try {
      const func = await restore(name, model);
      await func.computeQueryEmbeddings("hello");
    } catch (e) {
      return e as Error;
    }
    throw new Error("expected restoring to fail");
  }

  beforeEach(() => {
    constructed = [];
  });
  afterEach(() => {
    registry.reset();
  });

  it("does not show a secret used in a non-sensitive key", async () => {
    const name = registerMock();
    registry.setVar(`key-${name}`, secret);
    registry.setVar(`apiKey-${name}`, secret);

    const error = await restoreError(name, {
      model: `$var:key-${name}`,
      apiKey: `$var:apiKey-${name}`,
    });

    expect(error.message).toBe(`Variable "key-${name}" not found`);
    expect(JSON.stringify(constructed)).not.toContain(secret);
  });

  it("does not reveal whether a secret variable exists", async () => {
    const name = registerMock();
    registry.setVar(`set-${name}`, secret);

    const setError = await restoreError(name, { model: `$var:set-${name}` });
    const unsetError = await restoreError(name, {
      model: `$var:unset-${name}`,
    });
    expect(setError.message).toBe(`Variable "set-${name}" not found`);
    expect(unsetError.message).toBe(`Variable "unset-${name}" not found`);

    const func = await restore(name, { model: `$var:set-${name}:default` });
    expect((func as MockEmbeddingFunction).options.model).toBe("default");
  });

  it("does not leak a secret through constructor errors", async () => {
    const name = registerMock();
    registry.setVar(`model-${name}`, "unknown");

    // The secret would resolve to a model the constructor rejects by name.
    const error = await restoreError(name, { model: `$var:model-${name}` });

    expect(error.message).not.toContain("unknown");
  });

  it("resolves a secret in a sensitive key once approved", async () => {
    const name = registerMock();
    registry.setVar(`apiKey-${name}`, secret);

    const func = await restore(name, { apiKey: `$var:apiKey-${name}` });

    expect((func as MockEmbeddingFunction).options.apiKey).toBe(secret);
    expect(func.toJSON()).toEqual({ apiKey: `$var:apiKey-${name}` });
  });

  it("resolves a public variable in any key", async () => {
    const name = registerMock();
    registry.setVar(`model-${name}`, "default", { secret: false });

    const func = await restore(name, { model: `$var:model-${name}` });

    expect((func as MockEmbeddingFunction).options.model).toBe("default");
  });

  it("rejects a secret in a non-sensitive key in create()", () => {
    const name = registerMock();
    registry.setVar(`key-${name}`, secret);

    let error: Error | undefined;
    try {
      registry.get(name)!.create({ model: `$var:key-${name}` });
    } catch (e) {
      error = e as Error;
    }
    expect(error?.message).toContain(`Variable "key-${name}" is secret`);
    expect(error?.message).not.toContain(secret);
  });

  it("can make a variable public again", () => {
    const name = registerMock();
    registry.setVar(`model-${name}`, "default");
    registry.setVar(`model-${name}`, "default", { secret: false });

    const func = registry.get(name)!.create({ model: `$var:model-${name}` });

    expect((func as MockEmbeddingFunction).options.model).toBe("default");
  });

  it("needs approval for a model other than the default", async () => {
    const name = registerMock();
    registry.setVar(`apiKey-${name}`, secret);
    const model = { model: "attacker/model", apiKey: `$var:apiKey-${name}` };

    const func = await restore(name, model);
    expect(func).toBeInstanceOf(UntrustedEmbeddingFunction);
    // Nothing ran with the table's options, and secrets were never resolved.
    expect(constructed).toEqual([]);
    const error = await func.computeQueryEmbeddings("hello").catch((e) => e);
    expect(error).toBeInstanceOf(UntrustedEmbeddingConfigError);
    expect(error.message).toContain('allowModel("attacker/model")');
    expect(error.message).not.toContain(secret);
    // Writing the metadata again keeps the table's options.
    expect(
      registry.functionToMetadata({ sourceColumn: "text", function: func }),
    ).toMatchObject({ name, model });

    registry.allowModel("attacker/model");
    expect(await restore(name, model)).toBeInstanceOf(MockEmbeddingFunction);
  });

  it("needs approval for a model loaded through a public variable", async () => {
    const name = registerMock();
    registry.setVar(`model-${name}`, "attacker/public-model", {
      secret: false,
    });

    const func = await restore(name, { model: `$var:model-${name}` });

    expect(func).toBeInstanceOf(UntrustedEmbeddingFunction);
  });

  it("needs approval for a custom endpoint", async () => {
    const name = registerMock();
    const model = { baseUrl: "https://attacker.example/v1" };

    const func = await restore(name, model);
    expect(func).toBeInstanceOf(UntrustedEmbeddingFunction);
    const error = await func.computeQueryEmbeddings("hello").catch((e) => e);
    expect(error.message).toContain(
      'allowEndpoint("https://attacker.example/v1")',
    );

    registry.allowEndpoint("https://attacker.example/v1/");
    expect(await restore(name, model)).toBeInstanceOf(MockEmbeddingFunction);
  });

  it("trusts the models and endpoints of functions it created", async () => {
    const name = registerMock();
    const model = { model: "my/model", baseUrl: "https://my.example" };
    const func = registry.get(name)!.create(model);
    registry.getTableMetadata([{ sourceColumn: "text", function: func }]);

    expect(await restore(name, model)).toBeInstanceOf(MockEmbeddingFunction);
  });

  it("needs approval for a function not marked safe to restore", async () => {
    const name = registerMock({});
    registry.setVar(`apiKey-${name}`, secret);
    const model = { apiKey: `$var:apiKey-${name}` };

    const func = await restore(name, model);
    expect(func).toBeInstanceOf(UntrustedEmbeddingFunction);
    expect(constructed).toEqual([]);
    const error = await func.computeQueryEmbeddings("hello").catch((e) => e);
    expect(error.message).toContain(`allowFunction("${name}")`);

    registry.allowFunction(name);
    expect(await restore(name, model)).toBeInstanceOf(MockEmbeddingFunction);
  });

  it("trusts options of an unmarked function it created", async () => {
    const name = registerMock({});
    const func = registry.get(name)!.create({ other: 1 });
    registry.getTableMetadata([{ sourceColumn: "text", function: func }]);

    expect(await restore(name, { other: 1 })).toBeInstanceOf(
      MockEmbeddingFunction,
    );
    expect(await restore(name, { other: 2 })).toBeInstanceOf(
      UntrustedEmbeddingFunction,
    );
  });

  it("does not let a subclass inherit safeToRestore", async () => {
    registerMock();
    class Subclass extends MockEmbeddingFunction {}
    const name = registerMock({}, Subclass);

    expect(await restore(name, {})).toBeInstanceOf(UntrustedEmbeddingFunction);
  });

  it("needs approval for a huggingface model other than the default", () => {
    expect(TransformersEmbeddingFunction.loadedModels({})).toEqual([]);
    expect(
      TransformersEmbeddingFunction.loadedModels({
        model: "Xenova/all-MiniLM-L6-v2",
      }),
    ).toEqual([]);
    expect(
      TransformersEmbeddingFunction.loadedModels({
        model: "attacker/model",
        tokenizer: "attacker/tokenizer",
      }),
    ).toEqual([
      ["attacker/model", undefined],
      ["attacker/tokenizer", undefined],
    ]);
  });

  it("does not load an unapproved huggingface model", async () => {
    getRegistryWithBuiltIns();
    const func = await restore("huggingface", { model: "attacker/hf-model" });

    expect(func).toBeInstanceOf(UntrustedEmbeddingFunction);
  });
});
