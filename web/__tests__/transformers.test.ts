import { tableFromArrays, tableToIPC } from "apache-arrow";
import { __setWasmModuleLoaderForTests } from "../src";
import {
  __setTransformersModuleLoaderForTests,
  embed,
  embedMany,
  searchTable,
  transformersEmbedder,
  type EmbeddingModel,
} from "../src/transformers";
import type { WasmRemoteSearchHandle } from "../src/generated/lancedb_wasm";

type FetchFn = typeof globalThis.fetch;
type FetchInput = Parameters<FetchFn>[0];
type FetchInit = Parameters<FetchFn>[1];

interface PublishedMetadataFixture {
  version: number;
  manifestPath: string;
  manifestNamingScheme: string;
  latestManifestPath: string;
  latestVersionPath: string;
  webMetadataPath: string;
  snapshotPath: string;
  vectorColumns: string[];
  ftsColumns: string[];
  defaultVectorColumn?: string;
}

interface PublishedSnapshotFixture extends PublishedMetadataFixture {
  isComplete: boolean;
}

function createFetch(
  handler: (
    input: FetchInput,
    init?: FetchInit,
  ) => Promise<Response> | Response,
): FetchFn {
  return async (input: FetchInput, init?: FetchInit) =>
    await handler(input, init);
}

function installFetchMock(fetchFn: FetchFn = successfulFetch()): void {
  jest.spyOn(globalThis, "fetch").mockImplementation(fetchFn);
}

function makeArrowIpc() {
  return tableToIPC(
    tableFromArrays({
      id: [1, 2],
      doc: ["apple pie", "banana split"],
    }),
  );
}

function makeHandle(): jest.Mocked<WasmRemoteSearchHandle> {
  const bytes = makeArrowIpc();
  return {
    schema: jest.fn(async () => bytes),
    search: jest.fn(async (_requestJson: string) => bytes),
    refresh: jest.fn(async () => true),
    close: jest.fn(),
  };
}

function publishedMetadata(
  overrides: Partial<PublishedMetadataFixture> = {},
): PublishedMetadataFixture {
  return {
    version: 7,
    manifestPath: "_versions/7.manifest",
    manifestNamingScheme: "v2",
    latestManifestPath: "_latest.manifest",
    latestVersionPath: "_latest.version",
    webMetadataPath: "_web.json",
    snapshotPath: "_snapshot.json",
    vectorColumns: ["embedding"],
    ftsColumns: ["doc"],
    defaultVectorColumn: "embedding",
    ...overrides,
  };
}

function publishedSnapshot(
  overrides: Partial<PublishedSnapshotFixture> = {},
): PublishedSnapshotFixture {
  return {
    ...publishedMetadata(),
    isComplete: true,
    ...overrides,
  };
}

function successfulFetch({
  metadata = publishedMetadata(),
  snapshot = publishedSnapshot(),
}: {
  metadata?: PublishedMetadataFixture;
  snapshot?: PublishedSnapshotFixture;
} = {}): FetchFn {
  return createFetch(async (input) => {
    const url = input.toString();
    if (url.endsWith("/_web.json")) {
      return Response.json(metadata);
    }
    if (url.endsWith("/_snapshot.json")) {
      return Response.json(snapshot);
    }
    if (url.endsWith("/_latest.version")) {
      return new Response("7", { status: 200 });
    }
    if (url.endsWith("/_latest.manifest")) {
      return new Response(new Uint8Array([1]), {
        status: 206,
        headers: {
          "content-range": "bytes 0-0/1",
        },
      });
    }
    throw new Error(`unexpected fetch ${url}`);
  });
}

/**
 * Build a mock transformers module.  `tensor` describes the output for a
 * *single* input.  When the tokenizer receives a batch of N inputs the
 * model's forward() automatically replicates the per-element data N times
 * so the output tensor has the correct batch dimension.
 */
function makeTransformersModule(
  tensor: { dims: number[]; data: number[] },
  capture: { inputs: string[][] },
) {
  let lastBatchSize = 1;
  const tokenizer = jest.fn(
    async (input: string | string[]) => {
      const arr = Array.isArray(input) ? input : [input];
      capture.inputs.push(arr);
      lastBatchSize = arr.length;
      return {};
    },
  );
  const model = {
    forward: jest.fn(async () => {
      // Replicate per-element data for the batch.
      const batchData =
        lastBatchSize === 1
          ? tensor.data
          : Array.from({ length: lastBatchSize }, () => tensor.data).flat();
      const batchDims =
        tensor.dims.length === 3
          ? [lastBatchSize, tensor.dims[1], tensor.dims[2]]
          : tensor.dims.length === 2
            ? [lastBatchSize, tensor.dims[1]]
          : tensor.dims;
      return {
        last_hidden_state: {
          dims: batchDims,
          data: Float32Array.from(batchData),
        },
      };
    }),
  };

  return {
    AutoModel: {
      from_pretrained: jest.fn(async () => model),
    },
    AutoTokenizer: {
      from_pretrained: jest.fn(async () => tokenizer),
    },
  };
}

describe("@lancedb/lancedb-web/transformers", () => {
  afterEach(() => {
    __setWasmModuleLoaderForTests();
    __setTransformersModuleLoaderForTests();
    jest.restoreAllMocks();
  });

  it("embeds natural language queries and reuses the loaded model", async () => {
    const handle = makeHandle();
    __setWasmModuleLoaderForTests(async () => ({
      open_table: async () => handle,
    }));
    installFetchMock(
      successfulFetch({
        metadata: publishedMetadata({
          vectorColumns: ["embedding", "embedding_2"],
        }),
        snapshot: publishedSnapshot({
          vectorColumns: ["embedding", "embedding_2"],
        }),
      }),
    );

    const capture = { inputs: [] as string[][] };
    const transformers = makeTransformersModule(
      {
        dims: [1, 2, 2],
        data: [2, 0, 0, 2],
      },
      capture,
    );
    __setTransformersModuleLoaderForTests(async () => transformers);

    const table = await searchTable("https://example.com/search_table.lance", {
      normalize: false,
    });

    await table.search({
      text: "apple",
      limit: 3,
      vectorColumn: "embedding_2",
    });
    await table.search({
      text: "banana",
    });

    expect(transformers.AutoModel.from_pretrained).toHaveBeenCalledTimes(1);
    expect(transformers.AutoTokenizer.from_pretrained).toHaveBeenCalledTimes(1);
    expect(capture.inputs).toEqual([["apple"], ["banana"]]);
    expect(handle.search).toHaveBeenNthCalledWith(
      1,
      JSON.stringify({
        limit: 3,
        vectorColumn: "embedding_2",
        vector: [1, 1],
      }),
    );
    expect(handle.search).toHaveBeenNthCalledWith(
      2,
      JSON.stringify({
        vector: [1, 1],
        vectorColumn: "embedding",
      }),
    );
  });

  it("applies BGE query prefixes and CLS pooling defaults", async () => {
    const handle = makeHandle();
    __setWasmModuleLoaderForTests(async () => ({
      open_table: async () => handle,
    }));
    installFetchMock(successfulFetch());

    const capture = { inputs: [] as string[][] };
    const transformers = makeTransformersModule(
      {
        dims: [1, 3, 2],
        data: [1, 10, 4, 20, 7, 30],
      },
      capture,
    );
    __setTransformersModuleLoaderForTests(async () => transformers);

    const table = await searchTable("https://example.com/search_table.lance", {
      model: "BAAI/bge-small-en-v1.5",
      normalize: false,
    });

    await table.search({
      text: "best places to hike in colorado",
    });

    expect(capture.inputs).toEqual([
      [
        "Represent this sentence for searching relevant passages: best places to hike in colorado",
      ],
    ]);
    expect(handle.search).toHaveBeenCalledWith(
      JSON.stringify({
        vector: [1, 10],
        vectorColumn: "embedding",
      }),
    );
  });

  it("applies custom prepareQuery and logs debug output", async () => {
    const handle = makeHandle();
    __setWasmModuleLoaderForTests(async () => ({
      open_table: async () => handle,
    }));
    installFetchMock(successfulFetch());

    const capture = { inputs: [] as string[][] };
    const transformers = makeTransformersModule(
      {
        dims: [1, 1, 2],
        data: [3, 4],
      },
      capture,
    );
    __setTransformersModuleLoaderForTests(async () => transformers);

    const debugSpy = jest
      .spyOn(console, "debug")
      .mockImplementation(() => undefined);

    const table = await searchTable("https://example.com/search_table.lance", {
      normalize: false,
      prepareQuery: (query) => `search:${query}`,
    });

    await table.search({
      text: "hola",
      debug: true,
    });

    expect(capture.inputs).toEqual([["search:hola"]]);
    expect(debugSpy).toHaveBeenCalledWith(
      "[@lancedb/lancedb-web/transformers]",
      expect.objectContaining({
        query: "hola",
        dimensions: 2,
      }),
    );
  });

  // -----------------------------------------------------------------------
  // searchTable with string model arg (auto-promoted)
  // -----------------------------------------------------------------------

  it("accepts a string model as the second argument", async () => {
    const handle = makeHandle();
    __setWasmModuleLoaderForTests(async () => ({
      open_table: async () => handle,
    }));
    installFetchMock(successfulFetch());

    const capture = { inputs: [] as string[][] };
    const transformers = makeTransformersModule(
      { dims: [1, 1, 2], data: [3, 4] },
      capture,
    );
    __setTransformersModuleLoaderForTests(async () => transformers);

    const table = await searchTable(
      "https://example.com/search_table.lance",
      "BAAI/bge-small-en-v1.5",
    );

    await table.search({ text: "hello" });

    expect(transformers.AutoModel.from_pretrained).toHaveBeenCalledTimes(1);
    expect(capture.inputs).toEqual([
      [
        "Represent this sentence for searching relevant passages: hello",
      ],
    ]);
  });

  // -----------------------------------------------------------------------
  // searchTable with BYO EmbeddingModel
  // -----------------------------------------------------------------------

  it("accepts a custom EmbeddingModel as the second argument", async () => {
    const handle = makeHandle();
    __setWasmModuleLoaderForTests(async () => ({
      open_table: async () => handle,
    }));
    installFetchMock(successfulFetch());

    const customModel: EmbeddingModel = {
      async embed(value: string) {
        return { embedding: [value.length, value.length * 2] };
      },
      async embedMany(values: string[]) {
        return {
          embeddings: values.map((v) => [v.length, v.length * 2]),
        };
      },
    };

    const table = await searchTable(
      "https://example.com/search_table.lance",
      customModel,
    );

    await table.search({ text: "hello" });

    expect(handle.search).toHaveBeenCalledWith(
      JSON.stringify({
        vector: [5, 10],
        vectorColumn: "embedding",
      }),
    );
  });

  // -----------------------------------------------------------------------
  // table.model — exposed EmbeddingModel
  // -----------------------------------------------------------------------

  it("exposes the resolved EmbeddingModel on the table", async () => {
    const handle = makeHandle();
    __setWasmModuleLoaderForTests(async () => ({
      open_table: async () => handle,
    }));
    installFetchMock(successfulFetch());

    const capture = { inputs: [] as string[][] };
    const transformers = makeTransformersModule(
      { dims: [1, 1, 2], data: [3, 4] },
      capture,
    );
    __setTransformersModuleLoaderForTests(async () => transformers);

    const table = await searchTable("https://example.com/search_table.lance", {
      normalize: false,
    });

    const { embedding } = await table.model.embed("test");
    expect(embedding).toEqual([3, 4]);

    const { embeddings } = await table.model.embedMany(["a", "b"]);
    expect(embeddings).toEqual([[3, 4], [3, 4]]);
  });

  // -----------------------------------------------------------------------
  // transformersEmbedder factory
  // -----------------------------------------------------------------------

  it("transformersEmbedder() creates a standalone EmbeddingModel", async () => {
    const capture = { inputs: [] as string[][] };
    const transformers = makeTransformersModule(
      { dims: [1, 2, 2], data: [2, 0, 0, 2] },
      capture,
    );
    __setTransformersModuleLoaderForTests(async () => transformers);

    const model = transformersEmbedder({ normalize: false });

    const { embedding } = await model.embed("hello");
    expect(embedding).toEqual([1, 1]);
    expect(capture.inputs).toEqual([["hello"]]);
  });

  it("transformersEmbedder(string) uses the model id", async () => {
    const capture = { inputs: [] as string[][] };
    const transformers = makeTransformersModule(
      { dims: [1, 3, 2], data: [1, 10, 4, 20, 7, 30] },
      capture,
    );
    __setTransformersModuleLoaderForTests(async () => transformers);

    const model = transformersEmbedder("BAAI/bge-small-en-v1.5", {
      normalize: false,
    });

    const { embedding } = await model.embed("query");
    // CLS pooling for BGE models — first token
    expect(embedding).toEqual([1, 10]);
    // BGE English retrieval prefix applied
    expect(capture.inputs).toEqual([
      ["Represent this sentence for searching relevant passages: query"],
    ]);
  });

  // -----------------------------------------------------------------------
  // Standalone embed / embedMany
  // -----------------------------------------------------------------------

  it("embed() with string model id", async () => {
    const capture = { inputs: [] as string[][] };
    const transformers = makeTransformersModule(
      { dims: [1, 1, 2], data: [3, 4] },
      capture,
    );
    __setTransformersModuleLoaderForTests(async () => transformers);

    const result = await embed({ value: "test", model: "Xenova/all-MiniLM-L6-v2" });
    expect(result.embedding).toHaveLength(2);
    expect(capture.inputs).toEqual([["test"]]);
  });

  it("embed() with EmbeddingModel object", async () => {
    const customModel: EmbeddingModel = {
      async embed(value: string) {
        return { embedding: [value.length] };
      },
      async embedMany(values: string[]) {
        return { embeddings: values.map((v) => [v.length]) };
      },
    };

    const result = await embed({ value: "hello", model: customModel });
    expect(result.embedding).toEqual([5]);
  });

  it("embedMany() embeds multiple values", async () => {
    const capture = { inputs: [] as string[][] };
    const transformers = makeTransformersModule(
      { dims: [1, 1, 2], data: [3, 4] },
      capture,
    );
    __setTransformersModuleLoaderForTests(async () => transformers);

    const result = await embedMany({ values: ["a", "b", "c"] });
    expect(result.embeddings).toHaveLength(3);
    // Batch tokenize+forward: single call with all values
    expect(capture.inputs).toEqual([["a", "b", "c"]]);
  });

  it("embedMany() handles batched 2D pooled outputs", async () => {
    const capture = { inputs: [] as string[][] };
    const transformers = makeTransformersModule(
      { dims: [1, 2], data: [3, 4] },
      capture,
    );
    __setTransformersModuleLoaderForTests(async () => transformers);

    const model = transformersEmbedder({
      normalize: false,
    });
    const result = await model.embedMany(["a", "b", "c"]);

    expect(result.embeddings).toEqual([
      [3, 4],
      [3, 4],
      [3, 4],
    ]);
    expect(capture.inputs).toEqual([["a", "b", "c"]]);
  });

  it("embedMany() with EmbeddingModel object", async () => {
    const customModel: EmbeddingModel = {
      async embed(value: string) {
        return { embedding: [value.length] };
      },
      async embedMany(values: string[]) {
        return { embeddings: values.map((v) => [v.length]) };
      },
    };

    const result = await embedMany({
      values: ["hi", "hello", "hey"],
      model: customModel,
    });
    expect(result.embeddings).toEqual([[2], [5], [3]]);
  });

  // -----------------------------------------------------------------------
  // Model reuse across searchTable + standalone
  // -----------------------------------------------------------------------

  it("shares loaded model between searchTable and standalone embed", async () => {
    const handle = makeHandle();
    __setWasmModuleLoaderForTests(async () => ({
      open_table: async () => handle,
    }));
    installFetchMock(successfulFetch());

    const capture = { inputs: [] as string[][] };
    const transformers = makeTransformersModule(
      { dims: [1, 1, 2], data: [3, 4] },
      capture,
    );
    __setTransformersModuleLoaderForTests(async () => transformers);

    // Open table first — triggers model load
    await searchTable("https://example.com/search_table.lance");

    // Standalone embed should reuse the same loaded model
    await embed({ value: "reuse test" });

    // Model should only have been loaded once
    expect(transformers.AutoModel.from_pretrained).toHaveBeenCalledTimes(1);
    expect(transformers.AutoTokenizer.from_pretrained).toHaveBeenCalledTimes(1);
  });

  // -----------------------------------------------------------------------
  // #10 — last_token pooling
  // -----------------------------------------------------------------------

  it("uses last_token pooling for BAAI/bge-multilingual-gemma2", async () => {
    const capture = { inputs: [] as string[][] };
    // 3 tokens, hidden size 2: last token is [5, 6]
    const transformers = makeTransformersModule(
      { dims: [1, 3, 2], data: [1, 2, 3, 4, 5, 6] },
      capture,
    );
    __setTransformersModuleLoaderForTests(async () => transformers);

    const model = transformersEmbedder("BAAI/bge-multilingual-gemma2", {
      normalize: false,
    });
    const { embedding } = await model.embed("test query");

    // last_token pooling → last token [5, 6]
    expect(embedding).toEqual([5, 6]);
  });

  // -----------------------------------------------------------------------
  // #11 — normalization (the default normalize: true path)
  // -----------------------------------------------------------------------

  it("normalizes embeddings by default", async () => {
    const capture = { inputs: [] as string[][] };
    // Single token [3, 4] → magnitude 5 → normalized [0.6, 0.8]
    const transformers = makeTransformersModule(
      { dims: [1, 1, 2], data: [3, 4] },
      capture,
    );
    __setTransformersModuleLoaderForTests(async () => transformers);

    // Default normalize: true
    const model = transformersEmbedder();
    const { embedding } = await model.embed("test");

    expect(embedding[0]).toBeCloseTo(0.6);
    expect(embedding[1]).toBeCloseTo(0.8);
    // Verify it's a unit vector
    const magnitude = Math.sqrt(
      embedding.reduce((sum, v) => sum + v * v, 0),
    );
    expect(magnitude).toBeCloseTo(1.0);
  });

  // -----------------------------------------------------------------------
  // #12 — error paths
  // -----------------------------------------------------------------------

  it("throws when model load fails", async () => {
    __setTransformersModuleLoaderForTests(async () => ({
      AutoModel: {
        from_pretrained: async () => {
          throw new Error("network error");
        },
      },
      AutoTokenizer: {
        from_pretrained: async () => jest.fn(),
      },
    }));

    const model = transformersEmbedder();
    await expect(model.embed("test")).rejects.toThrow(
      /Failed to initialize transformers model/,
    );
  });

  it("throws when model output contains no tensor", async () => {
    __setTransformersModuleLoaderForTests(async () => ({
      AutoModel: {
        from_pretrained: async () => ({
          forward: async () => ({ some_string: "not a tensor" }),
        }),
      },
      AutoTokenizer: {
        from_pretrained: async () => jest.fn(async () => ({})),
      },
    }));

    const model = transformersEmbedder();
    await expect(model.embed("test")).rejects.toThrow(
      /did not contain an embedding tensor/,
    );
  });

  it("throws for unsupported tensor shape", async () => {
    __setTransformersModuleLoaderForTests(async () => ({
      AutoModel: {
        from_pretrained: async () => ({
          forward: async () => ({
            last_hidden_state: {
              dims: [1, 2, 3, 4],
              data: Float32Array.from([0]),
            },
          }),
        }),
      },
      AutoTokenizer: {
        from_pretrained: async () => jest.fn(async () => ({})),
      },
    }));

    const model = transformersEmbedder();
    await expect(model.embed("test")).rejects.toThrow(
      /Unsupported embedding tensor shape/,
    );
  });

  // -----------------------------------------------------------------------
  // #13 — Chinese BGE prefix and Gemma2 instruction format
  // -----------------------------------------------------------------------

  it("applies Chinese BGE retrieval prefix", async () => {
    const capture = { inputs: [] as string[][] };
    const transformers = makeTransformersModule(
      { dims: [1, 1, 2], data: [1, 0] },
      capture,
    );
    __setTransformersModuleLoaderForTests(async () => transformers);

    const model = transformersEmbedder("BAAI/bge-large-zh-v1.5", {
      normalize: false,
    });
    await model.embed("测试查询");

    expect(capture.inputs).toEqual([
      ["为这个句子生成表示以用于检索相关文章：测试查询"],
    ]);
  });

  it("applies Gemma2 multilingual instruction format", async () => {
    const capture = { inputs: [] as string[][] };
    const transformers = makeTransformersModule(
      { dims: [1, 3, 2], data: [1, 2, 3, 4, 5, 6] },
      capture,
    );
    __setTransformersModuleLoaderForTests(async () => transformers);

    const model = transformersEmbedder("BAAI/bge-multilingual-gemma2", {
      normalize: false,
    });
    await model.embed("search query");

    expect(capture.inputs[0][0]).toMatch(
      /^<instruct>Given a web search query.*\n<query>search query$/,
    );
  });

  // -----------------------------------------------------------------------
  // #14 — concurrent embed() calls share the same model load
  // -----------------------------------------------------------------------

  it("concurrent embed calls share a single model load", async () => {
    const capture = { inputs: [] as string[][] };
    const transformers = makeTransformersModule(
      { dims: [1, 1, 2], data: [1, 0] },
      capture,
    );
    __setTransformersModuleLoaderForTests(async () => transformers);

    const model = transformersEmbedder({ normalize: false });

    // Fire three embed calls concurrently
    const [r1, r2, r3] = await Promise.all([
      model.embed("a"),
      model.embed("b"),
      model.embed("c"),
    ]);

    // All should resolve successfully
    expect(r1.embedding).toEqual([1, 0]);
    expect(r2.embedding).toEqual([1, 0]);
    expect(r3.embedding).toEqual([1, 0]);

    // Model should only have been loaded once despite concurrent calls
    expect(transformers.AutoModel.from_pretrained).toHaveBeenCalledTimes(1);
    expect(transformers.AutoTokenizer.from_pretrained).toHaveBeenCalledTimes(1);
  });

  // -----------------------------------------------------------------------
  // preload / dispose
  // -----------------------------------------------------------------------

  it("preload() eagerly loads and dispose() releases cached resources", async () => {
    const capture = { inputs: [] as string[][] };
    const transformers = makeTransformersModule(
      { dims: [1, 1, 2], data: [1, 0] },
      capture,
    );
    __setTransformersModuleLoaderForTests(async () => transformers);

    const model = transformersEmbedder({ normalize: false });

    // preload triggers model load before first embed
    await model.preload!();
    expect(transformers.AutoModel.from_pretrained).toHaveBeenCalledTimes(1);

    // embed reuses the preloaded model
    await model.embed("test");
    expect(transformers.AutoModel.from_pretrained).toHaveBeenCalledTimes(1);

    // dispose releases — next embed reloads
    model.dispose!();
    await model.embed("test2");
    expect(transformers.AutoModel.from_pretrained).toHaveBeenCalledTimes(2);
  });

  // -----------------------------------------------------------------------
  // AbortSignal
  // -----------------------------------------------------------------------

  it("embed() rejects immediately when signal is already aborted", async () => {
    const capture = { inputs: [] as string[][] };
    const transformers = makeTransformersModule(
      { dims: [1, 1, 2], data: [1, 0] },
      capture,
    );
    __setTransformersModuleLoaderForTests(async () => transformers);

    const model = transformersEmbedder();
    const controller = new AbortController();
    controller.abort();

    await expect(
      model.embed("test", { signal: controller.signal }),
    ).rejects.toBeDefined();

    // Model forward should never have been called
    expect(transformers.AutoModel.from_pretrained).not.toHaveBeenCalled();
  });
});
