// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import * as http from "http";
import { RequestListener } from "http";
import { tableFromArrays } from "apache-arrow";
import packageJson = require("../package.json");
import {
  ClientAuthMethod,
  ClientConfig,
  Connection,
  ConnectionOptions,
  OAuthConfig,
  OAuthFlowType,
  TlsConfig,
  blob,
  connect,
} from "../lancedb";
import {
  Table as ArrowTable,
  Field,
  Int64,
  RecordBatch,
  RecordBatchReader,
  Schema,
  Struct,
  makeArrowTable,
  tableToIPC,
} from "../lancedb/arrow";
import {
  HeaderProvider,
  OAuthHeaderProvider,
  StaticHeaderProvider,
} from "../lancedb/header";
import { Index } from "../lancedb/indices";
import type { JobProgress } from "../lancedb/job";
import { LocalTable } from "../lancedb/table";

// Test-only header providers
class CustomProvider extends HeaderProvider {
  getHeaders(): Record<string, string> {
    return { "X-Custom": "custom-value" };
  }
}

class ErrorProvider extends HeaderProvider {
  private errorMessage: string;
  public callCount: number = 0;

  constructor(errorMessage: string = "Test error") {
    super();
    this.errorMessage = errorMessage;
  }

  getHeaders(): Record<string, string> {
    this.callCount++;
    throw new Error(this.errorMessage);
  }
}

class ConcurrentProvider extends HeaderProvider {
  private counter: number = 0;

  getHeaders(): Record<string, string> {
    this.counter++;
    return { "X-Request-Id": String(this.counter) };
  }
}

async function withMockDatabase(
  listener: RequestListener,
  callback: (db: Connection) => void,
  connectionOptions?: ConnectionOptions,
) {
  const server = http.createServer(listener);
  server.listen(8000);

  const db = await connect(
    "db://dev",
    Object.assign(
      {
        apiKey: "fake",
        hostOverride: "http://localhost:8000",
      },
      connectionOptions,
    ),
  );

  try {
    await callback(db);
  } finally {
    // `close()` alone leaves the port bound until keep-alive sockets drain, so
    // a single failing test would cascade into EADDRINUSE for every test after
    // it. Destroy the connections and wait for the port to actually be free.
    await new Promise<void>((resolve) => {
      server.closeAllConnections();
      server.close(() => resolve());
    });
  }
}

describe("remote connection", () => {
  it.each([
    { dataStorageVersion: "2.0" },
    { enableV2ManifestPaths: false },
    { storageOptions: { bogusOptionXyz: "1" } },
  ])("rejects local create options %j before sending data", async (options) => {
    const requests: string[] = [];
    await withMockDatabase(
      (req, res) => {
        requests.push(req.url ?? "");
        res.writeHead(200).end();
      },
      async (db) => {
        await expect(
          db.createTable("test", [{ value: 1 }], {
            ...options,
            mode: "overwrite",
          }),
        ).rejects.toThrow(/write_options.*LanceDB Enterprise/);
        await expect(
          db.createEmptyTable(
            "test",
            new Schema([new Field("value", new Int64(), true)]),
            options,
          ),
        ).rejects.toThrow(/write_options.*LanceDB Enterprise/);
      },
    );
    expect(requests).toEqual([]);
  });

  it.each([{ indexCacheSize: 5 }, { storageOptions: { x: "y" } }])(
    "rejects local open options %j before sending a request",
    async (options) => {
      const requests: string[] = [];
      await withMockDatabase(
        (req, res) => {
          requests.push(req.url ?? "");
          res.writeHead(200).end();
        },
        async (db) => {
          await expect(
            db.openTable("test", undefined, options),
          ).rejects.toThrow(
            /not supported for table access in LanceDB Enterprise/,
          );
        },
      );
      expect(requests).toEqual([]);
    },
  );

  it.each([false, true])(
    "normalizes remote blob writes (range target: %s)",
    async (wide) => {
      const field = (full: boolean) => {
        const f = blob("image");
        return full
          ? f
          : new Field(
              f.name,
              new Struct((f.type as Struct).children.slice(0, 2)),
              f.nullable,
              f.metadata,
            );
      };
      const schema = (full: boolean) =>
        new Schema([new Field("id", new Int64(), true), field(full)]);
      const writes: { path: string; schema: Schema }[] = [];
      let storedWide = wide;
      const describe = () => ({
        version: writes.length + 1,
        schema: {
          fields: [
            { name: "id", type: { type: "int64" }, nullable: true },
            {
              name: "image",
              nullable: true,
              metadata: { "ARROW:extension:name": "lance.blob.v2" },
              type: {
                type: "struct",
                fields: [
                  {
                    name: "data",
                    nullable: true,
                    type: { type: "large_binary" },
                  },
                  { name: "uri", nullable: true, type: { type: "string" } },
                  ...(storedWide
                    ? ["position", "size"].map((name) => ({
                        name,
                        nullable: true,
                        type: { type: "uint64" },
                      }))
                    : []),
                ],
              },
            },
          ],
        },
      });
      await withMockDatabase(
        (req, res) => {
          if (req.url?.includes("/describe/")) {
            res
              .writeHead(200, { "content-type": "application/json" })
              .end(JSON.stringify(describe()));
            return;
          }
          const chunks: Buffer[] = [];
          // Failed streaming input aborts before end; only complete bodies commit.
          req.on("error", () => {
            /* Aborted inputs must not commit. */
          });
          req.on("data", (chunk) => chunks.push(chunk));
          req.on("end", () => {
            const reader = RecordBatchReader.from(Buffer.concat(chunks)).open();
            // Only read the schema: Rust separately checks compressed wire values.
            writes.push({ path: req.url ?? "", schema: reader.schema });
            storedWide =
              (reader.schema.fields[1].type as Struct).children.length === 4;
            res.writeHead(200, { "content-type": "application/json" }).end(
              JSON.stringify({
                version: writes.length + 1,
                // biome-ignore lint/style/useNamingConvention: server wire format
                num_inserted_rows: 1,
              }),
            );
          });
        },
        async (db) => {
          const table = await db.openTable("test");
          const input = makeArrowTable(
            [{ id: 1n, image: Buffer.from("payload") }],
            { schema: schema(!wide) },
          );
          await table.add(input);
          await table
            .mergeInsert("id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute(input);
          expect(writes).toHaveLength(2);
          for (const write of writes)
            expect(write.schema).toEqual(schema(wide));
          expect(writes[0].path).toContain("/insert/");
          expect(writes[1].path).toContain("/merge_insert/");
          if (!wide) {
            const invalid = makeArrowTable(
              [
                {
                  id: 1n,
                  image: {
                    uri: "s3://registered/payload",
                    position: 4,
                    size: 6,
                  },
                },
              ],
              { schema: schema(true) },
            );
            await expect(table.add(invalid)).rejects.toThrow(
              /cannot store byte ranges/,
            );
            await expect(
              table
                .mergeInsert("id")
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute(invalid),
            ).rejects.toThrow(/cannot store byte ranges/);
            expect(writes).toHaveLength(2);
          }
          await table.add(input, { mode: "overwrite" });
          expect(writes).toHaveLength(3);
          expect(writes[2].path).toContain("mode=overwrite");
          expect(writes[2].schema).toEqual(schema(!wide));
          expect(await table.schema()).toEqual(schema(!wide));
        },
      );
    },
  );

  it("reports unsupported operations with backend-neutral errors", async () => {
    await withMockDatabase(
      (req, res) => {
        expect(req.url).toBe("/v1/table/test/describe/");
        res.writeHead(200, { "Content-Type": "application/json" }).end(
          JSON.stringify({
            version: 1,
            schema: {
              fields: [
                { name: "id", type: { type: "int64" }, nullable: false },
                {
                  name: "vector",
                  type: {
                    type: "fixed_size_list",
                    fields: [
                      { name: "item", type: { type: "float" }, nullable: true },
                    ],
                    length: 2,
                  },
                  nullable: false,
                },
              ],
            },
          }),
        );
      },
      async (db) => {
        const table = (await db.openTable("test")) as LocalTable;
        await expect(table.delta(1)).rejects.toThrow(
          "table.delta is only supported on LanceDB Embedded tables",
        );
        const cases: [() => Promise<unknown>, string][] = [
          [() => table.usesV2ManifestPaths(), "uses_v2_manifest_paths"],
          [() => table.migrateManifestPathsV2(), "migrate_manifest_paths_v2"],
          [
            () => table.createIndex("vector", { config: Index.hnswPq() }),
            "IVF_HNSW_PQ",
          ],
        ];
        for (const [run, operation] of cases) {
          let message = `LanceDBError: not supported: ${operation} is not supported in LanceDB Enterprise.`;
          if (operation === "IVF_HNSW_PQ") {
            message += " Please use IVF_HNSW_SQ instead.";
          }
          await expect(run()).rejects.toThrow(new Error(message));
        }
      },
    );
  });

  it("rejects nprobes(0) before sending a query", async () => {
    const requests: string[] = [];
    await withMockDatabase(
      (req, res) => {
        requests.push(req.url ?? "");
        if (req.url === "/v1/table/test/describe/") {
          res
            .writeHead(200, { "Content-Type": "application/json" })
            .end(JSON.stringify({ version: 1, schema: { fields: [] } }));
        } else {
          res.writeHead(404).end();
        }
      },
      async (db) => {
        const table = await db.openTable("test");
        expect(() => table.vectorSearch([0, 0]).nprobes(0)).toThrow(
          "Invalid input, nprobes must be greater than 0",
        );
        expect(() =>
          table.query().nearestTo([0, 0]).fullTextSearch("dog").nprobes(0),
        ).toThrow("Invalid input, nprobes must be greater than 0");
        await expect(
          table
            .vectorSearch(Promise.resolve([0, 0]))
            .nprobes(0)
            .toArrow(),
        ).rejects.toThrow("Invalid input, nprobes must be greater than 0");
      },
    );
    expect(requests).toEqual(["/v1/table/test/describe/"]);
  });

  it.each([false, true])(
    "preserves an empty query's schema with an empty batch: %s",
    async (withEmptyBatch) => {
      const schema = new Schema(
        [new Field("doubled", new Int64(), false)],
        new Map([["source", "query-output"]]),
      );
      const result = new ArrowTable(
        schema,
        withEmptyBatch ? [new RecordBatch(schema, undefined)] : [],
      );
      const response = Buffer.from(tableToIPC(result, "stream"));
      let queryRequests = 0;

      await withMockDatabase(
        (req, res) => {
          if (req.url?.endsWith("/describe/")) {
            res.writeHead(200, { "Content-Type": "application/json" }).end(
              JSON.stringify({
                name: "items",
                version: 1,
                schema: {
                  fields: [
                    { name: "id", type: { type: "int64" }, nullable: false },
                  ],
                },
              }),
            );
          } else if (req.url?.endsWith("/query/")) {
            queryRequests++;
            req.resume();
            req.on("end", () => {
              res
                .writeHead(200, {
                  "Content-Type": "application/vnd.apache.arrow.stream",
                })
                .end(response);
            });
          } else {
            res.writeHead(404).end();
          }
        },
        async (db) => {
          const table = await db.openTable("items");
          const result = await table
            .query()
            .where("id < 0")
            .select({ doubled: "id * 2" })
            .toArrow();

          expect(result.numRows).toBe(0);
          expect(result.schema).toEqual(schema);
          expect(result.getChild("doubled")?.length).toBe(0);
        },
      );

      expect(queryRequests).toBe(1);
    },
  );

  it("rejects the external blob opt-in without blocking regular adds", async () => {
    let insertRequests = 0;
    const describeRequests: string[] = [];

    await withMockDatabase(
      (req, res) => {
        const requestPath = req.url ?? "";
        if (requestPath.endsWith("/describe/")) {
          describeRequests.push(requestPath);
          res.writeHead(200, { "Content-Type": "application/json" }).end(
            JSON.stringify({
              name: "items",
              version: 1,
              schema: {
                fields: [
                  {
                    name: "id",
                    type: { type: "int64" },
                    nullable: true,
                  },
                ],
              },
            }),
          );
          return;
        }
        if (requestPath.endsWith("/insert/")) {
          insertRequests++;
          req.resume();
          req.on("end", () => {
            res
              .writeHead(200, { "Content-Type": "application/json" })
              .end(JSON.stringify({ version: insertRequests + 1 }));
          });
          return;
        }
        res.writeHead(404).end();
      },
      async (db) => {
        const table = await db.openTable("items");

        await expect(
          table.add([{ id: 1n }], {
            allowExternalBlobOutsideBases: true,
          }),
        ).rejects.toThrow("only supported in LanceDB Embedded");
        expect(insertRequests).toBe(0);

        await expect(table.add([{ id: 2n }])).resolves.toMatchObject({
          version: 2,
        });
        await expect(
          table.add([{ id: 3n }], {
            allowExternalBlobOutsideBases: false,
          }),
        ).resolves.toMatchObject({ version: 3 });
      },
    );

    expect(describeRequests.length).toBeGreaterThan(0);
    expect(insertRequests).toBe(2);
  });

  it.each([
    { status: 429, retries: undefined, attempts: 2 },
    { status: 429, retries: 0, attempts: 1 },
    { status: 504, retries: undefined, attempts: 1 },
    { status: 504, retries: 0, attempts: 1 },
  ])("applies per-call add retries: %j", async (scenario) => {
    let attempts = 0;
    await withMockDatabase(
      (req, res) => {
        if (req.url?.endsWith("/describe/")) {
          res.writeHead(200).end(
            JSON.stringify({
              version: 1,
              schema: {
                fields: [
                  { name: "id", type: { type: "int64" }, nullable: true },
                ],
              },
            }),
          );
          return;
        }
        expect(req.url).toContain("/insert/");
        req.resume();
        req.on("end", () => {
          attempts++;
          res
            .writeHead(attempts === 1 ? scenario.status : 200)
            .end('{"version":2}');
        });
      },
      async (db) => {
        const table = await db.openTable("items");
        const result = table.add([{ id: 1n }], { retries: scenario.retries });
        if (scenario.attempts === 2) {
          await expect(result).resolves.toMatchObject({ version: 2 });
        } else {
          await expect(result).rejects.toThrow();
        }
        for (const retries of [-1, 256, 1.5]) {
          await expect(table.add([{ id: 2n }], { retries })).rejects.toThrow(
            "retries must be an integer between 0 and 255",
          );
        }
      },
      {
        clientConfig: {
          retryConfig: {
            retries: 3,
            statuses: [429, 504],
            backoffFactor: 0,
            backoffJitter: 0,
          },
        },
      },
    );
    expect(attempts).toBe(scenario.attempts);
  });

  it.each(
    [409, 429, 502, 503, 504].flatMap((status) =>
      [undefined, 0, 2].flatMap((retries) =>
        [[], ["analytics", "features"]].map((namespacePath) => ({
          status,
          retries,
          namespacePath,
        })),
      ),
    ),
  )("applies per-call merge retries: %j", async (scenario) => {
    let attempts = 0;
    const identifier = [...scenario.namespacePath, "items"].join("$");
    const succeeds =
      [409, 429].includes(scenario.status) && scenario.retries !== 0;
    await withMockDatabase(
      (req, res) => {
        const url = new URL(req.url ?? "", "http://localhost");
        if (url.pathname === `/v1/table/${identifier}/describe/`) {
          res.writeHead(200).end(
            JSON.stringify({
              version: 1,
              schema: {
                fields: [
                  { name: "id", type: { type: "int64" }, nullable: true },
                ],
              },
            }),
          );
          return;
        }
        const chunks: Buffer[] = [];
        req.on("data", (chunk) => chunks.push(chunk));
        req.on("end", () => {
          try {
            expect(url.pathname).toBe(`/v1/table/${identifier}/merge_insert/`);
            expect(url.searchParams.get("use_index")).toBe("false");
            expect(req.headers["x-request-timeout-ms"]).toBe("10000");
            // Rust compresses the batches; Arrow JS can inspect the schema without a codec.
            const reader = RecordBatchReader.from(Buffer.concat(chunks)).open();
            expect(reader.schema.fields.map((field) => field.name)).toEqual([
              "id",
            ]);
          } finally {
            attempts++;
            res
              .writeHead(attempts === 1 ? scenario.status : 200)
              .end('{"version":2}');
          }
        });
      },
      async (db) => {
        const table = await db.openTable("items", scenario.namespacePath);
        const merge = table
          .mergeInsert("id")
          .whenNotMatchedInsertAll()
          .useIndex(false);
        const result = merge.execute([{ id: 1n }], {
          timeoutMs: 10_000,
          retries: scenario.retries,
        });
        if (succeeds) {
          await expect(result).resolves.toMatchObject({ version: 2 });
        } else {
          await expect(result).rejects.toThrow();
        }
      },
      {
        clientConfig: {
          retryConfig: {
            retries: 3,
            statuses: [409, 429, 502, 503, 504],
            backoffFactor: 0,
            backoffJitter: 0,
          },
        },
      },
    );
    expect(attempts).toBe(succeeds ? 2 : 1);
  });

  it("isolates merge retry overrides across reused builders and reads", async () => {
    let attempts = 0;
    let readAttempts = 0;
    await withMockDatabase(
      (req, res) => {
        if (req.url?.endsWith("/describe/")) {
          res.writeHead(200).end(
            JSON.stringify({
              version: 1,
              schema: {
                fields: [
                  { name: "id", type: { type: "int64" }, nullable: true },
                ],
              },
            }),
          );
          return;
        }
        req.resume();
        req.on("end", () => {
          if (req.url?.endsWith("/count_rows/")) {
            readAttempts++;
            res.writeHead(readAttempts % 2 ? 429 : 200).end("1");
          } else {
            attempts++;
            res
              .writeHead([1, 2, 4, 6].includes(attempts) ? 429 : 200)
              .end('{"version":2}');
            expect(req.url).toContain("/merge_insert/");
          }
        });
      },
      async (db) => {
        const table = await db.openTable("items");
        const merge = table.mergeInsert("id").whenNotMatchedInsertAll();
        for (const retries of [
          -1,
          256,
          1.5,
          Number.NaN,
          Number.POSITIVE_INFINITY,
        ]) {
          await expect(
            merge.execute([{ id: 1n }], { retries }),
          ).rejects.toThrow("retries must be an integer between 0 and 255");
        }
        expect(attempts).toBe(0);
        await expect(
          merge.execute([{ id: 1n }], { retries: 0 }),
        ).rejects.toThrow();
        expect(attempts).toBe(1);
        await expect(merge.execute([{ id: 1n }])).resolves.toMatchObject({
          version: 2,
        });
        await expect(
          merge.execute([{ id: 1n }], { retries: 255 }),
        ).resolves.toMatchObject({ version: 2 });
        expect(attempts).toBe(5);
        await expect(table.countRows()).resolves.toBe(1);
        expect(readAttempts).toBe(2);
        await expect(
          table
            .mergeInsert("id")
            .whenNotMatchedInsertAll()
            .execute([{ id: 1n }]),
        ).resolves.toMatchObject({ version: 2 });
        expect(attempts).toBe(7);
      },
      {
        clientConfig: {
          retryConfig: { retries: 3, backoffFactor: 0, backoffJitter: 0 },
        },
      },
    );
  });

  it("should accept partial connection options", async () => {
    await connect("db://test", {
      apiKey: "fake",
      clientConfig: {
        timeoutConfig: { readTimeout: 5 },
        retryConfig: { retries: 2 },
      },
    });
  });

  it("should accept overall timeout configuration", async () => {
    await connect("db://test", {
      apiKey: "fake",
      clientConfig: {
        timeoutConfig: { timeout: 30 },
      },
    });

    // Test with all timeout parameters
    await connect("db://test", {
      apiKey: "fake",
      clientConfig: {
        timeoutConfig: {
          timeout: 60,
          connectTimeout: 10,
          readTimeout: 20,
          poolIdleTimeout: 300,
        },
      },
    });
  });

  it("should pass down apiKey and userAgent", async () => {
    await withMockDatabase(
      (req, res) => {
        expect(req.headers["x-api-key"]).toEqual("fake");
        expect(req.headers["user-agent"]).toEqual(
          `LanceDB-Node-Client/${packageJson.version}`,
        );

        const body = JSON.stringify({ tables: [] });
        res.writeHead(200, { "Content-Type": "application/json" }).end(body);
      },
      async (db) => {
        const tableNames = await db.tableNames();
        expect(tableNames).toEqual([]);
      },
    );
  });

  it("allows customizing user agent", async () => {
    await withMockDatabase(
      (req, res) => {
        expect(req.headers["user-agent"]).toEqual("MyApp/1.0");

        const body = JSON.stringify({ tables: [] });
        res.writeHead(200, { "Content-Type": "application/json" }).end(body);
      },
      async (db) => {
        const tableNames = await db.tableNames();
        expect(tableNames).toEqual([]);
      },
      {
        clientConfig: {
          userAgent: "MyApp/1.0",
        },
      },
    );
  });

  it("rejects a zero blob request concurrency limit", async () => {
    await expect(
      connect("db://dev", {
        apiKey: "fake",
        clientConfig: { blobRequestConcurrency: 0 },
      }),
    ).rejects.toThrow("blob_request_concurrency");
  });

  it("shows the full error messages on retry errors", async () => {
    await withMockDatabase(
      (_req, res) => {
        // We retry on 500 errors, so we return 500s until the client gives up.
        res.writeHead(500).end("Internal Server Error");
      },
      async (db) => {
        try {
          await db.tableNames();
          fail("expected an error");
          // biome-ignore lint/suspicious/noExplicitAny: skip
        } catch (e: any) {
          expect(e.message).toContain("Hit retry limit for request_id=");
          expect(e.message).toContain("Caused by: Http error");
          expect(e.message).toContain("500 Internal Server Error");
        }
      },
      {
        clientConfig: {
          retryConfig: { retries: 2 },
        },
      },
    );
  });

  it("lists the rows a Function refresh skipped", async () => {
    const bodies: unknown[] = [];
    await withMockDatabase(
      (req, res) => {
        const path = req.url ?? "";
        if (path.endsWith("/describe/")) {
          res.writeHead(200, { "Content-Type": "application/json" }).end(
            JSON.stringify({
              name: "docs",
              version: 1,
              schema: { fields: [] },
            }),
          );
          return;
        }
        if (path === "/v1/table/docs/errors") {
          let body = "";
          req.on("data", (chunk) => {
            body += chunk;
          });
          req.on("end", () => {
            bodies.push(JSON.parse(body));
            res.writeHead(200, { "Content-Type": "application/json" }).end(
              `{"records": [{"job_id": "j-7", "fragment_id": 3, "row_offset": 9,
                "column": "embedding", "function": "embed", "function_version": "2",
                "table_version": 11, "error_type": "ValueError",
                "error_message": "bad input 'x'", "created_at_millis": 1700000000000}],
                "fragments": [{"job_id": "j-7", "fragment_id": 4, "rows_skipped": 500,
                "rows_recorded": 100}], "truncated": true}`,
            );
          });
          return;
        }
        res.writeHead(404).end();
      },
      async (db) => {
        const table = await db.openTable("docs");
        const errors = await table.functionErrors({
          jobId: "j-7",
          column: "embedding",
          limit: 2,
        });
        expect(errors.truncated).toBe(true);
        expect(errors.records.map((r) => r.errorMessage)).toEqual([
          "bad input 'x'",
        ]);
        expect(errors.records[0].rowOffset).toBe(9);
        expect(errors.fragments[0].rowsSkipped).toBe(500);
        expect(errors.workflowVersion).toBeUndefined();
        expect(errors.errorGroups).toBeUndefined();
        await expect(
          table.functionErrorRows({ columns: ["text"] }),
        ).rejects.toThrow("Function row-failure workflow");
        await table.functionErrors({ history: false });
        await expect(table.functionErrors({ limit: -1 })).rejects.toThrow(
          "limit must be a non-negative integer",
        );
        for (const operation of [
          () => table.functionErrors({ history: true }),
          () => table.retryFailed("embedding"),
          () => table.upgradeColumn("embedding", { version: "2" }),
          () => table.quarantineFailed("embedding"),
          () => table.quarantineFailed("embedding", { quarantined: false }),
          () => table.functionProvenance("embedding"),
        ]) {
          await expect(operation()).rejects.toThrow(
            "Function row-failure workflow",
          );
        }
      },
    );
    expect(bodies).toEqual([
      JSON.parse('{"job_id": "j-7", "column": "embedding", "limit": 2}'),
      {},
      {},
      ...Array.from({ length: 6 }, () => ({ limit: 1 })),
    ]);
  });

  it("reads Function failure history and joins current inputs by exact row identity", async () => {
    const stableId = 9_007_199_254_740_993n;
    const physicalAddress = 9_007_203_549_708_297n;
    const currentRows = tableFromArrays({
      text: ["physical failure", "moved stable row", "unrelated row"],
      ["_rowid"]: new BigUint64Array([
        physicalAddress,
        stableId,
        stableId - 1n,
      ]),
      ["_rowaddr"]: new BigUint64Array([physicalAddress, 14n, 10n]),
    });
    const records = `[
      {"job_id":"j-7", "fragment_id":0, "row_offset":10,
       "row_id":${stableId}, "row_address":10, "column":"embedding",
       "function":"embed", "function_version":"2", "table_version":11,
       "error_type":"ValueError", "error_message":"bad input",
       "created_at_millis":1700000000001, "first_seen_at_millis":1700000000000,
       "last_seen_at_millis":1700000000001, "attempts":3, "quarantined":true},
      {"job_id":"j-7", "fragment_id":2097153, "row_offset":9,
       "row_address":${physicalAddress}, "column":"embedding",
       "function":"embed", "function_version":"2", "table_version":11,
       "error_type":"ValueError", "error_message":"bad input", "created_at_millis":1700000000001},
      {"job_id":"j-8", "fragment_id":0, "row_offset":10,
       "row_id":${stableId}, "row_address":10, "column":"score",
       "function":"score", "function_version":"1", "table_version":11,
       "error_type":"TypeError", "error_message":"bad input", "created_at_millis":1700000000001}
    ]`;
    const groups = `[
      {"column":"embedding", "error_type":"ValueError", "error_message":"bad input", "rows_failed":2},
      {"column":"score", "error_type":"TypeError", "error_message":"bad input", "rows_failed":1}
    ]`;
    const errorRequests: unknown[] = [];
    const queryRequests: unknown[] = [];
    await withMockDatabase(
      (req, res) => {
        let body = "";
        req.on("data", (chunk) => {
          body += chunk;
        });
        req.on("end", () => {
          if (req.url === "/v1/table/docs/describe/") {
            res.writeHead(200).end('{"version":12,"schema":{"fields":[]}}');
          } else if (req.url === "/v1/table/docs/errors") {
            const request = JSON.parse(body);
            errorRequests.push(request);
            const empty = request.column === "empty";
            const truncated = request.limit === 2;
            res.writeHead(200).end(`{
              "workflow_version":1, "records":${empty ? "[]" : records},
              "fragments":[], "truncated":${truncated},
              "error_groups":${empty ? "[]" : groups}
            }`);
          } else if (req.url === "/v1/table/docs/query/") {
            const query = JSON.parse(body);
            queryRequests.push(query);
            const rows =
              queryRequests.length === 1
                ? currentRows.slice(0, 2)
                : currentRows.slice(0, 1);
            res
              .writeHead(200, {
                "Content-Type": "application/vnd.apache.arrow.stream",
              })
              .end(Buffer.from(tableToIPC(rows, "stream")));
          } else {
            res.writeHead(404).end();
          }
        });
      },
      async (db) => {
        const table = await db.openTable("docs");
        const errors = await table.functionErrors();
        expect(errors.workflowVersion).toBe(1);
        expect(errors.records[0]).toMatchObject({
          rowId: stableId,
          rowAddress: 10n,
          attempts: 3,
          firstSeenAtMillis: 1_700_000_000_000,
          lastSeenAtMillis: 1_700_000_000_001,
          quarantined: true,
        });
        expect(errors.records[1].rowAddress).toBe(physicalAddress);
        expect(errors.errorGroups).toEqual([
          {
            column: "embedding",
            errorType: "ValueError",
            errorMessage: "bad input",
            rowsFailed: 2n,
          },
          {
            column: "score",
            errorType: "TypeError",
            errorMessage: "bad input",
            rowsFailed: 1n,
          },
        ]);
        const rows = await table.functionErrorRows({ columns: ["text"] });
        expect(rows.map(({ input, error }) => [input, error.column])).toEqual([
          [{ text: "moved stable row" }, "embedding"],
          [{ text: "physical failure" }, "embedding"],
          [{ text: "moved stable row" }, "score"],
        ]);
        await expect(
          table.functionErrorRows({ columns: ["text"] }),
        ).rejects.toThrow("no longer addressable");
        const history = await table.functionErrors({
          history: true,
          jobId: "j-7",
        });
        expect(history.records[0].rowId).toBe(stableId);
        await expect(
          table.functionErrorRows({ columns: ["text"], limit: 2 }),
        ).rejects.toThrow("Function errors are incomplete");
        await expect(
          table.functionErrorRows({ column: "empty" }),
        ).resolves.toEqual([]);
      },
    );
    expect(errorRequests).toEqual([
      {},
      {},
      {},
      { limit: 1 },
      { history: true, ["job_id"]: "j-7" },
      { limit: 2 },
      { column: "empty" },
    ]);
    expect(queryRequests).toHaveLength(2);
    for (const request of queryRequests) {
      expect(request).toMatchObject({
        columns: ["text", "_rowid", "_rowaddr"],
        filter: `_rowid IN (${stableId}) OR _rowaddr IN (${physicalAddress})`,
        k: 2,
      });
    }
  });

  it("forwards exact Function recovery requests and preserves async job results", async () => {
    const requests: [string, unknown][] = [];
    const stableId = 9_007_199_254_740_993n;
    await withMockDatabase(
      (req, res) => {
        let body = "";
        req.on("data", (chunk) => {
          body += chunk;
        });
        req.on("end", () => {
          const path = req.url ?? "";
          const request = body ? JSON.parse(body) : {};
          requests.push([path, request]);
          if (path === "/v1/table/docs/describe/") {
            res.writeHead(200).end('{"version":8,"schema":{"fields":[]}}');
          } else if (path === "/v1/table/docs/errors") {
            res
              .writeHead(200)
              .end(
                '{"workflow_version":1,"records":[],"fragments":[],"truncated":false}',
              );
          } else if (path === "/v1/table/docs/backfill_column") {
            res.writeHead(202).end('{"job_id":"refresh-1"}');
          } else if (path === "/v1/jobs/describe") {
            res.writeHead(200).end(`{
              "job_id":"refresh-1", "job_type":"function_refresh", "job_state":"DONE",
              "result":{"rows_assigned":19, "rows_failed":1, "rows_remaining":0,
                "source_version":7, "published_version":8, "error_groups_truncated":true,
                "error_groups":[{"column":"embedding", "error_type":"ValueError",
                                  "error_message":"bad input", "rows_failed":1}]}
            }`);
          } else if (path === "/v1/table/docs/errors/quarantine") {
            res.writeHead(200).end('{"rows_affected":2}');
          } else if (path === "/v1/table/docs/function_provenance") {
            res.writeHead(200).end(`{"truncated":true,"records":[{
              "column":"embedding", "job_id":"refresh-1", "function_name":"embed",
              "function_version":"2", "row_id":${stableId}, "row_address":null,
              "source_version":7, "published_version":8
            }]}`);
          } else {
            res.writeHead(404).end();
          }
        });
      },
      async (db) => {
        const table = await db.openTable("docs");
        const job = await table.retryFailed("embedding", {
          errorType: "ValueError",
          jobId: "first",
          includeQuarantined: true,
          functionVersion: "2",
          maxFailedFraction: 1,
        });
        expect(job.id).toBe("refresh-1");
        expect(await job.wait()).toEqual({
          rowsAssigned: 19n,
          rowsFailed: 1n,
          rowsRemaining: 0n,
          sourceVersion: 7n,
          publishedVersion: 8n,
          errorGroupsTruncated: true,
          errorGroups: [
            {
              column: "embedding",
              errorType: "ValueError",
              errorMessage: "bad input",
              rowsFailed: 1n,
            },
          ],
        });
        await table.retryFailed("embedding");
        await table.upgradeColumn("embedding", {
          version: "3",
          recompute: "failed",
        });
        await table.upgradeColumn("embedding", { version: "4" });
        await table.upgradeColumn("embedding", {
          version: "5",
          recompute: "none",
        });
        await expect(
          table.quarantineFailed("embedding", {
            jobId: "first",
            afterAttempts: 3,
            errorType: "ValueError",
          }),
        ).resolves.toBe(2);
        await expect(
          table.quarantineFailed("embedding", { quarantined: false }),
        ).resolves.toBe(2);
        const provenance = await table.functionProvenance("embedding", {
          version: 8n,
          limit: 1,
        });
        expect(provenance.truncated).toBe(true);
        expect(provenance.records[0]).toMatchObject({
          jobId: "refresh-1",
          functionName: "embed",
          functionVersion: "2",
          rowId: stableId,
          sourceVersion: 7n,
          publishedVersion: 8n,
        });
        await expect(
          table.upgradeColumn("embedding", { version: "" }),
        ).rejects.toThrow("nonempty function_version");
        await expect(
          table.quarantineFailed("embedding", { afterAttempts: 2 ** 32 + 1 }),
        ).rejects.toThrow("afterAttempts");
        await expect(
          table.functionProvenance("embedding", { version: -(2n ** 64n) }),
        ).rejects.toThrow("positive lossless u64 bigint");
        const ordinary = await table.refreshColumnAsync("embedding");
        await expect(ordinary.wait()).resolves.toBeUndefined();
      },
    );
    const probe = ["/v1/table/docs/errors", { limit: 1 }];
    expect(requests).toEqual([
      ["/v1/table/docs/describe/", {}],
      probe,
      [
        "/v1/table/docs/backfill_column",
        JSON.parse(`{
        "column":"embedding", "retry_errors":true, "retry_job_id":"first",
        "retry_error_type":"ValueError", "include_quarantined":true,
        "function_version":"2", "recompute":"failed", "max_failed_fraction":1
      }`),
      ],
      ["/v1/jobs/describe", { ["job_id"]: "refresh-1" }],
      probe,
      [
        "/v1/table/docs/backfill_column",
        { column: "embedding", ["retry_errors"]: true },
      ],
      ...[
        ["3", "failed"],
        ["4", "all"],
        ["5", "none"],
      ].flatMap(([version, recompute]) => [
        probe,
        [
          "/v1/table/docs/backfill_column",
          { column: "embedding", ["function_version"]: version, recompute },
        ],
      ]),
      probe,
      [
        "/v1/table/docs/errors/quarantine",
        {
          column: "embedding",
          quarantined: true,
          ["job_id"]: "first",
          ["after_attempts"]: 3,
          ["error_type"]: "ValueError",
        },
      ],
      probe,
      [
        "/v1/table/docs/errors/quarantine",
        {
          column: "embedding",
          quarantined: false,
          ["job_id"]: null,
          ["after_attempts"]: null,
          ["error_type"]: null,
        },
      ],
      probe,
      [
        "/v1/table/docs/function_provenance",
        { column: "embedding", version: 8, limit: 1 },
      ],
      ["/v1/table/docs/backfill_column", { column: "embedding" }],
      ["/v1/jobs/describe", { ["job_id"]: "refresh-1" }],
    ]);
  });

  it("passes a pinned version and decodes Function scheduler status", async () => {
    const queries: Record<string, string>[] = [];
    await withMockDatabase(
      (req, res) => {
        const url = new URL(req.url ?? "/", "http://localhost");
        if (url.pathname.endsWith("/describe/")) {
          res.writeHead(200, { "Content-Type": "application/json" }).end(
            JSON.stringify({
              name: "docs",
              version: 42,
              schema: { fields: [] },
            }),
          );
          return;
        }
        if (url.pathname === "/v1/table/docs/function_columns/status") {
          queries.push(Object.fromEntries(url.searchParams.entries()));
          res.writeHead(200, { "Content-Type": "application/json" }).end(
            `[{
              "column":"embedding",
              "binding_id":"fb_01",
              "function":{
                "name":"embed",
                "object_id":"embed",
                "location":"objects/embed",
                "version":"7",
                "manifest_digest":"sha256:abc"
              },
              "table_version":42,
              "rows":4,
              "fresh":2,
              "stale":2,
              "never_computed":0,
              "failed":0,
              "last_refresh_job":{
                "job_id":"job-cancelled",
                "state":"cancelled",
                "automatic":true,
                "completed_at_millis":1790000002000
              },
              "last_failure_job":{
                "job_id":"job-failed",
                "state":"failed",
                "automatic":true,
                "completed_at_millis":1790000001000,
                "failure":{
                  "phase":"finalize",
                  "message":"worker timed out",
                  "traceback":"Traceback: timeout",
                  "retryable":true,
                  "sql_job_id":"sql-job-7"
                }
              },
              "auto_refresh":{
                "enabled":true,
                "interval_seconds":60,
                "active_job_id":"job-queued",
                "state":"queued",
                "failure_streak":1,
                "next_run_at_millis":null,
                "next_run_at_reason":"job_queued_not_started"
              }
            }]`,
          );
          return;
        }
        res.writeHead(404).end();
      },
      async (db) => {
        const table = await db.openTable("docs");
        await table.checkout(42);
        const status = await table.functionColumnStatus("embedding");
        expect(status.tableVersion).toBe(42n);
        expect(status.autoRefresh.state).toBe("queued");
        expect(status.autoRefresh.activeJobId).toBe("job-queued");
        expect(status.autoRefresh.failureStreak).toBe(1);
        expect(status.autoRefresh.nextRunAtReason).toBe(
          "job_queued_not_started",
        );
        expect(status.lastRefreshJob?.state).toBe("cancelled");
        expect(status.lastRefreshJob?.failure).toBeUndefined();
        expect(status.lastFailureJob?.jobId).toBe("job-failed");
        expect(status.lastFailureJob?.failure).toEqual({
          phase: "finalize",
          message: "worker timed out",
          traceback: "Traceback: timeout",
          retryable: true,
          sqlJobId: "sql-job-7",
        });
      },
    );
    expect(queries).toEqual([{ column: "embedding", version: "42" }]);
  });

  it("forwards refresh options to the backfill request", async () => {
    const bodies: unknown[] = [];
    await withMockDatabase(
      (req, res) => {
        const path = req.url ?? "";
        if (path.endsWith("/describe/")) {
          res.writeHead(200, { "Content-Type": "application/json" }).end(
            JSON.stringify({
              name: "docs",
              version: 1,
              schema: { fields: [] },
            }),
          );
          return;
        }
        if (path === "/v1/table/docs/backfill_column") {
          let body = "";
          req.on("data", (chunk) => {
            body += chunk;
          });
          req.on("end", () => {
            bodies.push(JSON.parse(body));
            res
              .writeHead(202, { "Content-Type": "application/json" })
              .end('{"job_id": "j-9"}');
          });
          return;
        }
        res.writeHead(404).end();
      },
      async (db) => {
        const table = await db.openTable("docs");
        const job = await table.refreshColumnAsync("embedding", {
          where: "id % 2 = 0",
          batchSize: 16,
          publicationIntervalRows: 60,
          maxConcurrency: 3,
          gpuClass: "a10g",
          totalGpuCount: 2,
          errorPolicy: "skip",
          maxFailedFraction: 0.01,
          maxFailedRows: 50,
          rowTimeoutSeconds: 30,
          initTimeoutSeconds: 120,
          maxExecutorProcessesPerWorker: 2,
        });
        expect(job.id).toBe("j-9");
        await table.refreshColumnAsync("embedding");
        await table.refreshColumnAsync("embedding", {
          publicationIntervalRows: undefined,
        });
        for (const publicationIntervalRows of [
          1,
          2 ** 32 + 1,
          Number.MAX_SAFE_INTEGER,
        ]) {
          await table.refreshColumnAsync("embedding", {
            publicationIntervalRows,
          });
        }
        await table.refreshColumnAsync("embedding", {
          totalCpuMillis: 8_000,
          totalMemoryBytes: 16 * 1024 ** 3,
        });
        await expect(
          table.refreshColumnAsync("embedding", {
            maxConcurrency: true as unknown as number,
          }),
        ).rejects.toThrow();
        for (const publicationIntervalRows of [true, "60", 60n]) {
          await expect(
            table.refreshColumnAsync("embedding", {
              publicationIntervalRows:
                publicationIntervalRows as unknown as number,
            }),
          ).rejects.toThrow();
        }
        // Invalid options reject before any request is sent.
        for (const [options, message] of [
          [{ maxFailedRows: -1 }, "maxFailedRows"],
          [{ errorPolicy: "retry" }, "errorPolicy"],
          [{ where: " " }, "where"],
          [{ maxFailedFraction: 2 }, "max_failed_fraction"],
          [{ rowTimeoutSeconds: 0 }, "row_timeout_seconds"],
          [{ publicationIntervalRows: 0 }, "publicationIntervalRows"],
          [{ publicationIntervalRows: -1 }, "publicationIntervalRows"],
          [{ publicationIntervalRows: 1.5 }, "publicationIntervalRows"],
          [{ publicationIntervalRows: Number.NaN }, "publicationIntervalRows"],
          [
            { publicationIntervalRows: Number.POSITIVE_INFINITY },
            "publicationIntervalRows",
          ],
          [
            { publicationIntervalRows: Number.NEGATIVE_INFINITY },
            "publicationIntervalRows",
          ],
          [
            { publicationIntervalRows: Number.MAX_SAFE_INTEGER + 1 },
            "publicationIntervalRows",
          ],
          [{ maxConcurrency: 1.5 }, "maxConcurrency"],
          [{ maxConcurrency: 0 }, "maxConcurrency"],
          [{ maxConcurrency: -1 }, "maxConcurrency"],
          [{ maxConcurrency: 2 ** 32 }, "maxConcurrency"],
          [{ maxConcurrency: Number.NaN }, "maxConcurrency"],
          [{ maxConcurrency: Number.POSITIVE_INFINITY }, "maxConcurrency"],
          [{ numWorkers: 0 }, "num_workers"],

          [
            {
              totalCpuMillis: 0,
              totalMemoryBytes: 1_024,
            },
            "totalCpuMillis",
          ],
          [
            {
              totalCpuMillis: 1_000,
              totalMemoryBytes: 1_024,
              totalGpuCount: 1.5,
            },
            "totalGpuCount",
          ],
          [
            {
              totalCpuMillis: 1_000,
              totalMemoryBytes: 1_024,
              totalGpuCount: -1,
            },
            "totalGpuCount",
          ],
          [
            {
              totalCpuMillis: 1_000,
              totalMemoryBytes: 1_024,
              totalGpuCount: 2 ** 32,
            },
            "totalGpuCount",
          ],
          [
            {
              totalCpuMillis: 1_000,
              totalMemoryBytes: 1_024,
              totalGpuCount: Number.NaN,
            },
            "totalGpuCount",
          ],
          [
            {
              totalCpuMillis: 1_000,
              totalMemoryBytes: 1_024,
              totalGpuCount: Number.POSITIVE_INFINITY,
            },
            "totalGpuCount",
          ],
          [
            {
              supervisorResources: {
                cpuMillis: 0,
                memoryBytes: 1_024,
                ephemeralStorageBytes: 2_048,
              },
            },
            "supervisorResources.cpuMillis",
          ],
          [
            {
              supervisorResources: {
                cpuMillis: 1_000,
                memoryBytes: 1_024,
                ephemeralStorageBytes: 0,
              },
            },
            "supervisorResources.ephemeralStorageBytes",
          ],
          [
            {
              planningWorkers: {
                resources: {
                  cpuMillis: 1_000,
                  memoryBytes: 1_024,
                  ephemeralStorageBytes: 2_048,
                },
                maxWorkers: 0,
                rowsPerWorker: 100,
                fragmentsPerWorker: 1,
              },
            },
            "planningWorkers.maxWorkers",
          ],
          [{ retryPolicy: { maxAttempts: 0 } }, "retry_policy.max_attempts"],
        ] as const) {
          await expect(
            table.refreshColumnAsync("embedding", options),
          ).rejects.toThrow(message);
        }
      },
    );
    expect(bodies).toEqual([
      JSON.parse(
        `{"column": "embedding", "where": "id % 2 = 0", "batch_size": 16,
          "publication_interval_rows": 60,
          "max_concurrency": 3, "gpu_class": "a10g", "total_gpu_count": 2,
          "error_policy": "skip",
          "max_failed_fraction": 0.01, "max_failed_rows": 50,
          "row_timeout_seconds": 30, "init_timeout_seconds": 120,
          "max_executor_processes_per_worker": 2}`,
      ),
      { column: "embedding" },
      { column: "embedding" },
      JSON.parse('{"column": "embedding", "publication_interval_rows": 1}'),
      JSON.parse(
        '{"column": "embedding", "publication_interval_rows": 4294967297}',
      ),
      JSON.parse(
        '{"column": "embedding", "publication_interval_rows": 9007199254740991}',
      ),
      JSON.parse(
        '{"column":"embedding","total_cpu_millis":8000,"total_memory_bytes":17179869184}',
      ),
    ]);
  });

  it("forwards structured Function refresh options and Node retry policy", async () => {
    const bodies: unknown[] = [];
    await withMockDatabase(
      (req, res) => {
        const path = req.url ?? "";
        if (path.endsWith("/describe/")) {
          res.writeHead(200, { "Content-Type": "application/json" }).end(
            JSON.stringify({
              name: "docs",
              version: 1,
              schema: { fields: [] },
            }),
          );
          return;
        }
        if (path === "/v1/table/docs/backfill_column") {
          let body = "";
          req.on("data", (chunk) => {
            body += chunk;
          });
          req.on("end", () => {
            bodies.push(JSON.parse(body));
            res
              .writeHead(202, { "Content-Type": "application/json" })
              .end('{"job_id": "j-structured"}');
          });
          return;
        }
        res.writeHead(404).end();
      },
      async (db) => {
        const table = await db.openTable("docs");
        const job = await table.refreshColumnAsync("embedding", {
          supervisorResources: {
            cpuMillis: 2_000,
            memoryBytes: 4 * 1024 ** 3,
            ephemeralStorageBytes: 10 * 1024 ** 3,
          },
          planningWorkers: {
            resources: {
              cpuMillis: 1_000,
              memoryBytes: 2 * 1024 ** 3,
              ephemeralStorageBytes: 5 * 1024 ** 3,
            },
            maxWorkers: 4,
            rowsPerWorker: 100_000,
            fragmentsPerWorker: 8,
          },
          retryPolicy: { transient: true },
        });
        expect(job.id).toBe("j-structured");
      },
    );
    expect(bodies).toEqual([
      {
        column: "embedding",
        ["supervisor_resources"]: {
          ["cpu_millis"]: 2_000,
          ["memory_bytes"]: 4 * 1024 ** 3,
          ["ephemeral_storage_bytes"]: 10 * 1024 ** 3,
        },
        ["planning_workers"]: {
          resources: {
            ["cpu_millis"]: 1_000,
            ["memory_bytes"]: 2 * 1024 ** 3,
            ["ephemeral_storage_bytes"]: 5 * 1024 ** 3,
          },
          ["max_workers"]: 4,
          ["rows_per_worker"]: 100_000,
          ["fragments_per_worker"]: 8,
        },
        ["retry_policy"]: {
          ["max_attempts"]: 3,
          ["initial_backoff_seconds"]: 0.2,
          ["max_backoff_seconds"]: 30,
          ["exception_types"]: [],
          ["message_pattern"]: null,
          ["error_codes"]: [],
          transient: true,
          ["honor_retry_after"]: true,
        },
      },
    ]);
  });

  it("surfaces JSON server errors from remote table operations", async () => {
    await withMockDatabase(
      (req, res) => {
        const path = req.url ?? "";
        if (path.endsWith("/describe/")) {
          res.writeHead(200, { "Content-Type": "application/json" }).end(
            JSON.stringify({
              name: "broken_table",
              version: 1,
              schema: { fields: [] },
            }),
          );
          return;
        }

        if (path.endsWith("/count_rows/")) {
          res
            .writeHead(400, { "Content-Type": "application/json" })
            .end(JSON.stringify({ error: "count rows failed" }));
          return;
        }

        res.writeHead(404).end();
      },
      async (db) => {
        const table = await db.openTable("broken_table");

        await expect(table.countRows()).rejects.toThrow("count rows failed");
      },
    );
  });

  it("should pass on requested extra headers", async () => {
    await withMockDatabase(
      (req, res) => {
        expect(req.headers["x-my-header"]).toEqual("my-value");

        const body = JSON.stringify({ tables: [] });
        res.writeHead(200, { "Content-Type": "application/json" }).end(body);
      },
      async (db) => {
        const tableNames = await db.tableNames();
        expect(tableNames).toEqual([]);
      },
      {
        clientConfig: {
          extraHeaders: {
            "x-my-header": "my-value",
          },
        },
      },
    );
  });

  it("supports version time-travel and branches on remote", async () => {
    await withMockDatabase(
      (req, res) => {
        const body = req.url?.includes("/branches/list")
          ? JSON.stringify({
              branches: {
                exp: { parentVersion: 1, createAt: 1, manifestSize: 1 },
              },
            })
          : JSON.stringify({ name: "t", version: 2, schema: { fields: [] } });
        res.writeHead(200, { "Content-Type": "application/json" }).end(body);
      },
      async (db) => {
        // version-only (and "main" + version) time-travel the main chain
        const v2 = await db.openTable("t", undefined, { version: 2 });
        expect(v2.currentBranch()).toBeNull();
        const mainV2 = await db.openTable("t", undefined, {
          branch: "main",
          version: 2,
        });
        expect(mainV2.currentBranch()).toBeNull();

        // a non-main branch opens a handle scoped to that branch
        const exp = await db.openTable("t", undefined, { branch: "exp" });
        expect(exp.currentBranch()).toBe("exp");
        const expV2 = await db.openTable("t", undefined, {
          branch: "exp",
          version: 2,
        });
        expect(expV2.currentBranch()).toBe("exp");
      },
    );
  });

  it("forks Enterprise branches from tags and defaults tag versions", async () => {
    const requests: { path: string; body: Record<string, unknown> }[] = [];
    await withMockDatabase(
      (req, res) => {
        const path = req.url ?? "";
        if (path.endsWith("/branches/list/")) {
          res.writeHead(200, { "Content-Type": "application/json" }).end(
            JSON.stringify({
              branches: {
                exp: { parentVersion: 1, createAt: 1, manifestSize: 1 },
              },
            }),
          );
          return;
        }
        if (path.endsWith("/describe/")) {
          res.writeHead(200, { "Content-Type": "application/json" }).end(
            JSON.stringify({
              name: "t",
              version: 2,
              schema: { fields: [] },
            }),
          );
          return;
        }

        let raw = "";
        req.on("data", (chunk) => {
          raw += chunk;
        });
        req.on("end", () => {
          const body = raw ? JSON.parse(raw) : {};
          requests.push({ path, body });
          if (path.endsWith("/tags/version/")) {
            res
              .writeHead(200, { "Content-Type": "application/json" })
              .end(JSON.stringify({ version: 3, branch: "base" }));
            return;
          }
          if (path.endsWith("/tags/create/")) {
            res
              .writeHead(200, { "Content-Type": "application/json" })
              .end("{}");
            return;
          }
          if (path.endsWith("/branches/create/")) {
            if (body["name"] === "missing") {
              res
                .writeHead(404, { "Content-Type": "application/json" })
                .end(JSON.stringify({ message: "not found" }));
              return;
            }
            res
              .writeHead(200, { "Content-Type": "application/json" })
              .end("{}");
            return;
          }
          res.writeHead(404).end();
        });
      },
      async (db) => {
        const table = await db.openTable("t", undefined, { branch: "exp" });
        await (await table.tags()).create("baseline");

        const branches = await table.branches();
        const fromTag = await branches.create("from-tag", {
          fromTag: "baseline",
        });
        expect(fromTag.currentBranch()).toBe("from-tag");

        // An Enterprise 404 is an ambiguous table-or-source failure, so the
        // Embedded branch hint must not be appended based on its message.
        const missing = await branches
          .create("missing", { fromTag: "baseline" })
          .then(
            () => undefined,
            (error: Error) => error,
          );
        expect(missing).toBeDefined();
        expect(missing?.message).not.toContain("fromTag");
      },
    );

    // Omitting the tag version resolves the handle's visible branch version.
    expect(requests).toContainEqual({
      path: "/v1/table/t/tags/create/",
      body: {
        tag: "baseline",
        version: 2,
        branch: "exp",
      },
    });
    // A tag source uses the existing coordinate lookup and the normal
    // branches/create body; there is no from_tag field on the wire.
    expect(requests).toContainEqual({
      path: "/v1/table/t/tags/version/",
      body: { tag: "baseline" },
    });
    expect(requests).toContainEqual({
      path: "/v1/table/t/branches/create/",
      // biome-ignore lint/style/useNamingConvention: snake_case is the server wire format
      body: { name: "from-tag", from_branch: "base", from_version: 3 },
    });
  });

  it("sends FTS options to remote tables", async () => {
    let createIndexBody: Record<string, unknown> | undefined;

    await withMockDatabase(
      (req, res) => {
        const path = req.url ?? "";
        if (path.endsWith("/describe/")) {
          res.writeHead(200, { "Content-Type": "application/json" }).end(
            JSON.stringify({
              name: "t",
              version: 1,
              schema: {
                fields: [
                  { name: "text", type: { type: "string" }, nullable: false },
                ],
              },
            }),
          );
          return;
        }

        if (path.endsWith("/create_index/")) {
          let raw = "";
          req.on("data", (chunk) => {
            raw += chunk;
          });
          req.on("end", () => {
            createIndexBody = JSON.parse(raw);
            res.writeHead(200).end();
          });
          return;
        }

        res.writeHead(404).end();
      },
      async (db) => {
        const table = await db.openTable("t");
        await table.createIndex("text", {
          config: Index.fts({
            blockSize: 256,
            removeStopWords: true,
            customStopWords: ["the"],
          }),
        });
      },
    );

    expect(createIndexBody?.["column"]).toBe("text");
    expect(createIndexBody?.["index_type"]).toBe("FTS");
    expect(createIndexBody?.["block_size"]).toBe(256);
    expect(createIndexBody?.["custom_stop_words"]).toEqual(["the"]);
  });

  it("sends RTree options to remote tables", async () => {
    const createIndexBodies: Record<string, unknown>[] = [];

    await withMockDatabase(
      (req, res) => {
        const path = req.url ?? "";
        if (path.endsWith("/describe/")) {
          res.writeHead(200, { "Content-Type": "application/json" }).end(
            JSON.stringify({
              name: "t",
              version: 1,
              schema: {
                fields: [
                  {
                    name: "geom",
                    type: {
                      type: "struct",
                      fields: [
                        {
                          name: "x",
                          type: { type: "double" },
                          nullable: false,
                        },
                        {
                          name: "y",
                          type: { type: "double" },
                          nullable: false,
                        },
                      ],
                    },
                    nullable: true,
                    metadata: { "ARROW:extension:name": "geoarrow.point" },
                  },
                ],
              },
            }),
          );
          return;
        }

        if (path.endsWith("/create_index/")) {
          let raw = "";
          req.on("data", (chunk) => {
            raw += chunk;
          });
          req.on("end", () => {
            createIndexBodies.push(JSON.parse(raw));
            res.writeHead(200).end();
          });
          return;
        }

        res.writeHead(404).end();
      },
      async (db) => {
        const table = await db.openTable("t");
        await table.createIndex("geom", { config: Index.rtree() });
        await table.createIndex("geom", {
          config: Index.rtree({ pageSize: 1024 }),
          name: "geom_rtree",
        });
      },
    );

    expect(createIndexBodies).toEqual([
      {
        column: "geom",
        // biome-ignore lint/style/useNamingConvention: server wire format
        index_type: "RTREE",
      },
      {
        column: "geom",
        // biome-ignore lint/style/useNamingConvention: server wire format
        index_type: "RTREE",
        name: "geom_rtree",
        // biome-ignore lint/style/useNamingConvention: server wire format
        page_size: 1024,
      },
    ]);
  });

  it("uses an explicit type and nested params for remote scalar plugin indexes", async () => {
    const createIndexBodies: Record<string, unknown>[] = [];

    await withMockDatabase(
      (req, res) => {
        const path = req.url ?? "";
        if (path.endsWith("/describe/")) {
          res.writeHead(200, { "Content-Type": "application/json" }).end(
            JSON.stringify({
              name: "t",
              version: 1,
              schema: {
                fields: [
                  { name: "id", type: { type: "int32" }, nullable: false },
                ],
              },
            }),
          );
          return;
        }

        if (path.endsWith("/create_index/")) {
          let raw = "";
          req.on("data", (chunk) => {
            raw += chunk;
          });
          req.on("end", () => {
            createIndexBodies.push(JSON.parse(raw));
            res.writeHead(200).end();
          });
          return;
        }

        res.writeHead(404).end();
      },
      async (db) => {
        const table = await db.openTable("t");
        await table.createIndex("id", { config: Index.plugin(" ZoneMap ") });
        await table.createIndex("id", {
          config: Index.plugin("bloomfilter", {
            // biome-ignore lint/style/useNamingConvention: plugin param name
            number_of_items: 4096,
            column: "not_id",
          }),
          name: "id_bloom",
        });
        await table.createIndex("id", {
          config: Index.plugin("bloomfilter", '{"probability": 0.01}'),
        });
        await table.createIndex("id", { config: Index.plugin("zonemap", {}) });
      },
    );

    expect(createIndexBodies).toEqual([
      // biome-ignore lint/style/useNamingConvention: wire format
      { column: "id", index_type: "PLUGIN", plugin_type: "ZONEMAP" },
      {
        column: "id",
        // biome-ignore lint/style/useNamingConvention: wire format
        index_type: "PLUGIN",
        // biome-ignore lint/style/useNamingConvention: wire format
        plugin_type: "BLOOMFILTER",
        name: "id_bloom",
        // biome-ignore lint/style/useNamingConvention: plugin param name
        params: { number_of_items: 4096, column: "not_id" },
      },
      {
        column: "id",
        // biome-ignore lint/style/useNamingConvention: wire format
        index_type: "PLUGIN",
        // biome-ignore lint/style/useNamingConvention: wire format
        plugin_type: "BLOOMFILTER",
        params: { probability: 0.01 },
      },
      {
        column: "id",
        // biome-ignore lint/style/useNamingConvention: wire format
        index_type: "PLUGIN",
        // biome-ignore lint/style/useNamingConvention: wire format
        plugin_type: "ZONEMAP",
        params: {},
      },
    ]);
  });

  it("diffs and cherry-picks remote branches", async () => {
    const sampleDiff = {
      fromBranch: "exp",
      parentVersion: 1,
      mainVersion: 2,
      branchVersion: 3,
      baseMoved: false,
      rowCountMain: 3,
      rowCountBranch: 3,
      rowSummary: {
        unchanged: 3,
        newOnBase: 0,
        newOnBranch: 0,
        staleRecompute: 0,
        inputsChanged: 0,
        deltaAvailable: false,
      },
      addedColumns: [{ name: "tag", dataType: "utf8", nullable: true }],
      removedColumns: [],
      changedColumns: [],
      addedIndexes: [],
      removedIndexes: [],
      errors: [],
    };
    const cherryPickBodies: Record<string, unknown>[] = [];

    await withMockDatabase(
      (req, res) => {
        const path = req.url ?? "";
        if (path.endsWith("/describe/")) {
          res.writeHead(200, { "Content-Type": "application/json" }).end(
            JSON.stringify({
              name: "t",
              version: 2,
              schema: { fields: [] },
            }),
          );
          return;
        }

        let raw = "";
        req.on("data", (chunk) => {
          raw += chunk;
        });
        req.on("end", () => {
          const body = raw ? JSON.parse(raw) : {};
          if (path.endsWith("/branches/diff/")) {
            // biome-ignore lint/style/useNamingConvention: snake_case mandated by the server wire format
            expect(body).toEqual({ from_branch: "exp" });
            res
              .writeHead(200, { "Content-Type": "application/json" })
              .end(JSON.stringify(sampleDiff));
            return;
          }
          if (path.endsWith("/branches/cherry_pick/")) {
            cherryPickBodies.push(body);
            const dryRun = body["dry_run"] === true;
            const response = {
              status: dryRun ? "ready" : "failed",
              diff: dryRun
                ? sampleDiff
                : {
                    ...sampleDiff,
                    errors: [
                      { code: "baseMoved", message: "main has advanced" },
                    ],
                  },
              preview: { promotedColumns: dryRun ? ["tag"] : [] },
            };
            res
              .writeHead(dryRun ? 200 : 409, {
                "Content-Type": "application/json",
              })
              .end(JSON.stringify(response));
            return;
          }
          res.writeHead(404).end();
        });
      },
      async (db) => {
        const table = await db.openTable("t");
        const branches = await table.branches();

        await expect(branches.diff("exp")).resolves.toEqual(sampleDiff);

        const failed = await branches.cherryPick("exp");
        expect(failed.status).toBe("failed");
        expect(failed.diff.errors).toEqual([
          { code: "baseMoved", message: "main has advanced" },
        ]);

        const preview = await branches.cherryPick("exp", true);
        expect(preview.status).toBe("ready");
        expect(preview.preview.promotedColumns).toEqual(["tag"]);
      },
    );

    expect(cherryPickBodies).toEqual([
      // biome-ignore lint/style/useNamingConvention: snake_case mandated by the server wire format
      { from_branch: "exp", dry_run: false },
      // biome-ignore lint/style/useNamingConvention: snake_case mandated by the server wire format
      { from_branch: "exp", dry_run: true },
    ]);
  });

  describe("OAuthConfig", () => {
    it("should expose client auth method values", () => {
      expect(ClientAuthMethod.None).toBe("none");
      expect(ClientAuthMethod.ClientSecretBasic).toBe("client_secret_basic");
      expect(ClientAuthMethod.ClientSecretPost).toBe("client_secret_post");
    });

    it("should accept a confidential client with basic auth", () => {
      const config: OAuthConfig = {
        issuerUrl: "https://issuer.example.com",
        clientId: "client-id",
        clientSecret: "secret",
        scopes: ["openid"],
        flow: OAuthFlowType.AuthorizationCode,
        clientAuthMethod: ClientAuthMethod.ClientSecretBasic,
      };

      expect(config.clientAuthMethod).toBe(ClientAuthMethod.ClientSecretBasic);
    });

    it("should accept a public PKCE client without auth method or secret", () => {
      const config: OAuthConfig = {
        issuerUrl: "https://issuer.example.com",
        clientId: "client-id",
        scopes: ["openid"],
        flow: OAuthFlowType.AuthorizationCode,
        usePkce: true,
      };

      expect(config.clientSecret).toBeUndefined();
      expect(config.clientAuthMethod).toBeUndefined();
    });
  });

  describe("TlsConfig", () => {
    it("should create TlsConfig with all fields", () => {
      const tlsConfig: TlsConfig = {
        certFile: "/path/to/cert.pem",
        keyFile: "/path/to/key.pem",
        sslCaCert: "/path/to/ca.pem",
        assertHostname: false,
      };

      expect(tlsConfig.certFile).toBe("/path/to/cert.pem");
      expect(tlsConfig.keyFile).toBe("/path/to/key.pem");
      expect(tlsConfig.sslCaCert).toBe("/path/to/ca.pem");
      expect(tlsConfig.assertHostname).toBe(false);
    });

    it("should create TlsConfig with partial fields", () => {
      const tlsConfig: TlsConfig = {
        certFile: "/path/to/cert.pem",
        keyFile: "/path/to/key.pem",
      };

      expect(tlsConfig.certFile).toBe("/path/to/cert.pem");
      expect(tlsConfig.keyFile).toBe("/path/to/key.pem");
      expect(tlsConfig.sslCaCert).toBeUndefined();
      expect(tlsConfig.assertHostname).toBeUndefined();
    });

    it("should create ClientConfig with TlsConfig", () => {
      const tlsConfig: TlsConfig = {
        certFile: "/path/to/cert.pem",
        keyFile: "/path/to/key.pem",
        sslCaCert: "/path/to/ca.pem",
        assertHostname: true,
      };

      const clientConfig: ClientConfig = {
        userAgent: "test-agent",
        tlsConfig: tlsConfig,
      };

      expect(clientConfig.userAgent).toBe("test-agent");
      expect(clientConfig.tlsConfig).toBeDefined();
      expect(clientConfig.tlsConfig?.certFile).toBe("/path/to/cert.pem");
      expect(clientConfig.tlsConfig?.keyFile).toBe("/path/to/key.pem");
      expect(clientConfig.tlsConfig?.sslCaCert).toBe("/path/to/ca.pem");
      expect(clientConfig.tlsConfig?.assertHostname).toBe(true);
    });

    it("should handle empty TlsConfig", () => {
      const tlsConfig: TlsConfig = {};

      expect(tlsConfig.certFile).toBeUndefined();
      expect(tlsConfig.keyFile).toBeUndefined();
      expect(tlsConfig.sslCaCert).toBeUndefined();
      expect(tlsConfig.assertHostname).toBeUndefined();
    });

    it("should accept TlsConfig in connection options", () => {
      const tlsConfig: TlsConfig = {
        certFile: "/path/to/cert.pem",
        keyFile: "/path/to/key.pem",
        sslCaCert: "/path/to/ca.pem",
        assertHostname: false,
      };

      // Just verify that the ClientConfig accepts the TlsConfig
      const clientConfig: ClientConfig = {
        tlsConfig: tlsConfig,
      };

      const connectionOptions: ConnectionOptions = {
        apiKey: "fake",
        clientConfig: clientConfig,
      };

      // Verify the configuration structure is correct
      expect(connectionOptions.clientConfig).toBeDefined();
      expect(connectionOptions.clientConfig?.tlsConfig).toBeDefined();
      expect(connectionOptions.clientConfig?.tlsConfig?.certFile).toBe(
        "/path/to/cert.pem",
      );
    });
  });

  describe("header providers", () => {
    it("should work with StaticHeaderProvider", async () => {
      const provider = new StaticHeaderProvider({
        authorization: "Bearer test-token",
        "X-Custom": "value",
      });

      const headers = provider.getHeaders();
      expect(headers).toEqual({
        authorization: "Bearer test-token",
        "X-Custom": "value",
      });

      // Test that it returns a copy
      headers["X-Modified"] = "modified";
      const headers2 = provider.getHeaders();
      expect(headers2).not.toHaveProperty("X-Modified");
    });

    it("should pass headers from StaticHeaderProvider to requests", async () => {
      const provider = new StaticHeaderProvider({
        "X-Custom-Auth": "secret-token",
        "X-Request-Source": "test-suite",
      });

      await withMockDatabase(
        (req, res) => {
          expect(req.headers["x-custom-auth"]).toEqual("secret-token");
          expect(req.headers["x-request-source"]).toEqual("test-suite");

          const body = JSON.stringify({ tables: [] });
          res.writeHead(200, { "Content-Type": "application/json" }).end(body);
        },
        async () => {
          // Use actual header provider mechanism instead of extraHeaders
          const conn = await connect(
            "db://dev",
            {
              apiKey: "fake",
              hostOverride: "http://localhost:8000",
            },
            undefined, // session
            provider, // headerProvider
          );

          const tableNames = await conn.tableNames();
          expect(tableNames).toEqual([]);
        },
      );
    });

    it("should work with CustomProvider", () => {
      const provider = new CustomProvider();
      const headers = provider.getHeaders();
      expect(headers).toEqual({ "X-Custom": "custom-value" });
    });

    it("should handle ErrorProvider errors", () => {
      const provider = new ErrorProvider("Authentication failed");

      expect(() => provider.getHeaders()).toThrow("Authentication failed");
      expect(provider.callCount).toBe(1);

      // Test that error is thrown each time
      expect(() => provider.getHeaders()).toThrow("Authentication failed");
      expect(provider.callCount).toBe(2);
    });

    it("should work with ConcurrentProvider", () => {
      const provider = new ConcurrentProvider();

      const headers1 = provider.getHeaders();
      const headers2 = provider.getHeaders();
      const headers3 = provider.getHeaders();

      expect(headers1).toEqual({ "X-Request-Id": "1" });
      expect(headers2).toEqual({ "X-Request-Id": "2" });
      expect(headers3).toEqual({ "X-Request-Id": "3" });
    });

    describe("OAuthHeaderProvider", () => {
      it("should initialize correctly", () => {
        const fetcher = () => ({
          accessToken: "token123",
          expiresIn: 3600,
        });

        const provider = new OAuthHeaderProvider(fetcher);
        expect(provider).toBeInstanceOf(HeaderProvider);
      });

      it("should fetch token on first use", async () => {
        let callCount = 0;
        const fetcher = () => {
          callCount++;
          return {
            accessToken: "token123",
            expiresIn: 3600,
          };
        };

        const provider = new OAuthHeaderProvider(fetcher);

        // Need to manually refresh first due to sync limitation
        await provider.refreshToken();

        const headers = provider.getHeaders();
        expect(headers).toEqual({ authorization: "Bearer token123" });
        expect(callCount).toBe(1);

        // Second call should not fetch again
        const headers2 = provider.getHeaders();
        expect(headers2).toEqual({ authorization: "Bearer token123" });
        expect(callCount).toBe(1);
      });

      it("should handle tokens without expiry", async () => {
        const fetcher = () => ({
          accessToken: "permanent_token",
        });

        const provider = new OAuthHeaderProvider(fetcher);
        await provider.refreshToken();

        const headers = provider.getHeaders();
        expect(headers).toEqual({ authorization: "Bearer permanent_token" });
      });

      it("should throw error when access_token is missing", async () => {
        const fetcher = () =>
          ({
            expiresIn: 3600,
          }) as { accessToken?: string; expiresIn?: number };

        const provider = new OAuthHeaderProvider(
          fetcher as () => {
            accessToken: string;
            expiresIn?: number;
          },
        );

        await expect(provider.refreshToken()).rejects.toThrow(
          "Token fetcher did not return 'accessToken'",
        );
      });

      it("should handle async token fetchers", async () => {
        const fetcher = async () => {
          // Simulate async operation
          await new Promise((resolve) => setTimeout(resolve, 10));
          return {
            accessToken: "async_token",
            expiresIn: 3600,
          };
        };

        const provider = new OAuthHeaderProvider(fetcher);
        await provider.refreshToken();

        const headers = provider.getHeaders();
        expect(headers).toEqual({ authorization: "Bearer async_token" });
      });
    });

    it("should merge header provider headers with extra headers", async () => {
      const provider = new StaticHeaderProvider({
        "X-From-Provider": "provider-value",
      });

      await withMockDatabase(
        (req, res) => {
          expect(req.headers["x-from-provider"]).toEqual("provider-value");
          expect(req.headers["x-extra-header"]).toEqual("extra-value");

          const body = JSON.stringify({ tables: [] });
          res.writeHead(200, { "Content-Type": "application/json" }).end(body);
        },
        async () => {
          // Use header provider with additional extraHeaders
          const conn = await connect(
            "db://dev",
            {
              apiKey: "fake",
              hostOverride: "http://localhost:8000",
              clientConfig: {
                extraHeaders: {
                  "X-Extra-Header": "extra-value",
                },
              },
            },
            undefined, // session
            provider, // headerProvider
          );

          const tableNames = await conn.tableNames();
          expect(tableNames).toEqual([]);
        },
      );
    });
  });

  describe("header provider integration", () => {
    it("should work with TypeScript StaticHeaderProvider", async () => {
      let requestCount = 0;

      await withMockDatabase(
        (req, res) => {
          requestCount++;

          // Check headers are present on each request
          expect(req.headers["authorization"]).toEqual("Bearer test-token-123");
          expect(req.headers["x-custom"]).toEqual("custom-value");

          // Return different responses based on the endpoint
          if (req.url === "/v1/table/test_table/describe/") {
            const body = JSON.stringify({
              name: "test_table",
              schema: { fields: [] },
            });
            res
              .writeHead(200, { "Content-Type": "application/json" })
              .end(body);
          } else {
            const body = JSON.stringify({ tables: ["test_table"] });
            res
              .writeHead(200, { "Content-Type": "application/json" })
              .end(body);
          }
        },
        async () => {
          // Create provider with static headers
          const provider = new StaticHeaderProvider({
            authorization: "Bearer test-token-123",
            "X-Custom": "custom-value",
          });

          // Connect with the provider
          const conn = await connect(
            "db://dev",
            {
              apiKey: "fake",
              hostOverride: "http://localhost:8000",
            },
            undefined, // session
            provider, // headerProvider
          );

          // Make multiple requests to verify headers are sent each time
          const tables1 = await conn.tableNames();
          expect(tables1).toEqual(["test_table"]);

          const tables2 = await conn.tableNames();
          expect(tables2).toEqual(["test_table"]);

          // Verify headers were sent with each request
          expect(requestCount).toBeGreaterThanOrEqual(2);
        },
      );
    });

    it("should work with JavaScript function provider", async () => {
      let requestId = 0;

      await withMockDatabase(
        (req, res) => {
          // Check dynamic header is present
          expect(req.headers["x-request-id"]).toBeDefined();
          expect(req.headers["x-request-id"]).toMatch(/^req-\d+$/);

          const body = JSON.stringify({ tables: [] });
          res.writeHead(200, { "Content-Type": "application/json" }).end(body);
        },
        async () => {
          // Create a JavaScript function that returns dynamic headers
          const getHeaders = async () => {
            requestId++;
            return {
              "X-Request-Id": `req-${requestId}`,
              "X-Timestamp": new Date().toISOString(),
            };
          };

          // Connect with the function directly
          const conn = await connect(
            "db://dev",
            {
              apiKey: "fake",
              hostOverride: "http://localhost:8000",
            },
            undefined, // session
            getHeaders, // headerProvider
          );

          // Make requests - each should have different headers
          const tables = await conn.tableNames();
          expect(tables).toEqual([]);
        },
      );
    });

    it("should support OAuth-like token refresh pattern", async () => {
      let tokenVersion = 0;

      await withMockDatabase(
        (req, res) => {
          // Verify authorization header
          const authHeader = req.headers["authorization"];
          expect(authHeader).toBeDefined();
          expect(authHeader).toMatch(/^Bearer token-v\d+$/);

          const body = JSON.stringify({ tables: [] });
          res.writeHead(200, { "Content-Type": "application/json" }).end(body);
        },
        async () => {
          // Simulate OAuth token fetcher
          const fetchToken = async () => {
            tokenVersion++;
            return {
              authorization: `Bearer token-v${tokenVersion}`,
            };
          };

          // Connect with the function directly
          const conn = await connect(
            "db://dev",
            {
              apiKey: "fake",
              hostOverride: "http://localhost:8000",
            },
            undefined, // session
            fetchToken, // headerProvider
          );

          // Each request will fetch a new token
          await conn.tableNames();

          // Token should be different on next request
          await conn.tableNames();
        },
      );
    });
  });

  describe("renameTable", () => {
    async function captureRenameRequest(
      call: (db: Connection) => Promise<void>,
    ): Promise<{ url: string; body: Record<string, unknown> }> {
      let captured: { url: string; body: Record<string, unknown> } | undefined;
      await withMockDatabase((req, res) => {
        let raw = "";
        req.on("data", (chunk) => {
          raw += chunk;
        });
        req.on("end", () => {
          captured = {
            url: req.url ?? "",
            body: raw ? JSON.parse(raw) : {},
          };
          res.writeHead(200, { "Content-Type": "application/json" }).end("");
        });
      }, call);
      if (!captured) {
        throw new Error("mock server never saw a request");
      }
      return captured;
    }

    it("sends rename request for a table in the root namespace", async () => {
      const { url, body } = await captureRenameRequest(async (db) => {
        await db.renameTable("table1", "table2");
      });
      expect(url).toBe("/v1/table/table1/rename/");
      // biome-ignore lint/style/useNamingConvention: snake_case mandated by the server wire format
      expect(body).toEqual({ new_table_name: "table2" });
    });

    it("omits new_namespace when only the current namespace is supplied", async () => {
      // Safe-default check: passing namespacePath alone must not send
      // `new_namespace`, so the server keeps the table in its current
      // namespace instead of silently moving it to root.
      const { url, body } = await captureRenameRequest(async (db) => {
        await db.renameTable("table1", "table2", {
          namespacePath: ["ns1"],
        });
      });
      expect(url).toBe("/v1/table/ns1$table1/rename/");
      // biome-ignore lint/style/useNamingConvention: snake_case mandated by the server wire format
      expect(body).toEqual({ new_table_name: "table2" });
    });

    it("includes new_namespace in the body for a cross-namespace rename", async () => {
      const { url, body } = await captureRenameRequest(async (db) => {
        await db.renameTable("table1", "table2", {
          namespacePath: ["ns1"],
          newNamespacePath: ["ns2"],
        });
      });
      expect(url).toBe("/v1/table/ns1$table1/rename/");
      expect(body).toEqual({
        // biome-ignore lint/style/useNamingConvention: snake_case mandated by the server wire format
        new_table_name: "table2",
        // biome-ignore lint/style/useNamingConvention: snake_case mandated by the server wire format
        new_namespace: ["ns2"],
      });
    });
  });
});

describe("remote connection jobs surface", () => {
  it("lists, describes, cancels, and reads history", async () => {
    const { tableFromArrays, tableToIPC } = await import("apache-arrow");
    const eventsTable = tableFromArrays({ state: ["created", "succeeded"] });
    const eventsBody = Buffer.from(tableToIPC(eventsTable, "stream"));
    const queryEventsPayloads: Record<string, unknown>[] = [];

    await withMockDatabase(
      (req, res) => {
        let body = "";
        req.on("data", (chunk) => {
          body += chunk;
        });
        req.on("end", () => {
          const payload = body.length > 0 ? JSON.parse(body) : {};
          if (req.url === "/v1/jobs/list") {
            if (payload["page_token"] === undefined) {
              res
                .writeHead(200, { "Content-Type": "application/json" })
                .end(
                  '{"jobs": [{"job_id": "job-1", "table": "t1", ' +
                    '"job_type": "create_index", "state": "in_progress", ' +
                    '"created_at_millis": 1000, ' +
                    '"progress": {"rows_planned": 4, "rows_computed": 2}, ' +
                    '"phase": "running", "paused": false}], "page_token": "next"}',
                );
            } else {
              res
                .writeHead(200, { "Content-Type": "application/json" })
                .end(
                  '{"jobs": [{"job_id": "job-2", "table": "t2", ' +
                    '"job_type": "create_index", "state": "succeeded", ' +
                    '"created_at_millis": 2000, "completed_at_millis": 2000, ' +
                    '"duration_millis": 0, "columns": ["embedding"], ' +
                    '"function_name": "features$embed", "function_version": "7"}, ' +
                    '{"job_id": "job-3", "table": "t2", "job_type": "function_refresh", ' +
                    '"state": "failed", "created_at_millis": 3000, "duration_millis": 200, ' +
                    '"completed_at_millis": 3200, "failure": {"phase": "execute", ' +
                    '"message": "worker failed", "retryable": false}}]}',
                );
            }
          } else if (req.url === "/v1/jobs/describe") {
            if (payload["job_id"] === "job-2") {
              res
                .writeHead(200, { "Content-Type": "application/json" })
                .end(
                  '{"job_id": "job-2", "job_type": "refresh_column", ' +
                    '"job_state": "DONE", "creation_ms": 2000, ' +
                    '"result": {"rows_assigned": 1000000}}',
                );
              return;
            }
            if (payload["job_id"] !== "job-1") {
              res.writeHead(404).end("no such job");
              return;
            }
            res
              .writeHead(200, { "Content-Type": "application/json" })
              .end(
                '{"job_id": "job-1", "job_type": "create_index", ' +
                  '"job_state": "FAILED", "creation_ms": 1000, ' +
                  '"progress": {"rows_planned": 4, "rows_computed": 2, ' +
                  '"vendor_extension": "kept"}, ' +
                  '"phase": "running", ' +
                  '"spec": {"column": "vec"}, "failure": {"phase": "execute", ' +
                  '"message": "worker died", "retryable": true}}',
              );
          } else if (req.url === "/v1/jobs/cancel") {
            if (payload["job_id"] !== "job-1") {
              res.writeHead(404).end("no such job");
              return;
            }
            res
              .writeHead(200, { "Content-Type": "application/json" })
              .end('{"job_id": "job-1"}');
          } else if (req.url === "/v1/jobs/pause") {
            if (payload["job_id"] !== "job-1") {
              res.writeHead(404).end("no such job");
              return;
            }
            res
              .writeHead(200, { "Content-Type": "application/json" })
              .end('{"job_id": "job-1", "paused": true}');
          } else if (req.url === "/v1/jobs/resume") {
            if (payload["job_id"] !== "job-1") {
              res.writeHead(404).end("no such job");
              return;
            }
            res
              .writeHead(200, { "Content-Type": "application/json" })
              .end(
                '{"job_id": "job-1", "resumed": false, "still_pausing": true}',
              );
          } else if (req.url === "/v1/jobs/query_events") {
            queryEventsPayloads.push(payload);
            res
              .writeHead(200, {
                "Content-Type": "application/vnd.apache.arrow.stream",
              })
              .end(eventsBody);
          } else {
            res.writeHead(404).end();
          }
        });
      },
      async (db) => {
        const jobs = await collect(db.listJobs());
        expect(jobs.map((job) => job.jobId)).toEqual([
          "job-1",
          "job-2",
          "job-3",
        ]);
        expect(jobs[0].state).toEqual("running");
        expect(jobs[1].state).toEqual("finished");
        expect(jobs[0].completedAtMillis).toBeUndefined();
        expect(jobs[0].durationMillis).toBeUndefined();
        expect(jobs[0].columns).toEqual([]);
        expect(jobs[0].functionName).toBeUndefined();
        expect(jobs[0].functionVersion).toBeUndefined();
        expect(jobs[0].failure).toBeUndefined();
        expect(jobs[1].completedAtMillis).toBe(2000);
        expect(jobs[1].durationMillis).toBe(0);
        expect(jobs[1].columns).toEqual(["embedding"]);
        expect(jobs[1].functionName).toBe("features$embed");
        expect(jobs[1].functionVersion).toBe("7");
        expect(jobs[1].failure).toBeUndefined();
        expect(jobs[2].state).toBe("failed");
        expect(jobs[2].durationMillis).toBe(200);
        expect(jobs[2].failure?.phase).toBe("execute");
        expect(jobs[2].failure?.message).toBe("worker failed");
        expect(jobs[2].failure?.retryable).toBe(false);
        expect(jobs[2].failure?.traceback).toBeUndefined();
        expect(jobs[0].progress).toEqual({
          // biome-ignore lint/style/useNamingConvention: server wire format
          job_type: "create_index",
          state: "running",
          // biome-ignore lint/style/useNamingConvention: server wire format
          creation_ms: 1000,
          paused: false,
          phase: "running",
          // biome-ignore lint/style/useNamingConvention: server wire format
          rows_planned: 4,
          // biome-ignore lint/style/useNamingConvention: server wire format
          rows_computed: 2,
        });

        expect(await db.cancelJob("job-1")).toBe(true);
        expect(await db.cancelJob("missing")).toBe(false);

        expect(await db.pauseJob("job-1")).toEqual("pausing");
        expect(await db.resumeJob("job-1")).toEqual("still_pausing");

        // Opening a job hands back a populated handle; a missing one rejects.
        await expect(db.openJob("missing")).rejects.toThrow("not found");
        const finished = await db.openJob("job-2");
        expect(finished.state).toEqual("finished");
        expect(finished.result).toEqual({
          // biome-ignore lint/style/useNamingConvention: snake_case mandated by the server wire format
          rows_assigned: 1000000,
        });

        const job = await db.openJob("job-1");
        expect(job.id).toEqual("job-1");

        // openJob already populated the handle; refresh() re-reads it.
        expect(job.state).toEqual("failed");
        await job.refresh();
        expect(job.state).toEqual("failed");
        expect(job.jobType).toEqual("create_index");
        expect(job.creationMs).toEqual(1000);
        expect(job.spec).toEqual({ column: "vec" });
        expect(job.result).toBeNull();
        expect(job.failure?.message).toEqual("worker died");
        expect(await job.progress()).toEqual({
          // biome-ignore lint/style/useNamingConvention: server wire format
          job_id: "job-1",
          // biome-ignore lint/style/useNamingConvention: server wire format
          job_type: "create_index",
          state: "failed",
          // biome-ignore lint/style/useNamingConvention: server wire format
          creation_ms: 1000,
          paused: false,
          phase: "running",
          // biome-ignore lint/style/useNamingConvention: server wire format
          rows_planned: 4,
          // biome-ignore lint/style/useNamingConvention: server wire format
          rows_computed: 2,
          // biome-ignore lint/style/useNamingConvention: server wire format
          vendor_extension: "kept",
        });

        // The handle reaches its own events, supplying its job id.
        const jobEvents = await job.events({
          limit: 500,
          filter: "state = 'claim_complete'",
        });
        expect(jobEvents.numRows).toEqual(2);
        expect(queryEventsPayloads.pop()).toEqual({
          // biome-ignore lint/style/useNamingConvention: snake_case mandated by the server wire format
          job_id: "job-1",
          limit: 500,
          filter: "state = 'claim_complete'",
        });

        // Printing lays every known field out on its own line, with the JSON
        // payloads indented rather than crammed onto one line.
        expect(`${job}`).toEqual(
          [
            "Job(",
            '    id="job-1",',
            '    state="failed",',
            '    jobType="create_index",',
            "    creationMs=1000,",
            "    spec={",
            '        "column": "vec"',
            "    },",
            "    failure={",
            '        "phase": "execute",',
            '        "message": "worker died",',
            '        "retryable": true',
            "    },",
            ")",
          ].join("\n"),
        );

        expect(await job.status()).toEqual("failed");
        await expect(job.wait()).rejects.toThrow("worker died");
      },
    );
  });

  it("observes structured progress through the existing wait poller", async () => {
    const now = Date.now();
    const unknownFleet = {
      desired: 2,
      launched: 1,
      // biome-ignore lint/style/useNamingConvention: server wire format
      pending_workers: 1,
      ready: 0,
      // biome-ignore lint/style/useNamingConvention: server wire format
      requested_gpus: 1,
      // biome-ignore lint/style/useNamingConvention: server wire format
      ready_gpus: 0,
      // biome-ignore lint/style/useNamingConvention: server wire format
      resource_classes: ["a10g"],
      // biome-ignore lint/style/useNamingConvention: server wire format
      first_launched_at_millis: now - 4000,
    };
    const knownFleet = {
      ...unknownFleet,
      // biome-ignore lint/style/useNamingConvention: server wire format
      warm_reused: true,
      // biome-ignore lint/style/useNamingConvention: server wire format
      wait_reason: "Job queued, waiting for GPU capacity in class 'a10g'",
    };
    const progress = (
      rowsComputed: number,
      rowsRemaining: number,
      sampleOffset: number,
      workerFleet: typeof knownFleet | typeof unknownFleet,
    ) =>
      JSON.stringify({
        // biome-ignore lint/style/useNamingConvention: server wire format
        rows_planned: 100,
        // biome-ignore lint/style/useNamingConvention: server wire format
        rows_computed: rowsComputed,
        // biome-ignore lint/style/useNamingConvention: server wire format
        rows_skipped: 0,
        // biome-ignore lint/style/useNamingConvention: server wire format
        rows_remaining: rowsRemaining,
        // biome-ignore lint/style/useNamingConvention: server wire format
        fragments_planned: 4,
        // biome-ignore lint/style/useNamingConvention: server wire format
        execution_epoch: 7,
        // biome-ignore lint/style/useNamingConvention: server wire format
        sample_at_millis: now - sampleOffset,
        // biome-ignore lint/style/useNamingConvention: server wire format
        worker_fleet: workerFleet,
        // biome-ignore lint/style/useNamingConvention: server wire format
        queue_reason: "Waiting for Function worker capacity",
      });
    const descriptions = [
      `{"job_id":"job-wait","job_type":"refresh_column","job_state":"IN_PROGRESS","creation_ms":1000,"queue_reason":"Waiting for Function worker capacity","progress":${progress(
        20,
        80,
        3000,
        unknownFleet,
      )}}`,
      `{"job_id":"job-wait","job_type":"refresh_column","job_state":"IN_PROGRESS","creation_ms":1000,"queue_reason":"Waiting for Function worker capacity","progress":${progress(
        20,
        80,
        2000,
        knownFleet,
      )}}`,
      `{"job_id":"job-wait","job_type":"refresh_column","job_state":"IN_PROGRESS","creation_ms":1000,"queue_reason":"Waiting for Function worker capacity","progress":${progress(
        40,
        60,
        1000,
        unknownFleet,
      )}}`,
      `{"job_id":"job-wait","job_type":"refresh_column","job_state":"DONE","creation_ms":1000,"result":{},"progress":${progress(
        60,
        0,
        0,
        unknownFleet,
      )}}`,
    ];
    let describeCount = 0;

    await withMockDatabase(
      (req, res) => {
        if (req.url === "/v1/jobs/describe") {
          describeCount++;
          const description = descriptions.shift();
          if (description === undefined) {
            res.writeHead(500).end("unexpected describe");
            return;
          }
          res
            .writeHead(200, { "Content-Type": "application/json" })
            .end(description);
          return;
        }
        res.writeHead(404).end();
      },
      async (db) => {
        const job = await db.openJob("job-wait");
        const first = await job.progress();
        expect(first).toMatchObject({
          state: "running",
          // biome-ignore lint/style/useNamingConvention: server wire format
          rows_computed: 20,
          // biome-ignore lint/style/useNamingConvention: server wire format
          rows_remaining: 80,
        });
        expect(first?.worker_fleet).toMatchObject(knownFleet);
        expect(
          (first?.worker_fleet as Record<string, unknown> | null | undefined)
            ?.warm_reused,
        ).toBe(true);
        expect(first?.queue_reason).toBe(
          "Waiting for Function worker capacity",
        );
        expect(first).not.toHaveProperty("estimated_remaining_time_millis");

        const observed: JobProgress[] = [];
        await job.wait((progress) => observed.push(progress));

        expect(observed).toHaveLength(2);
        expect(observed[0]).toMatchObject({
          state: "running",
          // biome-ignore lint/style/useNamingConvention: server wire format
          rows_computed: 40,
          // biome-ignore lint/style/useNamingConvention: server wire format
          estimated_remaining_time_millis: 3000,
        });
        expect(observed[0].worker_fleet).toEqual(unknownFleet);
        expect(observed[0].worker_fleet).not.toHaveProperty("warm_reused");
        expect(observed[0].queue_reason).toBe(
          "Waiting for Function worker capacity",
        );
        expect(observed[1]).toMatchObject({
          state: "finished",
          // biome-ignore lint/style/useNamingConvention: server wire format
          rows_remaining: 0,
          // biome-ignore lint/style/useNamingConvention: server wire format
          estimated_remaining_time_millis: 0,
        });
      },
    );

    expect(describeCount).toBe(4);
  });

  it("does not derive ETA across a zero-rate, epoch, or pause boundary", async () => {
    const now = Date.now();
    const descriptions = [
      `{"job_id":"job-eta","job_type":"refresh_column","job_state":"IN_PROGRESS","creation_ms":1000,"progress":{"rows_planned":100,"rows_computed":20,"rows_skipped":0,"rows_remaining":80,"fragments_planned":4,"execution_epoch":1,"sample_at_millis":${
        now - 5000
      }}}`,
      `{"job_id":"job-eta","job_type":"refresh_column","job_state":"IN_PROGRESS","creation_ms":1000,"progress":{"rows_planned":100,"rows_computed":20,"rows_skipped":0,"rows_remaining":80,"fragments_planned":4,"execution_epoch":1,"sample_at_millis":${
        now - 4000
      }}}`,
      `{"job_id":"job-eta","job_type":"refresh_column","job_state":"IN_PROGRESS","creation_ms":1000,"progress":{"rows_planned":100,"rows_computed":40,"rows_skipped":0,"rows_remaining":60,"fragments_planned":4,"execution_epoch":2,"sample_at_millis":${
        now - 3000
      }}}`,
      `{"job_id":"job-eta","job_type":"refresh_column","job_state":"IN_PROGRESS","creation_ms":1000,"phase":"initializing","progress":{"rows_planned":100,"rows_computed":60,"rows_skipped":0,"rows_remaining":40,"fragments_planned":4,"execution_epoch":2,"sample_at_millis":${
        now - 2000
      }}}`,
      `{"job_id":"job-eta","job_type":"refresh_column","job_state":"IN_PROGRESS","creation_ms":1000,"paused_at_millis":5000,"progress":{"rows_planned":100,"rows_computed":60,"rows_skipped":0,"rows_remaining":40,"fragments_planned":4,"execution_epoch":2,"sample_at_millis":${
        now - 1500
      }}}`,
      `{"job_id":"job-eta","job_type":"refresh_column","job_state":"IN_PROGRESS","creation_ms":1000,"progress":{"rows_planned":100,"rows_computed":80,"rows_skipped":0,"rows_remaining":20,"fragments_planned":4,"execution_epoch":2,"sample_at_millis":${
        now - 1000
      }}}`,
    ];

    await withMockDatabase(
      (req, res) => {
        if (req.url === "/v1/jobs/describe") {
          const description = descriptions.shift();
          if (description === undefined) {
            res.writeHead(500).end("unexpected describe");
            return;
          }
          res
            .writeHead(200, { "Content-Type": "application/json" })
            .end(description);
          return;
        }
        res.writeHead(404).end();
      },
      async (db) => {
        const job = await db.openJob("job-eta");
        for (let i = 0; i < 5; i++) {
          const progress = await job.progress();
          expect(progress).not.toHaveProperty(
            "estimated_remaining_time_millis",
          );
        }
      },
    );
  });

  it("keeps a thrown progress observer from escaping wait", async () => {
    const descriptions = [
      '{"job_id":"job-throw","job_type":"refresh_column","job_state":"IN_PROGRESS","creation_ms":1000,"progress":{"rows_planned":4,"rows_computed":2}}',
      '{"job_id":"job-throw","job_type":"refresh_column","job_state":"DONE","creation_ms":1000,"result":{}}',
    ];

    await withMockDatabase(
      (req, res) => {
        if (req.url === "/v1/jobs/describe") {
          const description = descriptions.shift();
          if (description === undefined) {
            res.writeHead(500).end("unexpected describe");
            return;
          }
          res
            .writeHead(200, { "Content-Type": "application/json" })
            .end(description);
          return;
        }
        res.writeHead(404).end();
      },
      async (db) => {
        const job = await db.openJob("job-throw");
        const warn = jest
          .spyOn(console, "warn")
          .mockImplementation(() => undefined);
        try {
          await expect(
            job.wait(() => {
              throw new Error("observer failed");
            }),
          ).resolves.toBeUndefined();
          expect(warn).toHaveBeenCalled();
        } finally {
          warn.mockRestore();
        }
      },
    );
  });

  it("leaves numDeletedRows absent when the server omits it", async () => {
    // A server that predates num_deleted_rows says nothing about deleted rows
    const statsReply =
      '{"total_bytes":1,"num_rows":3,"num_indices":0,"fragment_stats":' +
      '{"num_fragments":1,"num_small_fragments":0,' +
      '"lengths":{"min":3,"max":3,"mean":3,"p25":3,"p50":3,"p75":3,"p99":3}}}';

    await withMockDatabase(
      (req, res) => {
        const path = req.url ?? "";
        const body = path.endsWith("/describe/")
          ? JSON.stringify({ name: "t", version: 1, schema: { fields: [] } })
          : statsReply;
        res.writeHead(200, { "Content-Type": "application/json" }).end(body);
      },
      async (db) => {
        const stats = await (await db.openTable("t")).stats();
        expect("numDeletedRows" in stats).toBe(false);
        expect(stats.numDeletedRows).toBeUndefined();
      },
    );
  });
});

async function collect<T>(items: AsyncIterable<T>): Promise<T[]> {
  const result: T[] = [];
  for await (const item of items) result.push(item);
  return result;
}

describe("lazy resource listings", () => {
  for (const kind of ["jobs"] as const) {
    const listing = (
      db: Connection,
      options: { pageToken?: string; pageLimit?: number } = {},
    ) => {
      return db.listJobs(options);
    };
    const page = (names: string[], token?: string) => {
      const items =
        kind === "jobs"
          ? names.map((name) => ({
              // biome-ignore lint/style/useNamingConvention: server wire format
              job_id: name,
              table: "t",
              // biome-ignore lint/style/useNamingConvention: server wire format
              job_type: "create_index",
              state: "in_progress",
              // biome-ignore lint/style/useNamingConvention: server wire format
              created_at_millis: 1,
            }))
          : names;
      return {
        [kind === "jobs" ? "jobs" : "views"]: items,
        // biome-ignore lint/style/useNamingConvention: server wire format
        page_token: token,
      };
    };

    it(`${kind} fetches lazily, skips empty pages, and resumes`, async () => {
      const requests: { url: string; body: Record<string, unknown> }[] = [];
      const responses = [
        page(["a", "b"], "empty"),
        page([], "last"),
        page(["c", "d"], ""),
      ];
      await withMockDatabase(
        async (req, res) => {
          const chunks: Buffer[] = [];
          for await (const chunk of req) chunks.push(Buffer.from(chunk));
          const body = Buffer.concat(chunks).toString();
          requests.push({
            url: req.url ?? "",
            body: body ? JSON.parse(body) : {},
          });
          res
            .writeHead(200, { "content-type": "application/json" })
            .end(JSON.stringify(responses.shift()));
        },
        async (db) => {
          const items = listing(db, { pageToken: "start/token", pageLimit: 2 });
          expect(requests).toHaveLength(0);
          expect(items.pageToken()).toBe("start/token");
          expect(items.numPageResults()).toBe(0);
          expect((await items.next()).done).toBe(false);
          expect(items.numPageResults()).toBe(1);
          expect(items.pageToken()).toBe("empty");
          await items.next();
          expect(requests).toHaveLength(1);
          const resumed = listing(db, {
            pageToken: items.pageToken(),
            pageLimit: 2,
          });
          await resumed.next();
          expect(resumed.numPageResults()).toBe(1);
          expect(resumed.pageToken()).toBeUndefined();
          expect(requests).toHaveLength(3);
          await resumed.next();
          expect((await resumed.next()).done).toBe(true);
          expect((await resumed.next()).done).toBe(true);
          for (const [i, request] of requests.entries()) {
            const token = ["start/token", "empty", "last"][i];
            const url = new URL(request.url, "http://localhost");
            if (kind === "jobs") {
              expect(url.pathname).toBe("/v1/jobs/list");
              expect(request.body).toEqual({
                database: "dev",
                // biome-ignore lint/style/useNamingConvention: server wire format
                page_token: token,
                limit: 2,
              });
            } else {
              expect(url.pathname).toBe("/v1/namespace/team/view/list");
              expect(url.searchParams.get("page_token")).toBe(token);
              expect(url.searchParams.get("limit")).toBe("2");
            }
          }
        },
      );
    });

    it(`${kind} retains failed tokens and serializes concurrent advances`, async () => {
      let requests = 0;
      await withMockDatabase(
        (_req, res) => {
          requests += 1;
          res
            .writeHead(requests === 1 ? 401 : 200, {
              "content-type": "application/json",
            })
            .end(JSON.stringify(requests === 1 ? {} : page(["a", "b"])));
        },
        async (db) => {
          const failed = listing(db, { pageToken: "retry" });
          await expect(failed.next()).rejects.toThrow();
          expect(failed.pageToken()).toBe("retry");
          expect((await failed.next()).done).toBe(true);
          expect(requests).toBe(1);
          const resumed = listing(db, { pageToken: failed.pageToken() });
          const results = await Promise.all([
            resumed.next(),
            resumed.next(),
            resumed.next(),
          ]);
          expect(results.filter((result) => result.done)).toHaveLength(1);
          expect(requests).toBe(2);
          for (const limit of [
            0,
            -1,
            1.5,
            Number.NaN,
            Number.POSITIVE_INFINITY,
            2147483648,
          ]) {
            expect(() => listing(db, { pageLimit: limit })).toThrow("limit");
          }
          expect(requests).toBe(2);
        },
      );
    });
  }
});

describe("remote compaction job results", () => {
  test.each([true, false])(
    "reopens typed compaction result (counts=%s)",
    async (counts) => {
      await withMockDatabase(
        (request, response) => {
          expect(request.url).toBe("/v1/jobs/describe");
          const result = JSON.parse(`{
            "partial_commit": false,
            "rows_succeeded": 12,
            "rows_failed": 0,
            "completion_percentage": 100
          }`);
          if (counts) {
            Object.assign(
              result,
              JSON.parse(`{
              "files_added": 1,
              "files_removed": 3,
              "fragments_added": 1,
              "fragments_removed": 3
            }`),
            );
          }
          response.writeHead(200, { "Content-Type": "application/json" });
          response.end(
            `{"job_id":"compacting-job","job_type":"compact","job_state":"DONE","result":${JSON.stringify(result)}}`,
          );
        },
        async (db) => {
          const job =
            await db.openJob<import("../lancedb").CompactionResult>(
              "compacting-job",
            );
          const result = await job.wait();
          expect(result.rowsSucceeded).toBe(12);
          expect(result.moreWork).toBe(false);
          expect(result.fragmentsRemoved).toBe(counts ? 3 : null);
        },
      );
    },
  );
});

describe("data prewarm typed results", () => {
  test.each([null, {}])(
    "rejects a missing typed outcome (%p)",
    async (result) => {
      await withMockDatabase(
        (_request, response) => {
          response.writeHead(200, { "Content-Type": "application/json" });
          response.end(
            `{"job_id":"warming-job","job_type":"prewarm_page_cache","job_state":"DONE","result":${JSON.stringify(result)}}`,
          );
        },
        async (db) => {
          const job = await db.openJob("warming-job");
          await expect(job.wait()).rejects.toThrow();
        },
      );
    },
  );

  test.each([true, false])(
    "reopens a result (counters=%s)",
    async (counters) => {
      await withMockDatabase(
        (_request, response) => {
          const result = JSON.parse(
            counters
              ? '{"table_version":42,"executors_targeted":2,"executors_completed":2,"fragments_selected":4}'
              : '{"table_version":42}',
          );
          response.writeHead(200, { "Content-Type": "application/json" });
          response.end(
            JSON.stringify({
              ["job_id"]: "warming-job",
              ["job_type"]: "prewarm_page_cache",
              ["job_state"]: "DONE",
              progress: null,
              ["operation_progress"]: {
                phase: "reading",
                ["executors_completed"]: 2,
              },
              result,
            }),
          );
        },
        async (db) => {
          const job =
            await db.openJob<import("../lancedb").PrewarmDataResult>(
              "warming-job",
            );
          const result = await job.wait();
          expect(result.tableVersion).toBe(42);
          expect(result.executorsCompleted).toBe(counters ? 2 : null);
          expect(result.bytesRead).toBeNull();
          expect((await job.progress())?.executors_completed).toBe(2);
        },
      );
    },
  );
});

describe("job wait deadlines", () => {
  function response(kind: string, done: boolean) {
    const result =
      kind === "compact"
        ? '{"partial_commit":false,"rows_succeeded":12,"rows_failed":0,"completion_percentage":100}'
        : kind === "prewarm_page_cache"
          ? '{"table_version":42,"executors_targeted":2,"executors_completed":2}'
          : "null";
    return `{"job_id":"bounded-job","job_type":"${kind}","job_state":"${done ? "DONE" : "IN_PROGRESS"}","result":${done ? result : "null"}}`;
  }

  test.each(["compact", "prewarm_page_cache", "create_index"])(
    "%s stops polling at timeout, resumes, and reopens",
    async (kind) => {
      let done = false;
      let describes = 0;
      const progress: JobProgress[] = [];
      await withMockDatabase(
        (request, res) => {
          expect(request.url).toBe("/v1/jobs/describe");
          describes++;
          res.writeHead(200, { "Content-Type": "application/json" });
          res.end(response(kind, done));
        },
        async (db) => {
          const fallback = setTimeout(() => {
            done = true;
          }, 1000);
          try {
            const job = await db.openJob("bounded-job");
            await expect(
              job.wait({ timeoutMs: 50, onProgress: (p) => progress.push(p) }),
            ).rejects.toThrow(/bounded-job.*continues/);
            expect(job.state).toBe("running");
            await new Promise((resolve) => setImmediate(resolve));
            const requestsAtTimeout = describes;
            const updatesAtTimeout = progress.length;
            await new Promise((resolve) => setTimeout(resolve, 350));
            expect(describes).toBe(requestsAtTimeout);
            expect(progress).toHaveLength(updatesAtTimeout);
            done = true;
            const result = await job.wait({
              timeoutMs: 1000,
              onProgress: (p) => progress.push(p),
            });
            await new Promise((resolve) => setImmediate(resolve));
            expect(progress.some((p) => p.state === "finished")).toBe(true);
            expect(job.state).toBe("finished");
            expect(result).toEqual(
              kind === "compact"
                ? expect.objectContaining({ rowsSucceeded: 12 })
                : kind === "prewarm_page_cache"
                  ? expect.objectContaining({ tableVersion: 42 })
                  : undefined,
            );
            // Existing callback-only and argument-free calls remain supported.
            expect(await job.wait((p) => progress.push(p))).toEqual(result);
            expect(await job.wait()).toEqual(result);
            const reopened = await db.openJob("bounded-job");
            expect(await reopened.wait({ timeoutMs: 1000 })).toEqual(result);
          } finally {
            clearTimeout(fallback);
          }
        },
      );
    },
  );

  it("bounds a pending HTTP response and permits a later wait", async () => {
    let slow = false;
    let requests = 0;
    await withMockDatabase(
      (request, res) => {
        expect(request.url).toBe("/v1/jobs/describe");
        requests++;
        const send = () => {
          if (!res.destroyed) {
            res.writeHead(200, { "Content-Type": "application/json" });
            res.end(response("prewarm_page_cache", slow));
          }
        };
        if (slow) setTimeout(send, 300);
        else send();
      },
      async (db) => {
        const job = await db.openJob("bounded-job");
        slow = true;
        await expect(job.wait({ timeoutMs: 50 })).rejects.toThrow("continues");
        expect(job.state).toBe("running");
        const stoppedAt = requests;
        await new Promise((resolve) => setTimeout(resolve, 350));
        expect(requests).toBe(stoppedAt);
        const result = await job.wait({ timeoutMs: 1000 });
        expect(result).toEqual(expect.objectContaining({ tableVersion: 42 }));
      },
    );
  });

  it("accepts zero timeout and validates options before polling", async () => {
    let requests = 0;
    let done = false;
    await withMockDatabase(
      (_request, res) => {
        requests++;
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(response("compact", done));
      },
      async (db) => {
        const fallback = setTimeout(() => {
          done = true;
        }, 1000);
        try {
          const job = await db.openJob("bounded-job");
          for (const timeoutMs of [
            -1,
            1.5,
            Number.NaN,
            Number.POSITIVE_INFINITY,
            Number.MAX_SAFE_INTEGER + 1,
            "10",
            true,
          ]) {
            await expect(
              job.wait({ timeoutMs: timeoutMs as number }),
            ).rejects.toThrow("nonnegative safe integer");
          }
          await expect(job.wait({ timeout: 10 } as never)).rejects.toThrow(
            "Unknown job wait option",
          );
          await expect(job.wait({ onProgress: 7 } as never)).rejects.toThrow(
            "onProgress must be a function",
          );
          await expect(job.wait(10 as never)).rejects.toThrow("options object");
          expect(requests).toBe(1);
          await expect(job.wait({ timeoutMs: 0 })).rejects.toThrow("Timeout");
        } finally {
          clearTimeout(fallback);
        }
      },
    );
  });

  test.each(["FAILED", "CANCELLED"])(
    "preserves a %s outcome when a timeout is configured",
    async (state) => {
      await withMockDatabase(
        (_request, res) => {
          res.writeHead(200, { "Content-Type": "application/json" });
          res.end(
            `{"job_id":"bounded-job","job_type":"compact","job_state":"${state}"}`,
          );
        },
        async (db) => {
          const job = await db.openJob("bounded-job");
          await expect(job.wait({ timeoutMs: 1000 })).rejects.toThrow(
            state === "FAILED" ? /failed/i : /cancelled/i,
          );
        },
      );
    },
  );
});

describe("table overrides", () => {
  it.each([[], ["analytics", "features"]])(
    "reads, recursively merges, and resets in namespace %j",
    async (...namespacePath: string[]) => {
      const requests: { method?: string; url: string; body?: unknown }[] = [];
      const current = {
        // biome-ignore lint/style/useNamingConvention: server wire format
        job_types: {
          compaction: {
            enabled: true,
            // biome-ignore lint/style/useNamingConvention: server wire format
            target_rows_per_fragment: 1000,
          },
          cleanup: { enabled: false },
        },
        custom: { keep: true, replace: [1, 2], clear: "value" },
      };
      const patch = {
        // biome-ignore lint/style/useNamingConvention: server wire format
        job_types: { compaction: { enabled: false } },
        custom: { replace: [3], clear: null, label: "測試" },
      };
      const merged = {
        // biome-ignore lint/style/useNamingConvention: server wire format
        job_types: {
          compaction: {
            enabled: false,
            // biome-ignore lint/style/useNamingConvention: server wire format
            target_rows_per_fragment: 1000,
          },
          cleanup: { enabled: false },
        },
        custom: { keep: true, replace: [3], clear: null, label: "測試" },
      };
      await withMockDatabase(
        (req, res) => {
          const url = req.url ?? "";
          if (url.includes("/describe/")) {
            res
              .writeHead(200)
              .end(JSON.stringify({ version: 1, schema: { fields: [] } }));
            return;
          }
          const chunks: Buffer[] = [];
          req.on("data", (chunk) => chunks.push(chunk));
          req.on("end", () => {
            const body = chunks.length
              ? JSON.parse(Buffer.concat(chunks).toString())
              : undefined;
            requests.push({ method: req.method, url, body });
            res
              .writeHead(200)
              .end(
                JSON.stringify(
                  req.method === "GET"
                    ? current
                    : req.method === "PUT"
                      ? body.overrides
                      : {},
                ),
              );
          });
        },
        async (db) => {
          const table = await db.openTable("test", namespacePath);
          await expect(table.getTableOverrides()).resolves.toEqual(current);
          await expect(table.updateTableOverrides(patch)).resolves.toEqual(
            merged,
          );
          // The native conversion and merge must not mutate the caller's object.
          expect(patch.job_types.compaction).toEqual({ enabled: false });
          expect(patch.custom.replace).toEqual([3]);
          await expect(table.resetTableOverrides()).resolves.toEqual({});
        },
      );
      expect(requests.map((request) => request.method)).toEqual([
        "GET",
        "GET",
        "PUT",
        "POST",
      ]);
      for (const request of requests) {
        const url = new URL(request.url, "http://localhost");
        expect(url.pathname).toBe(
          request.method === "POST"
            ? "/admin/table/overrides/reset"
            : "/admin/table/overrides",
        );
        if (request.method === "PUT") {
          expect(request.body).toEqual({
            db: "dev",
            namespace: namespacePath,
            table: "test",
            overrides: merged,
          });
        } else {
          expect(url.searchParams.get("db")).toBe("dev");
          expect(url.searchParams.get("table")).toBe("test");
          expect(url.searchParams.getAll("namespace")).toEqual(namespacePath);
        }
      }
    },
  );

  it.each([{}, { custom: { keep: true } }])(
    "only reads the snapshot %j for an empty update",
    async (current) => {
      const methods: string[] = [];
      await withMockDatabase(
        (req, res) => {
          if (req.url?.includes("/describe/")) {
            res
              .writeHead(200)
              .end(JSON.stringify({ version: 1, schema: { fields: [] } }));
            return;
          }
          methods.push(req.method ?? "");
          res
            .writeHead(req.method === "GET" ? 200 : 400)
            .end(JSON.stringify(current));
        },
        async (db) => {
          const table = await db.openTable("test");
          await expect(table.updateTableOverrides({})).resolves.toEqual(
            current,
          );
        },
      );
      expect(methods).toEqual(["GET"]);
    },
  );

  it.each([null, [], true, 1, "bad"])(
    "rejects a non-object update %j before reading overrides",
    async (value) => {
      const requests: string[] = [];
      await withMockDatabase(
        (req, res) => {
          requests.push(req.url ?? "");
          res
            .writeHead(200)
            .end(JSON.stringify({ version: 1, schema: { fields: [] } }));
        },
        async (db) => {
          const table = await db.openTable("test");
          await expect(
            table.updateTableOverrides(
              value as unknown as Record<string, unknown>,
            ),
          ).rejects.toThrow(/must be a JSON object/);
        },
      );
      expect(requests).toEqual(["/v1/table/test/describe/"]);
    },
  );

  it.each([403, 404])(
    "propagates GET failure %i without a PUT",
    async (status) => {
      const methods: string[] = [];
      await withMockDatabase(
        (req, res) => {
          if (req.url?.includes("/describe/")) {
            res
              .writeHead(200)
              .end(JSON.stringify({ version: 1, schema: { fields: [] } }));
          } else {
            methods.push(req.method ?? "");
            res.writeHead(status).end("overrides unavailable");
          }
        },
        async (db) => {
          const table = await db.openTable("test");
          await expect(
            table.updateTableOverrides({ custom: true }),
          ).rejects.toThrow(/overrides unavailable/);
        },
      );
      expect(methods).toEqual(["GET"]);
    },
  );

  it("rejects writes on historical and closed handles before HTTP", async () => {
    const requests: string[] = [];
    await withMockDatabase(
      (req, res) => {
        requests.push(req.url ?? "");
        res
          .writeHead(200)
          .end(JSON.stringify({ version: 1, schema: { fields: [] } }));
      },
      async (db) => {
        const table = await db.openTable("test");
        await table.checkout(1);
        requests.length = 0;
        await expect(
          table.updateTableOverrides({ custom: true }),
        ).rejects.toThrow(/fixed at version 1/);
        await expect(table.resetTableOverrides()).rejects.toThrow(
          /fixed at version 1/,
        );
        table.close();
        await expect(table.getTableOverrides()).rejects.toThrow(/closed/);
        await expect(
          table.updateTableOverrides({ custom: true }),
        ).rejects.toThrow(/closed/);
        await expect(table.resetTableOverrides()).rejects.toThrow(/closed/);
      },
    );
    expect(requests).toEqual([]);
  });
});

describe("cleanup typed results", () => {
  test.each(["counts", "no_op", "unknown", "missing", "FAILED", "CANCELLED"])(
    "reopens and waits for cleanup (%s)",
    async (outcome) => {
      const known = outcome !== "unknown";
      await withMockDatabase(
        (request, response) => {
          expect(request.url).toBe("/v1/jobs/describe");
          response.writeHead(200, { "Content-Type": "application/json" });
          response.end(
            JSON.stringify({
              ["job_id"]: "cleanup-job",
              ["job_type"]: "cleanup",
              ["job_state"]: ["FAILED", "CANCELLED"].includes(outcome)
                ? outcome
                : "DONE",
              result:
                outcome === "missing"
                  ? null
                  : {
                      ["format_version"]: 1,
                      ["min_age_seconds"]: 0,
                      ["statistics_complete"]: known,
                      ["old_versions"]: known
                        ? outcome === "no_op"
                          ? 0
                          : 2
                        : null,
                      ["bytes_removed"]: known
                        ? outcome === "no_op"
                          ? 0
                          : 123
                        : null,
                    },
            }),
          );
        },
        async (db) => {
          const job =
            await db.openJob<import("../lancedb").CleanupResult>("cleanup-job");
          if (["missing", "FAILED", "CANCELLED"].includes(outcome)) {
            await expect(job.wait()).rejects.toThrow();
          } else {
            const result = await job.wait();
            expect(result.statisticsComplete).toBe(known);
            expect(result.oldVersions).toBe(
              known ? (outcome === "no_op" ? 0 : 2) : null,
            );
            expect(result.minAgeSeconds).toBe(0);
          }
        },
      );
    },
  );
});
