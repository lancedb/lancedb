// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { type Table, connect } from "../lancedb";
import {
  Table as ArrowTable,
  DataType,
  Field,
  Float32,
  LargeBinary,
  RecordBatch,
  Schema,
  Struct,
  Uint64,
  Utf8,
  makeArrowTable,
  tableToIPC,
  vectorFromArray,
} from "../lancedb/arrow";

const IDENTITY_A = `v1:${"a".repeat(64)}`;
const IDENTITY_B = `v1:${"b".repeat(64)}`;
const image = new Field(
  "image",
  new Struct([
    new Field("position", new Uint64(), true),
    new Field("size", new Uint64(), true),
  ]),
  true,
  new Map([["ARROW:extension:name", "lance.blob.v2"]]),
);
const schema = new Schema([new Field("label", new Utf8(), false), image]);
const description = {
  fields: [
    { name: "label", type: { type: "string" }, nullable: false },
    {
      name: "image",
      nullable: true,
      metadata: { "ARROW:extension:name": "lance.blob.v2" },
      type: {
        type: "struct",
        fields: ["position", "size"].map((name) => ({
          name,
          type: { type: "uint64" },
          nullable: true,
        })),
      },
    },
  ],
};

type Request = {
  path: string;
  body: Record<string, unknown>;
  raw: string;
  identity: string | string[] | undefined;
};

interface ServerOptions {
  describeBlobName?: string | null;
  nestedBlob?: boolean;
  queryVersion?: string | null;
  queryIdentity?: string | null;
  echo?: string | null;
  empty?: boolean;
  values?: boolean;
  advanceQueries?: boolean;
  otherLegIdentity?: string | null;
  otherLegVersion?: string;
}

// A real HTTP service: the query finishes on A, then the logical table changes
// before the deferred blob request. Recreating B reuses both version and row IDs.
async function withSnapshotServer(
  change: "recreate" | "overwrite",
  run: (table: Table, requests: Request[]) => Promise<void>,
  options: ServerOptions = {},
) {
  const requests: Request[] = [];
  let changed = false;
  const queryOutputSchema = options.nestedBlob
    ? new Schema([
        schema.fields[0],
        new Field("extra", new Struct([image]), true),
      ])
    : schema;
  const server = createServer((req, res) => {
    const chunks: Buffer[] = [];
    req.on("data", (chunk) => chunks.push(chunk));
    req.on("end", () => {
      try {
        const raw = Buffer.concat(chunks).toString() || "{}";
        const body = JSON.parse(raw);
        const path = req.url ?? "";
        const identity = req.headers["x-lancedb-query-snapshot"];
        requests.push({ path, body, raw, identity });
        res.setHeader("phalanx-version", "0.5.0");
        if (path.endsWith("/describe/")) {
          res.setHeader("content-type", "application/json");
          res.end(
            JSON.stringify({
              version: changed && change === "overwrite" ? 2 : 1,
              schema: {
                fields: description.fields.flatMap((field) => {
                  if (field.name !== "image") return [field];
                  if (options.describeBlobName === null) return [];
                  return [
                    { ...field, name: options.describeBlobName ?? "image" },
                  ];
                }),
              },
            }),
          );
        } else if (path.endsWith("/query/")) {
          const vector = (body.vector?.length ?? 0) > 0;
          const advanced = options.advanceQueries === true && changed;
          const label = advanced ? "B" : "A";
          const columns: [string, string][] =
            body.columns === undefined
              ? queryOutputSchema.fields.map((field) => [
                  field.name,
                  field.name,
                ])
              : Array.isArray(body.columns)
                ? body.columns.map((name: string) => [name, name])
                : Object.entries(body.columns);
          const queryFields = columns.map(([output, source]) =>
            queryOutputSchema.fields
              .find((field) => field.name === source)!
              .clone({ name: output }),
          );
          if (body.with_row_id) {
            queryFields.push(new Field("_rowid", new Uint64(), false));
          }
          if (vector || body.full_text_query) {
            queryFields.push(
              new Field(vector ? "_distance" : "_score", new Float32(), false),
            );
          }
          // Forged IPC metadata must never substitute for authoritative headers.
          const querySchema = new Schema(
            queryFields,
            new Map([
              ["lancedb:query_version", "999"],
              ["lancedb:query_snapshot", IDENTITY_B],
            ]),
          );
          const values = options.empty
            ? []
            : options.values
              ? ["A", null, ""]
              : [label];
          const rows = values.map((value, index) => {
            const row: Record<string, unknown> = {};
            for (const [output, source] of columns) {
              const descriptor =
                value === null
                  ? null
                  : { position: 0n, size: BigInt(value.length) };
              row[output] =
                source === "image"
                  ? descriptor
                  : source === "extra"
                    ? descriptor === null
                      ? null
                      : { image: descriptor }
                    : label;
            }
            if (body.with_row_id) row._rowid = BigInt(index);
            if (vector || body.full_text_query) {
              row[vector ? "_distance" : "_score"] = 0.5;
            }
            return row;
          });
          const result =
            rows.length === 0
              ? new ArrowTable(querySchema)
              : new ArrowTable(
                  querySchema,
                  Object.fromEntries(
                    queryFields.map((field) => [
                      field.name,
                      vectorFromArray(
                        rows.map((row) => row[field.name]),
                        field.type,
                      ),
                    ]),
                  ),
                );
          res.setHeader("content-type", "application/vnd.apache.arrow.stream");
          const version =
            vector && options.otherLegVersion !== undefined
              ? options.otherLegVersion
              : options.queryVersion;
          const snapshot =
            vector && options.otherLegIdentity !== undefined
              ? options.otherLegIdentity
              : options.queryIdentity;
          res.setHeader("x-lancedb-version", "1");
          if (version !== null) {
            res.setHeader(
              "x-lancedb-query-version",
              version ?? (advanced ? "2" : "1"),
            );
          }
          if (snapshot !== null) {
            res.setHeader(
              "x-lancedb-query-snapshot",
              snapshot ?? (advanced ? IDENTITY_B : IDENTITY_A),
            );
          }
          changed = true;
          res.end(tableToIPC(result, "stream"));
        } else if (path.endsWith("/fetch_blobs/")) {
          const requestedVersion = /"version":([0-9]+)/.exec(raw)?.[1];
          const readsOriginal =
            change === "overwrite" &&
            requestedVersion === (options.queryVersion ?? "1");
          const actualIdentity = readsOriginal ? IDENTITY_A : IDENTITY_B;
          // An older serving node may ignore the guard and answer successfully;
          // its echo must still be verified before the client uses any bytes.
          if (options.echo !== null) {
            res.setHeader(
              "x-lancedb-query-snapshot",
              options.echo ?? actualIdentity,
            );
          }
          res.setHeader("content-type", "application/vnd.apache.arrow.stream");
          const payloads = options.values
            ? ["A", null, ""]
            : [readsOriginal ? "A" : "B"];
          res.end(
            tableToIPC(
              makeArrowTable(
                (body.row_ids as number[]).map((id) => ({
                  [String(body.column)]:
                    payloads[id] === null ? null : Buffer.from(payloads[id]!),
                })),
                {
                  schema: new Schema([
                    new Field(String(body.column), new LargeBinary(), true),
                  ]),
                },
              ),
              "stream",
            ),
          );
        } else {
          res.writeHead(404).end(path);
        }
      } catch (error) {
        res.writeHead(500).end(String(error));
      }
    });
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  try {
    const db = await connect("db://dev", {
      apiKey: "fake",
      hostOverride: `http://127.0.0.1:${
        (server.address() as AddressInfo).port
      }`,
    });
    await run(await db.openTable("test"), requests);
  } finally {
    server.closeAllConnections();
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
}

const fetchRequests = (requests: Request[]) =>
  requests.filter((request) => request.path.endsWith("/fetch_blobs/"));
const queryRequests = (requests: Request[]) =>
  requests.filter((request) => request.path.endsWith("/query/"));

const keepFtsReranker = {
  async rerankHybrid(_query: string, _vector: RecordBatch, fts: RecordBatch) {
    const fields = fts.schema.fields.map((field) =>
      field.name === "_score"
        ? field.clone({ name: "_relevance_score" })
        : field,
    );
    return new RecordBatch(
      new Schema(fields, fts.schema.metadata),
      fts.data.clone(new Struct(fields)),
    );
  },
};

describe("remote blob query snapshot", () => {
  it.each([
    [null, false],
    [null, true],
    ["old_image", false],
    ["old_image", true],
  ] as const)(
    "uses the query schema after cached blob %s changes (alias: %s)",
    async (describeBlobName, alias) => {
      await withSnapshotServer(
        "overwrite",
        async (table, requests) => {
          expect(await table.blobColumns()).toEqual(
            describeBlobName === null ? [] : [describeBlobName],
          );
          const query = table.query();
          if (alias) query.select({ "photo.image": "image" });
          const output = alias ? "photo.image" : "image";
          const result = await query.toArrow({ blobMode: "bytes" });
          expect(DataType.isLargeBinary(result.getChild(output)!.type)).toBe(
            true,
          );
          expect(
            result
              .toArray()
              .map((row) =>
                row[output] === null
                  ? null
                  : Buffer.from(row[output]).toString(),
              ),
          ).toEqual(["A", null, ""]);
          expect(
            result.schema.fields.some((field) => field.name === "_rowid"),
          ).toBe(false);
          expect(queryRequests(requests)).toHaveLength(1);
          expect(fetchRequests(requests)).toHaveLength(1);
          expect(fetchRequests(requests)[0].body.column).toBe("image");
          expect(fetchRequests(requests)[0].identity).toBe(IDENTITY_A);
        },
        { describeBlobName, values: true },
      );
    },
  );

  it("maps nested query fields through literal aliases despite a stale schema", async () => {
    await withSnapshotServer(
      "overwrite",
      async (table, requests) => {
        expect(await table.blobColumns()).toEqual([]);
        const rows = await table
          .query()
          .select({ "container.alias": "extra" })
          .toArray({ blobMode: "bytes" });
        expect(
          rows.map((row) =>
            row["container.alias"] === null
              ? null
              : Buffer.from(row["container.alias"].image).toString(),
          ),
        ).toEqual(["A", null, ""]);
        expect(queryRequests(requests)).toHaveLength(1);
        expect(fetchRequests(requests)[0].body.column).toBe("extra.image");
        expect(fetchRequests(requests)[0].identity).toBe(IDENTITY_A);
      },
      { nestedBlob: true, describeBlobName: null, values: true },
    );
  });

  it.each(["toArrow", "toArray"] as const)(
    "%s rejects payloads from a recreated table with reused version and row IDs",
    async (terminal) => {
      await withSnapshotServer("recreate", async (table, requests) => {
        await expect(
          table.query()[terminal]({ blobMode: "bytes" }),
        ).rejects.toThrow(/snapshot identity/);
        const [fetch] = fetchRequests(requests);
        expect(fetch.body.version).toBe(1);
        expect(fetch.body.row_ids).toEqual([0]);
        expect(fetch.body.column).toBe("image");
        expect(fetch.identity).toBe(IDENTITY_A);
      });
    },
  );

  it("keeps the producing snapshot after an overwrite without rerunning the query", async () => {
    await withSnapshotServer(
      "overwrite",
      async (table, requests) => {
        const rows = await table.query().toArray({ blobMode: "bytes" });
        expect(rows[0].label).toBe("A");
        expect(Buffer.from(rows[0].image).toString()).toBe("A");
        expect(queryRequests(requests)).toHaveLength(1);
        const [fetch] = fetchRequests(requests);
        expect(fetch.body.version).toBe(1);
        expect(fetch.identity).toBe(IDENTITY_A);
        expect(await table.version()).toBe(2);
        const [latest] = await table.query().toArray({ blobMode: "bytes" });
        expect(latest.label).toBe("B");
        expect(Buffer.from(latest.image).toString()).toBe("B");
        expect(fetchRequests(requests)[1].identity).toBe(IDENTITY_B);
      },
      { advanceQueries: true },
    );
  });

  it.each<ServerOptions>([
    { queryVersion: null },
    { queryVersion: "0" },
    { queryVersion: "18446744073709551616" },
    { queryVersion: "invalid" },
    { queryIdentity: null },
    { queryIdentity: "v2:unsupported" },
  ])("refuses bytes without authoritative capability %j", async (options) => {
    await withSnapshotServer(
      "overwrite",
      async (table, requests) => {
        const [descriptor] = await table.query().toArray();
        expect(descriptor.image.size).toBe(1n);
        await expect(
          table.query().toArray({ blobMode: "bytes" }),
        ).rejects.toThrow(/blobMode.*bytes.*requires.*snapshot/);
        expect(fetchRequests(requests)).toHaveLength(0);
      },
      options,
    );
  });

  it("rejects a serving node without the snapshot echo", async () => {
    await withSnapshotServer(
      "overwrite",
      async (table) => {
        await expect(
          table.query().toArray({ blobMode: "bytes" }),
        ).rejects.toThrow(/snapshot identity/);
      },
      { echo: null },
    );
  });

  it("keeps aliases, NULL, empty bytes, and explicit row IDs", async () => {
    await withSnapshotServer(
      "overwrite",
      async (table, requests) => {
        const query = table
          .query()
          .select({ caption: "label", "photo.image": "image" });
        const rows = await query.withRowId().toArray({ blobMode: "bytes" });
        expect(rows.map((row) => row._rowid)).toEqual([0n, 1n, 2n]);
        expect(
          rows.map((row) =>
            row["photo.image"] === null
              ? null
              : Buffer.from(row["photo.image"]).toString(),
          ),
        ).toEqual(["A", null, ""]);
        expect(rows[0].caption).toBe("A");
        expect(fetchRequests(requests)[0].body.column).toBe("image");
        const [descriptor] = await query.toArray();
        expect(descriptor["photo.image"].size).toBe(1n);
      },
      { values: true },
    );
  });

  it("preserves an empty bytes schema without fetching payloads", async () => {
    await withSnapshotServer(
      "overwrite",
      async (table, requests) => {
        const result = await table.query().toArrow({ blobMode: "bytes" });
        expect(result.numRows).toBe(0);
        expect(DataType.isLargeBinary(result.getChild("image")!.type)).toBe(
          true,
        );
        expect(result.schema.fields.map((field) => field.name)).toEqual([
          "label",
          "image",
        ]);
        expect(fetchRequests(requests)).toHaveLength(0);
      },
      { empty: true },
    );
  });

  it("passes the query version without rounding through a JS number", async () => {
    await withSnapshotServer(
      "overwrite",
      async (table, requests) => {
        await table.query().toArray({ blobMode: "bytes" });
        expect(fetchRequests(requests)[0].raw).toContain(
          '"version":9007199254740993',
        );
      },
      { queryVersion: "9007199254740993" },
    );
  });

  it.each(["vector", "hybrid"])(
    "materializes the actual %s query entry point",
    async (kind) => {
      await withSnapshotServer("overwrite", async (table, requests) => {
        const query = table.query().nearestTo([0.1, 0.2]);
        if (kind === "hybrid") query.fullTextSearch("A");
        const rows = await query.toArray({ blobMode: "bytes" });
        expect(Buffer.from(rows[0].image).toString()).toBe("A");
        expect(fetchRequests(requests)[0].identity).toBe(IDENTITY_A);
        expect(queryRequests(requests)).toHaveLength(kind === "hybrid" ? 2 : 1);
      });
    },
  );

  it("allows scalar-only bytes queries on older servers", async () => {
    await withSnapshotServer(
      "overwrite",
      async (table, requests) => {
        const [row] = await table
          .query()
          .select(["label"])
          .toArray({ blobMode: "bytes" });
        expect(row.label).toBe("A");
        expect(Object.keys(row)).toEqual(["label"]);
        expect(fetchRequests(requests)).toHaveLength(0);
        // Row IDs are requested before the executed output schema is known,
        // then hidden from scalar-only results without requiring capability.
        expect(queryRequests(requests)[0].body.with_row_id).toBe(true);
      },
      { queryIdentity: null, queryVersion: null },
    );
  });

  it.each([false, true])(
    "preserves snapshot proof across a custom reranker (empty: %s)",
    async (empty) => {
      await withSnapshotServer(
        "overwrite",
        async (table, requests) => {
          const result = await table
            .query()
            .nearestTo([0.1, 0.2])
            .fullTextSearch("A")
            .rerank(keepFtsReranker)
            .toArrow({ blobMode: "bytes" });
          expect(result.numRows).toBe(empty ? 0 : 1);
          expect(DataType.isLargeBinary(result.getChild("image")!.type)).toBe(
            true,
          );
          expect(fetchRequests(requests)).toHaveLength(empty ? 0 : 1);
        },
        { empty },
      );
    },
  );

  it("does not trust a custom reranker's first-leg snapshot when the legs disagree", async () => {
    await withSnapshotServer(
      "overwrite",
      async (table, requests) => {
        const query = table
          .query()
          .nearestTo([0.1, 0.2])
          .fullTextSearch("A")
          .rerank(keepFtsReranker);
        await expect(query.toArray({ blobMode: "bytes" })).rejects.toThrow(
          /snapshot/,
        );
        expect(fetchRequests(requests)).toHaveLength(0);
      },
      { otherLegIdentity: IDENTITY_B },
    );
  });

  it.each<ServerOptions>([
    { otherLegIdentity: IDENTITY_B },
    { otherLegIdentity: null },
    { otherLegVersion: "2" },
  ])(
    "refuses hybrid bytes when its actual query legs disagree %j",
    async (options) => {
      await withSnapshotServer(
        "overwrite",
        async (table, requests) => {
          const query = table.query().nearestTo([0.1, 0.2]).fullTextSearch("A");
          await expect(query.toArray({ blobMode: "bytes" })).rejects.toThrow(
            /snapshot/,
          );
          expect(fetchRequests(requests)).toHaveLength(0);
          expect(await query.toArray()).toHaveLength(1);
        },
        options,
      );
    },
  );
});
