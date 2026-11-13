// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import { createHash } from "crypto";
import { getEventListeners } from "events";
import { createReadStream } from "fs";
import * as http from "http";
import { AddressInfo } from "net";
import {
  Table as ArrowTable,
  Binary,
  Field,
  LargeBinary,
  Struct,
  Uint8,
  Uint32,
  Uint64,
  Utf8,
  tableToIPC,
  vectorFromArray,
} from "apache-arrow";
import { BlobRangeRequest, Connection, Table, connect } from "../lancedb";

jest.setTimeout(30000);

const IDENTITY = `v1:${"a".repeat(64)}`;
const MIB = 1024 * 1024;
type Range = {
  // biome-ignore lint/style/useNamingConvention: Matches the server JSON contract.
  row_id: number;
  offset: number;
  length: number;
};
type Request = {
  column: string;
  branch: string | null;
  version: number | null;
  ranges: Range[];
  identity?: string;
};

function payload(row: number, offset: number, length: number, version: number) {
  const bytes = Buffer.alloc(length);
  for (let i = 0; i < length; i++) {
    bytes[i] = (row * 17 + ((offset + i) % 251) + version) % 251;
  }
  return bytes;
}

async function withRangeServer(
  run: (db: Connection, calls: Request[], paths: string[]) => Promise<void>,
  fault?: string | number,
  capability: string | null = "1",
  onStall?: () => void,
) {
  let takes = 0;
  const calls: Request[] = [];
  const paths: string[] = [];
  const server = http.createServer((req, res) => {
    req.setTimeout(15000);
    paths.push(req.url ?? "");
    if (
      req.url === "/v1/table/test/describe/" ||
      req.url === "/v1/table/test/branches/list/"
    ) {
      res.writeHead(200, {
        "Content-Type": "application/json",
        "phalanx-version": "1.13.0-beta.0",
        ...(req.url.endsWith("/describe/") && capability !== null
          ? { "x-lancedb-blob-ranges": capability }
          : {}),
      });
      const response = req.url.endsWith("branches/list/")
        ? {
            branches: {
              experiment: {
                parentBranch: null,
                parentVersion: 1,
                createAt: 1,
                manifestSize: 1,
              },
            },
          }
        : { version: 8, schema: { fields: [] } };
      res.end(JSON.stringify(response));
      return;
    }
    if (capability !== "1" && req.method === "GET") {
      const url = new URL(req.url!, "http://localhost");
      expect(url.searchParams.get("branch")).toBe("experiment");
      expect(url.searchParams.get("version")).toBe("3");
      const [start, end] = req.headers.range!.slice(6).split("-").map(Number);
      const bytes = Buffer.from("alpha").subarray(start, end + 1);
      res
        .writeHead(206, {
          "Content-Range": `bytes ${start}-${end}/5`,
          "Content-Length": bytes.length,
          "x-lancedb-version": "3",
        })
        .end(bytes);
      return;
    }
    expect(req.method).toBe("POST");
    expect(req.url).toBe(
      capability === "1"
        ? "/v1/table/test/fetch_blob_ranges/"
        : "/v1/table/test/query/",
    );
    const chunks: Buffer[] = [];
    let size = 0;
    req.on("data", (chunk: Buffer) => {
      size += chunk.length;
      expect(size).toBeLessThanOrEqual(256 * 1024);
      chunks.push(chunk);
    });
    req.on("end", () => {
      if (capability !== "1") {
        const body = JSON.parse(Buffer.concat(chunks).toString()) as {
          branch: string;
          version: number;
          filter: string;
        };
        expect(body.branch).toBe("experiment");
        expect(body.version).toBe(3);
        takes++;
        const ids = (body.filter.match(/\d+/g) ?? [])
          .map(Number)
          .filter((id) => [10, 20, 40].includes(id))
          .reverse();
        const descriptor = new Struct([
          new Field("kind", new Uint8(), false),
          new Field("position", new Uint64(), false),
          new Field("size", new Uint64(), false),
          new Field("blob_id", new Uint32(), false),
          new Field("blob_uri", new Utf8(), false),
        ]);
        const arrow = new ArrowTable({
          image: vectorFromArray(
            ids.map((id) =>
              id === 20
                ? null
                : {
                    kind: 0,
                    position: 0n,
                    size: id === 40 ? 0n : 5n,
                    // biome-ignore lint/style/useNamingConvention: Arrow descriptor field.
                    blob_id: 0,
                    // biome-ignore lint/style/useNamingConvention: Arrow descriptor field.
                    blob_uri: "",
                  },
            ),
            descriptor,
          ),
          _rowid: vectorFromArray(ids.map(BigInt), new Uint64()),
        });
        expect(takes).toBeGreaterThan(0);
        res
          .writeHead(200, {
            "Content-Type": "application/vnd.apache.arrow.stream",
            "x-lancedb-version": "3",
          })
          .end(tableToIPC(arrow, "stream"));
        return;
      }
      const body = JSON.parse(Buffer.concat(chunks).toString()) as Request;
      body.identity = req.headers["x-lancedb-query-snapshot"] as
        | string
        | undefined;
      calls.push(body);
      expect(body.column).toBe("image");
      expect(body.branch).toBe("experiment");
      expect(body.ranges.length).toBeLessThanOrEqual(1024);
      expect(
        body.ranges.reduce((sum, range) => sum + range.length, 0),
      ).toBeLessThanOrEqual(64 * MIB);
      const version = body.version ?? (calls.length === 1 ? 7 : 8);
      const failure =
        calls.length > 1 || String(fault).startsWith("first-")
          ? fault
          : undefined;
      if (failure === "recreated" || failure === "gone") {
        res
          .writeHead(failure === "recreated" ? 400 : 404)
          .end("query snapshot identity mismatch: source gone or replaced");
        return;
      }
      if (failure === "stall" && calls.length === 2) {
        res.writeHead(200, {
          "Content-Type": "application/vnd.apache.arrow.stream",
          "x-lancedb-version": "7",
          "x-lancedb-query-snapshot": IDENTITY,
        });
        res.flushHeaders();
        onStall?.();
        return;
      }
      if (typeof failure === "number") {
        res.writeHead(failure).end("ordinary server failure");
        return;
      }
      if (failure === "disconnect") {
        req.socket.destroy();
        return;
      }
      let values = body.ranges.map((range) =>
        range.row_id === 20
          ? null
          : payload(range.row_id, range.offset, range.length, version),
      );
      if (failure === "count") values = [];
      if (failure === "length") values = [Buffer.alloc(0)];
      const arrow = new ArrowTable({
        [failure === "column" ? "wrong" : "value"]: vectorFromArray(
          values,
          failure === "type" ? new Binary() : new LargeBinary(),
        ),
      });
      let ipc = tableToIPC(arrow, "stream");
      if (failure === "eos") ipc = ipc.subarray(0, ipc.length - 8);
      if (failure === "body") ipc = ipc.subarray(0, Math.floor(ipc.length / 2));
      res.writeHead(200, {
        "Content-Type": "application/vnd.apache.arrow.stream",
        connection: "close",
        "Content-Length": ipc.length + (failure === "transport" ? 10 : 0),
        ...(failure === "version" ? {} : { "x-lancedb-version": version }),
        ...(["missing-identity", "first-missing-identity"].includes(
          String(failure),
        )
          ? {}
          : {
              "x-lancedb-query-snapshot":
                failure === "wrong-identity"
                  ? `v1:${"b".repeat(64)}`
                  : failure === "first-invalid-identity"
                    ? "broken"
                    : IDENTITY,
            }),
      });
      // Fragment the generated IPC payload across actual HTTP writes.
      for (let i = 0; i < ipc.length; i += 8192)
        res.write(ipc.subarray(i, i + 8192));
      res.end();
    });
  });
  server.listen(0, "127.0.0.1");
  await new Promise<void>((resolve) => server.once("listening", resolve));
  const port = (server.address() as AddressInfo).port;
  const db = await connect("db://dev", {
    apiKey: "fake",
    hostOverride: `http://127.0.0.1:${port}`,
    clientConfig: {
      retryConfig: { retries: 2, connectRetries: 2, readRetries: 2 },
      blobRequestConcurrency: 1,
      timeoutConfig: { timeout: 20 },
    },
  });
  try {
    await run(db, calls, paths);
  } finally {
    await new Promise<void>((resolve) => {
      server.closeAllConnections();
      server.close(() => resolve());
    });
  }
}

async function open(db: Connection, version?: number): Promise<Table> {
  return db.openTable("test", [], { branch: "experiment", version });
}

describe("fetchBlobRanges", () => {
  it.each([null, "0", "2"])(
    "uses old routes before data requests for capability %s",
    async (capability) => {
      await withRangeServer(
        async (db, calls, paths) => {
          // Reopening exercises both the initial and cached database describe paths.
          for (let i = 0; i < 2; i++) {
            const table = await open(db, 3);
            const requests = [
              { rowId: 10n, offset: 1n, length: 3n },
              { rowId: 20n, offset: 0n, length: 1n },
              { rowId: 40n, offset: 0n, length: 0n },
              { rowId: 10n, offset: 5n, length: 0n },
              { rowId: 10n, offset: 1n, length: 3n },
            ];
            expect(await table.fetchBlobRanges("image", requests)).toEqual([
              Buffer.from("lph"),
              null,
              Buffer.alloc(0),
              Buffer.alloc(0),
              Buffer.from("lph"),
            ]);
            for (const row of [
              { rowId: 10n, offset: 4n, length: 2n },
              { rowId: 99n, offset: 0n, length: 1n },
            ])
              await expect(
                table.fetchBlobRanges("image", [row]),
              ).rejects.toThrow();
            const before = paths.length;
            expect(await table.fetchBlobRanges("image", [])).toEqual([]);
            expect(paths).toHaveLength(before);
          }
          expect(calls).toHaveLength(0);
          expect(paths.some((path) => path.includes("fetch_blob_ranges"))).toBe(
            false,
          );
        },
        undefined,
        capability,
      );
    },
  );

  it("renegotiates advertised support when reopening a cached table", async () => {
    await withRangeServer(async (db, calls) => {
      for (let i = 0; i < 2; i++) {
        const table = await open(db);
        expect(
          await table.fetchBlobRanges("image", [
            { rowId: 10n, offset: 0n, length: 1n },
          ]),
        ).toEqual([payload(10, 0, 1, i === 0 ? 7 : 8)]);
      }
      expect(calls).toHaveLength(2);
    });
  });

  it("cancels while waiting for a later response body and releases the only permit", async () => {
    let markStalled!: () => void;
    const stalled = new Promise<void>((resolve) => {
      markStalled = resolve;
    });
    await withRangeServer(
      async (db, calls) => {
        const table = await open(db);
        const controller = new AbortController();
        const pending = table.fetchBlobRanges(
          "image",
          Array.from({ length: 2049 }, () => ({
            rowId: 10n,
            offset: 0n,
            length: 1n,
          })),
          { signal: controller.signal },
        );
        const rejected = expect(pending).rejects.toMatchObject({
          name: "AbortError",
        });
        await stalled;
        controller.abort();
        await rejected;
        expect(getEventListeners(controller.signal, "abort")).toHaveLength(0);
        expect(calls).toHaveLength(2);
        expect(
          await table.fetchBlobRanges("image", [
            { rowId: 10n, offset: 0n, length: 1n },
          ]),
        ).toEqual([payload(10, 0, 1, 8)]);
        expect(calls).toHaveLength(3);
      },
      "stall",
      "1",
      () => markStalled(),
    );
  });
  it.each([undefined, 3])(
    "preserves order, nulls, duplicates and snapshot (checkout %s)",
    async (version) => {
      await withRangeServer(async (db, calls, paths) => {
        const table = await open(db, version);
        const requests: BlobRangeRequest[] = Array.from(
          { length: 1025 },
          (_, i) => ({
            rowId: i % 9 === 0 ? 20 : 10 + (i % 7),
            offset: BigInt(i),
            length: BigInt(i % 4),
          }),
        );
        requests.push(
          { rowId: 10n, offset: 0n, length: 0n },
          { rowId: 10n, offset: 2n, length: 3n },
          { rowId: 10n, offset: 2n, length: 3n },
        );
        const result = await table.fetchBlobRanges("image", requests);
        expect(result).toEqual(
          requests.map((range) =>
            Number(range.rowId) === 20
              ? null
              : payload(
                  Number(range.rowId),
                  Number(range.offset),
                  Number(range.length),
                  version ?? 7,
                ),
          ),
        );
        expect(calls.map((body) => body.version)).toEqual([
          version ?? null,
          version ?? 7,
        ]);
        expect(
          await table.fetchBlobRanges("image", [
            { rowId: 10n, offset: 0n, length: 1n },
          ]),
        ).toEqual([payload(10, 0, 1, version ?? 8)]);
        expect(calls[2].version).toBe(version ?? null);
        expect(
          paths.filter((path) => path.endsWith("fetch_blob_ranges/")),
        ).toHaveLength(3);
      });
    },
  );

  it("partitions the byte budget and reassembles one range larger than 64 MiB", async () => {
    await withRangeServer(async (db, calls) => {
      const table = await open(db);
      const requests = [
        { rowId: 10n, offset: 3n, length: BigInt(65 * MIB) },
        { rowId: 30n, offset: 7n, length: 2n },
      ];
      const result = await table.fetchBlobRanges("image", requests);
      for (const [i, range] of requests.entries()) {
        expect(result[i]?.length).toBe(Number(range.length));
        expect(
          createHash("sha256")
            .update(result[i] as Buffer)
            .digest("hex"),
        ).toBe(
          createHash("sha256")
            .update(
              payload(
                Number(range.rowId),
                Number(range.offset),
                Number(range.length),
                7,
              ),
            )
            .digest("hex"),
        );
      }
      expect(calls).toHaveLength(2);
      expect(calls.map((body) => body.version)).toEqual([null, 7]);
    });
  });

  it.each([
    "type",
    "column",
    "count",
    "length",
    "eos",
    "body",
    "transport",
    "version",
    "disconnect",
    401,
    403,
    404,
    429,
    500,
  ])("rejects the whole call after a later %s failure", async (fault) => {
    await withRangeServer(async (db, calls, paths) => {
      const table = await open(db);
      await expect(
        table.fetchBlobRanges(
          "image",
          Array.from({ length: 1025 }, () => ({
            rowId: 10n,
            offset: 0n,
            length: 1n,
          })),
        ),
      ).rejects.toThrow();
      expect(calls).toHaveLength(2);
      expect(
        paths.every(
          (path) =>
            path.endsWith("describe/") ||
            path.endsWith("branches/list/") ||
            path.endsWith("fetch_blob_ranges/"),
        ),
      ).toBe(true);
    }, fault);
  });

  it.each(["missing-identity", "wrong-identity", "recreated", "gone"])(
    "retains original identity through row and byte partitions (%s)",
    async (fault) => {
      for (const huge of [false, true]) {
        await withRangeServer(async (db, calls) => {
          const table = await open(db);
          const requests = huge
            ? [{ rowId: 10n, offset: 0n, length: BigInt(64 * MIB + 1) }]
            : Array.from({ length: 1025 }, () => ({
                rowId: 10n,
                offset: 0n,
                length: 1n,
              }));
          await expect(
            table.fetchBlobRanges("image", requests),
          ).rejects.toThrow();
          expect(calls.map((call) => call.identity)).toEqual([
            undefined,
            IDENTITY,
          ]);
          expect(calls.map((call) => call.version)).toEqual([null, 7]);
        }, fault);
      }
    },
  );

  it.each(["first-missing-identity", "first-invalid-identity"])(
    "requires a first authoritative bulk identity (%s)",
    async (fault) => {
      await withRangeServer(async (db, calls, paths) => {
        const table = await open(db);
        const before = paths.length;
        await expect(
          table.fetchBlobRanges(
            "image",
            Array.from({ length: 1025 }, () => ({
              rowId: 10n,
              offset: 0n,
              length: 1n,
            })),
          ),
        ).rejects.toThrow(/snapshot identity/);
        expect(calls).toHaveLength(1);
        expect(paths).toHaveLength(before + 1);
      }, fault);
    },
  );

  it("validates integers, overflow and cancellation and leaves empty calls at zero I/O", async () => {
    await withRangeServer(async (db, calls, paths) => {
      const table = await open(db);
      const before = paths.length;
      expect(await table.fetchBlobRanges("image", [])).toEqual([]);
      for (const request of [
        { rowId: -1, offset: 0n, length: 1n },
        { rowId: 1.5, offset: 0n, length: 1n },
        { rowId: Number.MAX_SAFE_INTEGER + 1, offset: 0n, length: 1n },
        { rowId: -1n, offset: 0n, length: 1n },
        { rowId: 1n << 64n, offset: 0n, length: 1n },
        { rowId: 10n, offset: -1n, length: 1n },
        { rowId: 10n, offset: 0n, length: -1n },
        { rowId: 10n, offset: 0n, length: 1n << 64n },
        { rowId: 20n, offset: (1n << 64n) - 1n, length: 1n },
      ]) {
        await expect(
          table.fetchBlobRanges("image", [request]),
        ).rejects.toThrow();
      }
      // Runtime type validation must also reject a number passed as an offset.
      await expect(
        table.fetchBlobRanges("image", [
          { rowId: 10n, offset: 0 as unknown as bigint, length: 1n },
        ]),
      ).rejects.toThrow();
      const controller = new AbortController();
      controller.abort(new Error("cancelled by caller"));
      await expect(
        table.fetchBlobRanges(
          "image",
          [{ rowId: 10n, offset: 0n, length: 1n }],
          { signal: controller.signal },
        ),
      ).rejects.toThrow("cancelled by caller");
      expect(paths).toHaveLength(before);
      expect(calls).toHaveLength(0);
    });
  });

  it("records the loaded native artifact", async () => {
    const artifacts = Object.keys(require.cache).filter(
      (path) => path.endsWith(".node") && path.includes("lancedb"),
    );
    expect(artifacts.length).toBeGreaterThan(0);
    for (const path of artifacts) {
      const hash = createHash("sha256");
      for await (const chunk of createReadStream(path)) hash.update(chunk);
      console.log(
        `loaded native artifact=${path} sha256=${hash.digest("hex")}`,
      );
    }
  });
});
