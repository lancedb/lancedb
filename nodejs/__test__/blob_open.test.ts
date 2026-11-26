// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import {
  type BlobDescriptor,
  type Connection,
  type Table,
  connect,
} from "../lancedb";

jest.setTimeout(60000);

const IDENTITY = `v1:${"a".repeat(64)}`;
const MIB = 1024 * 1024;

function descriptor(size: bigint): BlobDescriptor {
  return {
    kind: 2,
    position: 0n,
    size,
    // biome-ignore lint/style/useNamingConvention: Matches the descriptor.
    blob_id: 1,
    // biome-ignore lint/style/useNamingConvention: Matches the descriptor.
    blob_uri: "",
    source: {
      uri: "s3://fixture/table.lance",
      version: 46n,
      // biome-ignore lint/style/useNamingConvention: Matches the descriptor.
      committed_at_micros: 1795627189287568n,
      branch: null,
      column: "image",
      // biome-ignore lint/style/useNamingConvention: Matches the descriptor.
      row_reference: 0n,
      // biome-ignore lint/style/useNamingConvention: Matches the descriptor.
      row_is_address: false,
    },
  };
}

async function readBody(req: import("node:http").IncomingMessage) {
  const chunks: Buffer[] = [];
  for await (const chunk of req) {
    chunks.push(chunk as Buffer);
  }
  return Buffer.concat(chunks).toString();
}

async function withOpenServer(
  run: (
    db: Connection,
    table: Table,
    state: {
      directStatus: number;
      blobLocationsCalls: number;
      objectCalls: number;
      proxyCalls: number;
      lastLocationBody: Record<string, unknown> | undefined;
    },
  ) => Promise<void>,
  size: bigint = 10n * BigInt(MIB),
) {
  let port = 0;
  const state = {
    directStatus: 200,
    blobLocationsCalls: 0,
    objectCalls: 0,
    proxyCalls: 0,
    lastLocationBody: undefined as Record<string, unknown> | undefined,
  };
  const payload = Buffer.alloc(Number(size), 0x61);
  const server = createServer(async (req, res) => {
    const url = req.url ?? "";
    if (url === "/v1/table/test/describe/") {
      res.writeHead(200, {
        "content-type": "application/json",
        "phalanx-version": "1.15.0-beta.0",
        "x-lancedb-blob-locations": "1",
      });
      res.end(JSON.stringify({ version: 46, schema: { fields: [] } }));
      return;
    }
    if (url === "/v1/table/test/blob_locations/") {
      state.blobLocationsCalls++;
      const body = JSON.parse(await readBody(req)) as Record<string, unknown>;
      state.lastLocationBody = body;
      if (state.directStatus !== 200) {
        res.writeHead(state.directStatus).end("resolver unavailable");
        return;
      }
      res.writeHead(200, {
        "content-type": "application/json",
        "x-lancedb-version": "46",
        "x-lancedb-query-snapshot": IDENTITY,
      });
      res.end(
        JSON.stringify({
          sources: [
            {
              url: `http://127.0.0.1:${port}/object?signature=test`,
              // biome-ignore lint/style/useNamingConvention: Matches the resolver response.
              expires_at_unix_secs: 4102444800,
            },
          ],
          items: [
            {
              kind: "presigned",
              // biome-ignore lint/style/useNamingConvention: Matches the resolver response.
              source_index: 0,
              offset: 0,
              size: Number(size),
            },
          ],
        }),
      );
      return;
    }
    if (url.startsWith("/object")) {
      state.objectCalls++;
      const range = req.headers.range ?? "";
      const match = /^bytes=(\d+)-(\d+)$/.exec(range);
      if (match === null) {
        res.writeHead(416).end();
        return;
      }
      const start = Number(match[1]);
      const end = Number(match[2]);
      const body = payload.subarray(start, end + 1);
      res.writeHead(206, {
        "content-range": `bytes ${start}-${end}/${size}`,
        "content-length": body.length,
        "x-lancedb-version": "46",
        "x-lancedb-query-snapshot": IDENTITY,
        connection: "close",
      });
      res.end(body);
      return;
    }
    if (url.startsWith("/v1/table/test/blob/image/0/bytes")) {
      state.proxyCalls++;
      const range = req.headers.range ?? "";
      const match = /^bytes=(\d+)-(\d+)$/.exec(range);
      if (match === null) {
        res.writeHead(416).end();
        return;
      }
      const start = Number(match[1]);
      const end = Number(match[2]);
      const body = payload.subarray(start, end + 1);
      res.writeHead(206, {
        "content-range": `bytes ${start}-${end}/${size}`,
        "content-length": body.length,
        "x-lancedb-version": "46",
        "x-lancedb-query-snapshot": IDENTITY,
        connection: "close",
      });
      res.end(body);
      return;
    }
    res.writeHead(404).end(`unexpected request: ${req.method} ${url}`);
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  port = (server.address() as AddressInfo).port;
  const db = await connect("db://dev", {
    apiKey: "fake",
    hostOverride: `http://127.0.0.1:${port}`,
    clientConfig: { timeoutConfig: { timeout: 10 } },
  });
  const table = await db.openTable("test");
  try {
    await run(db, table, state);
  } finally {
    await new Promise<void>((resolve) => {
      server.closeAllConnections();
      server.close(() => resolve());
    });
  }
}

describe("openBlobFile", () => {
  it("opens a large descriptor through the direct resolver and reads bytes", async () => {
    await withOpenServer(async (_db, table, state) => {
      const handle = (await table.openBlobFile(descriptor(10n * BigInt(MIB))))!;
      expect(handle).not.toBeNull();
      // Below the per-read threshold the same handle stays on the proxy.
      expect(await handle.readRange(0n, 4n)).toEqual(Buffer.alloc(4, 0x61));
      expect(state.proxyCalls).toBe(1);
      expect(state.objectCalls).toBe(0);
      // The descriptor is at the inclusive 10 MiB boundary, so opening asks the
      // resolver for a presigned object-store URL.
      expect(state.blobLocationsCalls).toBe(1);
      expect(state.objectCalls).toBe(0);
      expect(state.lastLocationBody).toMatchObject({
        version: 46,
        // biome-ignore lint/style/useNamingConvention: Matches the resolver body.
        row_ids: [0],
        // biome-ignore lint/style/useNamingConvention: Matches the resolver body.
        source_uri: "s3://fixture/table.lance",
        // biome-ignore lint/style/useNamingConvention: Matches the resolver body.
        source_commit_micros: 1795627189287568,
        // biome-ignore lint/style/useNamingConvention: Matches the resolver body.
        row_is_address: false,
      });
      await handle.close();
    });
  });

  it("falls back to the proxy when the resolver returns 503", async () => {
    await withOpenServer(async (_db, table, state) => {
      state.directStatus = 503;
      const handle = (await table.openBlobFile(descriptor(10n * BigInt(MIB))))!;
      expect(handle).not.toBeNull();
      expect(await handle.readRange(0n, 4n)).toEqual(Buffer.alloc(4, 0x61));
      expect(state.blobLocationsCalls).toBe(1);
      expect(state.objectCalls).toBe(0);
      expect(state.proxyCalls).toBe(1);
      await handle.close();
    });
  });

  it("keeps descriptors below the direct threshold on the proxy path", async () => {
    await withOpenServer(async (_db, table, state) => {
      const handle = (await table.openBlobFile(descriptor(BigInt(MIB))))!;
      expect(await handle.readRange(0n, 4n)).toEqual(Buffer.alloc(4, 0x61));
      expect(state.blobLocationsCalls).toBe(0);
      expect(state.objectCalls).toBe(0);
      expect(state.proxyCalls).toBe(1);
      await handle.close();
    }, BigInt(MIB));
  });

  it("preserves nulls and order in openBlobFiles", async () => {
    await withOpenServer(async (_db, table, state) => {
      const handles = [];
      for await (const handle of table.openBlobFiles([
        null,
        descriptor(10n * BigInt(MIB)),
        null,
      ])) {
        handles.push(handle);
      }
      expect(handles[0]).toBeNull();
      expect(handles[1]).not.toBeNull();
      expect(handles[2]).toBeNull();
      expect(await handles[1]!.readRange(0n, 4n)).toEqual(
        Buffer.alloc(4, 0x61),
      );
      expect(state.blobLocationsCalls).toBe(1);
      await handles[1]!.close();
    });
  });
});
