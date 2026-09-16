// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import * as http from "http";
import { Catalog, connectCatalog } from "../lancedb";

type RecordedRequest = {
  url: string;
  headers: http.IncomingHttpHeaders;
  body: Record<string, unknown>;
};

async function withCatalog(
  responses: [number, unknown][],
  callback: (catalog: Catalog, requests: RecordedRequest[]) => Promise<void>,
) {
  const requests: RecordedRequest[] = [];
  const server = http.createServer(async (req, res) => {
    const chunks: Buffer[] = [];
    for await (const chunk of req) chunks.push(Buffer.from(chunk));
    const body = Buffer.concat(chunks).toString();
    requests.push({
      url: req.url ?? "",
      headers: req.headers,
      body: body ? JSON.parse(body) : {},
    });
    const [status, response] = responses.shift() ?? [
      500,
      { error: "Unexpected request" },
    ];
    res.writeHead(status, { "content-type": "application/json" });
    res.end(status === 204 ? undefined : JSON.stringify(response));
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  if (!address || typeof address === "string")
    throw new Error("Missing server address");
  try {
    const catalog = await connectCatalog(`http://127.0.0.1:${address.port}`, {
      apiKey: "secret",
      sqlHostOverride: "grpc+tls://sql.example.com:10026",
      clientConfig: {
        extraHeaders: {
          "X-LanceDB-Database": "wrong-static",
          "X-LanceDB-Database-Prefix": "wrong",
        },
      },
      headerProvider: () => ({
        "X-LanceDB-Database": "wrong-dynamic",
        "X-LanceDB-Database-Prefix": "wrong",
        authorization: "Bearer refreshed",
      }),
    });
    await callback(catalog, requests);
    expect(responses).toHaveLength(0);
  } finally {
    server.closeAllConnections();
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
}

describe("remote catalog", () => {
  it("uses root namespace routes and preserves independent database scope", async () => {
    await withCatalog(
      [
        [204, null],
        [200, { tables: [] }],
        [200, {}],
        [200, { tables: [] }],
        [200, { tables: [] }],
        // biome-ignore lint/style/useNamingConvention: server wire format
        [200, { namespaces: ["team/search"], page_token: "next" }],
        [204, null],
      ],
      async (catalog, requests) => {
        const first = await catalog.createDatabase("team/search", {
          existOk: true,
        });
        expect(await first.tableNames()).toEqual([]);
        const second = await catalog.connectDatabase("other");
        expect(await second.tableNames()).toEqual([]);
        expect(await first.tableNames()).toEqual([]);
        expect(
          await catalog.listDatabases({ limit: 1, pageToken: "a/b" }),
        ).toEqual({ databases: ["team/search"], pageToken: "next" });
        await catalog.dropDatabase("team/search", { ignoreMissing: true });
        expect(requests[0].url).toBe("/v1/namespace/team%2Fsearch/create");
        expect(requests[0].body).toEqual({ mode: "ExistOk" });
        expect(requests[5].url).toBe(
          "/v1/namespace/%24/list?limit=1&page_token=a%2Fb",
        );
        expect(requests[6].body).toEqual({
          mode: "Skip",
          behavior: "Restrict",
        });
        for (const [i, request] of requests.entries()) {
          expect(request.headers["x-lancedb-database"]).toBe(
            i === 1 || i === 4 ? "team/search" : i === 3 ? "other" : undefined,
          );
          expect(request.headers["x-lancedb-database-prefix"]).toBeUndefined();
          expect(request.headers.authorization).toBe("Bearer refreshed");
        }
      },
    );
  });

  it("propagates lifecycle errors and sends restricted drops", async () => {
    await withCatalog(
      [
        [404, {}],
        [409, {}],
        [400, {}],
        [404, {}],
      ],
      async (catalog, requests) => {
        await expect(catalog.connectDatabase("missing")).rejects.toThrow(
          "missing",
        );
        await expect(catalog.createDatabase("exists")).rejects.toThrow(
          "exists",
        );
        await expect(catalog.dropDatabase("full")).rejects.toThrow();
        await catalog.dropDatabase("missing", { ignoreMissing: true });
        expect(requests[2].body).toEqual({
          mode: "Fail",
          behavior: "Restrict",
        });
      },
    );
  });

  it("validates endpoints and pagination", async () => {
    await expect(connectCatalog("/tmp/catalog")).rejects.toThrow();
    const catalog = await connectCatalog("http://127.0.0.1:1");
    for (const limit of [0, -1, 1.5, 2147483648]) {
      await expect(catalog.listDatabases({ limit })).rejects.toThrow("limit");
    }
    await expect(catalog.connectDatabase("a$b")).rejects.toThrow(
      "Invalid database name",
    );
  });
});
