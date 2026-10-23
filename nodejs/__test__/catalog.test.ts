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
        const names = catalog.listDatabases({ pageLimit: 1, pageToken: "a/b" });
        expect(await names.next()).toEqual({
          done: false,
          value: "team/search",
        });
        expect(names.pageToken()).toBe("next");
        await catalog.dropDatabase("team/search", { ignoreMissing: true });
        expect(requests[0].url).toBe("/v1/namespace/team%2Fsearch/create");
        expect(requests[0].body).toEqual({ mode: "ExistOk" });
        expect(requests[5].url).toBe(
          "/v1/namespace/$/list?limit=1&page_token=a%2Fb",
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

  it("lazily traverses pages, including empty pages, and exposes cached state", async () => {
    await withCatalog(
      [
        // biome-ignore lint/style/useNamingConvention: server wire format
        [200, { namespaces: ["a", "b"], page_token: "empty" }],
        // biome-ignore lint/style/useNamingConvention: server wire format
        [200, { namespaces: [], page_token: "last" }],
        // biome-ignore lint/style/useNamingConvention: server wire format
        [200, { namespaces: ["c", "d"], page_token: "" }],
      ],
      async (catalog, requests) => {
        const names = catalog.listDatabases({ pageLimit: 2 });
        expect(names[Symbol.asyncIterator]()).toBe(names);
        expect(requests).toHaveLength(0);
        expect(names.numPageResults()).toBe(0);
        expect(names.pageToken()).toBeUndefined();
        expect(await names.next()).toEqual({ done: false, value: "a" });
        expect(names.numPageResults()).toBe(1);
        expect(names.pageToken()).toBe("empty");
        expect(await names.next()).toEqual({ done: false, value: "b" });
        expect(requests).toHaveLength(1);
        expect(await names.next()).toEqual({ done: false, value: "c" });
        expect(requests).toHaveLength(3);
        expect(names.pageToken()).toBeUndefined();
        expect(names.numPageResults()).toBe(1);
        const remaining = [];
        for await (const name of names) remaining.push(name);
        expect(remaining).toEqual(["d"]);
        expect(names.numPageResults()).toBe(0);
        expect(await names.next()).toEqual({ done: true, value: undefined });
        expect(requests.map((request) => request.url)).toEqual([
          "/v1/namespace/$/list?limit=2",
          "/v1/namespace/$/list?limit=2&page_token=empty",
          "/v1/namespace/$/list?limit=2&page_token=last",
        ]);
      },
    );
  });

  it("resumes from a saved token after draining the cache", async () => {
    await withCatalog(
      [
        // biome-ignore lint/style/useNamingConvention: server wire format
        [200, { namespaces: ["a", "b"], page_token: "next/token" }],
        [200, { namespaces: ["c"] }],
      ],
      async (catalog, requests) => {
        const names = catalog.listDatabases({
          pageToken: "start/token",
          pageLimit: 2,
        });
        expect(names.pageToken()).toBe("start/token");
        expect(await names.next()).toEqual({ done: false, value: "a" });
        while (names.numPageResults() > 0) await names.next();
        const resumed = catalog.listDatabases({
          pageToken: names.pageToken(),
          pageLimit: 2,
        });
        const results = [];
        for await (const name of resumed) results.push(name);
        expect(results).toEqual(["c"]);
        expect(requests.map((request) => request.url)).toEqual([
          "/v1/namespace/$/list?limit=2&page_token=start%2Ftoken",
          "/v1/namespace/$/list?limit=2&page_token=next%2Ftoken",
        ]);
      },
    );
  });

  it("terminates on errors and retains the failed request token", async () => {
    await withCatalog(
      [
        // biome-ignore lint/style/useNamingConvention: server wire format
        [200, { namespaces: ["a"], page_token: "retry" }],
        [400, {}],
        [200, { namespaces: ["b"] }],
      ],
      async (catalog, requests) => {
        const names = catalog.listDatabases();
        await names.next();
        await expect(names.next()).rejects.toThrow();
        expect(names.pageToken()).toBe("retry");
        expect(names.numPageResults()).toBe(0);
        expect(await names.next()).toEqual({ done: true, value: undefined });
        expect(requests).toHaveLength(2);
        const resumed = catalog.listDatabases({ pageToken: names.pageToken() });
        expect(await resumed.next()).toEqual({ done: false, value: "b" });
        expect(await resumed.next()).toEqual({ done: true, value: undefined });
      },
    );
  });

  it("handles an empty listing and concurrent advances", async () => {
    await withCatalog(
      [
        [200, { namespaces: [] }],
        [200, { namespaces: ["a", "b"] }],
      ],
      async (catalog, requests) => {
        const empty = catalog.listDatabases();
        expect(await empty.next()).toEqual({ done: true, value: undefined });
        expect(await empty.next()).toEqual({ done: true, value: undefined });
        const names = catalog.listDatabases();
        const pending = [names.next(), names.next(), names.next()];
        expect(names.numPageResults()).toBe(0);
        expect(names.pageToken()).toBeUndefined();
        const results = await Promise.all(pending);
        expect(
          results
            .filter((result) => !result.done)
            .map((result) => result.value)
            .sort(),
        ).toEqual(["a", "b"]);
        expect(results.filter((result) => result.done)).toHaveLength(1);
        expect(requests).toHaveLength(2);
      },
    );
  });

  it("validates endpoints and pagination", async () => {
    await expect(connectCatalog("/tmp/catalog")).rejects.toThrow();
    const catalog = await connectCatalog("http://127.0.0.1:1");
    for (const limit of [
      0,
      -1,
      1.5,
      2147483648,
      Number.NaN,
      Number.POSITIVE_INFINITY,
    ]) {
      expect(() => catalog.listDatabases({ pageLimit: limit })).toThrow(
        "limit",
      );
    }
  });

  it.each([
    "",
    ".",
    "..",
    "/db",
    "db/",
    "a//b",
    "a/./b",
    "a/../b",
    "a b",
    "a:b",
    "a$b",
    "a%b",
    "a?b",
    "a#b",
    "a\\b",
    "café",
    "a\nb",
  ])(
    "rejects invalid database name %j before sending requests",
    async (name) => {
      await withCatalog([], async (catalog, requests) => {
        for (const method of [
          "createDatabase",
          "connectDatabase",
          "dropDatabase",
        ] as const) {
          await expect(catalog[method](name)).rejects.toThrow(
            "Invalid database name",
          );
        }
        expect(requests).toEqual([]);
      });
    },
  );
});
