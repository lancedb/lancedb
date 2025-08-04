// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import * as http from "http";
import { RequestListener } from "http";
import { Connection, ConnectionOptions, connect } from "../lancedb";

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
    server.close();
  }
}

describe("remote connection", () => {
  it("should accept partial connection options", async () => {
    await connect("db://test", {
      apiKey: "fake",
      clientConfig: {
        timeoutConfig: { readTimeout: 5 },
        retryConfig: { retries: 2 },
      },
    });
  });

  it("should pass down apiKey and userAgent", async () => {
    await withMockDatabase(
      (req, res) => {
        expect(req.headers["x-api-key"]).toEqual("fake");
        expect(req.headers["user-agent"]).toEqual(
          `LanceDB-Node-Client/${process.env.npm_package_version}`,
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

  it("should pass on requested extra headers", async () => {
    await withMockDatabase(
      (req, res) => {
        expect(req.headers["foo"]).toEqual("1");
        expect(req.headers["bar"]).toEqual("2");
        expect(req.headers["baz"]).toEqual("3");
        expect(req.headers["x-log-attrs"]).toEqual("foo, bar, baz");

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
                "x-log-attrs": "foo, bar, baz",
                foo: "1",
                bar: "2",
                baz: "3",
            },
        },
      },
    );
  });
});
