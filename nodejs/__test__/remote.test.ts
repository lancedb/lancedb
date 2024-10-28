// Copyright 2024 Lance Developers.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
});
