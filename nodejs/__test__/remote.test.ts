// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import * as http from "http";
import { RequestListener } from "http";
import {
  ClientConfig,
  Connection,
  ConnectionOptions,
  TlsConfig,
  connect,
} from "../lancedb";

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
});
