// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import * as http from "http";
import { RequestListener } from "http";
import {
  ClientConfig,
  Connection,
  ConnectionOptions,
  NativeJsHeaderProvider,
  TlsConfig,
  connect,
} from "../lancedb";
import {
  HeaderProvider,
  OAuthHeaderProvider,
  StaticHeaderProvider,
} from "../lancedb/header";

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
});
