// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import * as fs from "fs";
import * as http from "http";
import * as os from "os";
import * as path from "path";
import { OAuthConfig, OAuthFlowType, OAuthSession } from "../lancedb/oauth";

function tempCacheDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), "lancedb-oauth-cache-"));
}

function deviceConfig(issuerUrl: string, cacheDir: string): OAuthConfig {
  return {
    issuerUrl,
    clientId: "client-id",
    scopes: ["openid"],
    flow: OAuthFlowType.DeviceCode,
    tokenCache: { cacheDir },
  };
}

describe("OAuthSession", () => {
  beforeAll(() => {
    // Point the Rust browser helper at a no-op so device-flow logins never
    // open a real browser window during tests.
    process.env.LANCEDB_OAUTH_BROWSER = "/usr/bin/true";
  });

  it("reports an absent session and logout is idempotent", async () => {
    const cacheDir = tempCacheDir();
    const session = new OAuthSession(
      deviceConfig("https://issuer.example.com", cacheDir),
    );

    const status = await session.status();
    expect(status.refreshable).toBe(false);
    expect(status.issuerUrl).toBe("https://issuer.example.com");
    expect(status.clientId).toBe("client-id");
    expect(status.scopes).toEqual(["openid"]);
    expect(status.flow).toBe("device_code");
    expect(status.obtainedAt).toBeUndefined();

    const logout = await session.logout();
    expect(logout.removed).toBe(false);
  });

  it("requires token cache options", () => {
    const config: OAuthConfig = {
      issuerUrl: "https://issuer.example.com",
      clientId: "client-id",
      scopes: ["openid"],
      flow: OAuthFlowType.DeviceCode,
    };
    expect(() => new OAuthSession(config)).toThrow(/token/);
  });

  it("rejects azure managed identity persistence", () => {
    const config: OAuthConfig = {
      issuerUrl: "https://login.microsoftonline.com/tenant/v2.0",
      clientId: "app-id",
      scopes: ["api://app/.default"],
      flow: OAuthFlowType.AzureManagedIdentity,
      tokenCache: { cacheDir: tempCacheDir() },
    };
    expect(() => new OAuthSession(config)).toThrow(/AzureManagedIdentity/);
  });

  it.each([
    {},
    {
      resource: "https://api.example.com/a?x=1&y=two",
      audience: "audience + & / ü",
    },
  ])(
    "logs in via device flow with target %j, caches, and logs out",
    async (target) => {
      const server = new MockIdp();
      await server.start();
      try {
        const cacheDir = tempCacheDir();
        const issuerUrl = server.issuerUrl();

        const config = { ...deviceConfig(issuerUrl, cacheDir), ...target };
        const session = new OAuthSession(config);
        const status = await session.login();
        expect(status.refreshable).toBe(true);
        expect(status.resource).toBe(config.resource);
        expect(status.audience).toBe(config.audience);
        expect(status.obtainedAt).toBeGreaterThan(0);
        expect(server.state.deviceAuthorizations).toBe(1);

        // An independent session (a fresh "process") sees the cached login.
        const other = new OAuthSession(config);
        const cached = await other.status();
        expect(cached.refreshable).toBe(true);

        const logout = await other.logout();
        expect(logout.removed).toBe(true);
        const again = await session.logout();
        expect(again.removed).toBe(false);
        expect((await session.status()).refreshable).toBe(false);

        // Only the initial login used the interactive device flow.
        expect(server.state.deviceAuthorizations).toBe(1);
        expect(server.state.refreshGrants).toBe(0);
        expect(server.requests).toHaveLength(2);
        for (const params of server.requests) {
          expect(params.getAll("resource")).toEqual(
            config.resource === undefined ? [] : [config.resource],
          );
          expect(params.getAll("audience")).toEqual(
            config.audience === undefined ? [] : [config.audience],
          );
        }
      } finally {
        server.close();
      }
    },
    15000,
  );
});

/** Mock IdP with discovery, device authorization, and rotating refresh. */
class MockIdp {
  readonly requests: URLSearchParams[] = [];
  readonly state = {
    deviceAuthorizations: 0,
    refreshGrants: 0,
    accessTokensIssued: 0,
    currentRefresh: null as string | null,
  };
  private server?: http.Server;
  private port = 0;

  issuerUrl(): string {
    return `http://127.0.0.1:${this.port}`;
  }

  async start(): Promise<void> {
    const server = http.createServer((req, res) => {
      const chunks: Buffer[] = [];
      req.on("data", (chunk) => chunks.push(chunk));
      req.on("end", () => {
        const body = Buffer.concat(chunks).toString();
        const params = new URLSearchParams(body);
        this.handle(req.url ?? "", params, res);
      });
    });
    await new Promise<void>((resolve) => {
      server.listen(0, "127.0.0.1", () => resolve());
    });
    const address = server.address();
    if (address && typeof address === "object") {
      this.port = address.port;
    }
    this.server = server;
  }

  private handle(
    url: string,
    params: URLSearchParams,
    res: http.ServerResponse,
  ): void {
    const respond = (status: number, payload: unknown): void => {
      const body = JSON.stringify(payload);
      res.writeHead(status, {
        "Content-Type": "application/json",
        "Content-Length": Buffer.byteLength(body),
      });
      res.end(body);
    };

    if (url === "/.well-known/openid-configuration") {
      respond(200, {
        // biome-ignore lint/style/useNamingConvention: OAuth wire format
        token_endpoint: `${this.issuerUrl()}/token`,
        // biome-ignore lint/style/useNamingConvention: OAuth wire format
        device_authorization_endpoint: `${this.issuerUrl()}/device`,
      });
      return;
    }

    if (url === "/device" || url === "/token") {
      this.requests.push(params);
    }

    if (url === "/device") {
      this.state.deviceAuthorizations += 1;
      respond(200, {
        // biome-ignore lint/style/useNamingConvention: OAuth wire format
        device_code: "device-code",
        // biome-ignore lint/style/useNamingConvention: OAuth wire format
        user_code: "ABCD-EFGH",
        // biome-ignore lint/style/useNamingConvention: OAuth wire format
        verification_uri: `${this.issuerUrl()}/verify`,
        // biome-ignore lint/style/useNamingConvention: OAuth wire format
        expires_in: 60,
        interval: 1,
      });
      return;
    }

    if (url === "/token") {
      const grantType = params.get("grant_type") ?? "";
      if (grantType === "refresh_token") {
        this.state.refreshGrants += 1;
        if (params.get("refresh_token") !== this.state.currentRefresh) {
          respond(400, { error: "invalid_grant" });
          return;
        }
      } else if (!grantType.includes("device_code")) {
        respond(400, { error: "unsupported_grant_type" });
        return;
      }
      this.state.accessTokensIssued += 1;
      const number = this.state.accessTokensIssued;
      const refresh = `refresh-${number}`;
      this.state.currentRefresh = refresh;
      respond(200, {
        // biome-ignore lint/style/useNamingConvention: OAuth wire format
        access_token: `access-${number}`,
        // biome-ignore lint/style/useNamingConvention: OAuth wire format
        refresh_token: refresh,
        // biome-ignore lint/style/useNamingConvention: OAuth wire format
        expires_in: 3600,
      });
      return;
    }

    respond(404, {});
  }

  close(): void {
    this.server?.close();
  }
}
