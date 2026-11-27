// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import * as http from "http";
import { type ChildProcess, spawn } from "node:child_process";
import * as path from "path";

// The child is killed after 20s. Jest's default 5s test timeout would fire
// first, including on macOS CI while the debug addon loads.
jest.setTimeout(60_000);

// Jest replaces process.env with a sandbox. Assigning LANCEDB_API_KEY there
// does not reach Rust's std::env, so these cases run in a child whose real
// environment is set explicitly. See nodejs/jest.config.js.
const CHILD_SOURCE = `
"use strict";
const { connect } = require(process.env.LANCEDB_PACKAGE_ENTRY);
(async () => {
  const mode = process.env.LANCEDB_CONNECT_MODE;
  const port = process.env.LANCEDB_CONNECT_PORT;
  const options = {};
  if (port) {
    options.hostOverride = "http://127.0.0.1:" + port;
  }
  if (mode === "explicit") {
    options.apiKey = "explicit-key";
  } else if (mode === "oauth") {
    options.oauthConfig = {
      issuerUrl: process.env.LANCEDB_OAUTH_ISSUER,
      clientId: "client-id",
      clientSecret: "secret",
      scopes: ["scope"],
      flow: "client_credentials",
    };
  }
  let stage = "connect";
  try {
    const db = await connect("db://dev", options);
    stage = "tableNames";
    const names = await db.tableNames();
    const body = { ok: true, names };
    if (mode === "oauth") {
      body.open = db.isOpen();
    }
    process.stdout.write(JSON.stringify(body));
  } catch (error) {
    const message = error && error.message ? error.message : String(error);
    process.stdout.write(JSON.stringify({ ok: false, stage, message }));
  }
})();
`;

const packageRoot = path.resolve(__dirname, "..");

type ConnectResult = {
  ok: boolean;
  names?: string[];
  open?: boolean;
  stage?: "connect" | "tableNames";
  message?: string;
};

type ChildRun = {
  mode: "env" | "explicit" | "oauth";
  apiKey?: string;
  port?: number;
  issuerUrl?: string;
};

function childEnvironment(apiKey: string | undefined): NodeJS.ProcessEnv {
  const env: NodeJS.ProcessEnv = {};
  for (const [key, value] of Object.entries(process.env)) {
    if (typeof value === "string") {
      env[key] = value;
    }
  }
  delete env.LANCEDB_API_KEY;
  if (apiKey !== undefined) {
    env.LANCEDB_API_KEY = apiKey;
  }
  env.LANCEDB_PACKAGE_ENTRY = path.join(packageRoot, "dist", "index.js");
  return env;
}

function runChild(options: ChildRun): Promise<ConnectResult> {
  const env = childEnvironment(options.apiKey);
  env.LANCEDB_CONNECT_MODE = options.mode;
  if (options.port !== undefined) {
    env.LANCEDB_CONNECT_PORT = String(options.port);
  }
  if (options.issuerUrl !== undefined) {
    env.LANCEDB_OAUTH_ISSUER = options.issuerUrl;
  }

  return new Promise((resolve, reject) => {
    const child: ChildProcess = spawn(process.execPath, ["-e", CHILD_SOURCE], {
      cwd: packageRoot,
      env,
    });
    let stdout = "";
    let stderr = "";
    const timer = setTimeout(() => {
      child.kill("SIGKILL");
      reject(new Error(`connect child timed out\nstderr:\n${stderr}`));
    }, 20_000);
    child.stdout?.setEncoding("utf8");
    child.stderr?.setEncoding("utf8");
    child.stdout?.on("data", (chunk: string) => {
      stdout += chunk;
    });
    child.stderr?.on("data", (chunk: string) => {
      stderr += chunk;
    });
    child.on("error", (error) => {
      clearTimeout(timer);
      reject(error);
    });
    child.on("close", () => {
      clearTimeout(timer);
      const line = stdout
        .trim()
        .split("\n")
        .filter((entry) => entry.length > 0)
        .at(-1);
      if (!line) {
        reject(
          new Error(`connect child produced no output\nstderr:\n${stderr}`),
        );
        return;
      }
      try {
        resolve(JSON.parse(line) as ConnectResult);
      } catch (error) {
        reject(
          new Error(
            `connect child output was not JSON: ${stdout}\nstderr:\n${stderr}\n${String(error)}`,
          ),
        );
      }
    });
  });
}

function headerValue(value: string | string[] | undefined): string | undefined {
  return Array.isArray(value) ? value[0] : value;
}

function writeJson(res: http.ServerResponse, body: unknown) {
  res
    .writeHead(200, { "Content-Type": "application/json" })
    .end(JSON.stringify(body));
}

function writeRawJson(res: http.ServerResponse, body: string) {
  res.writeHead(200, { "Content-Type": "application/json" }).end(body);
}

async function withApiKeyServer(
  onKey: (apiKey: string | undefined) => void,
  options?: { oauthIssuer?: boolean },
): Promise<{ port: number; close: () => Promise<void> }> {
  const server = http.createServer((req, res) => {
    const address = server.address();
    const port =
      address !== null && typeof address !== "string" ? address.port : 0;
    const pathname = new URL(req.url ?? "/", "http://127.0.0.1").pathname;
    const respond = () => {
      if (
        options?.oauthIssuer &&
        pathname === "/.well-known/openid-configuration"
      ) {
        writeRawJson(
          res,
          `{"token_endpoint":"http://127.0.0.1:${port}/token"}`,
        );
        return;
      }
      if (options?.oauthIssuer && pathname === "/token") {
        writeRawJson(
          res,
          '{"access_token":"access-token","expires_in":3600,"token_type":"Bearer"}',
        );
        return;
      }
      onKey(headerValue(req.headers["x-api-key"]));
      writeJson(res, { tables: [] });
    };
    if (req.method === "POST") {
      req.on("data", () => {
        // Drain the client-credentials body before responding.
      });
      req.on("end", respond);
      return;
    }
    respond();
  });
  const port = await new Promise<number>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (address === null || typeof address === "string") {
        reject(new Error("mock server did not bind a TCP port"));
        return;
      }
      resolve(address.port);
    });
  });
  return {
    port,
    close: () =>
      new Promise((resolve) => {
        server.closeAllConnections();
        server.close(() => resolve());
      }),
  };
}

describe("LANCEDB_API_KEY remote fallback", () => {
  it("uses the env var when apiKey is omitted", async () => {
    const seen: Array<string | undefined> = [];
    const server = await withApiKeyServer((apiKey) => seen.push(apiKey));
    try {
      const result = await runChild({
        mode: "env",
        apiKey: "env-key",
        port: server.port,
      });
      expect(result).toEqual({ ok: true, names: [] });
      expect(seen.length).toBeGreaterThan(0);
      expect(new Set(seen)).toEqual(new Set(["env-key"]));
    } finally {
      await server.close();
    }
  });

  it("keeps an explicit apiKey when the env var is set", async () => {
    const seen: Array<string | undefined> = [];
    const server = await withApiKeyServer((apiKey) => seen.push(apiKey));
    try {
      const result = await runChild({
        mode: "explicit",
        apiKey: "env-key",
        port: server.port,
      });
      expect(result).toEqual({ ok: true, names: [] });
      expect(seen.length).toBeGreaterThan(0);
      expect(new Set(seen)).toEqual(new Set(["explicit-key"]));
    } finally {
      await server.close();
    }
  });

  it("ignores the env var when oauthConfig is set", async () => {
    const seen: Array<string | undefined> = [];
    const server = await withApiKeyServer((apiKey) => seen.push(apiKey), {
      oauthIssuer: true,
    });
    try {
      const result = await runChild({
        mode: "oauth",
        apiKey: "env-key",
        port: server.port,
        issuerUrl: `http://127.0.0.1:${server.port}`,
      });
      expect(result).toEqual({ ok: true, open: true, names: [] });
      expect(seen.length).toBeGreaterThan(0);
      expect(seen.every((apiKey) => apiKey === undefined)).toBe(true);
    } finally {
      await server.close();
    }
  });

  it("still requires an api key when the env var is unset", async () => {
    // Loopback hostOverride so a connect() regression cannot reach the
    // default service. stage is connect only when connect() itself rejects;
    // tableNames() runs after connect() resolves.
    const seen: Array<string | undefined> = [];
    const server = await withApiKeyServer((apiKey) => {
      seen.push(apiKey);
    });
    try {
      const result = await runChild({ mode: "env", port: server.port });
      expect(result).toEqual({
        ok: false,
        stage: "connect",
        message:
          "Invalid input, An api_key or oauth_config is required when connecting to LanceDB Enterprise or LanceDB Cloud",
      });
      expect(seen).toEqual([]);
    } finally {
      await server.close();
    }
  });
});
