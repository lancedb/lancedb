// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { type IncomingHttpHeaders, createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { type Connection, type FunctionProfileKind, connect } from "../lancedb";

type Response = { body: unknown; status?: number };

function metadata(kind: FunctionProfileKind, status: string) {
  return {
    ["profile_id"]: "profile-1",
    ["job_id"]: "job-1",
    ["worker_id"]: "worker-1",
    ["execution_epoch"]: 3,
    ["session_slot"]: 0,
    kind,
    ["duration_seconds"]: 2,
    status,
    ["requested_at_millis"]: 1000,
    ["deadline_at_millis"]: 46000,
    ["expires_at_millis"]: 3601000,
    ["completed_at_millis"]: null,
    error: status === "failed" ? "gpu_unavailable" : null,
    ["artifact_id"]: status === "complete" ? "profile-1" : null,
  };
}

const artifacts: Record<FunctionProfileKind, Record<string, unknown>> = {
  cpu: {
    $schema: "https://www.speedscope.app/file-format-schema.json",
    shared: { frames: [{ name: "work" }] },
    profiles: [],
    futureField: 7,
  },
  memory: {
    format: "lancedb.tracemalloc.v1",
    allocations: [{ ["size_bytes"]: 4096 }],
    truncated: false,
  },
  gpu: {
    format: "lancedb.cuda.v1",
    traceEvents: [{ name: "compute", cat: "kernel", ph: "X", ts: 10, dur: 2 }],
    truncated: true,
  },
};

async function withProfileServer(
  handler: (
    path: string,
    body: Record<string, unknown>,
    headers: IncomingHttpHeaders,
  ) => Response,
  run: (db: Connection) => Promise<void>,
) {
  let serverError: unknown;
  const server = createServer(async (request, response) => {
    try {
      const chunks: Buffer[] = [];
      for await (const chunk of request) chunks.push(chunk);
      const body = JSON.parse(Buffer.concat(chunks).toString());
      const result = handler(request.url ?? "", body, request.headers);
      response.writeHead(result.status ?? 200, {
        "Content-Type": "application/json",
      });
      response.end(JSON.stringify(result.body));
    } catch (error) {
      serverError = error;
      response.writeHead(500);
      response.end("test request handler failed");
    }
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const port = (server.address() as AddressInfo).port;
  try {
    const db = await connect("db://dev", {
      apiKey: "fake",
      hostOverride: `http://127.0.0.1:${port}`,
    });
    await run(db);
    if (serverError) throw serverError;
  } finally {
    server.closeAllConnections();
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
}

describe("Function worker profiles", () => {
  it.each(
    (["cpu", "memory", "gpu"] as FunctionProfileKind[]).flatMap((kind) =>
      [false, true].map((submitted) => [kind, submitted] as const),
    ),
  )(
    "captures and exports retained %s evidence (submitted handle: %s)",
    async (kind, submitted) => {
      let polls = 0;
      let finished = false;
      const paths: string[] = [];
      const directory = await mkdtemp(join(tmpdir(), "lancedb-profile-"));
      try {
        await withProfileServer(
          (path, body, headers) => {
            paths.push(path);
            expect(headers["x-api-key"]).toBe("fake");
            expect(headers["x-lancedb-database"]).toBe("dev");
            if (path === "/v1/table/test/describe/")
              return { body: { version: 1, schema: { fields: [] } } };
            if (path === "/v1/table/test/backfill_column") {
              expect(body.column).toBe("result");
              return { status: 202, body: { ["job_id"]: "job-1" } };
            }
            if (path === "/v1/jobs/describe")
              return {
                body: {
                  ["job_id"]: "job-1",
                  ["job_type"]: "function_refresh",
                  ["job_state"]: finished ? "DONE" : "IN_PROGRESS",
                },
              };
            if (path === "/v1/jobs/profile/capture") {
              expect(body).toEqual({
                ["job_id"]: "job-1",
                ["worker_id"]: "worker-1",
                ["session_slot"]: 0,
                kind,
                ["duration_seconds"]: 2,
              });
              return { status: 202, body: metadata(kind, "pending") };
            }
            expect(body).toEqual({
              ["job_id"]: "job-1",
              ["profile_id"]: "profile-1",
            });
            if (path === "/v1/jobs/profile/status")
              return {
                body: metadata(kind, ++polls > 1 ? "complete" : "capturing"),
              };
            expect(path).toBe("/v1/jobs/profile/artifact");
            return { body: artifacts[kind] };
          },
          async (db) => {
            const job = submitted
              ? await (await db.openTable("test")).refreshColumnAsync("result")
              : await db.openJob("job-1");
            if (submitted) await job.refresh();
            const capture = await job.captureProfile("worker-1", {
              kind,
              durationSeconds: 2,
            });
            expect(capture).toMatchObject({
              profileId: "profile-1",
              workerId: "worker-1",
              executionEpoch: 3,
              sessionSlot: 0,
              status: "pending",
            });
            const complete = await job.waitForProfile(capture.profileId, {
              timeoutMs: 2000,
              pollIntervalMs: 10,
            });
            expect(complete.status).toBe("complete");
            expect(job.state).toBe("running");
            finished = true;
            await job.refresh();
            expect(job.state).toBe("finished");
            expect(await job.profileArtifact(capture.profileId)).toEqual(
              artifacts[kind],
            );
            const path = join(directory, "capture.json");
            await job.exportProfile(capture.profileId, path);
            expect(JSON.parse(await readFile(path, "utf8"))).toEqual(
              artifacts[kind],
            );
            expect(
              paths.filter((path) => path === "/v1/jobs/describe"),
            ).toHaveLength(2);
            expect(paths.some((path) => path.endsWith("/cancel"))).toBe(false);
          },
        );
      } finally {
        await rm(directory, { recursive: true, force: true });
      }
    },
  );

  it("can resume after a wait timeout without cancelling the refresh", async () => {
    let complete = false;
    const paths: string[] = [];
    await withProfileServer(
      (path) => {
        paths.push(path);
        if (path === "/v1/jobs/describe")
          return {
            body: {
              ["job_id"]: "job-1",
              ["job_type"]: "function_refresh",
              ["job_state"]: "IN_PROGRESS",
            },
          };
        expect(path).toBe("/v1/jobs/profile/status");
        return { body: metadata("gpu", complete ? "complete" : "pending") };
      },
      async (db) => {
        const job = await db.openJob("job-1");
        await expect(
          job.waitForProfile("profile-1", {
            timeoutMs: 50,
            pollIntervalMs: 10,
          }),
        ).rejects.toThrow(/waiting for Function worker profile/);
        expect(await job.status()).toBe("running");
        complete = true;
        expect(
          (await job.waitForProfile("profile-1", { timeoutMs: 2000 })).status,
        ).toBe("complete");
        expect(paths.some((path) => path.endsWith("/cancel"))).toBe(false);
      },
    );
  });

  it("reports failed captures independently of the parent", async () => {
    await withProfileServer(
      (path) => ({
        body:
          path === "/v1/jobs/describe"
            ? {
                ["job_id"]: "job-1",
                ["job_type"]: "function_refresh",
                ["job_state"]: "IN_PROGRESS",
              }
            : metadata("gpu", "failed"),
      }),
      async (db) => {
        const job = await db.openJob("job-1");
        expect(await job.profileStatus("profile-1")).toMatchObject({
          status: "failed",
          error: "gpu_unavailable",
        });
        await expect(job.waitForProfile("profile-1")).rejects.toThrow(
          "gpu_unavailable",
        );
        expect(job.state).toBe("running");
      },
    );
  });

  it("preserves permission errors and existing export files", async () => {
    const directory = await mkdtemp(join(tmpdir(), "lancedb-profile-"));
    const path = join(directory, "capture.json");
    await writeFile(path, "previous capture");
    try {
      await withProfileServer(
        (path) => {
          if (path === "/v1/jobs/describe")
            return {
              body: {
                ["job_id"]: "job-1",
                ["job_type"]: "function_refresh",
                ["job_state"]: "DONE",
              },
            };
          if (path.endsWith("/status"))
            return { body: metadata("gpu", "complete") };
          return { status: 403, body: { error: "System Operate required" } };
        },
        async (db) => {
          const job = await db.openJob("job-1");
          await expect(job.exportProfile("profile-1", path)).rejects.toThrow(
            "403",
          );
          expect(await readFile(path, "utf8")).toBe("previous capture");
        },
      );
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  });

  it("rejects invalid options before submitting a capture", async () => {
    await withProfileServer(
      (path) => {
        expect(path).toBe("/v1/jobs/describe");
        return {
          body: {
            ["job_id"]: "job-1",
            ["job_type"]: "function_refresh",
            ["job_state"]: "IN_PROGRESS",
          },
        };
      },
      async (db) => {
        const job = await db.openJob("job-1");
        for (const durationSeconds of [
          Number.NaN,
          Number.POSITIVE_INFINITY,
          -1,
          0,
          6,
        ]) {
          await expect(
            job.captureProfile("worker-1", { kind: "gpu", durationSeconds }),
          ).rejects.toThrow(/duration/);
        }
        for (const sessionSlot of [
          -1,
          0.5,
          Number.NaN,
          Number.MAX_SAFE_INTEGER + 1,
        ]) {
          await expect(
            job.captureProfile("worker-1", { sessionSlot }),
          ).rejects.toThrow("sessionSlot");
        }
        await expect(
          job.captureProfile("worker-1", { duration: 10 } as never),
        ).rejects.toThrow("Unknown profiling option");
        await expect(
          job.waitForProfile("profile-1", { timeoutMs: -1 }),
        ).rejects.toThrow("timeoutMs");
        await expect(
          job.waitForProfile("profile-1", { pollIntervalMs: 0 }),
        ).rejects.toThrow("poll interval");
      },
    );
  });
});
