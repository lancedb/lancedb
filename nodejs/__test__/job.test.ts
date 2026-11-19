// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import { Job } from "../lancedb/job";
import type { Job as NativeJob } from "../lancedb/native";

describe("Job attempt contract", () => {
  test("preserves lineage and unknown detailed states", async () => {
    const inner = {
      jobId: "attempt-2",
      operationId: "attempt-1",
      attempt: 2,
      retryOf: "attempt-1",
      lifecycleState: "future_publication_state",
      sourceTableVersion: 7,
      executionEpoch: 3,
      status: jest.fn().mockResolvedValue("future_publication_state"),
    } as unknown as NativeJob;
    const job = new Job(inner);
    expect(job.jobId).toBe("attempt-2");
    expect(job.operationId).toBe("attempt-1");
    expect(job.attempt).toBe(2);
    expect(job.retryOf).toBe("attempt-1");
    expect(await job.statusTyped()).toBe("future_publication_state");
    expect(job.sourceTableVersion).toBe(7);
    expect(job.executionEpoch).toBe(3);
  });

  test("passes wait deadlines through without cancelling execution", async () => {
    const wait = jest.fn().mockRejectedValue(new Error("Timeout error"));
    const cancel = jest.fn();
    const job = new Job({ wait, cancel } as unknown as NativeJob);
    await expect(job.wait({ timeoutMs: 1 })).rejects.toThrow("Timeout");
    expect(wait).toHaveBeenCalledWith(undefined, 1);
    expect(cancel).not.toHaveBeenCalled();
  });
  test("retry retains the typed result decoder", async () => {
    const successor = {
      waitResult: jest.fn().mockResolvedValue({ value: 7 }),
    } as unknown as NativeJob;
    const inner = {
      retry: jest.fn().mockResolvedValue(successor),
    } as unknown as NativeJob;
    const job = new Job(inner, (value) => (value as { value: number }).value);
    const retry = await job.retry();
    expect(await retry.wait()).toBe(7);
  });
});
