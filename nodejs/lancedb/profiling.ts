// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

/** Evidence collected from one Function worker's Python execution session. */
export type FunctionProfileKind = "cpu" | "gpu" | "memory";

/** A capture's lifecycle, independent of its parent refresh job. */
export type FunctionProfileStatus =
  | "pending"
  | "capturing"
  | "complete"
  | "failed";

/** Options for requesting a Function worker capture. */
export interface FunctionProfileOptions {
  /** CPU samples, retained Python allocations, or PyTorch CUDA activities. */
  kind?: FunctionProfileKind;
  /** 1–30 seconds for CPU/memory, default 10; 1–5 for GPU, default 5. */
  durationSeconds?: number;
  /** Python execution session within the worker; default zero. */
  sessionSlot?: number;
}

/** Client-side controls while waiting for a Function worker capture. */
export interface FunctionProfileWaitOptions {
  /** Maximum wait including HTTP requests and sleeps; defaults to 60,000 ms. */
  timeoutMs?: number;
  /** Delay between status requests; defaults to 2,000 ms and must be positive. */
  pollIntervalMs?: number;
}

/**
 * A capture identity and status snapshot. Keep profileId for retained lookup
 * after the Function refresh completes. Timestamp fields are epoch milliseconds.
 */
export interface FunctionProfile {
  readonly profileId: string;
  readonly jobId: string;
  readonly workerId: string;
  readonly executionEpoch: number;
  readonly sessionSlot: number;
  readonly kind: FunctionProfileKind;
  readonly durationSeconds: number;
  readonly status: FunctionProfileStatus;
  readonly requestedAtMillis: number;
  readonly deadlineAtMillis: number;
  /** Upper retention bound; job scratch reclamation can remove artifacts sooner. */
  readonly expiresAtMillis: number;
  readonly completedAtMillis: number | null;
  /** The server's capture failure reason; this is not a refresh job failure. */
  readonly error: string | null;
  readonly artifactId: string | null;
}

export function decodeFunctionProfile(value: string): FunctionProfile {
  const data = JSON.parse(value);
  return {
    profileId: data.profile_id,
    jobId: data.job_id,
    workerId: data.worker_id,
    executionEpoch: data.execution_epoch,
    sessionSlot: data.session_slot,
    kind: data.kind,
    durationSeconds: data.duration_seconds,
    status: data.status,
    requestedAtMillis: data.requested_at_millis,
    deadlineAtMillis: data.deadline_at_millis,
    expiresAtMillis: data.expires_at_millis,
    completedAtMillis: data.completed_at_millis,
    error: data.error,
    artifactId: data.artifact_id,
  };
}

export function validateProfileOptions(options: object, names: string[]): void {
  if (
    typeof options !== "object" ||
    options === null ||
    Array.isArray(options)
  ) {
    throw new TypeError("profiling expects an options object");
  }
  for (const key of Object.keys(options)) {
    if (!names.includes(key)) {
      throw new TypeError(`Unknown profiling option: ${key}`);
    }
  }
}
