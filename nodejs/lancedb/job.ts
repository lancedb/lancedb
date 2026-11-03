// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import { Table as ArrowTable, tableFromIPC } from "apache-arrow";
import { JobFailureInfo, Job as NativeJob } from "./native";

/** Which of a job's events {@link Job.events} returns. */
export interface JobEventsOptions {
  /** Maximum event rows to return, up to the server maximum of 10,000. */
  limit?: number;
  /** SQL-like filter over the event columns. */
  filter?: string;
}

/**
 * A forward-compatible structured progress snapshot for a server-side job.
 *
 * Fields are optional because older servers and different job types report
 * different counters. A missing counter is unknown; it is not zero.
 */
export interface JobProgress {
  /** Lifecycle state, including states from newer servers. */
  state?: string;
  /** Coarse server-derived phase, including phases from newer servers. */
  phase?: string | null;
  /** Whether an operator has parked the job. */
  paused?: boolean;
  /**
   * Planned, computed, skipped, remaining, and failed row counters, when the
   * server reports them.
   */
  // biome-ignore lint/style/useNamingConvention: server wire format
  rows_planned?: number | null;
  // biome-ignore lint/style/useNamingConvention: server wire format
  rows_computed?: number | null;
  // biome-ignore lint/style/useNamingConvention: server wire format
  rows_skipped?: number | null;
  // biome-ignore lint/style/useNamingConvention: server wire format
  rows_remaining?: number | null;
  // biome-ignore lint/style/useNamingConvention: server wire format
  rows_failed?: number | null;
  /** Execution identity and sample time used for client-side ETA derivation. */
  // biome-ignore lint/style/useNamingConvention: server wire format
  execution_epoch?: number | null;
  // biome-ignore lint/style/useNamingConvention: server wire format
  sample_at_millis?: number | null;
  /** Best-effort remaining milliseconds, present only for a reliable sample. */
  // biome-ignore lint/style/useNamingConvention: server wire format
  estimated_remaining_time_millis?: number;
  /** Retain job-type-specific and newer-server fields without interpretation. */
  [field: string]: unknown;
}

/** Callback for progress snapshots observed while waiting for a job. */
export type JobProgressCallback = (progress: JobProgress) => void;

/**
 * A handle to an operation that may still be running.
 *
 * The operation may already be complete when the handle is created.
 *
 * The detail getters read what the handle last observed. Submitting an
 * operation returns only a job id, so populating them eagerly would cost an
 * extra round trip on every call:
 *
 * - {@link Job.refresh} and {@link Job.status} fetch the whole record.
 * - {@link Job.wait} records the terminal state it establishes, but not the
 *   rest of the record.
 * - Registry details are null until one of those runs. Query handles report
 *   jobType "query" immediately.
 *
 * @hideconstructor
 */
export class Job {
  private readonly inner: NativeJob;

  constructor(inner: NativeJob) {
    this.inner = inner;
  }

  /**
   * Identifies the operation on the server that is running it.
   *
   * Registry handles expose a registry job ID; query handles expose a
   * connection-scoped SQL query UUID string. Local handles have no ID.
   * Query handles report jobType "query". Their IDs are not registry IDs or
   * durable resume tokens.
   */
  get id(): string | null {
    return this.inner.id ?? null;
  }

  /** The last observed lifecycle state, without contacting the backend. */
  get state(): string | null {
    return this.inner.state ?? null;
  }

  /**
   * "query" immediately for SQL-backed handles; otherwise the cached
   * server-reported job type. Null for in-process handles.
   */
  get jobType(): string | null {
    return this.inner.jobType ?? null;
  }

  /** When the job was created, in milliseconds since the epoch. */
  get creationMs(): number | null {
    return this.inner.creationMs ?? null;
  }

  /** The job-type-specific specification it was submitted with. */
  // biome-ignore lint/suspicious/noExplicitAny: shape varies by job type
  get spec(): any | null {
    return parseJson(this.inner.specJson);
  }

  /**
   * The job-type-specific terminal result. Null until the job succeeds, so a
   * job that never terminates reports its progress through {@link Job.events}
   * instead.
   */
  // biome-ignore lint/suspicious/noExplicitAny: shape varies by job type
  get result(): any | null {
    return parseJson(this.inner.resultJson);
  }

  /** Why the job failed, when it failed and the server reports a reason. */
  get failure(): JobFailureInfo | null {
    return this.inner.failure ?? null;
  }

  /**
   * The operation's current lifecycle state: "running", "finished", "failed",
   * or "cancelled".
   *
   * A point snapshot; unlike {@link Job.wait} it does not block or reject on a
   * terminal failure state. Also refreshes the getters above.
   */
  async status(): Promise<string> {
    return this.inner.status();
  }

  /**
   * Fetch a structured progress snapshot.
   *
   * Registry jobs include lifecycle and queue metadata even when their job
   * type does not report counters. The returned object is forward-compatible
   * with newer servers and job types. In-process operations return null.
   */
  async progress(): Promise<JobProgress | null> {
    const progress = await this.inner.progress();
    return progress === null ? null : this.enrichProgress(progress);
  }

  /**
   * Wait until the operation reaches a terminal state.
   *
   * The optional callback observes the same snapshots fetched by the backend
   * poller that drives completion; it does not start another poller. Without a
   * callback, this preserves the original wait behavior. The callback is a
   * synchronous observer: its return value is ignored and not awaited. If it
   * throws, the error is logged with `console.warn` and waiting continues; a
   * callback exception does not reject `wait`.
   */
  async wait(progressCallback?: JobProgressCallback): Promise<void> {
    if (progressCallback === undefined) {
      return this.inner.wait();
    }
    return this.inner.wait((progress) => {
      try {
        progressCallback(this.enrichProgress(progress));
      } catch (error) {
        console.warn("Job progress callback threw:", error);
      }
    });
  }

  /** Request cancellation. Cancelling a finished operation is a no-op. */
  async cancel(): Promise<void> {
    return this.inner.cancel();
  }

  /**
   * Ask the backend for this job's current state, and for a server-side job
   * its full record, then cache it for the getters above.
   */
  async refresh(): Promise<void> {
    return this.inner.refresh();
  }

  /**
   * This job's recorded lifecycle events.
   *
   * Where the getters above report a terminal result only once the job reaches
   * one, events are written as the job runs and outlive the workers that
   * produced them. A distributed job records a `claim`/`claim_complete` pair
   * per unit of work, each carrying `rows_processed`, so a job that never
   * finishes still accounts for what it did.
   *
   * The server caps results at 1000 rows by default and 10,000 at most, and
   * truncates without saying so, so pass `limit` for a job that emits an event
   * per fragment. `filter` is a SQL-like expression over the `state`,
   * `updated_by`, `emitted_from`, `emitted_by`, and `claim_entity` columns.
   */
  async events(options?: JobEventsOptions): Promise<ArrowTable> {
    const buf = await this.inner.events(options?.limit, options?.filter);
    if (buf.length === 0) {
      return new ArrowTable();
    }
    return tableFromIPC(buf);
  }

  private lastProgress: JobProgress | null = null;

  private enrichProgress(progress: Record<string, unknown>): JobProgress {
    const enriched = withProgressEta(
      this.lastProgress,
      progress as JobProgress,
    );
    this.lastProgress = enriched;
    return enriched;
  }

  /**
   * Every field the handle currently knows, one per line, with the JSON
   * payloads indented -- a refresh job's spec and result are the point of
   * printing it.
   */
  toString(): string {
    if (this.state === null) {
      const known = this.id === null ? "" : `id=${JSON.stringify(this.id)}, `;
      return `Job(${known}not refreshed)`;
    }
    const fields: string[] = [];
    if (this.id !== null) {
      fields.push(`id=${JSON.stringify(this.id)}`);
    }
    fields.push(`state=${JSON.stringify(this.state)}`);
    if (this.jobType !== null) {
      fields.push(`jobType=${JSON.stringify(this.jobType)}`);
    }
    if (this.creationMs !== null) {
      fields.push(`creationMs=${this.creationMs}`);
    }
    for (const [name, value] of [
      ["spec", this.spec],
      ["result", this.result],
    ] as const) {
      if (value !== null) {
        fields.push(`${name}=${indentJson(value)}`);
      }
    }
    if (this.failure !== null) {
      fields.push(`failure=${indentJson(this.failure)}`);
    }
    return `Job(${fields.map((field) => `\n${REPR_INDENT}${field},`).join("")}\n)`;
  }

  [Symbol.for("nodejs.util.inspect.custom")](): string {
    return this.toString();
  }
}

const REPR_INDENT = "    ";

const ETA_MAX_SAMPLE_INTERVAL_MILLIS = 10 * 60 * 1_000;

function withProgressEta(
  previous: JobProgress | null,
  current: JobProgress,
): JobProgress {
  const snapshot = { ...current };
  const estimate = progressEta(previous, current);
  if (estimate !== null) {
    snapshot.estimated_remaining_time_millis = estimate;
  }
  return snapshot;
}

function progressEta(
  previous: JobProgress | null,
  current: JobProgress,
): number | null {
  if (
    previous === null ||
    (current.state !== "running" && current.state !== "finished")
  ) {
    return null;
  }
  const remaining = integerValue(current.rows_remaining);
  if (current.state === "finished") {
    return remaining === 0 ? 0 : null;
  }
  if (
    current.paused === true ||
    previous.paused === true ||
    isTransitionalPhase(current.phase) ||
    isTransitionalPhase(previous.phase)
  ) {
    return null;
  }
  const epoch = integerValue(current.execution_epoch);
  const previousEpoch = integerValue(previous.execution_epoch);
  if (epoch === null || previousEpoch !== epoch) {
    return null;
  }
  const sampleAt = integerValue(current.sample_at_millis);
  const previousAt = integerValue(previous.sample_at_millis);
  if (sampleAt === null || previousAt === null) {
    return null;
  }
  const elapsed = sampleAt - previousAt;
  if (
    elapsed <= 0 ||
    elapsed > ETA_MAX_SAMPLE_INTERVAL_MILLIS ||
    Date.now() - sampleAt > ETA_MAX_SAMPLE_INTERVAL_MILLIS
  ) {
    return null;
  }
  const planned = integerValue(current.rows_planned);
  const currentRemaining = integerValue(current.rows_remaining);
  const previousRemaining = integerValue(previous.rows_remaining);
  if (
    planned === null ||
    currentRemaining === null ||
    previousRemaining === null
  ) {
    return null;
  }
  if (currentRemaining === 0) {
    return 0;
  }
  if (planned !== previous.rows_planned) {
    return null;
  }
  if (current.fragments_planned !== previous.fragments_planned) {
    return null;
  }
  const currentComputed = integerValue(current.rows_computed);
  const previousComputed = integerValue(previous.rows_computed);
  if (currentComputed === null || previousComputed === null) {
    return null;
  }
  const currentAccounted =
    currentComputed + (integerValue(current.rows_skipped) ?? 0);
  const previousAccounted =
    previousComputed + (integerValue(previous.rows_skipped) ?? 0);
  const advanced = currentAccounted - previousAccounted;
  if (advanced <= 0 || currentRemaining > previousRemaining) {
    return null;
  }
  return Math.floor((currentRemaining * elapsed) / advanced);
}

function integerValue(value: unknown): number | null {
  return typeof value === "number" && Number.isInteger(value) ? value : null;
}

function isTransitionalPhase(phase: unknown): boolean {
  return (
    phase === "queued" ||
    phase === "starting" ||
    phase === "initializing" ||
    phase === "paused"
  );
}

// biome-ignore lint/suspicious/noExplicitAny: shape varies by job type
function indentJson(value: any): string {
  return JSON.stringify(value, null, 4).replace(/\n/g, `\n${REPR_INDENT}`);
}

// biome-ignore lint/suspicious/noExplicitAny: shape varies by job type
function parseJson(raw: string | null | undefined): any | null {
  return raw === null || raw === undefined ? null : JSON.parse(raw);
}
