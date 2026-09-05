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
 * - Everything is null until one of those runs.
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
   * Operations that run in this process have no server id. The value is
   * opaque: parsing it or storing it to resume the job later is not supported.
   */
  get id(): string | null {
    return this.inner.id ?? null;
  }

  /** The last observed lifecycle state, without contacting the backend. */
  get state(): string | null {
    return this.inner.state ?? null;
  }

  /**
   * The job's type, as the server names it. Null for an in-process job, which
   * has no server-side record.
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

  /** Wait until the operation reaches a terminal state. */
  async wait(): Promise<void> {
    return this.inner.wait();
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
}

// biome-ignore lint/suspicious/noExplicitAny: shape varies by job type
function parseJson(raw: string | null | undefined): any | null {
  return raw === null || raw === undefined ? null : JSON.parse(raw);
}
