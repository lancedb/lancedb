[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / JobInfo

# Interface: JobInfo

A row from `Connection.listJobs`: one server-side job.

## Properties

### columns

```ts
columns: string[];
```

Affected columns; empty when unavailable or not column-scoped.

***

### completedAtMillis?

```ts
optional completedAtMillis: number;
```

Terminal timestamp in milliseconds since the epoch, when known.

***

### createdAtMillis

```ts
createdAtMillis: number;
```

When the job was created, in milliseconds since the epoch.

***

### durationMillis?

```ts
optional durationMillis: number;
```

Creation-to-completion wall time, including queueing and pauses.

***

### failure?

```ts
optional failure: JobFailureInfo;
```

Durable failure summary, when available.

***

### functionName?

```ts
optional functionName: string;
```

Pinned Function catalog name, with namespace segments joined by `$`.

***

### functionVersion?

```ts
optional functionVersion: string;
```

Pinned Function version, when known.

***

### jobId

```ts
jobId: string;
```

The job id -- what `Connection.openJob` and `Connection.cancelJob`
accept.

***

### jobType

```ts
jobType: string;
```

***

### paused

```ts
paused: boolean;
```

Whether an operator has parked this job.

***

### progress

```ts
progress: Record<string, unknown>;
```

Structured lifecycle, queue, pause, and job-type-specific progress.

***

### state

```ts
state: string;
```

Lifecycle state: "running", "finished", "failed", or "cancelled".

***

### table

```ts
table: string;
```

The table the job runs against, without URI or namespace.
