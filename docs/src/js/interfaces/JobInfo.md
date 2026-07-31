[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / JobInfo

# Interface: JobInfo

A row from `Connection.listJobs`: one server-side job.

## Properties

### createdAtMillis

```ts
createdAtMillis: number;
```

When the job was created, in milliseconds since the epoch.

***

### jobId

```ts
jobId: string;
```

The job id -- what `Connection.getJob` and `Connection.cancelJob`
accept.

***

### jobType

```ts
jobType: string;
```

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
