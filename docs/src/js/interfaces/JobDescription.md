[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / JobDescription

# Interface: JobDescription

A described job from `Connection.getJob`.

## Properties

### creationMs

```ts
creationMs: number;
```

When the job was created, in milliseconds since the epoch.

***

### failure?

```ts
optional failure: JobFailureInfo;
```

Why the job failed, when the job is failed and the server reports a
reason.

***

### jobId

```ts
jobId: string;
```

***

### jobType

```ts
jobType: string;
```

***

### specJson?

```ts
optional specJson: string;
```

The job-type-specific specification as a JSON string, when present.

***

### state

```ts
state: string;
```

Lifecycle state: "running", "finished", "failed", or "cancelled".
