[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / Job

# Class: Job

A handle to an operation that may still be running.

## Constructors

### new Job()

```ts
new Job(): Job
```

#### Returns

[`Job`](Job.md)

## Accessors

### creationMs

```ts
get creationMs(): null | number
```

When the job was created, in milliseconds since the epoch.

#### Returns

`null` \| `number`

***

### failure

```ts
get failure(): null | JobFailureInfo
```

Why the job failed, when it failed and the server reports a reason.

#### Returns

`null` \| [`JobFailureInfo`](../interfaces/JobFailureInfo.md)

***

### id

```ts
get id(): null | string
```

Identifies the operation on the server that is running it. Operations
that run in this process have no server id. The value is opaque.

#### Returns

`null` \| `string`

***

### jobType

```ts
get jobType(): null | string
```

The job's type, as the server names it. Null for an in-process job,
which has no server-side record.

#### Returns

`null` \| `string`

***

### resultJson

```ts
get resultJson(): null | string
```

The job-type-specific terminal result as a JSON string. Null until the
job succeeds, so a job that never terminates reports its progress
through `Connection.queryJobEvents` instead.

#### Returns

`null` \| `string`

***

### specJson

```ts
get specJson(): null | string
```

The job-type-specific specification as a JSON string, when present.

#### Returns

`null` \| `string`

***

### state

```ts
get state(): null | string
```

The last observed lifecycle state, without contacting the backend.

#### Returns

`null` \| `string`

## Methods

### cancel()

```ts
cancel(): Promise<void>
```

Request cancellation. Cancelling a finished operation is a no-op.

#### Returns

`Promise`&lt;`void`&gt;

***

### refresh()

```ts
refresh(): Promise<void>
```

Ask the backend for this job's current state, and for a server-side job
its full record, then cache it for the getters below.

They are all null until this runs, because submitting an operation
returns only a job id. [Job.status](Job.md#status) and [Job.wait](Job.md#wait) refresh
too.

#### Returns

`Promise`&lt;`void`&gt;

***

### status()

```ts
status(): Promise<string>
```

The operation's current lifecycle state: "running", "finished",
"failed", or "cancelled".

A point snapshot; unlike [Job.wait](Job.md#wait) it does not block or reject
on a terminal failure state. States a newer server reports that this
client version does not know pass through as-is.

#### Returns

`Promise`&lt;`string`&gt;

***

### wait()

```ts
wait(): Promise<void>
```

Wait until the operation reaches a terminal state.

#### Returns

`Promise`&lt;`void`&gt;
