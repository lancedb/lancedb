[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / Job

# Class: Job

A handle to an operation that may still be running.

The operation may already be complete when the handle is created.

The detail getters read what the handle last observed. Submitting an
operation returns only a job id, so populating them eagerly would cost an
extra round trip on every call:

- [Job.refresh](Job.md#refresh) and [Job.status](Job.md#status) fetch the whole record.
- [Job.wait](Job.md#wait) records the terminal state it establishes, but not the
  rest of the record.
- Registry details are null until one of those runs. Query handles report
  jobType "query" immediately.

## Accessors

### attempt

```ts
get attempt(): null | number
```

One-based attempt ordinal. Worker replacement does not increment it.

#### Returns

`null` \| `number`

***

### creationMs

```ts
get creationMs(): null | number
```

When the job was created, in milliseconds since the epoch.

#### Returns

`null` \| `number`

***

### executionEpoch

```ts
get executionEpoch(): null | number
```

Most recent execution's fencing token, when reported.

#### Returns

`null` \| `number`

***

### failure

```ts
get failure(): any
```

Why the job failed, when it failed and the server reports a reason.

#### Returns

`any`

***

### id

```ts
get id(): null | string
```

Identifies the operation on the server that is running it.

Registry handles expose a registry job ID; query handles expose a
connection-scoped SQL query UUID string. Local handles have no ID.
Query handles report jobType "query". Their IDs are not registry IDs or
durable resume tokens.

#### Returns

`null` \| `string`

***

### jobId

```ts
get jobId(): null | string
```

Stable attempt identity. Local IDs cannot be reopened on a server.

#### Returns

`null` \| `string`

***

### jobType

```ts
get jobType(): null | string
```

"query" immediately for SQL-backed handles; otherwise the cached
server-reported job type. Null for in-process handles.

#### Returns

`null` \| `string`

***

### lifecycleState

```ts
get lifecycleState(): null | JobState
```

Detailed state last observed; unknown values never establish success.

#### Returns

`null` \| [`JobState`](../type-aliases/JobState.md)

***

### operationId

```ts
get operationId(): null | string
```

Root of the explicit retry lineage, when reported.

#### Returns

`null` \| `string`

***

### result

```ts
get result(): any
```

The job-type-specific terminal result. Null until the job succeeds, so a
job that never terminates reports its progress through [Job.events](Job.md#events)
instead.

#### Returns

`any`

***

### retryOf

```ts
get retryOf(): null | string
```

Immediate predecessor attempt, when reported.

#### Returns

`null` \| `string`

***

### sourceTableVersion

```ts
get sourceTableVersion(): null | number
```

Source snapshot pinned for this attempt, when reported.

#### Returns

`null` \| `number`

***

### spec

```ts
get spec(): any
```

The job-type-specific specification it was submitted with.

#### Returns

`any`

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

### events()

```ts
events(options?): Promise<Table<any>>
```

This job's recorded lifecycle events.

Where the getters above report a terminal result only once the job reaches
one, events are written as the job runs and outlive the workers that
produced them. A distributed job records a `claim`/`claim_complete` pair
per unit of work, each carrying `rows_processed`, so a job that never
finishes still accounts for what it did.

The server caps results at 1000 rows by default and 10,000 at most, and
truncates without saying so, so pass `limit` for a job that emits an event
per fragment. `filter` is a SQL-like expression over the `state`,
`updated_by`, `emitted_from`, `emitted_by`, and `claim_entity` columns.

#### Parameters

* **options?**: [`JobEventsOptions`](../interfaces/JobEventsOptions.md)

#### Returns

`Promise`&lt;`Table`&lt;`any`&gt;&gt;

***

### history()

```ts
history(options?): AsyncGenerator<Table<any>, any, unknown>
```

Walk retained history one page at a time using the server's continuation.

#### Parameters

* **options?**: [`JobEventsOptions`](../interfaces/JobEventsOptions.md)

#### Returns

`AsyncGenerator`&lt;`Table`&lt;`any`&gt;, `any`, `unknown`&gt;

***

### progress()

```ts
progress(): Promise<null | JobProgress>
```

Fetch a structured progress snapshot.

Registry jobs include lifecycle and queue metadata even when their job
type does not report counters. The returned object is forward-compatible
with newer servers and job types. In-process operations return null.

#### Returns

`Promise`&lt;`null` \| [`JobProgress`](../interfaces/JobProgress.md)&gt;

***

### refresh()

```ts
refresh(): Promise<void>
```

Ask the backend for this job's current state, and for a server-side job
its full record, then cache it for the getters above.

#### Returns

`Promise`&lt;`void`&gt;

***

### retry()

```ts
retry(): Promise<Job>
```

Select this exact attempt's pinned successor. Requires backend support.

#### Returns

`Promise`&lt;[`Job`](Job.md)&gt;

***

### status()

```ts
status(): Promise<string>
```

The operation's current lifecycle state: "running", "finished", "failed",
or "cancelled".

A point snapshot; unlike [Job.wait](Job.md#wait) it does not block or reject on a
terminal failure state. Also refreshes the getters above.

#### Returns

`Promise`&lt;`string`&gt;

***

### statusTyped()

```ts
statusTyped(): Promise<JobState>
```

Refresh and return the detailed lifecycle state.

#### Returns

`Promise`&lt;[`JobState`](../type-aliases/JobState.md)&gt;

***

### toString()

```ts
toString(): string
```

Every field the handle currently knows, one per line, with the JSON
payloads indented -- a refresh job's spec and result are the point of
printing it.

#### Returns

`string`

***

### wait()

```ts
wait(options?): Promise<void>
```

Wait until the operation reaches a terminal state.

The optional callback observes the same snapshots fetched by the backend
poller that drives completion; it does not start another poller. Without a
callback, this preserves the original wait behavior. The callback is a
synchronous observer: its return value is ignored and not awaited. If it
throws, the error is logged with `console.warn` and waiting continues; a
callback exception does not reject `wait`.

#### Parameters

* **options?**: [`JobProgressCallback`](../type-aliases/JobProgressCallback.md) \| [`JobWaitOptions`](../interfaces/JobWaitOptions.md)

#### Returns

`Promise`&lt;`void`&gt;
