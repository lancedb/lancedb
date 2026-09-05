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
- Everything is null until one of those runs.

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

Identifies the operation on the server that is running it.

Operations that run in this process have no server id. The value is
opaque: parsing it or storing it to resume the job later is not supported.

#### Returns

`null` \| `string`

***

### jobType

```ts
get jobType(): null | string
```

The job's type, as the server names it. Null for an in-process job, which
has no server-side record.

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

### refresh()

```ts
refresh(): Promise<void>
```

Ask the backend for this job's current state, and for a server-side job
its full record, then cache it for the getters above.

#### Returns

`Promise`&lt;`void`&gt;

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
wait(): Promise<void>
```

Wait until the operation reaches a terminal state.

#### Returns

`Promise`&lt;`void`&gt;
