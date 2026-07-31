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

### id

```ts
get id(): null | string
```

Identifies the operation on the server that is running it. Operations
that run in this process have no server id. The value is opaque.

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
