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

### wait()

```ts
wait(): Promise<void>
```

Wait until the operation reaches a terminal state.

#### Returns

`Promise`&lt;`void`&gt;
