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
