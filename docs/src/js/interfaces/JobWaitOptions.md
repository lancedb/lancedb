[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / JobWaitOptions

# Interface: JobWaitOptions

Client-side controls for waiting without cancelling the underlying job.

## Properties

### onProgress?

```ts
optional onProgress: JobProgressCallback;
```

Observe progress using the same poller that waits for completion.

***

### timeoutMs?

```ts
optional timeoutMs: number;
```

Maximum wait in milliseconds, including status requests and polling backoff.
Must be a nonnegative safe integer. Omit to wait indefinitely; zero allows
only an immediately ready result.
