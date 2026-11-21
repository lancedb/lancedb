[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / FunctionProfileWaitOptions

# Interface: FunctionProfileWaitOptions

Client-side controls while waiting for a Function worker capture.

## Properties

### pollIntervalMs?

```ts
optional pollIntervalMs: number;
```

Delay between status requests; defaults to 2,000 ms and must be positive.

***

### timeoutMs?

```ts
optional timeoutMs: number;
```

Maximum wait including HTTP requests and sleeps; defaults to 60,000 ms.
