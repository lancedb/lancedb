[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / FunctionErrorRecord

# Interface: FunctionErrorRecord

One row a Function refresh skipped, as the server recorded it.

## Properties

### attempts?

```ts
optional attempts: number;
```

Number of failed invocation attempts, when recorded.

***

### column

```ts
column: string;
```

***

### createdAtMillis

```ts
createdAtMillis: number;
```

***

### errorMessage

```ts
errorMessage: string;
```

***

### errorType

```ts
errorType: string;
```

***

### firstSeenAtMillis?

```ts
optional firstSeenAtMillis: number;
```

First failure time for this row, when recorded.

***

### fragmentId

```ts
fragmentId: number;
```

***

### function

```ts
function: string;
```

***

### functionVersion

```ts
functionVersion: string;
```

***

### input?

```ts
optional input: unknown;
```

Source identities for a materialized-view Function invocation.

***

### jobId

```ts
jobId: string;
```

***

### lastSeenAtMillis?

```ts
optional lastSeenAtMillis: number;
```

Latest failure time for this row, when recorded.

***

### quarantined?

```ts
optional quarantined: boolean;
```

Whether the failure is quarantined. Absent on older listings.

***

### rowAddress?

```ts
optional rowAddress: bigint;
```

Physical row address at the recorded version. A rewrite can change it.

***

### rowId?

```ts
optional rowId: bigint;
```

Stable row identity when recorded. It survives compaction and rewrite.

***

### rowOffset?

```ts
optional rowOffset: number;
```

The row's offset within the fragment; absent when the fragment's
detail was capped.

***

### tableVersion

```ts
tableVersion: number;
```

***

### traceback?

```ts
optional traceback: string;
```

User-code traceback, possibly truncated by the server.
