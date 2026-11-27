[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / FunctionProfile

# Interface: FunctionProfile

A capture identity and status snapshot. Keep profileId for retained lookup
after the Function refresh completes. Timestamp fields are epoch milliseconds.

## Properties

### artifactId

```ts
readonly artifactId: null | string;
```

***

### completedAtMillis

```ts
readonly completedAtMillis: null | number;
```

***

### deadlineAtMillis

```ts
readonly deadlineAtMillis: number;
```

***

### durationSeconds

```ts
readonly durationSeconds: number;
```

***

### error

```ts
readonly error: null | string;
```

The server's capture failure reason; this is not a refresh job failure.

***

### executionEpoch

```ts
readonly executionEpoch: number;
```

***

### expiresAtMillis

```ts
readonly expiresAtMillis: number;
```

Upper retention bound; job scratch reclamation can remove artifacts sooner.

***

### jobId

```ts
readonly jobId: string;
```

***

### kind

```ts
readonly kind: FunctionProfileKind;
```

***

### profileId

```ts
readonly profileId: string;
```

***

### requestedAtMillis

```ts
readonly requestedAtMillis: number;
```

***

### sessionSlot

```ts
readonly sessionSlot: number;
```

***

### status

```ts
readonly status: FunctionProfileStatus;
```

***

### workerId

```ts
readonly workerId: string;
```
