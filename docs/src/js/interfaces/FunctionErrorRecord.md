[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / FunctionErrorRecord

# Interface: FunctionErrorRecord

One row a Function refresh skipped, as the server recorded it.

## Properties

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

### jobId

```ts
jobId: string;
```

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
