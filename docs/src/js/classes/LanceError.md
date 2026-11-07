[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / LanceError

# Class: LanceError

Base class for errors raised by the LanceDB native core.

Subclasses exist so application code can branch with `instanceof`. A kind
that does not have its own class is still a [LanceError](LanceError.md); `code` then
carries the Rust variant name.

## Extends

- `Error`

## Extended by

- [`ColumnAlreadyExistsError`](ColumnAlreadyExistsError.md)
- [`ColumnNotFoundError`](ColumnNotFoundError.md)
- [`CreateDirError`](CreateDirError.md)
- [`DatabaseAlreadyExistsError`](DatabaseAlreadyExistsError.md)
- [`DatabaseNotFoundError`](DatabaseNotFoundError.md)
- [`EmbeddingFunctionNotFoundError`](EmbeddingFunctionNotFoundError.md)
- [`ExternalError`](ExternalError.md)
- [`HttpError`](HttpError.md)
- [`IndexNotFoundError`](IndexNotFoundError.md)
- [`InvalidInputError`](InvalidInputError.md)
- [`JobCancelledError`](JobCancelledError.md)
- [`JobFailedError`](JobFailedError.md)
- [`JobNotFoundError`](JobNotFoundError.md)
- [`LanceArrowError`](LanceArrowError.md)
- [`LanceFormatError`](LanceFormatError.md)
- [`LanceRuntimeError`](LanceRuntimeError.md)
- [`NotAComputedColumnError`](NotAComputedColumnError.md)
- [`NotSupportedError`](NotSupportedError.md)
- [`ObjectStoreError`](ObjectStoreError.md)
- [`OtherError`](OtherError.md)
- [`RetryError`](RetryError.md)
- [`SqlError`](SqlError.md)
- [`TableAlreadyExistsError`](TableAlreadyExistsError.md)
- [`TableCorruptedError`](TableCorruptedError.md)
- [`TableNotFoundError`](TableNotFoundError.md)
- [`TimeoutError`](TimeoutError.md)

## Constructors

### new LanceError()

```ts
new LanceError(message, options?): LanceError
```

#### Parameters

* **message**: `string`

* **options?**: [`LanceErrorOptions`](../interfaces/LanceErrorOptions.md)

#### Returns

[`LanceError`](LanceError.md)

#### Overrides

`Error.constructor`

## Properties

### cause?

```ts
optional cause: unknown;
```

#### Inherited from

`Error.cause`

***

### code

```ts
readonly code: string;
```

***

### columnName?

```ts
readonly optional columnName: string;
```

***

### connectFailures?

```ts
readonly optional connectFailures: number;
```

***

### databaseName?

```ts
readonly optional databaseName: string;
```

***

### errorCode?

```ts
readonly optional errorCode: string;
```

***

### functionName?

```ts
readonly optional functionName: string;
```

***

### grpcCode?

```ts
readonly optional grpcCode: number;
```

***

### indexName?

```ts
readonly optional indexName: string;
```

***

### jobId?

```ts
readonly optional jobId: string;
```

***

### jobRequestId?

```ts
readonly optional jobRequestId: string;
```

***

### maxConnectFailures?

```ts
readonly optional maxConnectFailures: number;
```

***

### maxReadFailures?

```ts
readonly optional maxReadFailures: number;
```

***

### maxRequestFailures?

```ts
readonly optional maxRequestFailures: number;
```

***

### message

```ts
message: string;
```

#### Inherited from

`Error.message`

***

### name

```ts
name: string;
```

#### Inherited from

`Error.name`

***

### namespacePath?

```ts
readonly optional namespacePath: string[];
```

***

### path?

```ts
readonly optional path: string;
```

***

### phase?

```ts
readonly optional phase: string;
```

***

### readFailures?

```ts
readonly optional readFailures: number;
```

***

### requestFailures?

```ts
readonly optional requestFailures: number;
```

***

### requestId?

```ts
readonly optional requestId: string;
```

***

### resourceKind?

```ts
readonly optional resourceKind: string;
```

***

### resourceName?

```ts
readonly optional resourceName: string;
```

***

### retryable?

```ts
readonly optional retryable: boolean;
```

***

### sqlJobId?

```ts
readonly optional sqlJobId: string;
```

***

### stack?

```ts
optional stack: string;
```

#### Inherited from

`Error.stack`

***

### statusCode?

```ts
readonly optional statusCode: number;
```

***

### tableName?

```ts
readonly optional tableName: string;
```

***

### code

```ts
readonly static code: string = "Error";
```

***

### prepareStackTrace()?

```ts
static optional prepareStackTrace: (err, stackTraces) => any;
```

Optional override for formatting stack traces

#### Parameters

* **err**: `Error`

* **stackTraces**: `CallSite`[]

#### Returns

`any`

#### See

https://v8.dev/docs/stack-trace-api#customizing-stack-traces

#### Inherited from

`Error.prepareStackTrace`

***

### stackTraceLimit

```ts
static stackTraceLimit: number;
```

#### Inherited from

`Error.stackTraceLimit`

## Methods

### captureStackTrace()

```ts
static captureStackTrace(targetObject, constructorOpt?): void
```

Create .stack property on a target object

#### Parameters

* **targetObject**: `object`

* **constructorOpt?**: `Function`

#### Returns

`void`

#### Inherited from

`Error.captureStackTrace`
