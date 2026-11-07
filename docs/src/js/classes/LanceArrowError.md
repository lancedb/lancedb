[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / LanceArrowError

# Class: LanceArrowError

Arrow returned an error while reading or writing batches.

## Extends

- [`LanceError`](LanceError.md)

## Constructors

### new LanceArrowError()

```ts
new LanceArrowError(message, options?): LanceArrowError
```

#### Parameters

* **message**: `string`

* **options?**: [`LanceErrorOptions`](../interfaces/LanceErrorOptions.md)

#### Returns

[`LanceArrowError`](LanceArrowError.md)

#### Inherited from

[`LanceError`](LanceError.md).[`constructor`](LanceError.md#constructors)

## Properties

### cause?

```ts
optional cause: unknown;
```

#### Inherited from

[`LanceError`](LanceError.md).[`cause`](LanceError.md#cause)

***

### code

```ts
readonly code: string;
```

#### Inherited from

[`LanceError`](LanceError.md).[`code`](LanceError.md#code)

***

### columnName?

```ts
readonly optional columnName: string;
```

#### Inherited from

[`LanceError`](LanceError.md).[`columnName`](LanceError.md#columnname)

***

### connectFailures?

```ts
readonly optional connectFailures: number;
```

#### Inherited from

[`LanceError`](LanceError.md).[`connectFailures`](LanceError.md#connectfailures)

***

### databaseName?

```ts
readonly optional databaseName: string;
```

#### Inherited from

[`LanceError`](LanceError.md).[`databaseName`](LanceError.md#databasename)

***

### errorCode?

```ts
readonly optional errorCode: string;
```

#### Inherited from

[`LanceError`](LanceError.md).[`errorCode`](LanceError.md#errorcode)

***

### functionName?

```ts
readonly optional functionName: string;
```

#### Inherited from

[`LanceError`](LanceError.md).[`functionName`](LanceError.md#functionname)

***

### grpcCode?

```ts
readonly optional grpcCode: number;
```

#### Inherited from

[`LanceError`](LanceError.md).[`grpcCode`](LanceError.md#grpccode)

***

### indexName?

```ts
readonly optional indexName: string;
```

#### Inherited from

[`LanceError`](LanceError.md).[`indexName`](LanceError.md#indexname)

***

### jobId?

```ts
readonly optional jobId: string;
```

#### Inherited from

[`LanceError`](LanceError.md).[`jobId`](LanceError.md#jobid)

***

### jobRequestId?

```ts
readonly optional jobRequestId: string;
```

#### Inherited from

[`LanceError`](LanceError.md).[`jobRequestId`](LanceError.md#jobrequestid)

***

### maxConnectFailures?

```ts
readonly optional maxConnectFailures: number;
```

#### Inherited from

[`LanceError`](LanceError.md).[`maxConnectFailures`](LanceError.md#maxconnectfailures)

***

### maxReadFailures?

```ts
readonly optional maxReadFailures: number;
```

#### Inherited from

[`LanceError`](LanceError.md).[`maxReadFailures`](LanceError.md#maxreadfailures)

***

### maxRequestFailures?

```ts
readonly optional maxRequestFailures: number;
```

#### Inherited from

[`LanceError`](LanceError.md).[`maxRequestFailures`](LanceError.md#maxrequestfailures)

***

### message

```ts
message: string;
```

#### Inherited from

[`LanceError`](LanceError.md).[`message`](LanceError.md#message)

***

### name

```ts
name: string;
```

#### Inherited from

[`LanceError`](LanceError.md).[`name`](LanceError.md#name)

***

### namespacePath?

```ts
readonly optional namespacePath: string[];
```

#### Inherited from

[`LanceError`](LanceError.md).[`namespacePath`](LanceError.md#namespacepath)

***

### path?

```ts
readonly optional path: string;
```

#### Inherited from

[`LanceError`](LanceError.md).[`path`](LanceError.md#path)

***

### phase?

```ts
readonly optional phase: string;
```

#### Inherited from

[`LanceError`](LanceError.md).[`phase`](LanceError.md#phase)

***

### readFailures?

```ts
readonly optional readFailures: number;
```

#### Inherited from

[`LanceError`](LanceError.md).[`readFailures`](LanceError.md#readfailures)

***

### requestFailures?

```ts
readonly optional requestFailures: number;
```

#### Inherited from

[`LanceError`](LanceError.md).[`requestFailures`](LanceError.md#requestfailures)

***

### requestId?

```ts
readonly optional requestId: string;
```

#### Inherited from

[`LanceError`](LanceError.md).[`requestId`](LanceError.md#requestid)

***

### resourceKind?

```ts
readonly optional resourceKind: string;
```

#### Inherited from

[`LanceError`](LanceError.md).[`resourceKind`](LanceError.md#resourcekind)

***

### resourceName?

```ts
readonly optional resourceName: string;
```

#### Inherited from

[`LanceError`](LanceError.md).[`resourceName`](LanceError.md#resourcename)

***

### retryable?

```ts
readonly optional retryable: boolean;
```

#### Inherited from

[`LanceError`](LanceError.md).[`retryable`](LanceError.md#retryable)

***

### sqlJobId?

```ts
readonly optional sqlJobId: string;
```

#### Inherited from

[`LanceError`](LanceError.md).[`sqlJobId`](LanceError.md#sqljobid)

***

### stack?

```ts
optional stack: string;
```

#### Inherited from

[`LanceError`](LanceError.md).[`stack`](LanceError.md#stack)

***

### statusCode?

```ts
readonly optional statusCode: number;
```

#### Inherited from

[`LanceError`](LanceError.md).[`statusCode`](LanceError.md#statuscode)

***

### tableName?

```ts
readonly optional tableName: string;
```

#### Inherited from

[`LanceError`](LanceError.md).[`tableName`](LanceError.md#tablename)

***

### code

```ts
readonly static code: "Arrow" = "Arrow";
```

#### Overrides

[`LanceError`](LanceError.md).[`code`](LanceError.md#code-1)

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

[`LanceError`](LanceError.md).[`prepareStackTrace`](LanceError.md#preparestacktrace)

***

### stackTraceLimit

```ts
static stackTraceLimit: number;
```

#### Inherited from

[`LanceError`](LanceError.md).[`stackTraceLimit`](LanceError.md#stacktracelimit)

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

[`LanceError`](LanceError.md).[`captureStackTrace`](LanceError.md#capturestacktrace)
