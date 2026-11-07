[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / SchemaError

# Class: SchemaError

The data or query does not match the expected schema.

## Extends

- [`InvalidInputError`](InvalidInputError.md)

## Constructors

### new SchemaError()

```ts
new SchemaError(message, options?): SchemaError
```

#### Parameters

* **message**: `string`

* **options?**: [`LanceErrorOptions`](../interfaces/LanceErrorOptions.md)

#### Returns

[`SchemaError`](SchemaError.md)

#### Inherited from

[`InvalidInputError`](InvalidInputError.md).[`constructor`](InvalidInputError.md#constructors)

## Properties

### cause?

```ts
optional cause: unknown;
```

#### Inherited from

[`InvalidInputError`](InvalidInputError.md).[`cause`](InvalidInputError.md#cause)

***

### code

```ts
readonly code: string;
```

#### Inherited from

[`InvalidInputError`](InvalidInputError.md).[`code`](InvalidInputError.md#code)

***

### columnName?

```ts
readonly optional columnName: string;
```

#### Inherited from

[`InvalidInputError`](InvalidInputError.md).[`columnName`](InvalidInputError.md#columnname)

***

### connectFailures?

```ts
readonly optional connectFailures: number;
```

#### Inherited from

[`InvalidInputError`](InvalidInputError.md).[`connectFailures`](InvalidInputError.md#connectfailures)

***

### databaseName?

```ts
readonly optional databaseName: string;
```

#### Inherited from

[`InvalidInputError`](InvalidInputError.md).[`databaseName`](InvalidInputError.md#databasename)

***

### errorCode?

```ts
readonly optional errorCode: string;
```

#### Inherited from

[`InvalidInputError`](InvalidInputError.md).[`errorCode`](InvalidInputError.md#errorcode)

***

### functionName?

```ts
readonly optional functionName: string;
```

#### Inherited from

[`InvalidInputError`](InvalidInputError.md).[`functionName`](InvalidInputError.md#functionname)

***

### grpcCode?

```ts
readonly optional grpcCode: number;
```

#### Inherited from

[`InvalidInputError`](InvalidInputError.md).[`grpcCode`](InvalidInputError.md#grpccode)

***

### indexName?

```ts
readonly optional indexName: string;
```

#### Inherited from

[`InvalidInputError`](InvalidInputError.md).[`indexName`](InvalidInputError.md#indexname)

***

### jobId?

```ts
readonly optional jobId: string;
```

#### Inherited from

[`InvalidInputError`](InvalidInputError.md).[`jobId`](InvalidInputError.md#jobid)

***

### jobRequestId?

```ts
readonly optional jobRequestId: string;
```

#### Inherited from

[`InvalidInputError`](InvalidInputError.md).[`jobRequestId`](InvalidInputError.md#jobrequestid)

***

### maxConnectFailures?

```ts
readonly optional maxConnectFailures: number;
```

#### Inherited from

[`InvalidInputError`](InvalidInputError.md).[`maxConnectFailures`](InvalidInputError.md#maxconnectfailures)

***

### maxReadFailures?

```ts
readonly optional maxReadFailures: number;
```

#### Inherited from

[`InvalidInputError`](InvalidInputError.md).[`maxReadFailures`](InvalidInputError.md#maxreadfailures)

***

### maxRequestFailures?

```ts
readonly optional maxRequestFailures: number;
```

#### Inherited from

[`InvalidInputError`](InvalidInputError.md).[`maxRequestFailures`](InvalidInputError.md#maxrequestfailures)

***

### message

```ts
message: string;
```

#### Inherited from

[`InvalidInputError`](InvalidInputError.md).[`message`](InvalidInputError.md#message)

***

### name

```ts
name: string;
```

#### Inherited from

[`InvalidInputError`](InvalidInputError.md).[`name`](InvalidInputError.md#name)

***

### namespacePath?

```ts
readonly optional namespacePath: string[];
```

#### Inherited from

[`InvalidInputError`](InvalidInputError.md).[`namespacePath`](InvalidInputError.md#namespacepath)

***

### path?

```ts
readonly optional path: string;
```

#### Inherited from

[`InvalidInputError`](InvalidInputError.md).[`path`](InvalidInputError.md#path)

***

### phase?

```ts
readonly optional phase: string;
```

#### Inherited from

[`InvalidInputError`](InvalidInputError.md).[`phase`](InvalidInputError.md#phase)

***

### readFailures?

```ts
readonly optional readFailures: number;
```

#### Inherited from

[`InvalidInputError`](InvalidInputError.md).[`readFailures`](InvalidInputError.md#readfailures)

***

### requestFailures?

```ts
readonly optional requestFailures: number;
```

#### Inherited from

[`InvalidInputError`](InvalidInputError.md).[`requestFailures`](InvalidInputError.md#requestfailures)

***

### requestId?

```ts
readonly optional requestId: string;
```

#### Inherited from

[`InvalidInputError`](InvalidInputError.md).[`requestId`](InvalidInputError.md#requestid)

***

### resourceKind?

```ts
readonly optional resourceKind: string;
```

#### Inherited from

[`InvalidInputError`](InvalidInputError.md).[`resourceKind`](InvalidInputError.md#resourcekind)

***

### resourceName?

```ts
readonly optional resourceName: string;
```

#### Inherited from

[`InvalidInputError`](InvalidInputError.md).[`resourceName`](InvalidInputError.md#resourcename)

***

### retryable?

```ts
readonly optional retryable: boolean;
```

#### Inherited from

[`InvalidInputError`](InvalidInputError.md).[`retryable`](InvalidInputError.md#retryable)

***

### sqlJobId?

```ts
readonly optional sqlJobId: string;
```

#### Inherited from

[`InvalidInputError`](InvalidInputError.md).[`sqlJobId`](InvalidInputError.md#sqljobid)

***

### stack?

```ts
optional stack: string;
```

#### Inherited from

[`InvalidInputError`](InvalidInputError.md).[`stack`](InvalidInputError.md#stack)

***

### statusCode?

```ts
readonly optional statusCode: number;
```

#### Inherited from

[`InvalidInputError`](InvalidInputError.md).[`statusCode`](InvalidInputError.md#statuscode)

***

### tableName?

```ts
readonly optional tableName: string;
```

#### Inherited from

[`InvalidInputError`](InvalidInputError.md).[`tableName`](InvalidInputError.md#tablename)

***

### code

```ts
readonly static code: "Schema" = "Schema";
```

#### Overrides

[`InvalidInputError`](InvalidInputError.md).[`code`](InvalidInputError.md#code-1)

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

[`InvalidInputError`](InvalidInputError.md).[`prepareStackTrace`](InvalidInputError.md#preparestacktrace)

***

### stackTraceLimit

```ts
static stackTraceLimit: number;
```

#### Inherited from

[`InvalidInputError`](InvalidInputError.md).[`stackTraceLimit`](InvalidInputError.md#stacktracelimit)

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

[`InvalidInputError`](InvalidInputError.md).[`captureStackTrace`](InvalidInputError.md#capturestacktrace)
