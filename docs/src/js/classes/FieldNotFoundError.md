[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / FieldNotFoundError

# Class: FieldNotFoundError

A query referenced a field the schema does not contain.

This is also a [ColumnNotFoundError](ColumnNotFoundError.md). `code` remains `"FieldNotFound"`
so callers can tell a schema-qualified miss from a plain missing column.

## Extends

- [`ColumnNotFoundError`](ColumnNotFoundError.md)

## Constructors

### new FieldNotFoundError()

```ts
new FieldNotFoundError(message, options?): FieldNotFoundError
```

#### Parameters

* **message**: `string`

* **options?**: [`LanceErrorOptions`](../interfaces/LanceErrorOptions.md)

#### Returns

[`FieldNotFoundError`](FieldNotFoundError.md)

#### Inherited from

[`ColumnNotFoundError`](ColumnNotFoundError.md).[`constructor`](ColumnNotFoundError.md#constructors)

## Properties

### cause?

```ts
optional cause: unknown;
```

#### Inherited from

[`ColumnNotFoundError`](ColumnNotFoundError.md).[`cause`](ColumnNotFoundError.md#cause)

***

### code

```ts
readonly code: string;
```

#### Inherited from

[`ColumnNotFoundError`](ColumnNotFoundError.md).[`code`](ColumnNotFoundError.md#code)

***

### columnName?

```ts
readonly optional columnName: string;
```

#### Inherited from

[`ColumnNotFoundError`](ColumnNotFoundError.md).[`columnName`](ColumnNotFoundError.md#columnname)

***

### connectFailures?

```ts
readonly optional connectFailures: number;
```

#### Inherited from

[`ColumnNotFoundError`](ColumnNotFoundError.md).[`connectFailures`](ColumnNotFoundError.md#connectfailures)

***

### databaseName?

```ts
readonly optional databaseName: string;
```

#### Inherited from

[`ColumnNotFoundError`](ColumnNotFoundError.md).[`databaseName`](ColumnNotFoundError.md#databasename)

***

### errorCode?

```ts
readonly optional errorCode: string;
```

#### Inherited from

[`ColumnNotFoundError`](ColumnNotFoundError.md).[`errorCode`](ColumnNotFoundError.md#errorcode)

***

### functionName?

```ts
readonly optional functionName: string;
```

#### Inherited from

[`ColumnNotFoundError`](ColumnNotFoundError.md).[`functionName`](ColumnNotFoundError.md#functionname)

***

### grpcCode?

```ts
readonly optional grpcCode: number;
```

#### Inherited from

[`ColumnNotFoundError`](ColumnNotFoundError.md).[`grpcCode`](ColumnNotFoundError.md#grpccode)

***

### indexName?

```ts
readonly optional indexName: string;
```

#### Inherited from

[`ColumnNotFoundError`](ColumnNotFoundError.md).[`indexName`](ColumnNotFoundError.md#indexname)

***

### jobId?

```ts
readonly optional jobId: string;
```

#### Inherited from

[`ColumnNotFoundError`](ColumnNotFoundError.md).[`jobId`](ColumnNotFoundError.md#jobid)

***

### jobRequestId?

```ts
readonly optional jobRequestId: string;
```

#### Inherited from

[`ColumnNotFoundError`](ColumnNotFoundError.md).[`jobRequestId`](ColumnNotFoundError.md#jobrequestid)

***

### maxConnectFailures?

```ts
readonly optional maxConnectFailures: number;
```

#### Inherited from

[`ColumnNotFoundError`](ColumnNotFoundError.md).[`maxConnectFailures`](ColumnNotFoundError.md#maxconnectfailures)

***

### maxReadFailures?

```ts
readonly optional maxReadFailures: number;
```

#### Inherited from

[`ColumnNotFoundError`](ColumnNotFoundError.md).[`maxReadFailures`](ColumnNotFoundError.md#maxreadfailures)

***

### maxRequestFailures?

```ts
readonly optional maxRequestFailures: number;
```

#### Inherited from

[`ColumnNotFoundError`](ColumnNotFoundError.md).[`maxRequestFailures`](ColumnNotFoundError.md#maxrequestfailures)

***

### message

```ts
message: string;
```

#### Inherited from

[`ColumnNotFoundError`](ColumnNotFoundError.md).[`message`](ColumnNotFoundError.md#message)

***

### name

```ts
name: string;
```

#### Inherited from

[`ColumnNotFoundError`](ColumnNotFoundError.md).[`name`](ColumnNotFoundError.md#name)

***

### namespacePath?

```ts
readonly optional namespacePath: string[];
```

#### Inherited from

[`ColumnNotFoundError`](ColumnNotFoundError.md).[`namespacePath`](ColumnNotFoundError.md#namespacepath)

***

### path?

```ts
readonly optional path: string;
```

#### Inherited from

[`ColumnNotFoundError`](ColumnNotFoundError.md).[`path`](ColumnNotFoundError.md#path)

***

### phase?

```ts
readonly optional phase: string;
```

#### Inherited from

[`ColumnNotFoundError`](ColumnNotFoundError.md).[`phase`](ColumnNotFoundError.md#phase)

***

### readFailures?

```ts
readonly optional readFailures: number;
```

#### Inherited from

[`ColumnNotFoundError`](ColumnNotFoundError.md).[`readFailures`](ColumnNotFoundError.md#readfailures)

***

### requestFailures?

```ts
readonly optional requestFailures: number;
```

#### Inherited from

[`ColumnNotFoundError`](ColumnNotFoundError.md).[`requestFailures`](ColumnNotFoundError.md#requestfailures)

***

### requestId?

```ts
readonly optional requestId: string;
```

#### Inherited from

[`ColumnNotFoundError`](ColumnNotFoundError.md).[`requestId`](ColumnNotFoundError.md#requestid)

***

### resourceKind?

```ts
readonly optional resourceKind: string;
```

#### Inherited from

[`ColumnNotFoundError`](ColumnNotFoundError.md).[`resourceKind`](ColumnNotFoundError.md#resourcekind)

***

### resourceName?

```ts
readonly optional resourceName: string;
```

#### Inherited from

[`ColumnNotFoundError`](ColumnNotFoundError.md).[`resourceName`](ColumnNotFoundError.md#resourcename)

***

### retryable?

```ts
readonly optional retryable: boolean;
```

#### Inherited from

[`ColumnNotFoundError`](ColumnNotFoundError.md).[`retryable`](ColumnNotFoundError.md#retryable)

***

### sqlJobId?

```ts
readonly optional sqlJobId: string;
```

#### Inherited from

[`ColumnNotFoundError`](ColumnNotFoundError.md).[`sqlJobId`](ColumnNotFoundError.md#sqljobid)

***

### stack?

```ts
optional stack: string;
```

#### Inherited from

[`ColumnNotFoundError`](ColumnNotFoundError.md).[`stack`](ColumnNotFoundError.md#stack)

***

### statusCode?

```ts
readonly optional statusCode: number;
```

#### Inherited from

[`ColumnNotFoundError`](ColumnNotFoundError.md).[`statusCode`](ColumnNotFoundError.md#statuscode)

***

### tableName?

```ts
readonly optional tableName: string;
```

#### Inherited from

[`ColumnNotFoundError`](ColumnNotFoundError.md).[`tableName`](ColumnNotFoundError.md#tablename)

***

### code

```ts
readonly static code: "FieldNotFound" = "FieldNotFound";
```

#### Overrides

[`ColumnNotFoundError`](ColumnNotFoundError.md).[`code`](ColumnNotFoundError.md#code-1)

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

[`ColumnNotFoundError`](ColumnNotFoundError.md).[`prepareStackTrace`](ColumnNotFoundError.md#preparestacktrace)

***

### stackTraceLimit

```ts
static stackTraceLimit: number;
```

#### Inherited from

[`ColumnNotFoundError`](ColumnNotFoundError.md).[`stackTraceLimit`](ColumnNotFoundError.md#stacktracelimit)

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

[`ColumnNotFoundError`](ColumnNotFoundError.md).[`captureStackTrace`](ColumnNotFoundError.md#capturestacktrace)
