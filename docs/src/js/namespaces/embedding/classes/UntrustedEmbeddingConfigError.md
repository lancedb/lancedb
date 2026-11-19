[**@lancedb/lancedb**](../../../README.md) • **Docs**

***

[@lancedb/lancedb](../../../globals.md) / [embedding](../README.md) / UntrustedEmbeddingConfigError

# Class: UntrustedEmbeddingConfigError

Raised when an embedding function restored from table metadata is used
before its options are approved.

## Extends

- `Error`

## Constructors

### new UntrustedEmbeddingConfigError()

```ts
new UntrustedEmbeddingConfigError(message): UntrustedEmbeddingConfigError
```

#### Parameters

* **message**: `string`

#### Returns

[`UntrustedEmbeddingConfigError`](UntrustedEmbeddingConfigError.md)

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

### stack?

```ts
optional stack: string;
```

#### Inherited from

`Error.stack`

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
