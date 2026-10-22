[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / NativeJsHeaderProvider

# Class: NativeJsHeaderProvider

JavaScript HeaderProvider implementation that wraps a JavaScript callback.
This is the only native header provider - all header provider implementations
should provide a JavaScript function that returns headers.

## Constructors

### new NativeJsHeaderProvider()

```ts
new NativeJsHeaderProvider(getHeadersCallback): NativeJsHeaderProvider
```

Create a new JsHeaderProvider from a JavaScript callback

#### Parameters

* **getHeadersCallback**

#### Returns

[`NativeJsHeaderProvider`](NativeJsHeaderProvider.md)
