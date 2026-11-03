[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / DataTypeLike

# Type Alias: DataTypeLike

```ts
type DataTypeLike: DataType | object;
```

A `DataType` from any copy or version of apache-arrow.

Arrow 21 brands its classes with `unique symbol` properties, so a type
object from a second copy of the library no longer satisfies the `DataType`
type of this one even though it is structurally identical. Inputs that only
need to be sanitized accept this looser shape instead.
