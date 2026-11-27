[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / func

# Function: func()

```ts
function func(name, ...args): Expr
```

Call a registered SQL function, such as `func("lower", col("firstName"))`.

## Parameters

* **name**: `string`

* ...**args**: [`ExprLike`](../type-aliases/ExprLike.md)[]

## Returns

[`Expr`](../classes/Expr.md)
