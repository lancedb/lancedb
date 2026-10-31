[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / Expr

# Class: Expr

A SQL expression backed by the shared LanceDB expression builder.

## Example

```ts
table.query().where(col("firstName").eq("Alice")).toArray();
```

## Constructors

### new Expr()

```ts
new Expr(inner): Expr
```

#### Parameters

* **inner**: `Expr`

#### Returns

[`Expr`](Expr.md)

## Properties

### inner

```ts
inner: Expr;
```

## Methods

### add()

```ts
add(other): Expr
```

Add a value.

#### Parameters

* **other**: [`ExprLike`](../type-aliases/ExprLike.md)

#### Returns

[`Expr`](Expr.md)

***

### and()

```ts
and(other): Expr
```

Combine two predicates with AND.

#### Parameters

* **other**: [`Expr`](Expr.md)

#### Returns

[`Expr`](Expr.md)

***

### cast()

```ts
cast(dataType): Expr
```

Cast to an Arrow data type, such as "Int64".

#### Parameters

* **dataType**: `string`

#### Returns

[`Expr`](Expr.md)

***

### contains()

```ts
contains(substr): Expr
```

Test whether a string contains a substring.

#### Parameters

* **substr**: [`ExprLike`](../type-aliases/ExprLike.md)

#### Returns

[`Expr`](Expr.md)

***

### div()

```ts
div(other): Expr
```

Divide by a value.

#### Parameters

* **other**: [`ExprLike`](../type-aliases/ExprLike.md)

#### Returns

[`Expr`](Expr.md)

***

### eq()

```ts
eq(other): Expr
```

Compare for equality.

#### Parameters

* **other**: [`ExprLike`](../type-aliases/ExprLike.md)

#### Returns

[`Expr`](Expr.md)

***

### gt()

```ts
gt(other): Expr
```

Compare for greater than.

#### Parameters

* **other**: [`ExprLike`](../type-aliases/ExprLike.md)

#### Returns

[`Expr`](Expr.md)

***

### gte()

```ts
gte(other): Expr
```

Compare for greater than or equal to.

#### Parameters

* **other**: [`ExprLike`](../type-aliases/ExprLike.md)

#### Returns

[`Expr`](Expr.md)

***

### isIn()

```ts
isIn(values): Expr
```

Test membership in a list of values.

#### Parameters

* **values**: [`ExprLike`](../type-aliases/ExprLike.md)[]

#### Returns

[`Expr`](Expr.md)

***

### lower()

```ts
lower(): Expr
```

Convert a string to lowercase.

#### Returns

[`Expr`](Expr.md)

***

### lt()

```ts
lt(other): Expr
```

Compare for less than.

#### Parameters

* **other**: [`ExprLike`](../type-aliases/ExprLike.md)

#### Returns

[`Expr`](Expr.md)

***

### lte()

```ts
lte(other): Expr
```

Compare for less than or equal to.

#### Parameters

* **other**: [`ExprLike`](../type-aliases/ExprLike.md)

#### Returns

[`Expr`](Expr.md)

***

### mul()

```ts
mul(other): Expr
```

Multiply by a value.

#### Parameters

* **other**: [`ExprLike`](../type-aliases/ExprLike.md)

#### Returns

[`Expr`](Expr.md)

***

### ne()

```ts
ne(other): Expr
```

Compare for inequality.

#### Parameters

* **other**: [`ExprLike`](../type-aliases/ExprLike.md)

#### Returns

[`Expr`](Expr.md)

***

### not()

```ts
not(): Expr
```

Negate this predicate.

#### Returns

[`Expr`](Expr.md)

***

### or()

```ts
or(other): Expr
```

Combine two predicates with OR.

#### Parameters

* **other**: [`Expr`](Expr.md)

#### Returns

[`Expr`](Expr.md)

***

### sub()

```ts
sub(other): Expr
```

Subtract a value.

#### Parameters

* **other**: [`ExprLike`](../type-aliases/ExprLike.md)

#### Returns

[`Expr`](Expr.md)

***

### toSql()

```ts
toSql(): string
```

Serialize with the LanceDB SQL dialect, preserving column case.

#### Returns

`string`

***

### upper()

```ts
upper(): Expr
```

Convert a string to uppercase.

#### Returns

[`Expr`](Expr.md)
