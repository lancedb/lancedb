[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / TableDeltaTransaction

# Interface: TableDeltaTransaction

A committed transaction in a table delta range.

## Properties

### operation

```ts
operation: string;
```

***

### readVersion

```ts
readVersion: bigint;
```

The version the transaction was based on. Exact u64 value.

***

### tag?

```ts
optional tag: string;
```

***

### transactionProperties?

```ts
optional transactionProperties: Record<string, string>;
```

***

### uuid

```ts
uuid: string;
```
