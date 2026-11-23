[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / PrewarmPruningSummary

# Interface: PrewarmPruningSummary

Conservative metadata proof, rather than exact matching rows.

## Properties

### fallbackReason

```ts
fallbackReason: null | string;
```

***

### inputFragments

```ts
inputFragments: number;
```

***

### mode

```ts
mode: string;
```

Server-reported proof mode; future servers may add modes.

***

### prunedFragments

```ts
prunedFragments: number;
```

***

### selectedFragments

```ts
selectedFragments: number;
```

***

### selectedRowRanges

```ts
selectedRowRanges: null | number;
```

Candidate physical intervals, or null when unknown.
