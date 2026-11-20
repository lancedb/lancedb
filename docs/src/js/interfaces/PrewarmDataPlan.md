[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / PrewarmDataPlan

# Interface: PrewarmDataPlan

Metadata-only snapshot preview; no warming job is submitted.

## Properties

### plan

```ts
plan: string;
```

Readable display text. Use pruning for structured facts.

***

### pruning

```ts
pruning: PrewarmPruningSummary;
```

***

### tableVersion

```ts
tableVersion: number;
```

Snapshot used for this preview. A later submission plans its own snapshot.
