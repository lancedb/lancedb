[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / FunctionQuarantineOptions

# Interface: FunctionQuarantineOptions

Select current failures to quarantine or release.

## Properties

### afterAttempts?

```ts
optional afterAttempts: number;
```

Only failures with at least this many attempts. Defaults to 1.

***

### errorType?

```ts
optional errorType: string;
```

Only failures with this exception class.

***

### jobId?

```ts
optional jobId: string;
```

Only failures recorded by this job.

***

### quarantined?

```ts
optional quarantined: boolean;
```

Quarantine by default. Set false to release failures for later refreshes.
