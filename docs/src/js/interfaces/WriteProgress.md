[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / WriteProgress

# Interface: WriteProgress

Progress snapshot for a write operation, delivered to the `progress`
callback passed to [Table.add](../classes/Table.md#add).

## Properties

### activeTasks

```ts
activeTasks: number;
```

Number of parallel write tasks currently in flight.

***

### done

```ts
done: boolean;
```

`true` for the final callback; `false` otherwise.

***

### elapsedSeconds

```ts
elapsedSeconds: number;
```

Wall-clock seconds since the write started.

***

### outputBytes

```ts
outputBytes: number;
```

Number of bytes written so far.

***

### outputRows

```ts
outputRows: number;
```

Number of rows written so far.

***

### totalRows?

```ts
optional totalRows: number;
```

Total rows expected, when the input source reports it.

Always set on the final callback (the one with `done: true`), falling
back to the actual number of rows written when the source could not
report a row count up front.

***

### totalTasks

```ts
totalTasks: number;
```

Total number of parallel write tasks (the write parallelism).
