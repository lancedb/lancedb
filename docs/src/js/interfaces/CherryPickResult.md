[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / CherryPickResult

# Interface: CherryPickResult

Result of previewing or attempting a cherry-pick.

## Properties

### diff

```ts
diff: BranchDiff;
```

***

### mainVersionAfter?

```ts
optional mainVersionAfter: number;
```

***

### preview

```ts
preview: CherryPickPreview;
```

***

### status

```ts
status:
  | "failed"
  | "unknown"
  | "ready"
  | "notImplemented"
  | "cherryPicked";
```
