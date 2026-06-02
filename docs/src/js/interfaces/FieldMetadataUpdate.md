[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / FieldMetadataUpdate

# Interface: FieldMetadataUpdate

A per-field metadata update, addressed by dot-path.

## Properties

### metadata

```ts
metadata: Record<string, null | string>;
```

Metadata key/value pairs. Merged into the field's existing metadata by
default; a value of `null` deletes that key.

***

### path

```ts
path: string;
```

Dot-separated path to the field. For a top-level column this is just its
name; for a nested field it's the path, e.g. "a.b.c".

***

### replace?

```ts
optional replace: boolean;
```

If true, replace the field's entire metadata map instead of merging.
