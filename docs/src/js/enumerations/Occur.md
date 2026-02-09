[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / Occur

# Enumeration: Occur

Enum representing the occurrence of terms in full-text queries.

- `Must`: The term must be present in the document.
- `Should`: The term should contribute to the document score, but is not required.
- `MustNot`: The term must not be present in the document.

## Enumeration Members

### Must

```ts
Must: "MUST";
```

***

### MustNot

```ts
MustNot: "MUST_NOT";
```

***

### Should

```ts
Should: "SHOULD";
```
