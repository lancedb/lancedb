[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / FullTextQueryType

# Enumeration: FullTextQueryType

Enum representing the types of full-text queries supported.

- `Match`: Performs a full-text search for terms in the query string.
- `MatchPhrase`: Searches for an exact phrase match in the text.
- `Boost`: Boosts the relevance score of specific terms in the query.
- `MultiMatch`: Searches across multiple fields for the query terms.

## Enumeration Members

### Boolean

```ts
Boolean: "boolean";
```

***

### Boost

```ts
Boost: "boost";
```

***

### Match

```ts
Match: "match";
```

***

### MatchPhrase

```ts
MatchPhrase: "match_phrase";
```

***

### MultiMatch

```ts
MultiMatch: "multi_match";
```
