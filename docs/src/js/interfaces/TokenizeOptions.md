[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / TokenizeOptions

# Interface: TokenizeOptions

Options for tokenizing a full-text search query without a table index.

## Properties

### analyzer?

```ts
optional analyzer: "code" | "text";
```

The analyzer family to use. See [FtsOptions.analyzer](FtsOptions.md#analyzer).

***

### asciiFolding?

```ts
optional asciiFolding: boolean;
```

Whether to fold ASCII characters.

***

### baseTokenizer?

```ts
optional baseTokenizer: BaseTokenizer;
```

The tokenizer to use. The default is "simple".

***

### customStopWords?

```ts
optional customStopWords: string[];
```

Custom stop words that replace the built-in list for `language`.

This option only affects tokenization when `removeStopWords` is true.

`undefined` keeps the built-in language list. An empty array explicitly
replaces it with no stop words.

***

### indexOperators?

```ts
optional indexOperators: boolean;
```

Emit code operators such as `::`, `->` and `!=`.

***

### language?

```ts
optional language: string;
```

Language for stemming and stop words.

***

### lowercase?

```ts
optional lowercase: boolean;
```

Whether to lowercase tokens.

***

### maxTokenLength?

```ts
optional maxTokenLength: number;
```

Maximum token length; tokens longer than this are ignored.

***

### ngramMaxLength?

```ts
optional ngramMaxLength: number;
```

N-gram maximum length.

***

### ngramMinLength?

```ts
optional ngramMinLength: number;
```

N-gram minimum length.

***

### prefixOnly?

```ts
optional prefixOnly: boolean;
```

Whether to only emit token prefixes for the n-gram tokenizer.

***

### preserveOriginal?

```ts
optional preserveOriginal: boolean;
```

Also emit the complete identifier next to its subwords.

***

### removeStopWords?

```ts
optional removeStopWords: boolean;
```

Whether to remove stop words.

***

### splitIdentifiers?

```ts
optional splitIdentifiers: boolean;
```

Split code identifiers into subwords. See [FtsOptions.splitIdentifiers](FtsOptions.md#splitidentifiers).

***

### splitOnNumerics?

```ts
optional splitOnNumerics: boolean;
```

Split identifier subwords at letter and number boundaries.

***

### stem?

```ts
optional stem: boolean;
```

Whether to stem tokens.
