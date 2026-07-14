[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / TokenizeOptions

# Interface: TokenizeOptions

Options for tokenizing a full-text search query without a table index.

## Properties

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

### removeStopWords?

```ts
optional removeStopWords: boolean;
```

Whether to remove stop words.

***

### stem?

```ts
optional stem: boolean;
```

Whether to stem tokens.
