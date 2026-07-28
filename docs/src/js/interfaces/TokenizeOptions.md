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

### customStopWords?

```ts
optional customStopWords: CustomStopWordsSource;
```

Custom stop words that replace the built-in list for `language`.

The source can be an inline string array, a newline-delimited UTF-8 file
on this client, or a string column from a local/native LanceDB table.
Remote table sources are rejected because they cannot currently guarantee
a complete snapshot. This option is only applied when `removeStopWords` is
true.

`undefined` keeps the built-in language list. An empty array explicitly
replaces it with no stop words.

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
