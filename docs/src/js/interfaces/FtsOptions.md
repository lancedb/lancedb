[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / FtsOptions

# Interface: FtsOptions

Options to create a full text search index

## Properties

### asciiFolding?

```ts
optional asciiFolding: boolean;
```

whether to remove punctuation

***

### baseTokenizer?

```ts
optional baseTokenizer: "raw" | "simple" | "whitespace";
```

The tokenizer to use when building the index.
The default is "simple".

The following tokenizers are available:

"simple" - Simple tokenizer. This tokenizer splits the text into tokens using whitespace and punctuation as a delimiter.

"whitespace" - Whitespace tokenizer. This tokenizer splits the text into tokens using whitespace as a delimiter.

"raw" - Raw tokenizer. This tokenizer does not split the text into tokens and indexes the entire text as a single token.

***

### language?

```ts
optional language: string;
```

language for stemming and stop words
this is only used when `stem` or `remove_stop_words` is true

***

### lowercase?

```ts
optional lowercase: boolean;
```

whether to lowercase tokens

***

### maxTokenLength?

```ts
optional maxTokenLength: number;
```

maximum token length
tokens longer than this length will be ignored

***

### removeStopWords?

```ts
optional removeStopWords: boolean;
```

whether to remove stop words

***

### stem?

```ts
optional stem: boolean;
```

whether to stem tokens

***

### withPosition?

```ts
optional withPosition: boolean;
```

Whether to build the index with positions.
True by default.
If set to false, the index will not store the positions of the tokens in the text,
which will make the index smaller and faster to build, but will not support phrase queries.
