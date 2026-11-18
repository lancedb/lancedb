[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / FtsOptions

# Interface: FtsOptions

Options to create a full text search index

## Properties

### analyzer?

```ts
optional analyzer: "code" | "text";
```

The analyzer family to use.

"code" selects the code analyzer, which is the same as
`baseTokenizer: "code"`. "text" selects a text-family analyzer with the
selected `baseTokenizer`. When it is not set, the analyzer is inferred
from `baseTokenizer`.

"code" cannot be combined with a `baseTokenizer` other than "simple" or
"code", and "text" cannot be combined with `baseTokenizer: "code"`.

***

### asciiFolding?

```ts
optional asciiFolding: boolean;
```

whether to remove punctuation

***

### baseTokenizer?

```ts
optional baseTokenizer: BaseTokenizer;
```

The tokenizer to use when building the index.
The default is "simple".

The following tokenizers are available:

"simple" - Simple tokenizer. This tokenizer splits the text into tokens using whitespace and punctuation as a delimiter.

"whitespace" - Whitespace tokenizer. This tokenizer splits the text into tokens using whitespace as a delimiter.

"raw" - Raw tokenizer. This tokenizer does not split the text into tokens and indexes the entire text as a single token.

"code" - Code tokenizer. This tokenizer keeps snake_case and camelCase identifiers as single tokens.

"icu" - ICU dictionary-based word segmentation.

"icu/split" - ICU segmentation with simple-style delimiter splitting.

***

### blockSize?

```ts
optional blockSize: 128 | 256;
```

Number of documents per compressed posting block.

The default is 128. Supported values are 128 and 256. A value of 256 uses
the experimental FTS V3 format and may introduce breaking changes.

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

Index code operators such as `::`, `->` and `!=`.

The code analyzer default is false.

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

### ngramMaxLength?

```ts
optional ngramMaxLength: number;
```

ngram max length

***

### ngramMinLength?

```ts
optional ngramMinLength: number;
```

ngram min length

***

### prefixOnly?

```ts
optional prefixOnly: boolean;
```

whether to only index the prefix of the token for ngram tokenizer

***

### preserveOriginal?

```ts
optional preserveOriginal: boolean;
```

Also index the complete identifier next to its subwords.

The code analyzer default is true, but this only takes effect when
`splitIdentifiers` is true.

***

### removeStopWords?

```ts
optional removeStopWords: boolean;
```

whether to remove stop words

***

### splitIdentifiers?

```ts
optional splitIdentifiers: boolean;
```

Split code identifiers such as `camelCase` and `snake_case` into subwords.

Setting this to true requires the code analyzer. The default is false, so
`analyzer: "code"` alone keeps identifiers whole.

Note: with the default `preserveOriginal`, partial-identifier AND and
phrase queries on JSON columns can miss matches
(https://github.com/lance-format/lance/issues/9674). Spell phrase queries
with the same identifier boundaries as the indexed text
(https://github.com/lance-format/lance/issues/9658).

***

### splitOnNumerics?

```ts
optional splitOnNumerics: boolean;
```

Split identifier subwords at letter and number boundaries, for example
`utf8` into `utf` and `8`.

The code analyzer default is true, but this only takes effect when
`splitIdentifiers` is true.

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
