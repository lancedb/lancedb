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
optional baseTokenizer: BaseTokenizer;
```

The tokenizer to use when building the index.
The default is "simple".

The following tokenizers are available:

"simple" - Simple tokenizer. This tokenizer splits the text into tokens using whitespace and punctuation as a delimiter.

"whitespace" - Whitespace tokenizer. This tokenizer splits the text into tokens using whitespace as a delimiter.

"raw" - Raw tokenizer. This tokenizer does not split the text into tokens and indexes the entire text as a single token.

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
optional customStopWords: CustomStopWordsSource;
```

Custom stop words that replace the built-in list for `language`.

This option only affects tokenization when `removeStopWords` is true. The
source can be an inline string array, a newline-delimited UTF-8 file on
this client, or a string column from a local/native LanceDB table.
Remote table sources are rejected because they cannot currently guarantee
a complete snapshot; the target table may still be remote.

`undefined` keeps the built-in language list. An empty array explicitly
replaces it with no stop words.

Embedded/local fuzzy queries with `fuzziness > 0` are rejected while this
snapshot and `removeStopWords` are active. Use `fuzziness: 0`, or omit the
custom snapshot. Remote tables reject every explicit `fuzziness > 0`
query, regardless of stop-word configuration, because the server protocol
does not declare tokenizer-snapshot-safe fuzzy search; omit `fuzziness` or
use `fuzziness: 0`.

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
