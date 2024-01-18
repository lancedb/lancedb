Due to the nature of vector embeddings, they can be used to represent any kind of data, from text to images to audio. This makes them a very powerful tool for machine learning practitioners. However, there's no one-size-fits-all solution for generating embeddings - there are many different libraries and APIs (both commercial and open source) that can be used to generate embeddings from structured/unstructured data.

LanceDB supports 2 methods of vectorizing your raw data into embeddings.

1. **Explicit**: By manually calling LanceDB's `with_embedding` function to vectorize your data via an `embed_func` of your choice
2. **Implicit**: Allow LanceDB to embed the data and queries in the background as they come in, by using the table's `EmbeddingRegistry` information

See the [explicit](embedding_explicit.md) and [implicit](embedding_functions.md) embedding sections for more details.