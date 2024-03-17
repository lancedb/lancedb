Due to the nature of vector embeddings, they can be used to represent any kind of data, from text to images to audio. 
This makes them a very powerful tool for machine learning practitioners. 
However, there's no one-size-fits-all solution for generating embeddings - there are many different libraries and APIs 
(both commercial and open source) that can be used to generate embeddings from structured/unstructured data.

LanceDB supports 3 methods of working with embeddings.

1. You can manually generate embeddings for the data and queries. This is done outside of LanceDB.
2. You can use the built-in [embedding functions](./embedding_functions.md) to embed the data and queries in the background.
3. For python users, you can define your own [custom embedding function](./custom_embedding_function.md)
   that extends the default embedding functions.

For python users, there is also a legacy [with_embeddings API](./legacy.md).
It is retained for compatibility and will be removed in a future version.