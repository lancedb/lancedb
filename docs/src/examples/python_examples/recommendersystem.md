**Recommender Systems: Personalized Discovery🍿📺**
==============================================================
Deliver personalized experiences with Recommender Systems. 🎁

**Technical Overview📜**

🔍️ LanceDB's powerful vector database capabilities can efficiently store and query item embeddings. Recommender Systems can utilize it and provide personalized recommendations based on user preferences 🤝 and item features 📊 and therefore enhance the user experience.🗂️ 

| **Recommender System** | **Description** | **Links** |
| ---------------------- | --------------- | --------- |
| **Movie Recommender System🎬** | 🤝 Use **collaborative filtering** to predict user preferences, assuming similar users will like similar movies, and leverage **Singular Value Decomposition** (SVD) from Numpy for precise matrix factorization and accurate recommendations📊 | [![Github](../../assets/github.svg)][movie_github] <br>[![Open In Collab](../../assets/colab.svg)][movie_colab] <br>[![Python](../../assets/python.svg)][movie_python] |
| **🎥 Movie Recommendation with Genres** | 🔍 Creates movie embeddings using **Doc2Vec**, capturing genre and characteristic nuances, and leverages VectorDB for efficient storage and querying, enabling accurate genre classification and personalized movie recommendations through **similarity searches**🎥 | [![Github](../../assets/github.svg)][genre_github] <br>[![Open In Collab](../../assets/colab.svg)][genre_colab] <br>[![Ghost](../../assets/ghost.svg)][genre_ghost] |
| **🛍️ Product Recommender using Collaborative Filtering and LanceDB** | 📈 Using **Collaborative Filtering** and **LanceDB** to analyze your past purchases, recommends products based on user's past purchases. Demonstrated with the Instacart dataset in our example🛒 | [![Github](../../assets/github.svg)][product_github] <br>[![Open In Collab](../../assets/colab.svg)][product_colab] <br>[![Python](../../assets/python.svg)][product_python] |
| **🔍 Arxiv Search with OpenCLIP and LanceDB** | 💡 Build a semantic search engine for **Arxiv papers** using **LanceDB**, and benchmarks its performance against traditional keyword-based search on **Nomic's Atlas**, to demonstrate the power of semantic search in finding relevant research papers📚 | [![Github](../../assets/github.svg)][arxiv_github] <br>[![Open In Collab](../../assets/colab.svg)][arxiv_colab] <br>[![Python](../../assets/python.svg)][arxiv_python] |
| **Food Recommendation System🍴** | 🍔 Build a food recommendation system with **LanceDB**, featuring vector-based recommendations, full-text search, hybrid search, and reranking model integration for personalized and accurate food suggestions👌 | [![Github](../../assets/github.svg)][food_github] <br>[![Open In Collab](../../assets/colab.svg)][food_colab] |

[movie_github]: https://github.com/lancedb/vectordb-recipes/blob/main/examples/movie-recommender
[movie_colab]: https://colab.research.google.com/github/lancedb/vectordb-recipes/blob/main/examples/movie-recommender/main.ipynb
[movie_python]: https://github.com/lancedb/vectordb-recipes/blob/main/examples/movie-recommender/main.py


[genre_github]: https://github.com/lancedb/vectordb-recipes/blob/main/examples/movie-recommendation-with-genres
[genre_colab]: https://colab.research.google.com/github/lancedb/vectordb-recipes/blob/main/examples/movie-recommendation-with-genres/movie_recommendation_with_doc2vec_and_lancedb.ipynb
[genre_ghost]: https://blog.lancedb.com/movie-recommendation-system-using-lancedb-and-doc2vec/

[product_github]: https://github.com/lancedb/vectordb-recipes/blob/main/examples/product-recommender
[product_colab]: https://colab.research.google.com/github/lancedb/vectordb-recipes/blob/main/examples/product-recommender/main.ipynb
[product_python]: https://github.com/lancedb/vectordb-recipes/blob/main/examples/product-recommender/main.py


[arxiv_github]: https://github.com/lancedb/vectordb-recipes/blob/main/examples/arxiv-recommender
[arxiv_colab]: https://colab.research.google.com/github/lancedb/vectordb-recipes/blob/main/examples/arxiv-recommender/main.ipynb
[arxiv_python]: https://github.com/lancedb/vectordb-recipes/blob/main/examples/arxiv-recommender/main.py
 

[food_github]: https://github.com/lancedb/vectordb-recipes/blob/main/examples/Food_recommendation
[food_colab]: https://colab.research.google.com/github/lancedb/vectordb-recipes/blob/main/examples/Food_recommendation/main.ipynb
