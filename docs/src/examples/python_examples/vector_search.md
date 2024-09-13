**Vector Search: Efficient Retrieval 🔓👀**
====================================================================

Vector search with LanceDB, is a solution for  efficient and accurate similarity searches in large datasets 📊. 

**Vector Search Capabilities in LanceDB🔝**

LanceDB implements vector search algorithms for efficient document retrieval and analysis 📊. This enables fast and accurate discovery of relevant documents, leveraging dense vector representations 🤖. The platform supports scalable indexing and querying of high-dimensional vector spaces, facilitating precise document matching and retrieval 📈.

| **Vector Search** | **Description** | **Links** |
|:-----------------|:---------------|:---------|
| **Inbuilt Hybrid Search 🔄** | Perform hybrid search in **LanceDB** by combining the results of semantic and full-text search via a reranking algorithm of your choice 📊 | [![Github](../../assets/github.svg)][inbuilt_hybrid_search_github] <br>[![Open In Collab](../../assets/colab.svg)][inbuilt_hybrid_search_colab] |                                                                                                                                                                                                                                                                                                    
| **Hybrid Search with BM25 and LanceDB 💡** | Use **Synergizes BM25's** keyword-focused precision (term frequency, document length normalization, bias-free retrieval) with **LanceDB's** semantic understanding (contextual analysis, query intent alignment) for nuanced search results in complex datasets 📈 | [![Github](../../assets/github.svg)][BM25_github] <br>[![Open In Collab](../../assets/colab.svg)][BM25_colab] <br>[![Ghost](../../assets/ghost.svg)][BM25_ghost] |                                                                                                                                                                                                                                                                                                    
| **NER-powered Semantic Search 🔎** | Extract and identify essential information from text with Named Entity Recognition **(NER)** methods: Dictionary-Based, Rule-Based, and Deep Learning-Based, to accurately extract and categorize entities, enabling precise semantic search results 🗂️ | [![Github](../../assets/github.svg)][NER_github] <br>[![Open In Collab](../../assets/colab.svg)][NER_colab] <br>[![Ghost](../../assets/ghost.svg)][NER_ghost]|                                                                                                                                                                                                                                                                                                    
| **Audio Similarity Search using Vector Embeddings 🎵** | Create vector **embeddings of audio files** to find similar audio content, enabling efficient audio similarity search and retrieval in **LanceDB's** vector store 📻 |[![Github](../../assets/github.svg)][audio_search_github] <br>[![Open In Collab](../../assets/colab.svg)][audio_search_colab] <br>[![Python](../../assets/python.svg)][audio_search_python]|                                                                                                                                                                                                                                                                                                    
| **LanceDB Embeddings API: Multi-lingual Semantic Search 🌎** | Build a universal semantic search table with **LanceDB's Embeddings API**, supporting multiple languages (e.g., English, French) using **cohere's** multi-lingual model, for accurate cross-lingual search results 📄 | [![Github](../../assets/github.svg)][mls_github] <br>[![Open In Collab](../../assets/colab.svg)][mls_colab] <br>[![Python](../../assets/python.svg)][mls_python] |                                                                                                                                                                                                                                                                                                    
| **Facial Recognition: Face Embeddings 🤖** | Detect, crop, and embed faces using Facenet, then store and query face embeddings in **LanceDB** for efficient facial recognition and top-K matching results 👥 | [![Github](../../assets/github.svg)][fr_github] <br>[![Open In Collab](../../assets/colab.svg)][fr_colab] |                                                                                                                                                                                                                                                                                                    
| **Sentiment Analysis: Hotel Reviews 🏨** | Analyze customer sentiments towards the hotel industry using **BERT models**, storing sentiment labels, scores, and embeddings in **LanceDB**, enabling queries on customer opinions and potential areas for improvement 💬 | [![Github](../../assets/github.svg)][sentiment_analysis_github] <br>[![Open In Collab](../../assets/colab.svg)][sentiment_analysis_colab] <br>[![Ghost](../../assets/ghost.svg)][sentiment_analysis_ghost] |                                                                                                                                                                                                                                                                                                    
| **Vector Arithmetic with LanceDB ⚖️** | Perform **vector arithmetic** on embeddings, enabling complex relationships and nuances in data to be captured, and simplifying the process of retrieving semantically similar results 📊 | [![Github](../../assets/github.svg)][arithmetic_github] <br>[![Open In Collab](../../assets/colab.svg)][arithmetic_colab] <br>[![Ghost](../../assets/ghost.svg)][arithmetic_ghost] |                                                                                                                                                                                                                                                                                                    
| **Imagebind Demo 🖼️** | Explore the multi-modal capabilities of **Imagebind** through a Gradio app, use **LanceDB API** for seamless image search and retrieval experiences 📸 | [![Github](../../assets/github.svg)][imagebind_github] <br> [![Open in Spaces](../../assets/open_hf_space.svg)][imagebind_huggingface] |                                                                                                                                                                                                                                                                                                    
| **Search Engine using SAM & CLIP 🔍** | Build a search engine within an image using **SAM** and **CLIP** models, enabling object-level search and retrieval, with LanceDB indexing and search capabilities to find the closest match between image embeddings and user queries 📸 | [![Github](../../assets/github.svg)][swi_github] <br>[![Open In Collab](../../assets/colab.svg)][swi_colab] <br>[![Ghost](../../assets/ghost.svg)][swi_ghost] |                                                                                                                                                                                                                                                                                                    
| **Zero Shot Object Localization and Detection with CLIP 🔎** | Perform object detection on images using **OpenAI's CLIP**, enabling zero-shot localization and detection of objects, with capabilities to split images into patches, parse with CLIP, and plot bounding boxes 📊 | [![Github](../../assets/github.svg)][zsod_github] <br>[![Open In Collab](../../assets/colab.svg)][zsod_colab] |                                                                                                                                                                                                                                                                                                    
| **Accelerate Vector Search with OpenVINO 🚀** | Boost vector search applications using **OpenVINO**, achieving significant speedups with **CLIP** for text-to-image and image-to-image searching, through PyTorch model optimization, FP16 and INT8 format conversion, and quantization with **OpenVINO NNCF** 📈 | [![Github](../../assets/github.svg)][openvino_github] <br>[![Open In Collab](../../assets/colab.svg)][openvino_colab] <br>[![Ghost](../../assets/ghost.svg)][openvino_ghost] |                                                                                                                                                                                                                                                                                                    
| **Zero-Shot Image Classification with CLIP and LanceDB 📸** | Achieve zero-shot image classification using **CLIP** and **LanceDB**, enabling models to classify images without prior training on specific use cases, unlocking flexible and adaptable image classification capabilities 🔓 | [![Github](../../assets/github.svg)][zsic_github] <br>[![Open In Collab](../../assets/colab.svg)][zsic_colab] <br>[![Ghost](../../assets/ghost.svg)][zsic_ghost] |                                                                                                                                                                                                                                                                                                    




[inbuilt_hybrid_search_github]: https://github.com/lancedb/vectordb-recipes/blob/main/examples/Inbuilt-Hybrid-Search
[inbuilt_hybrid_search_colab]: https://colab.research.google.com/github/lancedb/vectordb-recipes/blob/main/examples/Inbuilt-Hybrid-Search/Inbuilt_Hybrid_Search_with_LanceDB.ipynb

[BM25_github]: https://github.com/lancedb/vectordb-recipes/blob/main/examples/Hybrid_search_bm25_lancedb
[BM25_colab]: https://colab.research.google.com/github/lancedb/vectordb-recipes/blob/main/examples/Hybrid_search_bm25_lancedb/main.ipynb
[BM25_ghost]: https://blog.lancedb.com/hybrid-search-combining-bm25-and-semantic-search-for-better-results-with-lan-1358038fe7e6

[NER_github]: https://github.com/lancedb/vectordb-recipes/blob/main/tutorials/NER-powered-Semantic-Search
[NER_colab]: https://colab.research.google.com/github/lancedb/vectordb-recipes/blob/main/tutorials/NER-powered-Semantic-Search/NER_powered_Semantic_Search_with_LanceDB.ipynb
[NER_ghost]: https://blog.lancedb.com/ner-powered-semantic-search-using-lancedb-51051dc3e493

[audio_search_github]: https://github.com/lancedb/vectordb-recipes/blob/main/examples/audio_search
[audio_search_colab]: https://colab.research.google.com/github/lancedb/vectordb-recipes/blob/main/examples/audio_search/main.ipynb
[audio_search_python]: https://github.com/lancedb/vectordb-recipes/blob/main/examples/audio_search/main.py

[mls_github]: https://github.com/lancedb/vectordb-recipes/blob/main/examples/multi-lingual-wiki-qa
[mls_colab]: https://colab.research.google.com/github/lancedb/vectordb-recipes/blob/main/examples/multi-lingual-wiki-qa/main.ipynb
[mls_python]: https://github.com/lancedb/vectordb-recipes/blob/main/examples/multi-lingual-wiki-qa/main.py

[fr_github]: https://github.com/lancedb/vectordb-recipes/blob/main/examples/facial_recognition
[fr_colab]: https://colab.research.google.com/github/lancedb/vectordb-recipes/blob/main/examples/facial_recognition/main.ipynb

[sentiment_analysis_github]: https://github.com/lancedb/vectordb-recipes/blob/main/examples/Sentiment-Analysis-Analyse-Hotel-Reviews
[sentiment_analysis_colab]: https://colab.research.google.com/github/lancedb/vectordb-recipes/blob/main/examples/Sentiment-Analysis-Analyse-Hotel-Reviews/Sentiment_Analysis_using_LanceDB.ipynb
[sentiment_analysis_ghost]: https://blog.lancedb.com/sentiment-analysis-using-lancedb-2da3cb1e3fa6

[arithmetic_github]: https://github.com/lancedb/vectordb-recipes/blob/main/examples/Vector-Arithmetic-with-LanceDB
[arithmetic_colab]: https://colab.research.google.com/github/lancedb/vectordb-recipes/blob/main/examples/Vector-Arithmetic-with-LanceDB/main.ipynb
[arithmetic_ghost]: https://blog.lancedb.com/vector-arithmetic-with-lancedb-an-intro-to-vector-embeddings/

[imagebind_github]: https://github.com/lancedb/vectordb-recipes/blob/main/examples/imagebind_demo
[imagebind_huggingface]: https://huggingface.co/spaces/raghavd99/imagebind2

[swi_github]: https://github.com/lancedb/vectordb-recipes/blob/main/examples/search-within-images-with-sam-and-clip
[swi_colab]: https://colab.research.google.com/github/lancedb/vectordb-recipes/blob/main/examples/search-within-images-with-sam-and-clip/main.ipynb
[swi_ghost]: https://blog.lancedb.com/search-within-an-image-331b54e4285e

[zsod_github]: https://github.com/lancedb/vectordb-recipes/blob/main/examples/zero-shot-object-detection-CLIP
[zsod_colab]: https://colab.research.google.com/github/lancedb/vectordb-recipes/blob/main/examples/zero-shot-object-detection-CLIP/zero_shot_object_detection_clip.ipynb

[openvino_github]: https://github.com/lancedb/vectordb-recipes/blob/main/examples/Accelerate-Vector-Search-Applications-Using-OpenVINO
[openvino_colab]: https://colab.research.google.com/github/lancedb/vectordb-recipes/blob/main/examples/Accelerate-Vector-Search-Applications-Using-OpenVINO/clip_text_image_search.ipynb
[openvino_ghost]: https://blog.lancedb.com/accelerate-vector-search-applications-using-openvino-lancedb/

[zsic_github]: https://github.com/lancedb/vectordb-recipes/blob/main/examples/zero-shot-image-classification
[zsic_colab]: https://colab.research.google.com/github/lancedb/vectordb-recipes/blob/main/examples/zero-shot-image-classification/main.ipynb
[zsic_ghost]: https://blog.lancedb.com/zero-shot-image-classification-with-vector-search/





