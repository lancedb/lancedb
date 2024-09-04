# 📚 LanceDB Embedding Functions

There are various embedding functions available out of the box with LanceDB to manage your embeddings implicitly. We're actively working on adding other popular embedding APIs and models. 🚀

## Text Embedding Functions 📝 
These functions are registered by default to handle text embeddings.

- 🔄 **Embedding functions** have an inbuilt rate limit handler wrapper for source and query embedding function calls that retry with **exponential backoff**. 

- 🌕 Each `EmbeddingFunction` implementation automatically takes `max_retries` as an argument which has the default value of 7. 

**Available Text Embeddings**:

- [Sentence Transformers](available_embedding_models/text_embedding_functions/sentence_transformers.md)
- [Huggingface Embedding Models](available_embedding_models/text_embedding_functions/huggingface_embedding.md)
- [Ollama Embeddings](available_embedding_models/text_embedding_functions/ollama_embedding.md)
- [OpenAI Embeddings](available_embedding_models/text_embedding_functions/openai_embedding.md)
- [Instructor Embeddings](available_embedding_models/text_embedding_functions/instructor_embedding.md)
- [Gemini Embeddings](available_embedding_models/text_embedding_functions/gemini_embedding.md)
- [Cohere Embeddings](available_embedding_models/text_embedding_functions/cohere_embedding.md)
- [Jina Embeddings](available_embedding_models/text_embedding_functions/jina_embedding.md)
- [AWS Bedrock Text Embedding Functions](available_embedding_models/text_embedding_functions/aws_bedrock_embedding.md)
- [IBM Watsonx.ai Embeddings](available_embedding_models/text_embedding_functions/ibm_watsonx_ai_embedding.md)


## Multi-modal Embedding Functions🖼️ 

Multi-modal embedding functions allow you to query your table using both images and text. 💬🖼️

**Available Multi-modal Embeddings** :

- [OpenClip Embeddings](available_embedding_models/multimodal_embedding_functions/openclip_embedding.md)
- [Imagebind Embeddings](available_embedding_models/multimodal_embedding_functions/imagebind_embedding.md)
- [Jina Embeddings](available_embedding_models/multimodal_embedding_functions/jina_multimodal_embedding.md)