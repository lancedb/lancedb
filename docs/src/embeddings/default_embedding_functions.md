# 📚 Available Embedding Models

There are various embedding functions available out of the box with LanceDB to manage your embeddings implicitly. We're actively working on adding other popular embedding APIs and models. 🚀

Before jumping on the list of available models, let's understand how to get an embedding model initialized and configured to use in our code: 

!!! example "Example usage"
    ```python
    model = get_registry()
              .get("openai")
              .create(name="text-embedding-ada-002")
    ```

Now let's understand the above syntax: 
```python
model = get_registry().get("model_id").create(...params)
```
**This👆 line effectively creates a configured instance of an `embedding function` with `model` of choice that is ready for use.**

- `get_registry()` :  This function call returns an instance of a `EmbeddingFunctionRegistry` object. This registry manages the registration and retrieval of embedding functions.

- `.get("model_id")` : This method call on the registry object and retrieves the **embedding models functions** associated with the `"model_id"` (1) .
    { .annotate }

    1.  Hover over the names in table below to find out the `model_id` of different embedding functions.

- `.create(...params)` : This method call is on the object returned by the `get` method. It instantiates an embedding model function using the **specified parameters**. 

??? question "What parameters does the `.create(...params)` method accepts?"
    **Checkout the documentation of specific embedding models (links in the table below👇) to know what parameters it takes**.

!!! tip "Moving on"
    Now that we know how to get the **desired embedding model** and use it in our code, let's explore the comprehensive **list** of embedding models **supported by LanceDB**, in the tables below.

## Text Embedding Functions 📝 
These functions are registered by default to handle text embeddings.

- 🔄 **Embedding functions** have an inbuilt rate limit handler wrapper for source and query embedding function calls that retry with **exponential backoff**. 

- 🌕 Each `EmbeddingFunction` implementation automatically takes `max_retries` as an argument which has the default value of 7. 

🌟 **Available Text Embeddings**

| **Embedding** :material-information-outline:{ title="Hover over the name to find out the model_id" } | **Description** | **Documentation** |
|-----------|-------------|---------------|
| [**Sentence Transformers**](available_embedding_models/text_embedding_functions/sentence_transformers.md "sentence-transformers")  | 🧠 **SentenceTransformers** is a Python framework for state-of-the-art sentence, text, and image embeddings. | [<img src="https://raw.githubusercontent.com/lancedb/assets/main/docs/assets/logos/sbert_2.png" alt="Sentence Transformers Icon" width="90" height="35">](available_embedding_models/text_embedding_functions/sentence_transformers.md)|                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               
| [**Huggingface Models**](available_embedding_models/text_embedding_functions/huggingface_embedding.md "huggingface") |🤗 We offer support for all **Huggingface** models. The default model is `colbert-ir/colbertv2.0`. | [<img src="https://raw.githubusercontent.com/lancedb/assets/main/docs/assets/logos/hugging_face.png" alt="Huggingface Icon" width="130" height="35">](available_embedding_models/text_embedding_functions/huggingface_embedding.md) |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               
| [**Ollama Embeddings**](available_embedding_models/text_embedding_functions/ollama_embedding.md "ollama") | 🔍 Generate embeddings via the **Ollama** python library. Ollama supports embedding models, making it possible to build RAG apps. | [<img src="https://raw.githubusercontent.com/lancedb/assets/main/docs/assets/logos/Ollama.png" alt="Ollama Icon" width="110" height="35">](available_embedding_models/text_embedding_functions/ollama_embedding.md)|                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               
| [**OpenAI Embeddings**](available_embedding_models/text_embedding_functions/openai_embedding.md "openai")| 🔑 **OpenAI’s** text embeddings measure the relatedness of text strings. **LanceDB** supports state-of-the-art embeddings from OpenAI. | [<img src="https://raw.githubusercontent.com/lancedb/assets/main/docs/assets/logos/openai.png" alt="OpenAI Icon" width="100" height="35">](available_embedding_models/text_embedding_functions/openai_embedding.md)|                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               
| [**Instructor Embeddings**](available_embedding_models/text_embedding_functions/instructor_embedding.md "instructor") | 📚 **Instructor**: An instruction-finetuned text embedding model that can generate text embeddings tailored to any task and domains by simply providing the task instruction, without any finetuning. | [<img src="https://raw.githubusercontent.com/lancedb/assets/main/docs/assets/logos/instructor_embedding.png" alt="Instructor Embedding Icon" width="140" height="35">](available_embedding_models/text_embedding_functions/instructor_embedding.md) |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               
| [**Gemini Embeddings**](available_embedding_models/text_embedding_functions/gemini_embedding.md "gemini-text") | 🌌 Google’s Gemini API generates state-of-the-art embeddings for words, phrases, and sentences. | [<img src="https://raw.githubusercontent.com/lancedb/assets/main/docs/assets/logos/gemini.png" alt="Gemini Icon" width="95" height="35">](available_embedding_models/text_embedding_functions/gemini_embedding.md) |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               
| [**Cohere Embeddings**](available_embedding_models/text_embedding_functions/cohere_embedding.md "cohere") | 💬 This will help you get started with **Cohere** embedding models using LanceDB. Using cohere API requires cohere package. Install it via `pip`. | [<img src="https://raw.githubusercontent.com/lancedb/assets/main/docs/assets/logos/cohere.png" alt="Cohere Icon" width="140" height="35">](available_embedding_models/text_embedding_functions/cohere_embedding.md) |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               
| [**Jina Embeddings**](available_embedding_models/text_embedding_functions/jina_embedding.md "jina") | 🔗 World-class embedding models to improve your search and RAG systems. You will need **jina api key**. | [<img src="https://raw.githubusercontent.com/lancedb/assets/main/docs/assets/logos/jina.png" alt="Jina Icon" width="90" height="35">](available_embedding_models/text_embedding_functions/jina_embedding.md) |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               
| [ **AWS Bedrock Functions**](available_embedding_models/text_embedding_functions/aws_bedrock_embedding.md "bedrock-text") | ☁️ AWS Bedrock supports multiple base models for generating text embeddings. You need to setup the AWS credentials to use this embedding function. | [<img src="https://raw.githubusercontent.com/lancedb/assets/main/docs/assets/logos/aws_bedrock.png" alt="AWS Bedrock Icon" width="120" height="35">](available_embedding_models/text_embedding_functions/aws_bedrock_embedding.md) |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               
| [**IBM Watsonx.ai**](available_embedding_models/text_embedding_functions/ibm_watsonx_ai_embedding.md "watsonx") | 💡 Generate text embeddings using IBM's watsonx.ai platform. **Note**: watsonx.ai library is an optional dependency. | [<img src="https://raw.githubusercontent.com/lancedb/assets/main/docs/assets/logos/watsonx.png" alt="Watsonx Icon" width="140" height="35">](available_embedding_models/text_embedding_functions/ibm_watsonx_ai_embedding.md) |



[st-key]: "sentence-transformers"
[hf-key]: "huggingface"
[ollama-key]: "ollama"
[openai-key]: "openai"
[instructor-key]: "instructor"
[gemini-key]: "gemini-text"
[cohere-key]: "cohere"
[jina-key]: "jina"
[aws-key]: "bedrock-text"
[watsonx-key]: "watsonx"


## Multi-modal Embedding Functions🖼️ 

Multi-modal embedding functions allow you to query your table using both images and text. 💬🖼️

🌐 **Available Multi-modal Embeddings**

| Embedding :material-information-outline:{ title="Hover over the name to find out the model_id" }  | Description | Documentation  |
|-----------|-------------|---------------|
| [**OpenClip Embeddings**](available_embedding_models/multimodal_embedding_functions/openclip_embedding.md "open-clip") | 🎨 We support CLIP model embeddings using the open source alternative, **open-clip** which supports various customizations. | [<img src="https://raw.githubusercontent.com/lancedb/assets/main/docs/assets/logos/openclip_github.png" alt="openclip Icon" width="150" height="35">](available_embedding_models/multimodal_embedding_functions/openclip_embedding.md) |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    
| [**Imagebind Embeddings**](available_embedding_models/multimodal_embedding_functions/imagebind_embedding.md "imageind") | 🌌  We have support for **imagebind model embeddings**. You can download our version of the packaged model via - `pip install imagebind-packaged==0.1.2`. | [<img src="https://raw.githubusercontent.com/lancedb/assets/main/docs/assets/logos/imagebind_meta.png" alt="imagebind Icon" width="150" height="35">](available_embedding_models/multimodal_embedding_functions/imagebind_embedding.md)|                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    
| [**Jina Multi-modal Embeddings**](available_embedding_models/multimodal_embedding_functions/jina_multimodal_embedding.md "jina") | 🔗 **Jina embeddings** can also be used to embed both **text** and **image** data, only some of the models support image data and you can check the detailed documentation. 👉 | [<img src="https://raw.githubusercontent.com/lancedb/assets/main/docs/assets/logos/jina.png" alt="jina Icon" width="90" height="35">](available_embedding_models/multimodal_embedding_functions/jina_multimodal_embedding.md) |

!!! note
    If you'd like to request support for additional **embedding functions**, please feel free to open an issue on our LanceDB [GitHub issue page](https://github.com/lancedb/lancedb/issues).