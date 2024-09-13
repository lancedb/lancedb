# Understand Embeddings

The term **dimension** is a synonym for the number of elements in a feature vector. Each feature can be thought of as a different axis in a geometric space. 

High-dimensional data means there are many features(or attributes) in the data.

!!! example
     1. An image is a data point and it might have thousands of dimensions because each pixel could be considered as a feature. 

     2. Text data, when represented by each word or character, can also lead to high dimensions, especially when considering all possible words in a language.

Embedding captures **meaning and relationships** within data by mapping high-dimensional data into a lower-dimensional space. It captures it by placing inputs that are more **similar in meaning** closer together in the **embedding space**. 

## What are Vector Embeddings?

Vector embeddings is a way to convert complex data, like text, images, or audio into numerical coordinates (called vectors) that can be plotted in an n-dimensional space(embedding space). 

The closer these data points are related in the real world, the closer their corresponding numerical coordinates (vectors) will be to each other in the embedding space. This proximity in the embedding space reflects their semantic similarities, allowing machines to intuitively understand and process the data in a way that mirrors human perception of relationships and meaning.

In a way, it captures the most important aspects of the data while ignoring the less important ones. As a result, tasks like searching for related content or identifying patterns become more efficient and accurate, as the embeddings make it possible to quantify how **closely related** different **data points** are and **reduce** the **computational complexity**.

??? question "Are vectors and embeddings the same thing?"

    When we say “vectors” we mean - **list of numbers** that **represents the data**. 
    When we say “embeddings” we mean - **list of numbers** that **capture important details and relationships**.

    Although the terms are often used interchangeably, “embeddings” highlight how the data is represented with meaning and structure, while “vector” simply refers to the numerical form of that representation.

## Embedding vs Indexing

We already saw that creating **embeddings** on data is a method of creating **vectors** for a **n-dimensional embedding space** that captures the meaning and relationships inherent in the data.

Once we have these **vectors**, indexing comes into play. Indexing is a method of organizing these vector embeddings, that allows us to quickly and efficiently locate and retrieve them from the entire dataset of vector embeddings.

## What types of data/objects can be embedded?

The following are common types of data that can be embedded:

1. **Text**: Text data includes sentences, paragraphs, documents, or any written content.
2. **Images**:  Image data encompasses photographs, illustrations, or any visual content.
3. **Audio**: Audio data includes sounds, music, speech, or any auditory content.
4. **Video**:  Video data consists of moving images and sound, which can convey complex information.

Large datasets of multi-modal data (text, audio, images, etc.) can be converted into embeddings with the appropriate model.

!!! tip "LanceDB vs Other traditional Vector DBs"
    While many vector databases primarily focus on the storage and retrieval of vector embeddings, **LanceDB** uses **Lance file format** (operates on a disk-based architecture), which allows for the storage and management of not just embeddings but also **raw file data (bytes)**. This capability means that users can integrate various types of data, including images and text, alongside their vector embeddings in a unified system.

    With the ability to store both vectors and associated file data, LanceDB enhances the querying process. Users can perform semantic searches that not only retrieve similar embeddings but also access related files and metadata, thus streamlining the workflow.

## How does embedding works?

As mentioned, after creating embedding, each data point is represented as a vector in a n-dimensional space (embedding space). The dimensionality of this space can vary depending on the complexity of the data and the specific embedding technique used.

Points that are close to each other in vector space are considered similar (or appear in similar contexts), and points that are far away are considered dissimilar. To quantify this closeness, we use distance as a metric which can be measured in the  following way - 

1. **Euclidean Distance (L2)**: It calculates the straight-line distance between two points (vectors) in a multidimensional space.
2. **Cosine Similarity**: It measures the cosine of the angle between two vectors, providing a normalized measure of similarity based on their direction.
3. **Dot product**: It is calculated as the sum of the products of their corresponding components. To measure relatedness it considers both the magnitude and direction of the vectors.

## How do you create and store vector embeddings for your data?

1. **Creating embeddings**: Choose an embedding model, it can be a pre-trained model (open-source or commercial) or you can train a custom embedding model for your scenario. Then feed your preprocessed data into the chosen model to obtain embeddings.

??? question "Popular choices for embedding models"
    For text data, popular choices are OpenAI’s text-embedding models, Google Gemini text-embedding models, Cohere’s Embed models, and SentenceTransformers, etc.

    For image data, popular choices are CLIP (Contrastive Language–Image Pretraining), Imagebind embeddings by meta (supports audio, video, and image), and Jina multi-modal embeddings, etc.

2. **Storing vector embeddings**: This effectively requires **specialized databases** that can handle the complexity of vector data, as traditional databases often struggle with this task. Vector databases are designed specifically for storing and querying vector embeddings. They optimize for efficient nearest-neighbor searches and provide built-in indexing mechanisms.

!!! tip "Why LanceDB"
    LanceDB **automates** the entire process of creating and storing embeddings for your data. LanceDB allows you to define and use **embedding functions**, which can be **pre-trained models** or **custom models**. 
    
    This enables you to **generate** embeddings tailored to the nature of your data (e.g., text, images) and **store** both the **original data** and **embeddings** in a **structured schema** thus providing efficient querying capabilities for similarity searches.

Let's quickly [get started](./index.md) and learn how to manage embeddings in LanceDB. 

## Bonus: As a developer, what you can create using embeddings?

As a developer, you can create a variety of innovative applications using vector embeddings. Check out the following - 

<div class="grid cards" markdown>

-   __Chatbots__

    ---

    Develop chatbots that utilize embeddings to retrieve relevant context and generate coherent, contextually aware responses to user queries.

    [:octicons-arrow-right-24: Check out examples](../examples/python_examples/chatbot.md)

-   __Recommendation Systems__

    ---

    Develop systems that recommend content (such as articles, movies, or products) based on the similarity of keywords and descriptions, enhancing user experience.

    [:octicons-arrow-right-24: Check out examples](../examples/python_examples/recommendersystem.md)

-   __Vector Search__

    ---

    Build powerful applications that harness the full potential of semantic search, enabling them to retrieve relevant data quickly and effectively. 

    [:octicons-arrow-right-24: Check out examples](../examples/python_examples/vector_search.md)

-   __RAG Applications__

    ---

    Combine the strengths of large language models (LLMs) with retrieval-based approaches to create more useful applications.

    [:octicons-arrow-right-24: Check out examples](../examples/python_examples/rag.md)

-   __Many more examples__

    ---

    Explore applied examples available as Colab notebooks or Python scripts to integrate into your applications.

    [:octicons-arrow-right-24: More](../examples/examples_python.md)

</div>








