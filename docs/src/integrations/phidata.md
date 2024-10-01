**phidata** is a framework for building **AI Assistants** with long-term memory, contextual knowledge, and the ability to take actions using function calling. It helps turn general-purpose LLMs into specialized assistants tailored to your use case by extending its capabilities using **memory**, **knowledge**, and **tools**. 

- **Memory**: Stores chat history in a **database** and enables LLMs to have long-term conversations.
- **Knowledge**: Stores information in a **vector database** and provides LLMs with business context. (Here we will use LanceDB)
- **Tools**: Enable LLMs to take actions like pulling data from an **API**, **sending emails** or **querying a database**, etc.

![example](https://raw.githubusercontent.com/lancedb/assets/refs/heads/main/docs/assets/integration/phidata_assistant.png)

Memory & knowledge make LLMs smarter while tools make them autonomous.

LanceDB is a vector database and its integration into phidata makes it easy for us to provide a **knowledge base** to LLMs. It enables us to store information as [embeddings](../embeddings/understanding_embeddings.md) and search for the **results** similar to ours using **query**. 

??? Question "What is Knowledge Base?"
    Knowledge Base is a database of information that the Assistant can search to improve its responses. This information is stored in a vector database and provides LLMs with business context, which makes them respond in a context-aware manner.

    While any type of storage can act as a knowledge base, vector databases offer the best solution for retrieving relevant results from dense information quickly.

Let's see how using LanceDB inside phidata helps in making LLM more useful:

## Prerequisites: install and import necessary dependencies

**Create a virtual environment**

1. install virtualenv package
    ```python
    pip install virtualenv
    ```
2. Create a directory for your project and go to the directory and create a virtual environment inside it.
    ```python
    mkdir phi
    ```
    ```python
    cd phi
    ```
    ```python
    python -m venv phidata_
    ```

**Activating virtual environment**

1. from inside the project directory, run the following command to activate the virtual environment.
    ```python
    phidata_/Scripts/activate
    ```

**Install the following packages in the virtual environment**
```python
pip install lancedb phidata youtube_transcript_api openai ollama pandas numpy
```

**Create python files and import necessary libraries**

You need to create two files - `transcript.py` and `ollama_assistant.py` or `openai_assistant.py`

=== "openai_assistant.py"

    ```python
    import os, openai
    from rich.prompt import Prompt
    from phi.assistant import Assistant
    from phi.knowledge.text import TextKnowledgeBase
    from phi.vectordb.lancedb import LanceDb
    from phi.llm.openai import OpenAIChat
    from phi.embedder.openai import OpenAIEmbedder
    from transcript import extract_transcript

    if "OPENAI_API_KEY" not in os.environ:
    # OR set the key here as a variable
        openai.api_key = "sk-..."

    # The code below creates a file "transcript.txt" in the directory, the txt file will be used below
    youtube_url = "https://www.youtube.com/watch?v=Xs33-Gzl8Mo" 
    segment_duration = 20
    transcript_text,dict_transcript = extract_transcript(youtube_url,segment_duration)
    ```

=== "ollama_assistant.py"

    ```python
    from rich.prompt import Prompt
    from phi.assistant import Assistant
    from phi.knowledge.text import TextKnowledgeBase
    from phi.vectordb.lancedb import LanceDb
    from phi.llm.ollama import Ollama
    from phi.embedder.ollama import OllamaEmbedder
    from transcript import extract_transcript

    # The code below creates a file "transcript.txt" in the directory, the txt file will be used below
    youtube_url = "https://www.youtube.com/watch?v=Xs33-Gzl8Mo"
    segment_duration = 20
    transcript_text,dict_transcript = extract_transcript(youtube_url,segment_duration)
    ```

=== "transcript.py"

    ``` python
    from youtube_transcript_api import YouTubeTranscriptApi
    import re

    def smodify(seconds):
        hours, remainder = divmod(seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{int(hours):02}:{int(minutes):02}:{int(seconds):02}"
        
    def extract_transcript(youtube_url,segment_duration):
        # Extract video ID from the URL
        video_id = re.search(r'(?<=v=)[\w-]+', youtube_url)
        if not video_id:
            video_id = re.search(r'(?<=be/)[\w-]+', youtube_url)
        if not video_id:
            return None

        video_id = video_id.group(0)

        # Attempt to fetch the transcript
        try:
            # Try to get the official transcript
            transcript = YouTubeTranscriptApi.get_transcript(video_id, languages=['en'])
        except Exception:
            # If no official transcript is found, try to get auto-generated transcript
            try:
                transcript_list = YouTubeTranscriptApi.list_transcripts(video_id)
                for transcript in transcript_list:
                    transcript = transcript.translate('en').fetch()
            except Exception:
                return None

        # Format the transcript into 120s chunks
        transcript_text,dict_transcript = format_transcript(transcript,segment_duration)
        # Open the file in write mode, which creates it if it doesn't exist
        with open("transcript.txt", "w",encoding="utf-8") as file:
            file.write(transcript_text)
        return transcript_text,dict_transcript

    def format_transcript(transcript,segment_duration):
        chunked_transcript = []
        chunk_dict = []
        current_chunk = []
        current_time = 0
        # 2 minutes in seconds
        start_time_chunk = 0  # To track the start time of the current chunk

        for segment in transcript:
            start_time = segment['start']
            end_time_x = start_time + segment['duration']
            text = segment['text']
            
            # Add text to the current chunk
            current_chunk.append(text)
            
            # Update the current time with the duration of the current segment
            # The duration of the current segment is given by segment['start'] - start_time_chunk
            if current_chunk:
                current_time = start_time - start_time_chunk
            
            # If current chunk duration reaches or exceeds 2 minutes, save the chunk
            if current_time >= segment_duration:
                # Use the start time of the first segment in the current chunk as the timestamp
                chunked_transcript.append(f"[{smodify(start_time_chunk)} to {smodify(end_time_x)}] " + " ".join(current_chunk))
                current_chunk = re.sub(r'[\xa0\n]', lambda x: '' if x.group() == '\xa0' else ' ', "\n".join(current_chunk))
                chunk_dict.append({"timestamp":f"[{smodify(start_time_chunk)} to {smodify(end_time_x)}]", "text": "".join(current_chunk)})
                current_chunk = []  # Reset the chunk
                start_time_chunk = start_time + segment['duration'] # Update the start time for the next chunk
                current_time = 0  # Reset current time

        # Add any remaining text in the last chunk
        if current_chunk:
            chunked_transcript.append(f"[{smodify(start_time_chunk)} to {smodify(end_time_x)}] " + " ".join(current_chunk))
            current_chunk = re.sub(r'[\xa0\n]', lambda x: '' if x.group() == '\xa0' else ' ', "\n".join(current_chunk))
            chunk_dict.append({"timestamp":f"[{smodify(start_time_chunk)} to {smodify(end_time_x)}]", "text": "".join(current_chunk)})

        return "\n\n".join(chunked_transcript), chunk_dict
    ```

!!! warning
    If creating Ollama assistant, download and install Ollama [from here](https://ollama.com/) and then run the Ollama instance in the background. Also, download the required models using `ollama pull <model-name>`. Check out the models [here](https://ollama.com/library) 


**Run the following command to deactivate the virtual environment if needed**
```python
deactivate
```

## **Step 1** - Create a Knowledge Base for AI Assistant using LanceDB

=== "openai_assistant.py"

    ```python
    # Create knowledge Base with OpenAIEmbedder in LanceDB
    knowledge_base = TextKnowledgeBase(
        path="transcript.txt",
        vector_db=LanceDb(
            embedder=OpenAIEmbedder(api_key = openai.api_key),
            table_name="transcript_documents",
            uri="./t3mp/.lancedb",
        ),
        num_documents = 10
    )
    ```

=== "ollama_assistant.py"

    ```python
    # Create knowledge Base with OllamaEmbedder in LanceDB
    knowledge_base = TextKnowledgeBase(
        path="transcript.txt",
        vector_db=LanceDb(
            embedder=OllamaEmbedder(model="nomic-embed-text",dimensions=768),
            table_name="transcript_documents",
            uri="./t2mp/.lancedb",
        ),
        num_documents = 10
    )
    ```
Check out the list of **embedders** supported by **phidata** and their usage [here](https://docs.phidata.com/embedder/introduction).

Here we have used `TextKnowledgeBase`, which loads text/docx files to the knowledge base.

Let's see all the parameters that `TextKnowledgeBase` takes -

| Name| Type | Purpose | Default |
|:----|:-----|:--------|:--------|
|`path`|`Union[str, Path]`| Path to text file(s). It can point to a single text file or a directory of text files.| provided by user |
|`formats`|`List[str]`| File formats accepted by this knowledge base. |`[".txt"]`|
|`vector_db`|`VectorDb`| Vector Database for the Knowledge Base. phidata provides a wrapper around many vector DBs, you can import it like this - `from phi.vectordb.lancedb import LanceDb` | provided by user |
|`num_documents`|`int`| Number of results (documents/vectors) that vector search should return. |`5`|
|`reader`|`TextReader`| phidata provides many types of reader objects which read data, clean it and create chunks of data, encapsulate each chunk inside an object of the `Document` class, and return **`List[Document]`**.  | `TextReader()` |
|`optimize_on`|`int`| It is used to specify the number of documents on which to optimize the vector database. Supposed to create an index. |`1000`|

??? Tip "Wonder! What is `Document` class?"
    We know that, before storing the data in vectorDB, we need to split the data into smaller chunks upon which embeddings will be created and these embeddings along with the chunks will be stored in vectorDB. When the user queries over the vectorDB, some of these embeddings will be returned as the result based on the semantic similarity with the query.
    
    When the user queries over vectorDB, the queries are converted into embeddings, and a nearest neighbor search is performed over these query embeddings which returns the embeddings that correspond to most semantically similar chunks(parts of our data) present in vectorDB. 

    Here, a “Document” is a class in phidata. Since there is an option to let phidata create and manage embeddings, it splits our data into smaller chunks(as expected). It does not directly create embeddings on it. Instead, it takes each chunk and encapsulates it inside the object of the `Document` class along with various other metadata related to the chunk. Then embeddings are created on these `Document` objects and stored in vectorDB.

    ```python
    class Document(BaseModel):
        """Model for managing a document"""

        content: str # <--- here data of chunk is stored 
        id: Optional[str] = None
        name: Optional[str] = None
        meta_data: Dict[str, Any] = {}
        embedder: Optional[Embedder] = None
        embedding: Optional[List[float]] = None
        usage: Optional[Dict[str, Any]] = None
    ```

However, using phidata you can load many other types of data in the knowledge base(other than text). Check out [phidata Knowledge Base](https://docs.phidata.com/knowledge/introduction) for more information.

Let's dig deeper into the `vector_db` parameter and see what parameters `LanceDb` takes -

| Name| Type | Purpose | Default |
|:----|:-----|:--------|:--------|
|`embedder`|`Embedder`| phidata provides many Embedders that abstract the interaction with embedding APIs and utilize it to generate embeddings. Check out other embedders [here](https://docs.phidata.com/embedder/introduction) | `OpenAIEmbedder` |
|`distance`|`List[str]`| The choice of distance metric used to calculate the similarity between vectors, which directly impacts search results and performance in vector databases. |`Distance.cosine`|
|`connection`|`lancedb.db.LanceTable`| LanceTable can be accessed through `.connection`. You can connect to an existing table of LanceDB, created outside of phidata, and utilize it. If not provided, it creates a new table using `table_name` parameter and adds it to `connection`. |`None`|
|`uri`|`str`| It specifies the directory location of **LanceDB database** and establishes a connection that can be used to interact with the database. | `"/tmp/lancedb"` |
|`table_name`|`str`|  If `connection` is not provided, it initializes and connects to a new **LanceDB table** with a specified(or default) name in the database present at `uri`. |`"phi"`|
|`nprobes`|`int`| It refers to the number of partitions that the search algorithm examines to find the nearest neighbors of a given query vector. Higher values will yield better recall (more likely to find vectors if they exist) at the expense of latency. |`20`|


!!! note
    Since we just initialized the KnowledgeBase. The VectorDB table that corresponds to this Knowledge Base is not yet populated with our data. It will be populated in **Step 3**, once we perform the `load` operation. 
    
    You can check the state of the LanceDB table using - `knowledge_base.vector_db.connection.to_pandas()`

Now that the Knowledge Base is initialized, , we can go to **step 2**.

## **Step 2** -  Create an assistant with our choice of LLM and reference to the knowledge base.


=== "openai_assistant.py"

    ```python
    # define an assistant with gpt-4o-mini llm and reference to the knowledge base created above
    assistant = Assistant(
        llm=OpenAIChat(model="gpt-4o-mini", max_tokens=1000, temperature=0.3,api_key = openai.api_key),
        description="""You are an Expert in explaining youtube video transcripts. You are a bot that takes transcript of a video and answer the question based on it.
        
        This is transcript for the above timestamp: {relevant_document}
        The user input is: {user_input}
        generate highlights only when asked.
        When asked to generate highlights from the video, understand the context for each timestamp and create key highlight points, answer in following way - 
        [timestamp] - highlight 1
        [timestamp] - highlight 2
        ... so on
        
        Your task is to understand the user question, and provide an answer using the provided contexts. Your answers are correct, high-quality, and written by an domain expert. If the provided context does not contain the answer, simply state,'The provided context does not have the answer.'""",
        knowledge_base=knowledge_base,
        add_references_to_prompt=True,
    )
    ```

=== "ollama_assistant.py"

    ```python
    # define an assistant with llama3.1 llm and reference to the knowledge base created above
    assistant = Assistant(
        llm=Ollama(model="llama3.1"),
        description="""You are an Expert in explaining youtube video transcripts. You are a bot that takes transcript of a video and answer the question based on it.

        This is transcript for the above timestamp: {relevant_document}
        The user input is: {user_input}
        generate highlights only when asked.
        When asked to generate highlights from the video, understand the context for each timestamp and create key highlight points, answer in following way - 
        [timestamp] - highlight 1
        [timestamp] - highlight 2
        ... so on

        Your task is to understand the user question, and provide an answer using the provided contexts. Your answers are correct, high-quality, and written by an domain expert. If the provided context does not contain the answer, simply state,'The provided context does not have the answer.'""",
        knowledge_base=knowledge_base,
        add_references_to_prompt=True,
    )
    ```

Assistants add **memory**, **knowledge**, and **tools** to LLMs. Here we will add only **knowledge** in this example. 

Whenever we will give a query to LLM, the assistant will retrieve relevant information from our **Knowledge Base**(table in LanceDB) and pass it to LLM along with the user query in a structured way. 

- The `add_references_to_prompt=True` always adds information from the knowledge base to the prompt, regardless of whether it is relevant to the question.

To know more about an creating assistant in phidata, check out [phidata docs](https://docs.phidata.com/assistants/introduction) here.

## **Step 3** - Load data to Knowledge Base.

```python
# load out data into the knowledge_base (populating the LanceTable)
assistant.knowledge_base.load(recreate=False)
```
The above code loads the data to the Knowledge Base(LanceDB Table) and now it is ready to be used by the assistant. 

| Name| Type | Purpose | Default |
|:----|:-----|:--------|:--------|
|`recreate`|`bool`| If True, it drops the existing table and recreates the table in the vectorDB. |`False`|
|`upsert`|`bool`| If True and the vectorDB supports upsert, it will upsert documents to the vector db. | `False` |
|`skip_existing`|`bool`| If True, skips documents that already exist in the vectorDB when inserting. |`True`|

??? tip "What is upsert?"
    Upsert is a database operation that combines "update" and "insert". It updates existing records if a document with the same identifier does exist, or inserts new records if no matching record exists. This is useful for maintaining the most current information without manually checking for existence.

During the Load operation, phidata directly interacts with the LanceDB library and performs the loading of the table with our data in the following steps -     

1. **Creates** and **initializes** the table if it does not exist.

2. Then it **splits** our data into smaller **chunks**.

    ??? question "How do they create chunks?"
        **phidata** provides many types of **Knowledge Bases** based on the type of data. Most of them :material-information-outline:{ title="except LlamaIndexKnowledgeBase and LangChainKnowledgeBase"} has a property method called `document_lists` of type `Iterator[List[Document]]`. During the load operation, this property method is invoked. It traverses on the data provided by us (in this case, a text file(s)) using `reader`. Then it **reads**, **creates chunks**, and **encapsulates** each chunk inside a `Document` object and yields **lists of `Document` objects** that contain our data.

3. Then **embeddings** are created on these chunks are **inserted** into the LanceDB Table 

    ??? question "How do they insert your data as different rows in LanceDB Table?"
        The chunks of your data are in the form - **lists of `Document` objects**. It was yielded in the step above.

        for each `Document` in `List[Document]`, it does the following operations:
        
        - Creates embedding on `Document`.
        - Cleans the **content attribute**(chunks of our data is here) of `Document`.
        - Prepares data by creating `id` and loading `payload` with the metadata related to this chunk. (1)
            { .annotate }

            1.  Three columns will be added to the table - `"id"`, `"vector"`, and `"payload"` (payload contains various metadata including **`content`**)
                
        - Then add this data to LanceTable. 

4. Now the internal state of `knowledge_base` is changed (embeddings are created and loaded in the table ) and it **ready to be used by assistant**.

## **Step 4** - Start a cli chatbot with access to the Knowledge base  

```python
# start cli chatbot with knowledge base
assistant.print_response("Ask me about something from the knowledge base")
while True:
    message = Prompt.ask(f"[bold] :sunglasses: User [/bold]")
    if message in ("exit", "bye"):
        break
    assistant.print_response(message, markdown=True)
```  


For more information and amazing cookbooks of phidata, read the [phidata documentation](https://docs.phidata.com/introduction) and also visit [LanceDB x phidata docmentation](https://docs.phidata.com/vectordb/lancedb).