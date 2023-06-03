# Serverless QA Bot with Modal and LangChain

## use LanceDB's LangChain integration with Modal to run a serverless app

<img id="splash" width="400" alt="modal" src="">

We're going to build a QA bot for your documentation using LanceDB's LangChain integration and use Modal for deployment.

Modal is an end-to-end compute platform for model inference, batch jobs, task queues, web apps and more. It's a great way to deploy your LanceDB models and apps.

To get started, ensure that you have created an account and logged into [Modal](https://modal.com/). To follow along, the full source code is available on Github [here](https://github.com/lancedb/lancedb/blob/main/docs/src/examples/modal_langchain.py).

### Setting up Modal

We'll start by specifying our dependencies and creating a new Modal `Stub`:

```python
lancedb_image = Image.debian_slim().pip_install(
    "lancedb",
    "langchain",
    "openai",
    "pandas",
    "tiktoken",
    "unstructured",
    "tabulate"
)

stub = Stub(
    name="example-langchain-lancedb",
    image=lancedb_image,
    secrets=[Secret.from_name("my-openai-secret")],
)
```

### Setting up caches for LanceDB and LangChain

Next, we can setup some globals to cache our LanceDB database, as well as our LangChain docsource:

```python
docsearch = None
docs_path = Path("docs.pkl")
db_path = Path("lancedb")
```

### Downloading our dataset

We're going use a pregenerated dataset, which stores HTML files of the Pandas 2.0 documentation. 
You could switch this out for your own dataset.

```python
def download_docs():
    pandas_docs = requests.get("https://eto-public.s3.us-west-2.amazonaws.com/datasets/pandas_docs/pandas.documentation.zip")
    with open(Path("pandas.documentation.zip"), "wb") as f:
        f.write(pandas_docs.content)

    file = zipfile.ZipFile(Path("pandas.documentation.zip"))
    file.extractall(path=Path("pandas_docs"))
```

### Pre-processing the dataset and generating metadata

Once we've downloaded it, we want to parse and pre-process them using LangChain, and then vectorize them and store it in LanceDB.
Let's first create a function that uses LangChains `UnstructuredHTMLLoader` to parse them.
We can then add our own metadata to it and store it alongside the data, we'll later be able to use this for filtering metadata.

```python
def store_docs():
    docs = []

    if not docs_path.exists():
        for p in Path("pandas_docs/pandas.documentation").rglob("*.html"):
            if p.is_dir():
                continue
            loader = UnstructuredHTMLLoader(p)
            raw_document = loader.load()

            m = {}
            m["title"] = get_document_title(raw_document[0])
            m["version"] = "2.0rc0"
            raw_document[0].metadata = raw_document[0].metadata | m
            raw_document[0].metadata["source"] = str(raw_document[0].metadata["source"])
            docs = docs + raw_document

        with docs_path.open("wb") as fh:
            pickle.dump(docs, fh)
    else:
        with docs_path.open("rb") as fh:
            docs = pickle.load(fh)

    return docs
```

### Simple LangChain chain for a QA bot

Now we can create a simple LangChain chain for our QA bot. We'll use the `RecursiveCharacterTextSplitter` to split our documents into chunks, and then use the `OpenAIEmbeddings` to vectorize them.

Lastly, we'll create a LanceDB table and store the vectorized documents in it, then create a `RetrievalQA` model from the chain and return it.

```python
def qanda_langchain(query):
    download_docs()
    docs = store_docs()

    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=1000,
        chunk_overlap=200,
    )
    documents = text_splitter.split_documents(docs)
    embeddings = OpenAIEmbeddings()

    db = lancedb.connect(db_path) 
    table = db.create_table("pandas_docs", data=[
        {"vector": embeddings.embed_query("Hello World"), "text": "Hello World", "id": "1"}
    ], mode="overwrite")
    docsearch = LanceDB.from_documents(documents, embeddings, connection=table)
    qa = RetrievalQA.from_chain_type(llm=OpenAI(), chain_type="stuff", retriever=docsearch.as_retriever())
    return qa.run(query)
```

### Creating our Modal entry points

Now we can create our Modal entry points for our CLI and web endpoint:

```python
@stub.function()
@web_endpoint(method="GET")
def web(query: str):
    answer = qanda_langchain(query)
    return {
        "answer": answer,
    }
    
@stub.function()
def cli(query: str):
    answer = qanda_langchain(query)
    print(answer)
```

# Testing it out!

Testing the CLI:

```bash
modal run modal_langchain.py --query "What are the major differences in pandas 2.0?"
```

Testing the web endpoint:

```bash
modal serve modal_langchain.py
```

In the CLI, Modal will provide you a web endpoint. Copy this endpoint URI for the next step.
Once this is served, then we can hit it with `curl`. 

Note, the first time this runs, it will take a few minutes to download the dataset and vectorize it.
An actual production example would pre-cache/load the dataset and vectorized documents prior

```bash
curl --get --data-urlencode "query=What are the major differences in pandas 2.0?" https://your-modal-endpoint-app.modal.run

{"answer":" The major differences in pandas 2.0 include the ability to use any numpy numeric dtype in a Index, installing optional dependencies with pip extras, and enhancements, bug fixes, and performance improvements."}
```

