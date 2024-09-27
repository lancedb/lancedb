**Adaptive RAG 🤹‍♂️**
====================================================================
Adaptive RAG introduces a RAG technique that combines query analysis with self-corrective RAG. 

For Query Analysis, it uses a small classifier(LLM), to decide the query’s complexity. Query Analysis helps routing smoothly to adjust between different retrieval strategies No retrieval, Single-shot RAG or Iterative RAG.

**[Official Paper](https://arxiv.org/pdf/2403.14403)**

<figure markdown="span">
  ![agent-based-rag](https://raw.githubusercontent.com/lancedb/assets/main/docs/assets/rag/adaptive_rag.png)
  <figcaption>Adaptive-RAG: <a href="https://github.com/starsuzi/Adaptive-RAG">Source</a>
  </figcaption>
</figure>

**[Offical Implementation](https://github.com/starsuzi/Adaptive-RAG)**

Here’s a code snippet for query analysis

```python
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.pydantic_v1 import BaseModel, Field
from langchain_openai import ChatOpenAI

class RouteQuery(BaseModel):
    """Route a user query to the most relevant datasource."""

    datasource: Literal["vectorstore", "web_search"] = Field(
        ...,
        description="Given a user question choose to route it to web search or a vectorstore.",
    )


# LLM with function call
llm = ChatOpenAI(model="gpt-3.5-turbo-0125", temperature=0)
structured_llm_router = llm.with_structured_output(RouteQuery)
```

For defining and querying retriever

```python
# add documents in LanceDB
vectorstore = LanceDB.from_documents(
    documents=doc_splits,
    embedding=OpenAIEmbeddings(),
)
retriever = vectorstore.as_retriever()

# query using defined retriever
question = "How adaptive RAG works"
docs = retriever.get_relevant_documents(question)
```