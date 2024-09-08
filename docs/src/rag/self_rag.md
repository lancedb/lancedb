**Self RAG 🤳**
====================================================================
Self-RAG is a strategy for Retrieval-Augmented Generation (RAG) to get better retrieved information, generated text, and  checking their own work, all without losing their flexibility. Unlike the traditional Retrieval-Augmented Generation (RAG) method, Self-RAG retrieves information as needed, can skip retrieval if not needed, and evaluates its own output while generating text. It also uses a process to pick the best output based on different preferences.

**[Official Paper](https://arxiv.org/pdf/2310.11511)**

<figure markdown="span">
  ![agent-based-rag](https://raw.githubusercontent.com/lancedb/assets/main/docs/assets/rag/self_rag.png)
  <figcaption>Self RAG: <a href="https://github.com/AkariAsai/self-rag">Source</a>
  </figcaption>
</figure>

**[Offical Implementation](https://github.com/AkariAsai/self-rag)**

Self-RAG starts by generating a response without retrieving extra info if it's not needed. For questions that need more details, it retrieves to get the necessary information.

Here’s a code snippet for defining retriever using Langchain

```python
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_community.document_loaders import WebBaseLoader
from langchain_community.vectorstores import LanceDB
from langchain_openai import OpenAIEmbeddings

urls = [
    "https://lilianweng.github.io/posts/2023-06-23-agent/",
    "https://lilianweng.github.io/posts/2023-03-15-prompt-engineering/",
    "https://lilianweng.github.io/posts/2023-10-25-adv-attack-llm/",
]


docs = [WebBaseLoader(url).load() for url in urls]
docs_list = [item for sublist in docs for item in sublist]

text_splitter = RecursiveCharacterTextSplitter.from_tiktoken_encoder(
    chunk_size=100, chunk_overlap=50
)
doc_splits = text_splitter.split_documents(docs_list)

# add documents in LanceDB
vectorstore = LanceDB.from_documents(
    documents=doc_splits,
    embedding=OpenAIEmbeddings(),
)
retriever = vectorstore.as_retriever()

```

Functions that grades the retrieved documents and if required formulates an improved query for better retrieval results  

```python
def grade_documents(state) -> Literal["generate", "rewrite"]:
    class grade(BaseModel):
        binary_score: str = Field(description="Relevance score 'yes' or 'no'")

    model = ChatOpenAI(temperature=0, model="gpt-4-0125-preview", streaming=True)
    llm_with_tool = model.with_structured_output(grade)
    prompt = PromptTemplate(
        template="""You are a grader assessing relevance of a retrieved document to a user question. \n
        Here is the retrieved document: \n\n {context} \n\n
        Here is the user question: {question} \n
        If the document contains keyword(s) or semantic meaning related to the user question, grade it as relevant. \n
        Give a binary score 'yes' or 'no' score to indicate whether the document is relevant to the question.""",
        input_variables=["context", "question"],
    )
    chain = prompt | llm_with_tool

    messages = state["messages"]
    last_message = messages[-1]
    question = messages[0].content
    docs = last_message.content

    scored_result = chain.invoke({"question": question, "context": docs})
    score = scored_result.binary_score

    return "generate" if score == "yes" else "rewrite"


def rewrite(state):
    messages = state["messages"]
    question = messages[0].content
    msg = [
        HumanMessage(
            content=f""" \n
            Look at the input and try to reason about the underlying semantic intent / meaning. \n
            Here is the initial question:
            \n ------- \n
            {question}
            \n ------- \n
            Formulate an improved question: """,
        )
    ]
    model = ChatOpenAI(temperature=0, model="gpt-4-0125-preview", streaming=True)
    response = model.invoke(msg)
    return {"messages": [response]}
```