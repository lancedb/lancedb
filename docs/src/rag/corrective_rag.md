**Corrective RAG ✅**
====================================================================

Corrective-RAG (CRAG) is a strategy for Retrieval-Augmented Generation (RAG) that includes self-reflection and self-grading of retrieved documents. Here’s a simplified breakdown of the steps involved:

1. **Relevance Check**: If at least one document meets the relevance threshold, the process moves forward to the generation phase.
2. **Knowledge Refinement**: Before generating an answer, the process refines the knowledge by dividing the document into smaller segments called "knowledge strips."
3. **Grading and Filtering**: Each "knowledge strip" is graded, and irrelevant ones are filtered out.
4. **Additional Data Source**: If all documents are below the relevance threshold, or if the system is unsure about their relevance, it will seek additional information by performing a web search to supplement the retrieved data.

Above steps are mentioned in 
**[Official Paper](https://arxiv.org/abs/2401.15884)**

<figure markdown="span">
  ![agent-based-rag](https://raw.githubusercontent.com/lancedb/assets/main/docs/assets/rag/crag_paper.png)
  <figcaption>Corrective RAG: <a href="https://github.com/HuskyInSalt/CRAG">Source</a>
  </figcaption>
</figure>

Corrective Retrieval-Augmented Generation (CRAG) is a method that works like a **built-in fact-checker**.

**[Offical Implementation](https://github.com/HuskyInSalt/CRAG)**

[![Open In Colab](../assets/colab.svg)](https://colab.research.google.com/github/lancedb/vectordb-recipes/blob/main/tutorials/Corrective-RAG-with_Langgraph/CRAG_with_Langgraph.ipynb)

Here’s a code snippet for defining a table with the [Embedding API](https://lancedb.github.io/lancedb/embeddings/embedding_functions/), and retrieves the relevant documents.

```python
import pandas as pd
import lancedb
from lancedb.pydantic import LanceModel, Vector
from lancedb.embeddings import get_registry

db = lancedb.connect("/tmp/db")
model = get_registry().get("sentence-transformers").create(name="BAAI/bge-small-en-v1.5", device="cpu")

class Docs(LanceModel):
    text: str = model.SourceField()
    vector: Vector(model.ndims()) = model.VectorField()

table = db.create_table("docs", schema=Docs)

# considering chunks are in list format
df = pd.DataFrame({'text':chunks})
table.add(data=df)

# as per document feeded
query = "How Transformers work?" 
actual = table.search(query).limit(1).to_list()[0]
print(actual.text)
```

Code snippet for grading retrieved documents, filtering out irrelevant ones, and performing a web search if necessary:

```python
def grade_documents(state):
    """
        Determines whether the retrieved documents are relevant to the question

        Args:
            state (dict): The current graph state

        Returns:
            state (dict): Updates documents key with relevant documents
        """

    state_dict = state["keys"]
    question = state_dict["question"]
    documents = state_dict["documents"]

    class grade(BaseModel):
        """
            Binary score for relevance check
        """

        binary_score: str = Field(description="Relevance score 'yes' or 'no'")

    model = ChatOpenAI(temperature=0, model="gpt-4-0125-preview", streaming=True)
    # grading using openai
    grade_tool_oai = convert_to_openai_tool(grade)
    llm_with_tool = model.bind(
        tools=[convert_to_openai_tool(grade_tool_oai)],
        tool_choice={"type": "function", "function": {"name": "grade"}},
    )

    parser_tool = PydanticToolsParser(tools=[grade])
    prompt = PromptTemplate(
        template="""You are a grader assessing relevance of a retrieved document to a user question. \n
        Here is the retrieved document: \n\n {context} \n\n
        Here is the user question: {question} \n
        If the document contains keyword(s) or semantic meaning related to the user question, grade it as relevant. \n
        Give a binary score 'yes' or 'no' score to indicate whether the document is relevant to the question.""",
        input_variables=["context", "question"],
    )

    chain = prompt | llm_with_tool | parser_tool

    filtered_docs = []
    search = "No" 
    for d in documents:
        score = chain.invoke({"question": question, "context": d.page_content})
        grade = score[0].binary_score
        if grade == "yes":
            filtered_docs.append(d)
        else:
            search = "Yes" 
            continue

    return {
        "keys": {
            "documents": filtered_docs,
            "question": question,
            "run_web_search": search,
        }
    }
```

Check Colab for the Implementation of CRAG with Langgraph 

[![Open In Colab](../assets/colab.svg)](https://colab.research.google.com/github/lancedb/vectordb-recipes/blob/main/tutorials/Corrective-RAG-with_Langgraph/CRAG_with_Langgraph.ipynb)