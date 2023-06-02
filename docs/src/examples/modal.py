import sys
from modal import Secret, Stub, Image
import lancedb
import re
import pickle
from pathlib import Path

from langchain.document_loaders import UnstructuredHTMLLoader
from langchain.embeddings import OpenAIEmbeddings
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.vectorstores import LanceDB
from langchain.llms import OpenAI
from langchain.chains import RetrievalQA

lancedb_image = Image.debian_slim().pip_install("lancedb", "langchain", "openai", "tiktoken")

stub = Stub(
    name="example-langchain-lancedb",
    image=lancedb_image,
    secrets=[Secret.from_name("my-openai-secret")],
)

docsearch = None
docs_path = Path("docs.pkl")
db_path = Path("lancedb")
doc_cache = []

def get_document_title(document):
    m = str(document.metadata["source"])
    title = re.findall("pandas.documentation(.*).html", m)
    if title[0] is not None:
        return(title[0])
    return ''

def store_docs():
    docs = []

    if not docs_path.exists():
        for p in Path("./pandas.documentation").rglob("*.html"):
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

    doc_cache = docs

def qanda_langchain(query):
    store_docs()

    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=1000,
        chunk_overlap=200,
    )
    documents = text_splitter.split_documents(doc_cache)
    embeddings = OpenAIEmbeddings()

    db = lancedb.connect(db_path) 
    table = db.create_table("pandas_docs", data=[
        {"vector": embeddings.embed_query("Hello World")}
    ], mode="overwrite")
    docsearch = LanceDB.from_documents(documents, embeddings, connection=table)
    qa = RetrievalQA.from_chain_type(llm=OpenAI(), chain_type="stuff", retriever=docsearch.as_retriever())
    return qa.run(query)

@stub.function()
def cli(query: str, show_sources: bool = False):
    answer = qanda_langchain(query)
    # Terminal codes for pretty-printing.
    bold, end = "\033[1m", "\033[0m"

    print(f"🦜 {bold}ANSWER:{end}")
    print(answer)
    if show_sources:
        print(f"🔗 {bold}SOURCES:{end}")
        for text in sources:
            print(text)
            print("----")
