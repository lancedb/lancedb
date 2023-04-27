# Code Documentation Q&A Bot 2.0

## Simple Pandas 2.0 documentation Q&A answering bot using LangChain

<img id="splash" src="https://user-images.githubusercontent.com/917119/234710587-3b488467-e8a4-46ec-b377-fe24c17eecca.png"/>

To demonstrate using Lance, we’re going to build a simple Q&A answering bot using LangChain — an open-source framework that allows you to build composable LLM-based applications easily. We’ll use chat-langchain, a simple Q&A answering bot app as an example. Note: in this fork of chat-langchain, we’re also using a forked version of LangChain integration where we’ve built a Lance integration.

The first step is to generate embeddings. You could build a bot using your own data, like a wiki page or internal documentation. For this example, we’re going to use the Pandas API documentation. LangChain offers document loaders to read and pre-process many document types. Since the Pandas API is in HTML, reading the docs is straightforward:

```python
for p in Path("./pandas.documentation").rglob("*.html"):
   if p.is_dir():
     continue
   loader = UnstructuredHTMLLoader(p)
   raw_document = loader.load()
   docs = docs + raw_document
