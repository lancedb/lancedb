# YouTube transcript QA bot with NodeJS

## use LanceDB's Javascript API and OpenAI to build a QA bot for YouTube transcripts

This Q&A bot will allow you to search through youtube transcripts using natural language! We'll introduce how you can use LanceDB's Javascript API to store and manage your data easily.

For this example we're using a HuggingFace dataset that contains YouTube transcriptions: `jamescalam/youtube-transcriptions`, to make it easier, we've converted it to a LanceDB `db` already, which you can download and put in a working directory:

```wget -c https://eto-public.s3.us-west-2.amazonaws.com/lancedb_demo.tar.gz -O - | tar -xz -C .```

Now, we'll create a simple app that can:
1. Take a text based query and search for contexts in our corpus, using embeddings generated from the OpenAI Embedding API.
2. Create a prompt with the contexts, and call the OpenAI Completion API to answer the text based query.

Dependencies and setup of OpenAI API:

```javascript
const lancedb = require("vectordb");
const { Configuration, OpenAIApi } = require("openai");

const configuration = new Configuration({
    apiKey: process.env.OPENAI_API_KEY,
    });
const openai = new OpenAIApi(configuration);
```

First, let's set our question and the context amount. The context amount will be used to query similar documents in our corpus.

```javascript
const QUESTION = "who was the 12th person on the moon and when did they land?";
const CONTEXT_AMOUNT = 3;
```

Now, let's generate an embedding from this question:

```javascript
const embeddingResponse = await openai.createEmbedding({
    model: "text-embedding-ada-002",
    input: QUESTION,
});

const embedding = embeddingResponse.data["data"][0]["embedding"];
```

Once we have the embedding, we can connect to LanceDB (using the database we downloaded earlier), and search through the chatbot table.
We'll extract 3 similar documents found.

```javascript
const db = await lancedb.connect('./lancedb');
const tbl = await db.openTable('chatbot');
const query = tbl.search(embedding);
query.limit = CONTEXT_AMOUNT;
const context = await query.execute();
```

Let's combine the context together so we can pass it into our prompt:

```javascript
for (let i = 1; i < context.length; i++) {
    context[0]["text"] += " " + context[i]["text"];
}
```

Lastly, let's construct the prompt. You could play around with this to create more accurate/better prompts to yield results.

```javascript
const prompt = "Answer the question based on the context below.\n\n" +
    "Context:\n" +
    `${context[0]["text"]}\n` +
    `\n\nQuestion: ${QUESTION}\nAnswer:`;
```

We pass the prompt, along with the context, to the completion API.

```javascript
const completion = await openai.createCompletion({
    model: "text-davinci-003",
    prompt,
    temperature: 0,
    max_tokens: 400,
    top_p: 1,
    frequency_penalty: 0,
    presence_penalty: 0,
});
```

And that's it!

```javascript
console.log(completion.data.choices[0].text);
```

The response is (which is non deterministic):

```
The 12th person on the moon was Harrison Schmitt and he landed on December 11, 1972.
```
