# YouTube transcript QA bot with NodeJS

## use LanceDB's Javascript API and OpenAI to build a QA bot for YouTube transcripts

<img id="splash" width="400" alt="nodejs" src="https://github.com/lancedb/lancedb/assets/917119/3a140e75-bf8e-438a-a1e4-af14a72bcf98">

This Q&A bot will allow you to search through youtube transcripts using natural language! We'll introduce how to use LanceDB's Javascript API to store and manage your data easily.

```bash
npm install vectordb
```

## Download the data

For this example, we're using a sample of a HuggingFace dataset that contains YouTube transcriptions: `jamescalam/youtube-transcriptions`. Download and extract this file under the `data` folder:

```bash
wget -c https://eto-public.s3.us-west-2.amazonaws.com/datasets/youtube_transcript/youtube-transcriptions_sample.jsonl
```

## Prepare Context

Each item in the dataset contains just a short chunk of text. We'll need to merge a bunch of these chunks together on a rolling basis. For this demo, we'll look back 20 records to create a more complete context for each sentence.

First, we need to read and parse the input file.

```javascript
const lines = (await fs.readFile(INPUT_FILE_NAME, 'utf-8'))
  .toString()
  .split('\n')
  .filter(line => line.length > 0)
  .map(line => JSON.parse(line))

const data = contextualize(lines, 20, 'video_id')
```

The contextualize function groups the transcripts by video_id and then creates the expanded context for each item.

```javascript
function contextualize (rows, contextSize, groupColumn) {
  const grouped = []
  rows.forEach(row => {
    if (!grouped[row[groupColumn]]) {
      grouped[row[groupColumn]] = []
    }
    grouped[row[groupColumn]].push(row)
  })

  const data = []
  Object.keys(grouped).forEach(key => {
    for (let i = 0; i < grouped[key].length; i++) {
      const start = i - contextSize > 0 ? i - contextSize : 0
      grouped[key][i].context = grouped[key].slice(start, i + 1).map(r => r.text).join(' ')
    }
    data.push(...grouped[key])
  })
  return data
}
```

## Create the LanceDB Table

To load our data into LanceDB, we need to create embedding (vectors) for each item. For this example, we will use the OpenAI embedding functions, which have a native integration with LanceDB.

```javascript
// You need to provide an OpenAI API key, here we read it from the OPENAI_API_KEY environment variable
const apiKey = process.env.OPENAI_API_KEY
// The embedding function will create embeddings for the 'context' column
const embedFunction = new lancedb.OpenAIEmbeddingFunction('context', apiKey)
// Connects to LanceDB
const db = await lancedb.connect('data/youtube-lancedb')
const tbl = await db.createTable('vectors', data, embedFunction)
```

## Create and answer the prompt

We will accept questions in natural language and use our corpus stored in LanceDB to answer them. First, we need to set up the OpenAI client:

```javascript
const configuration = new Configuration({ apiKey })
const openai = new OpenAIApi(configuration)
```

Then we can prompt questions and use LanceDB to retrieve the three most relevant transcripts for this prompt.

```javascript
const query = await rl.question('Prompt: ')
const results = await tbl
  .search(query)
  .select(['title', 'text', 'context'])
  .limit(3)
  .execute()
```

The query and the transcripts' context are appended together in a single prompt:

```javascript
function createPrompt (query, context) {
    let prompt =
        'Answer the question based on the context below.\n\n' +
        'Context:\n'

    // need to make sure our prompt is not larger than max size
    prompt = prompt + context.map(c => c.context).join('\n\n---\n\n').substring(0, 3750)
    prompt = prompt + `\n\nQuestion: ${query}\nAnswer:`
    return prompt
}
```

We can now use the OpenAI Completion API to process our custom prompt and give us an answer.

```javascript
const response = await openai.createCompletion({
  model: 'text-davinci-003',
  prompt: createPrompt(query, results),
  max_tokens: 400,
  temperature: 0,
  top_p: 1,
  frequency_penalty: 0,
  presence_penalty: 0
})
console.log(response.data.choices[0].text)
```

## Let's put it all together now

Now we can provide queries and have them answered based on your local LanceDB data.

```bash
Prompt: who was the 12th person on the moon and when did they land?
 The 12th person on the moon was Harrison Schmitt and he landed on December 11, 1972.
Prompt: Which training method should I use for sentence transformers when I only have pairs of related sentences?
 NLI with multiple negative ranking loss.
```

## That's a wrap

In this example, you learned how to use LanceDB to store and query embedding representations of your local data. The complete example code is on [GitHub](https://github.com/lancedb/lancedb/tree/main/node/examples), and you can also download the LanceDB dataset using [this link](https://eto-public.s3.us-west-2.amazonaws.com/datasets/youtube_transcript/youtube-lancedb.zip).

