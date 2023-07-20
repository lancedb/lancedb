# Vector embedding search using TransformersJS

## Embed and query data from LanceDB using TransformersJS

<img id="splash" width="400" alt="transformersjs" src="https://github.com/lancedb/lancedb/assets/43097991/88a31e30-3d6f-4eef-9216-4b7c688f1b4f">

This example shows how to use the [transformers.js](https://github.com/xenova/transformers.js) library to perform vector embedding search using LanceDB's Javascript API.


### Setting up
First, install the dependencies:
```bash
npm install vectordb
npm i @xenova/transformers
```

We will also be using the [all-MiniLM-L6-v2](https://huggingface.co/Xenova/all-MiniLM-L6-v2) model to make it compatible with Transformers.js

Within our `index.js` file we will import the necessary libraries and define our model and database:

```javascript
const lancedb = require('vectordb')
const { pipeline } = await import('@xenova/transformers')
const pipe = await pipeline('feature-extraction', 'Xenova/all-MiniLM-L6-v2');
```

### Creating the embedding function

Next, we will create a function that will take in a string and return the vector embedding of that string. We will use the `pipe` function we defined earlier to get the vector embedding of the string.

```javascript
// Define the function. `sourceColumn` is required for LanceDB to know
// which column to use as input.
const embed_fun = {}
embed_fun.sourceColumn = 'text'
embed_fun.embed = async function (batch) {
    let result = []
    // Given a batch of strings, we will use the `pipe` function to get
    // the vector embedding of each string.
    for (let text of batch) {
        // 'mean' pooling and normalizing allows the embeddings to share the
        // same length.
        const res = await pipe(text, { pooling: 'mean', normalize: true })
        result.push(Array.from(res['data']))
    }
    return (result)
}
```

### Creating the database

Now, we will create the LanceDB database and add the embedding function we defined earlier.

```javascript
// Link a folder and create a table with data
const db = await lancedb.connect('data/sample-lancedb')

// You can also import any other data, but make sure that you have a column
// for the embedding function to use.
const data = [
    { id: 1, text: 'Cherry', type: 'fruit' },
    { id: 2, text: 'Carrot', type: 'vegetable' },
    { id: 3, text: 'Potato', type: 'vegetable' },
    { id: 4, text: 'Apple', type: 'fruit' },
    { id: 5, text: 'Banana', type: 'fruit' }
]

// Create the table with the embedding function
const table = await db.createTable('food_table', data, "create", embed_fun)
```

### Performing the search

Now, we can perform the search using the `search` function. LanceDB automatically uses the embedding function we defined earlier to get the vector embedding of the query string.

```javascript
// Query the table
const results = await table
    .search("a sweet fruit to eat")
    .metricType("cosine")
    .limit(2)
    .execute()
console.log(results.map(r => r.text))
```
```bash
[ 'Banana', 'Cherry' ]
```

Output of `results`:
```bash
[
  {
    vector: Float32Array(384) [
      -0.057455405592918396,
      0.03617725893855095,
      -0.0367760956287384,
      ... 381 more items
    ],
    id: 5,
    text: 'Banana',
    type: 'fruit',
    score: 0.4919965863227844
  },
  {
    vector: Float32Array(384) [
      0.0009714411571621895,
      0.008223623037338257,
      0.009571489877998829,
      ... 381 more items
    ],
    id: 1,
    text: 'Cherry',
    type: 'fruit',
    score: 0.5540297031402588
  }
]
```

### Wrapping it up

In this example, we showed how to use the `transformers.js` library to perform vector embedding search using LanceDB's Javascript API. You can find the full code for this example on [Github](https://github.com/lancedb/lancedb/blob/main/node/examples/js-transformers/index.js)!
