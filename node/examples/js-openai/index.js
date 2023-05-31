// Copyright 2023 Lance Developers.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

'use strict'

async function example () {
  const lancedb = require('vectordb')
  // You need to provide an OpenAI API key, here we read it from the OPENAI_API_KEY environment variable
  const apiKey = process.env.OPENAI_API_KEY
  // The embedding function will create embeddings for the 'text' column(text in this case)
  const embedding = new lancedb.OpenAIEmbeddingFunction('text', apiKey)

  const db = await lancedb.connect('data/sample-lancedb')

  const data = [
    { id: 1, text: 'Black T-Shirt', price: 10 },
    { id: 2, text: 'Leather Jacket', price: 50 }
  ]

  const table = await db.createTable('vectors', data, embedding)
  console.log(await db.tableNames())

  const results = await table
    .search('keeps me warm')
    .limit(1)
    .execute()
  console.log(results[0].text)
}

example().then(_ => { console.log('All done!') })
