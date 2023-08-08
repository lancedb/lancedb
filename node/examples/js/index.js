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

(async () => {

  const vectordb = require('vectordb')
  const db = await vectordb.connect('data/sample-lancedb')

  let data = []
  for (let i = 0; i < 10_000; i++) {
    data.push({vector: Array(1536).fill(i), id: `${i}`, content: "", longId: `${i}`},)
  }
  const table = await db.createTable('my_vectors', data)

  console.log('Create Index')
  await table.createIndex({ type: 'ivf_pq', column: 'vector', num_partitions: 256, num_sub_vectors: 96 })

  console.log('results 1')
  const results_1 = await table
      .search(Array(1536).fill(1.2))
      .limit(2)
      .nprobes(20)
      .refineFactor(10)
      .execute()

  console.log('results 2')
  const results_2 = await table
      .search(Array(1536).fill(1.2))
      .where("id != '1141'")
      .execute()

  console.log('results 3')
  const results_3 = await table
      .search(Array(1536).fill(1.2))
      .select(["id"])
      .execute()

})();
