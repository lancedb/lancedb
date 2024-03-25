(async () => {

     const vectordb = require('vectordb')
     const db = await vectordb.connect('data/sample-lancedb')

     let data = []
     for (let i = 0; i < 10_000; i++) {
         data.push({vector: Array(1536).fill(i), id: `${i}`, content: "", longId: `${i}`},)
     }
     const table = await db.createTable('my_vectors', data)
     await table.createIndex({ type: 'ivf_pq', column: 'vector', num_partitions: 256, num_sub_vectors: 96 })


     console.log("here 1")
     const results_1 = await table
         .search(Array(1536).fill(1.2))
         .limit(2)
         .nprobes(20)
         .refineFactor(10)
         .execute()

    console.log("here 2")
     const results_2 = await table
         .search(Array(1536).fill(1.2))
         .where("id != '1141'")
         .limit(2)
         .execute()

         console.log("here 3")
     const results_3 = await table
         .search(Array(1536).fill(1.2))
         .select(["id"])
         .limit(2)
         .execute()

})();