(async () => {

const vectordb_setup = require('vectordb')
const db_setup = await vectordb_setup.connect('data/sample-lancedb')

let data = []
for (let i = 0; i < 10_000; i++) {
     data.push({vector: Array(1536).fill(i), id: `${i}`, content: "", longId: `${i}`},)
}
await db_setup.createTable('my_vectors', data)


    const vectordb = require('vectordb')
    const db = await vectordb.connect('data/sample-lancedb')

    const tbl = await db.openTable("my_vectors")

    const results_1 = await tbl.search(Array(1536).fill(1.2))
        .limit(10)
        .execute()


    const results_2 = await tbl.search(Array(1536).fill(1.2))
        .metricType("cosine")
        .limit(10)
        .execute()

})();