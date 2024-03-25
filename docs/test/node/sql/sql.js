(async () => {

const vectordb = require('vectordb')
const db = await vectordb.connect('data/sample-lancedb')

let data = []
for (let i = 0; i < 10_000; i++) {
     data.push({vector: Array(1536).fill(i), id: `${i}`, content: "", longId: `${i}`},)
}
const tbl = await db.createTable('my_vectors', data)


    tbl.search([100, 102])
       .where(`(
            (label IN [10, 20])
            AND
            (note.email IS NOT NULL)
        ) OR NOT note.created
       `)

})();