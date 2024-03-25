(async () => {

const lancedb = require('vectordb');
const { Schema, Field, Int32, FixedSizeList, Float32 } = require('apache-arrow/Arrow.node')

// connect to a remote DB
const db = await lancedb.connect({
  uri: "db://your-project-name",
  apiKey: "sk_...",
  region: "us-east-1"
});
// create a new table
const tableName = "my_table"
const data = [
    { id: 1, vector: [0.1, 1.0], item: "foo", price: 10.0 },
    { id: 2, vector: [3.9, 0.5], item: "bar", price: 20.0 }
]
const schema = new Schema(
    [
        new Field('id', new Int32()), 
        new Field('vector', new FixedSizeList(2, new Field('float32', new Float32()))),
        new Field('item', new String()),
        new Field('price', new Float32())
    ]
)
const table = await db.createTable({
    name: tableName,
    schema,
}, data)

// list the table
const tableNames_1 = await db.tableNames('')
// add some data and search should be okay
const newData = [
      { id: 3, vector: [10.3, 1.9], item: "test1", price: 30.0 }, 
      { id: 4, vector: [6.2, 9.2], item: "test2", price: 40.0 }
]
table.add(newData)
// create the index for the table
await table.createIndex({
      metric_type: 'L2', 
      column: 'vector'
})
let result = await table.search([2.8, 4.3]).select(["vector", "price"]).limit(1).execute()
// update the data
await table.update({ 
      where: "id == 1", 
      values: { item: "foo1" } 
})
//drop the table
await db.dropTable(tableName)

})();