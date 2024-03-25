(async () => {

const lancedb = require('vectordb');
const db = await lancedb.connect('data/sample-lancedb');
const table = await db.createTable("my_table",
      [{ id: 1, vector: [0.1, 1.0], item: "foo", price: 10.0 },
      { id: 2, vector: [3.9, 0.5], item: "bar", price: 20.0 }])
const results = await table.search([0.1, 0.3]).limit(20).execute();
console.log(results);

})();