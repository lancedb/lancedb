(async () => {

      const lancedb = require("vectordb");

      const uri = "data/sample-lancedb";
      const db = await lancedb.connect(uri);
      const table = await db.createTable("my_table",
            [{ id: 1, vector: [3.1, 4.1], item: "foo", price: 10.0 },
            { id: 2, vector: [5.9, 26.5], item: "bar", price: 20.0 }])
      const results = await table.search([100, 100]).limit(2).execute();

})();