(async () => {

      const lancedb = require("vectordb");

      const uri = "data/sample-lancedb";
      const db = await lancedb.connect(uri);


      const tb = await db.createTable("my_table",
                        data=[{"vector": [3.1, 4.1], "item": "foo", "price": 10.0},
                              {"vector": [5.9, 26.5], "item": "bar", "price": 20.0}])


      const tbl = await db.openTable("my_table");


      console.log(await db.tableNames());


      await tbl.add([{vector: [1.3, 1.4], item: "fizz", price: 100.0},
              {vector: [9.5, 56.2], item: "buzz", price: 200.0}])


      const query = await tbl.search([100, 100]).limit(2).execute();


      await tbl.delete('item = "fizz"')


/** @type {import('next').NextConfig} */
module.exports = ({
  webpack(config) {
    config.externals.push({ vectordb: 'vectordb' })
    return config;
  }
})

})();