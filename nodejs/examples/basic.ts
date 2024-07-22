// --8<--  [start:imports]
import * as lancedb from "@lancedb/lancedb";
import * as arrow from "apache-arrow";
import {
  Field,
  FixedSizeList,
  Float16,
  Int32,
  Schema,
  Utf8,
} from "apache-arrow";

// --8<-- [end:imports]

// --8<-- [start:connect]
const uri = "/tmp/lancedb/";
const db = await lancedb.connect(uri);
// --8<-- [end:connect]
{
  // --8<-- [start:create_table]
  const tbl = await db.createTable(
    "myTable",
    [
      { vector: [3.1, 4.1], item: "foo", price: 10.0 },
      { vector: [5.9, 26.5], item: "bar", price: 20.0 },
    ],
    { mode: "overwrite" },
  );
  // --8<-- [end:create_table]

  const data = [
    { vector: [3.1, 4.1], item: "foo", price: 10.0 },
    { vector: [5.9, 26.5], item: "bar", price: 20.0 },
  ];

  {
    // --8<-- [start:create_table_exists_ok]
    const tbl = await db.createTable("myTable", data, {
      existsOk: true,
    });
    // --8<-- [end:create_table_exists_ok]
  }
  {
    // --8<-- [start:create_table_overwrite]
    const _tbl = await db.createTable("myTable", data, {
      mode: "overwrite",
    });
    // --8<-- [end:create_table_overwrite]
  }
}

{
  // --8<-- [start:create_table_with_schema]
  const schema = new arrow.Schema([
    new arrow.Field(
      "vector",
      new arrow.FixedSizeList(
        2,
        new arrow.Field("item", new arrow.Float32(), true),
      ),
    ),
    new arrow.Field("item", new arrow.Utf8(), true),
    new arrow.Field("price", new arrow.Float32(), true),
  ]);
  const data = [
    { vector: [3.1, 4.1], item: "foo", price: 10.0 },
    { vector: [5.9, 26.5], item: "bar", price: 20.0 },
  ];
  const _tbl = await db.createTable("myTable", data, {
    schema,
  });
  // --8<-- [end:create_table_with_schema]
}

{
  // --8<-- [start:create_empty_table]

  const schema = new arrow.Schema([
    new arrow.Field("id", new arrow.Int32()),
    new arrow.Field("name", new arrow.Utf8()),
  ]);

  const empty_tbl = await db.createEmptyTable("empty_table", schema);
  // --8<-- [end:create_empty_table]
}
{
  // --8<-- [start:open_table]
  const _tbl = await db.openTable("myTable");
  // --8<-- [end:open_table]
}

{
  // --8<-- [start:table_names]
  const tableNames = await db.tableNames();
  console.log(tableNames);
  // --8<-- [end:table_names]
}

const tbl = await db.openTable("myTable");
{
  // --8<-- [start:add_data]
  const data = [
    { vector: [1.3, 1.4], item: "fizz", price: 100.0 },
    { vector: [9.5, 56.2], item: "buzz", price: 200.0 },
  ];
  await tbl.add(data);
  // --8<-- [end:add_data]
}
{
  // --8<-- [start:vector_search]
  const _res = tbl.search([100, 100]).limit(2).toArray();
  // --8<-- [end:vector_search]
}
{
  const data = Array.from({ length: 1000 })
    .fill(null)
    .map(() => ({
      vector: [Math.random(), Math.random()],
      item: "autogen",
      price: Math.round(Math.random() * 100),
    }));

  await tbl.add(data);
}

// --8<-- [start:create_index]
await tbl.createIndex("vector");
// --8<-- [end:create_index]

// --8<-- [start:delete_rows]
await tbl.delete('item = "fizz"');
// --8<-- [end:delete_rows]

// --8<-- [start:drop_table]
await db.dropTable("myTable");
// --8<-- [end:drop_table]
await db.dropTable("empty_table");

{
  // --8<-- [start:create_f16_table]
  const db = await lancedb.connect("/tmp/lancedb");
  const dim = 16;
  const total = 10;
  const f16Schema = new Schema([
    new Field("id", new Int32()),
    new Field(
      "vector",
      new FixedSizeList(dim, new Field("item", new Float16(), true)),
      false,
    ),
  ]);
  const data = lancedb.makeArrowTable(
    Array.from(Array(total), (_, i) => ({
      id: i,
      vector: Array.from(Array(dim), Math.random),
    })),
    { schema: f16Schema },
  );
  const _table = await db.createTable("f16_tbl", data);
  // --8<-- [end:create_f16_table]
  await db.dropTable("f16_tbl");
}
