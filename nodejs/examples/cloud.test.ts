// --8<--  [start:imports]
import * as lancedb from "@lancedb/lancedb";
// --8<-- [end:imports]

// --8<-- [start:generate_data]
function genData(numRows: number, numVectorDim: number): any[] {
    const data = [];
    for (let i = 0; i < numRows; i++) {
      const vector = [];
      for (let j = 0; j < numVectorDim; j++) {
        vector.push(i + j * 0.1);
      }
      data.push({
        id: i,
        name: `name_${i}`,
        vector,
      });
    }
    return data;
}
// --8<-- [end:generate_data]

test("cloud quickstart", async () => {
    {
        // --8<-- [start:connect]
        const db = await lancedb.connect({
            uri: "db://your-project-slug",
            apiKey: "your-api-key",
            region: "your-cloud-region",
        });
        // --8<-- [end:connect]
        // --8<-- [start:create_table]
        const tableName = "myTable"
        const data = genData(5000, 1536)
        const table = await db.createTable(tableName, data);
        // --8<-- [end:create_table]
        // --8<-- [start:create_index_search]
        // create a vector index
        await table.createIndex({
            column: "vector",
            metric_type: lancedb.MetricType.Cosine,
            type: "ivf_pq",
        });
        const result = await table.search([0.01, 0.02])
            .select(["vector", "item"])
            .limit(1)
            .execute();
        // --8<-- [end:create_index_search]
        // --8<-- [start:drop_table]
        await db.dropTable(tableName);
        // --8<-- [end:drop_table]
    }
});

test("ingest data", async () => {
    // --8<-- [start:ingest_data]
    import { Schema, Field, Float32, FixedSizeList, Utf8 } from "apache-arrow";

    const db = await lancedb.connect({
        uri: "db://your-project-slug",
        apiKey: "your-api-key",
        region: "us-east-1"
    });

    const data = [
        { vector: [3.1, 4.1], item: "foo", price: 10.0 },
        { vector: [5.9, 26.5], item: "bar", price: 20.0 },
        { vector: [10.2, 100.8], item: "baz", price: 30.0},
        { vector: [1.4, 9.5], item: "fred", price: 40.0},
    ]
    // create an empty table with schema
    const schema = new Schema([
        new Field(
        "vector",
        new FixedSizeList(2, new Field("float32", new Float32())),
        ),
        new Field("item", new Utf8()),
        new Field("price", new Float32()),
    ]);
    const tableName = "myTable";
    const table = await db.createTable({
        name: tableName,
        schema,
    });
    await table.add(data);
    // --8<-- [end:ingest_data]
});

test("update data", async () => {
    // --8<-- [start:connect_db_and_open_table]
    const db = await lancedb.connect({
        uri: "db://your-project-slug",
        apiKey: "your-api-key",
        region: "us-east-1"
    });
    const tableName = "myTable"
    const table = await db.openTable(tableName);
    // --8<-- [end:connect_db_and_open_table]
    // --8<-- [start:update_data]
    await table.update({
        where: "price < 20.0",
        values: { vector: [2, 2], item: "foo-updated" },
    });
    // --8<-- [end:update_data]
    // --8<-- [start:merge_insert]
      let newData = [
        {vector: [1, 1], item: 'foo-updated', price: 50.0}
      ];
      // upsert
      await table.mergeInsert("item", newData, {
        whenMatchedUpdateAll: true,
        whenNotMatchedInsertAll: true,
      });
      // --8<-- [end:merge_insert]
      // --8<-- [start:delete_data]
      // delete data
      const predicate = "price = 30.0";
      await table.delete(predicate);
      // --8<-- [end:delete_data]
});

test("create index", async () => {
    const db = await lancedb.connect({
        uri: "db://your-project-slug",
        apiKey: "your-api-key",
        region: "us-east-1"
    });

    const tableName = "myTable";
    const table = await db.openTable(tableName);
    // --8<-- [start:create_index]
    // the vector column only needs to be specified when there are 
    // multiple vector columns or the column is not named as "vector"
    // L2 is used as the default distance metric
    await table.createIndex({
            column: "vector",
            metric_type: lancedb.MetricType.Cosine,
        });

    // --8<-- [end:create_index]
    // --8<-- [start:create_scalar_index]
    await table.createScalarIndex("item");
    // --8<-- [end:create_scalar_index]
    // --8<-- [start:create_fts_index]
    const db = await lancedb.connect({
        uri: "db://your-project-slug",
        apiKey: "your-api-key",
        region: "us-east-1"
    });

    const tableName = "myTable"
    const data = [
        { vector: [3.1, 4.1], text: "Frodo was a happy puppy" },
        { vector: [5.9, 26.5], text: "There are several kittens playing" },
    ];
    const table = createTable(tableName, data);
    await table.createIndex("text", {
        config: lancedb.Index.fts(),
    });
    // --8<-- [end:create_fts_index]
});

test("vector search", async () => {
    // --8<-- [start:vector_search]
    const db = await lancedb.connect({
        uri: "db://your-project-slug",
        apiKey: "your-api-key",
        region: "us-east-1"
    });

    const tableName = "myTable"
    const table = await db.openTable(tableName);
    const result = await table.search([0.4, 1.4])
        .where("price > 10.0")
        .prefilter(true)
        .select(["item", "vector"])
        .limit(2)
        .execute();
    // --8<-- [end:vector_search]
});

test("full-text search", async () => {
    // --8<-- [start:full_text_search]
    const db = await lancedb.connect({
        uri: "db://your-project-slug",
        apiKey: "your-api-key",
        region: "us-east-1"
    });

    const data = [
    { vector: [3.1, 4.1], text: "Frodo was a happy puppy" },
    { vector: [5.9, 26.5], text: "There are several kittens playing" },
    ];
    const tableName = "myTable"
    const table = await db.createTable(tableName, data);
    await table.createIndex("text", {
        config: lancedb.Index.fts(),
    });

    await tableName
        .search("puppy", queryType="fts")
        .select(["text"])
        .limit(10)
        .toArray();
    // --8<-- [end:full_text_search]
});

test("metadata filtering", async () => {
    // --8<-- [start:filtering]
    const db = await lancedb.connect({
        uri: "db://your-project-slug",
        apiKey: "your-api-key",
        region: "us-east-1"
    });
    const tableName = "myTable"
    const table = await db.openTable(tableName);
    await table
        .search(Array(2).fill(0.1))
        .where("(item IN ('foo', 'bar')) AND (price > 10.0)")
        .postfilter()
        .toArray();
    // --8<-- [end:filtering]
    // --8<-- [start:sql_filtering]
    await table
        .search(Array(2).fill(0.1))
        .where("(item IN ('foo', 'bar')) AND (price > 10.0)")
        .postfilter()
        .toArray();
    // --8<-- [end:sql_filtering]
});