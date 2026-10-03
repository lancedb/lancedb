// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors
import { expect, test } from "@jest/globals";
import * as arrow from "apache-arrow";
import * as lancedb from "@lancedb/lancedb";
import { withTempDirectory } from "./util.ts";

async function updateConnectEnterpriseExample() {
  // --8<-- [start:update_connect_enterprise]
  const db = await lancedb.connect("db://your-project-slug", {
    apiKey: "your-api-key",
    region: "us-east-1",
  });
  // --8<-- [end:update_connect_enterprise]
  return db;
}

async function updateConnectLocalExample() {
  // --8<-- [start:update_connect_local]
  const db = await lancedb.connect("./data");
  // --8<-- [end:update_connect_local]
  return db;
}

test("table creation snippets (async)", async () => {
  await withTempDirectory(async (databaseDir) => {
    const db = await lancedb.connect(databaseDir);

    // --8<-- [start:create_table_from_dicts]
    type Location = {
      vector: number[];
      lat: number;
      long: number;
    };

    const data: Location[] = [
      { vector: [1.1, 1.2], lat: 45.5, long: -122.7 },
      { vector: [0.2, 1.8], lat: 40.1, long: -74.1 },
    ];
    const table = await db.createTable("test_table", data, {
      mode: "overwrite",
    });
    // --8<-- [end:create_table_from_dicts]
    expect(await table.countRows()).toBe(2);

    await db.createTable("conflict_table", data);
    // --8<-- [start:create_table_conflict_handling]
    // Idempotent open: reuse the existing table if it exists.
    // The provided data is ignored; the schema is validated against the
    // existing table and a mismatch raises an error.
    let conflictTable = await db.createTable("conflict_table", data, {
      existOk: true,
    });

    // Overwrite: drop the existing table and create a new one with the
    // provided data. This permanently discards the old table's data.
    conflictTable = await db.createTable("conflict_table", data, {
      mode: "overwrite",
    });
    // --8<-- [end:create_table_conflict_handling]
    expect(await conflictTable.countRows()).toBe(2);

    // --8<-- [start:create_table_custom_schema]
    const customSchema = new arrow.Schema([
      new arrow.Field(
        "vector",
        new arrow.FixedSizeList(
          4,
          new arrow.Field("item", new arrow.Float32(), true),
        ),
      ),
      new arrow.Field("lat", new arrow.Float32()),
      new arrow.Field("long", new arrow.Float32()),
    ]);

    const customSchemaData = lancedb.makeArrowTable(
      [
        { vector: [1.1, 1.2, 1.3, 1.4], lat: 45.5, long: -122.7 },
        { vector: [0.2, 1.8, 0.4, 3.6], lat: 40.1, long: -74.1 },
      ],
      { schema: customSchema },
    );
    const customSchemaTable = await db.createTable(
      "my_table_custom_schema",
      customSchemaData,
      { mode: "overwrite" },
    );
    // --8<-- [end:create_table_custom_schema]
    expect(await customSchemaTable.countRows()).toBe(2);

    // --8<-- [start:create_table_from_arrow]
    const arrowSchema = new arrow.Schema([
      new arrow.Field(
        "vector",
        new arrow.FixedSizeList(
          16,
          new arrow.Field("item", new arrow.Float32(), true),
        ),
      ),
      new arrow.Field("text", new arrow.Utf8()),
    ]);
    const arrowData = lancedb.makeArrowTable(
      [
        { vector: Array(16).fill(0.1), text: "foo" },
        { vector: Array(16).fill(0.2), text: "bar" },
      ],
      { schema: arrowSchema },
    );
    const arrowTable = await db.createTable("f32_tbl", arrowData, {
      mode: "overwrite",
    });
    // --8<-- [end:create_table_from_arrow]
    expect(await arrowTable.countRows()).toBe(2);

    // --8<-- [start:create_table_from_iterator]
    const batchSchema = new arrow.Schema([
      new arrow.Field(
        "vector",
        new arrow.FixedSizeList(
          4,
          new arrow.Field("item", new arrow.Float32(), true),
        ),
      ),
      new arrow.Field("item", new arrow.Utf8()),
      new arrow.Field("price", new arrow.Float32()),
    ]);

    const tableForBatches = await db.createEmptyTable(
      "batched_table",
      batchSchema,
      {
        mode: "overwrite",
      },
    );

    const rows = Array.from({ length: 10 }, (_, i) => ({
      vector: [i + 0.1, i + 0.2, i + 0.3, i + 0.4],
      item: `item-${i + 1}`,
      price: (i + 1) * 10,
    }));

    const chunkSize = 2;
    for (let i = 0; i < rows.length; i += chunkSize) {
      const batch = lancedb.makeArrowTable(rows.slice(i, i + chunkSize), {
        schema: batchSchema,
      });
      await tableForBatches.add(batch);
    }
    // --8<-- [end:create_table_from_iterator]
    expect(await tableForBatches.countRows()).toBe(10);

    // --8<-- [start:open_existing_table]
    const openTableData = [{ vector: [1.1, 1.2], lat: 45.5, long: -122.7 }];
    await db.createTable("test_table_open", openTableData, {
      mode: "overwrite",
    });

    console.log(await db.tableNames());

    const openedTable = await db.openTable("test_table_open");
    // --8<-- [end:open_existing_table]
    expect(await openedTable.countRows()).toBe(1);

    // --8<-- [start:create_empty_table]
    const emptySchema = new arrow.Schema([
      new arrow.Field(
        "vector",
        new arrow.FixedSizeList(
          2,
          new arrow.Field("item", new arrow.Float32(), true),
        ),
      ),
      new arrow.Field("item", new arrow.Utf8()),
      new arrow.Field("price", new arrow.Float32()),
    ]);
    const emptyTable = await db.createEmptyTable(
      "test_empty_table",
      emptySchema,
      {
        mode: "overwrite",
      },
    );
    // --8<-- [end:create_empty_table]
    expect(await emptyTable.countRows()).toBe(0);

    // --8<-- [start:drop_table]
    await db.createTable("my_table", [{ vector: [1.1, 1.2], lat: 45.5 }], {
      mode: "overwrite",
    });

    await db.dropTable("my_table");
    // --8<-- [end:drop_table]
    expect(await db.tableNames()).not.toContain("my_table");
  });
});

test("schema evolution snippets (async)", async () => {
  await withTempDirectory(async (databaseDir) => {
    const db = await lancedb.connect(databaseDir);

    // --8<-- [start:schema_add_setup]
    const schemaAddData = [
      {
        id: 1,
        name: "Laptop",
        price: 1200.0,
        vector: Array.from({ length: 128 }, () => Math.random()),
      },
      {
        id: 2,
        name: "Smartphone",
        price: 800.0,
        vector: Array.from({ length: 128 }, () => Math.random()),
      },
      {
        id: 3,
        name: "Headphones",
        price: 150.0,
        vector: Array.from({ length: 128 }, () => Math.random()),
      },
    ];
    const schemaAddTable = await db.createTable(
      "schema_evolution_add_example",
      schemaAddData,
      { mode: "overwrite" },
    );
    // --8<-- [end:schema_add_setup]
    expect(await schemaAddTable.countRows()).toBe(3);

    // --8<-- [start:add_columns_calculated]
    // Add a discounted price column (10% discount)
    await schemaAddTable.addColumns([
      {
        name: "discounted_price",
        valueSql: "cast((price * 0.9) as float)",
      },
    ]);
    // --8<-- [end:add_columns_calculated]

    // --8<-- [start:add_columns_default_values]
    // Add a stock status column with default value
    await schemaAddTable.addColumns([
      {
        name: "in_stock",
        valueSql: "cast(true as boolean)",
      },
    ]);
    // --8<-- [end:add_columns_default_values]

    // --8<-- [start:add_columns_nullable]
    // Add a nullable timestamp column
    await schemaAddTable.addColumns([
      {
        name: "last_ordered",
        valueSql: "cast(NULL as timestamp)",
      },
    ]);
    // --8<-- [end:add_columns_nullable]

    // --8<-- [start:add_feature_columns_sql]
    await schemaAddTable.addColumns([
      {
        name: "price_per_id",
        valueSql: "cast(price / id as float)",
      },
      {
        name: "price_log",
        valueSql: "ln(price)",
      },
      {
        name: "price_score",
        valueSql: "cast(price / (price + 100.0) as float)",
      },
    ]);
    // --8<-- [end:add_feature_columns_sql]
    expect((await schemaAddTable.schema()).fields.map((field) => field.name)).toEqual(
      expect.arrayContaining(["price_per_id", "price_log", "price_score"]),
    );

    // --8<-- [start:schema_alter_setup]
    const schemaAlter = new arrow.Schema([
      new arrow.Field("id", new arrow.Int64()),
      new arrow.Field("name", new arrow.Utf8()),
      new arrow.Field("price", new arrow.Int32()),
      new arrow.Field("discount_price", new arrow.Float64()),
      new arrow.Field(
        "vector",
        new arrow.FixedSizeList(
          128,
          new arrow.Field("item", new arrow.Float32(), true),
        ),
      ),
    ]);
    const schemaAlterData = lancedb.makeArrowTable(
      [
        {
          id: 1,
          name: "Laptop",
          price: 1200,
          discount_price: 1080.0,
          vector: Array.from({ length: 128 }, () => Math.random()),
        },
        {
          id: 2,
          name: "Smartphone",
          price: 800,
          discount_price: 720.0,
          vector: Array.from({ length: 128 }, () => Math.random()),
        },
      ],
      { schema: schemaAlter },
    );
    const schemaAlterTable = await db.createTable(
      "schema_evolution_alter_example",
      schemaAlterData,
      { mode: "overwrite" },
    );
    // --8<-- [end:schema_alter_setup]
    expect(await schemaAlterTable.countRows()).toBe(2);

    // --8<-- [start:alter_columns_rename]
    // Rename discount_price to sale_price
    await schemaAlterTable.alterColumns([
      { path: "discount_price", rename: "sale_price" },
    ]);
    // --8<-- [end:alter_columns_rename]

    // --8<-- [start:alter_columns_data_type]
    // Change price from int32 to int64 for larger numbers
    await schemaAlterTable.alterColumns([
      { path: "price", dataType: new arrow.Int64() },
    ]);
    // --8<-- [end:alter_columns_data_type]

    // --8<-- [start:alter_columns_nullable]
    // Make the name column nullable
    await schemaAlterTable.alterColumns([{ path: "name", nullable: true }]);
    // --8<-- [end:alter_columns_nullable]

    // --8<-- [start:alter_columns_multiple]
    // Rename, change type, and make nullable in one operation
    await schemaAlterTable.alterColumns([
      {
        path: "sale_price",
        rename: "final_price",
        dataType: new arrow.Float64(),
        nullable: true,
      },
    ]);
    // --8<-- [end:alter_columns_multiple]

    // --8<-- [start:alter_columns_with_expression]
    // For custom transforms, create a new column from a SQL expression.
    const expressionTable = await db.createTable(
      "schema_evolution_expression_example",
      [{ id: 1, price_text: "$100" }],
      { mode: "overwrite" },
    );

    await expressionTable.addColumns([
      {
        name: "price_numeric",
        valueSql: "cast(replace(price_text, '$', '') as int)",
      },
    ]);
    await expressionTable.dropColumns(["price_text"]);
    await expressionTable.alterColumns([
      { path: "price_numeric", rename: "price" },
    ]);
    // --8<-- [end:alter_columns_with_expression]
    expect(await expressionTable.countRows()).toBe(1);

    // --8<-- [start:schema_drop_setup]
    const schemaDropData = [
      {
        id: 1,
        name: "Laptop",
        price: 1200.0,
        temp_col1: "X",
        temp_col2: 100,
        vector: Array.from({ length: 128 }, () => Math.random()),
      },
      {
        id: 2,
        name: "Smartphone",
        price: 800.0,
        temp_col1: "Y",
        temp_col2: 200,
        vector: Array.from({ length: 128 }, () => Math.random()),
      },
      {
        id: 3,
        name: "Headphones",
        price: 150.0,
        temp_col1: "Z",
        temp_col2: 300,
        vector: Array.from({ length: 128 }, () => Math.random()),
      },
    ];
    const schemaDropTable = await db.createTable(
      "schema_evolution_drop_example",
      schemaDropData,
      { mode: "overwrite" },
    );
    // --8<-- [end:schema_drop_setup]
    expect(await schemaDropTable.countRows()).toBe(3);

    // --8<-- [start:drop_columns_single]
    // Remove the first temporary column
    await schemaDropTable.dropColumns(["temp_col1"]);
    // --8<-- [end:drop_columns_single]

    // --8<-- [start:drop_columns_multiple]
    // Remove the second temporary column
    await schemaDropTable.dropColumns(["temp_col2"]);
    // --8<-- [end:drop_columns_multiple]

    // --8<-- [start:alter_vector_column]
    const oldDim = 384;
    const newDim = 1024;
    const vectorSchema = new arrow.Schema([
      new arrow.Field("id", new arrow.Int64()),
      new arrow.Field(
        "embedding",
        new arrow.FixedSizeList(
          oldDim,
          new arrow.Field("item", new arrow.Float16(), true),
        ),
        true,
      ),
    ]);
    const vectorData = lancedb.makeArrowTable(
      [{ id: 1, embedding: Array.from({ length: oldDim }, () => Math.random()) }],
      { schema: vectorSchema },
    );
    const vectorTable = await db.createTable("vector_alter_example", vectorData, {
      mode: "overwrite",
    });

    // Changing FixedSizeList dimensions (384 -> 1024) is not supported via alterColumns.
    // Use addColumns + dropColumns + alterColumns(rename) to replace the column.
    await vectorTable.addColumns([
      {
        name: "embedding_v2",
        valueSql: `arrow_cast(NULL, 'FixedSizeList(${newDim}, Float16)')`,
      },
    ]);
    await vectorTable.dropColumns(["embedding"]);
    await vectorTable.alterColumns([{ path: "embedding_v2", rename: "embedding" }]);
    // --8<-- [end:alter_vector_column]
    expect(await vectorTable.countRows()).toBe(1);

    const fieldMetadataTable = await db.createTable(
      "schema_field_metadata_example",
      [
        { id: 0, category: "a" },
        { id: 1, category: "b" },
      ],
      { mode: "overwrite" },
    );

    // --8<-- [start:schema_field_metadata_merge]
    // Set two metadata keys on the `category` field.
    const res = await fieldMetadataTable.updateFieldMetadata([
      { path: "category", metadata: { unit: "label", pii: "false" } },
    ]);
    console.log(res.version);

    // Merge: add a new key, delete one via null, keep the rest.
    await fieldMetadataTable.updateFieldMetadata([
      { path: "category", metadata: { source: "import", pii: null } },
    ]);
    // --8<-- [end:schema_field_metadata_merge]

    // --8<-- [start:schema_field_metadata_replace]
    await fieldMetadataTable.updateFieldMetadata([
      {
        path: "category",
        metadata: { owner: "search-team" },
        replace: true,
      },
    ]);
    // --8<-- [end:schema_field_metadata_replace]
  });
});

test("update snippets (async)", async () => {
  // Keep connection snippets in this file, but do not run enterprise/local examples in CI.
  void updateConnectEnterpriseExample;
  void updateConnectLocalExample;

  await withTempDirectory(async (databaseDir) => {
    const db = await lancedb.connect(databaseDir);

    {
      // --8<-- [start:update_example_table_setup]
      const table = await db.createTable(
        "users_example",
        [
          { id: 1, name: "Alice", login_count: 10 },
          { id: 2, name: "Bob", login_count: 20 },
        ],
        { mode: "overwrite" },
      );
      // --8<-- [end:update_example_table_setup]
      await table.countRows();
    }

    {
      // --8<-- [start:update_operation]
      const table = await db.createTable(
        "users_example",
        [
          { id: 1, name: "Alice", login_count: 10 },
          { id: 2, name: "Bob", login_count: 20 },
        ],
        { mode: "overwrite" },
      );
      await table.update({ where: "id = 2", values: { name: "Bobby" } });
      // --8<-- [end:update_operation]
      await table.countRows();
    }

    {
      // --8<-- [start:update_using_sql]
      const table = await db.createTable(
        "users_example",
        [
          { id: 1, name: "Alice", login_count: 10 },
          { id: 2, name: "Bob", login_count: 20 },
        ],
        { mode: "overwrite" },
      );
      await table.update({
        where: "id = 2",
        valuesSql: { login_count: "login_count + 1" },
      });
      // --8<-- [end:update_using_sql]
      await table.countRows();
    }

    {
      // --8<-- [start:merge_matched_update_only]
      const table = await db.createTable(
        "users_example",
        [
          { id: 1, name: "Alice", login_count: 10 },
          { id: 2, name: "Bob", login_count: 20 },
        ],
        { mode: "overwrite" },
      );

      const incomingUsers = [
        { id: 2, name: "Bobby", login_count: 21 },
        { id: 3, name: "Charlie", login_count: 5 },
      ];

      await table
        .mergeInsert("id")
        .whenMatchedUpdateAll()
        .execute(incomingUsers);
      // --8<-- [end:merge_matched_update_only]
      await table.countRows();
    }

    {
      // --8<-- [start:insert_if_not_exists]
      const table = await db.createTable(
        "users_example",
        [
          { id: 1, name: "Alice", login_count: 10 },
          { id: 2, name: "Bob", login_count: 20 },
        ],
        { mode: "overwrite" },
      );

      const incomingUsers = [
        { id: 2, name: "Bobby", login_count: 21 },
        { id: 3, name: "Charlie", login_count: 5 },
      ];

      await table
        .mergeInsert("id")
        .whenNotMatchedInsertAll()
        .execute(incomingUsers);
      // --8<-- [end:insert_if_not_exists]
      await table.countRows();
    }

    {
      // --8<-- [start:merge_update_insert]
      const table = await db.createTable(
        "users_example",
        [
          { id: 1, name: "Alice", login_count: 10 },
          { id: 2, name: "Bob", login_count: 20 },
        ],
        { mode: "overwrite" },
      );

      const incomingUsers = [
        { id: 2, name: "Bobby", login_count: 21 },
        { id: 3, name: "Charlie", login_count: 5 },
      ];

      await table
        .mergeInsert("id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute(incomingUsers);
      // --8<-- [end:merge_update_insert]
      await table.countRows();
    }

    {
      // --8<-- [start:merge_delete_missing_by_source]
      const table = await db.createTable(
        "users_example",
        [
          { id: 1, name: "Alice", login_count: 10 },
          { id: 2, name: "Bob", login_count: 20 },
          { id: 3, name: "Charlie", login_count: 5 },
        ],
        { mode: "overwrite" },
      );

      const incomingUsers = [
        { id: 2, name: "Bobby", login_count: 21 },
        { id: 3, name: "Charlie", login_count: 5 },
      ];

      await table
        .mergeInsert("id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceDelete()
        .execute(incomingUsers);
      // --8<-- [end:merge_delete_missing_by_source]
      await table.countRows();
    }

    {
      // --8<-- [start:merge_partial_columns]
      const table = await db.createTable(
        "users_example",
        [
          { id: 1, name: "Alice", login_count: 10 },
          { id: 2, name: "Bob", login_count: 20 },
        ],
        { mode: "overwrite" },
      );

      const incomingUsers = [
        { id: 2, name: "Bobby" },
        { id: 3, name: "Charlie" },
      ];

      await table
        .mergeInsert("id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute(incomingUsers);
      // --8<-- [end:merge_partial_columns]
      await table.countRows();
    }

    {
      const table = await db.createTable(
        "users_example",
        [
          { id: 1, name: "Alice", login_count: 10 },
          { id: 2, name: "Bob", login_count: 20 },
          { id: 3, name: "Charlie", login_count: 5 },
        ],
        { mode: "overwrite" },
      );

      // --8<-- [start:delete_operation]
      // delete data
      const predicate = "id = 3";
      await table.delete(predicate);
      // --8<-- [end:delete_operation]
      await table.countRows();
    }

    {
      const table = await db.createTable(
        "users_cleanup_example",
        [
          { id: 1, name: "Alice", login_count: 10 },
          { id: 2, name: "Bob", login_count: 20 },
          { id: 3, name: "Charlie", login_count: 5 },
        ],
        { mode: "overwrite" },
      );

      // --8<-- [start:update_optimize_cleanup]
      const olderThan = new Date();
      olderThan.setDate(olderThan.getDate() - 1);
      await table.optimize({ cleanupOlderThan: olderThan });
      // --8<-- [end:update_optimize_cleanup]
    }
  });
});

test("versioning snippets (async)", async () => {
  await withTempDirectory(async (databaseDir) => {
    const db = await lancedb.connect(databaseDir);

    // --8<-- [start:versioning_basic_setup]
    const tableName = "quotes_versioning_example";
    const data = [
      { id: 1, author: "Richard", quote: "Wubba Lubba Dub Dub!" },
      { id: 2, author: "Morty", quote: "Rick, what's going on?" },
      {
        id: 3,
        author: "Richard",
        quote: "I turned myself into a pickle, Morty!",
      },
    ];
    const table = await db.createTable(tableName, data, { mode: "overwrite" });
    // --8<-- [end:versioning_basic_setup]
    expect(await table.countRows()).toBe(3);

    // --8<-- [start:versioning_check_initial_version]
    const versions = await table.listVersions();
    const currentVersion = await table.version();
    console.log(`Number of versions after creation: ${versions.length}`);
    console.log(`Current version: ${currentVersion}`);
    // --8<-- [end:versioning_check_initial_version]
    expect(versions.length).toBe(1);
    expect(currentVersion).toBe(versions[versions.length - 1].version);

    // --8<-- [start:versioning_update_data]
    await table.update({
      where: "author = 'Richard'",
      values: { author: "Richard Daniel Sanchez" },
    });
    const rowsAfterUpdate = await table.countRows(
      "author = 'Richard Daniel Sanchez'",
    );
    console.log(`Rows updated to Richard Daniel Sanchez: ${rowsAfterUpdate}`);
    // --8<-- [end:versioning_update_data]
    expect(rowsAfterUpdate).toBe(2);

    // --8<-- [start:versioning_add_data]
    const moreData = [
      {
        id: 4,
        author: "Richard Daniel Sanchez",
        quote: "That's the way the news goes!",
      },
      { id: 5, author: "Morty", quote: "Aww geez, Rick!" },
    ];
    await table.add(moreData);
    // --8<-- [end:versioning_add_data]
    expect(await table.countRows()).toBe(5);

    // --8<-- [start:versioning_check_versions_after_mod]
    const versionsAfterMod = await table.listVersions();
    const versionCountAfterMod = versionsAfterMod.length;
    const versionAfterMod = await table.version();
    console.log(
      `Number of versions after modifications: ${versionCountAfterMod}`,
    );
    console.log(`Current version: ${versionAfterMod}`);
    // --8<-- [end:versioning_check_versions_after_mod]
    expect(versionCountAfterMod).toBeGreaterThanOrEqual(2);
    expect(versionAfterMod).toBe(versionsAfterMod[versionsAfterMod.length - 1].version);

    // --8<-- [start:versioning_list_all_versions]
    const allVersions = await table.listVersions();
    for (const v of allVersions) {
      console.log(`Version ${v.version}, created at ${v.timestamp}`);
    }
    // --8<-- [end:versioning_list_all_versions]
    expect(allVersions.length).toBeGreaterThanOrEqual(1);

    // --8<-- [start:versioning_rollback]
    await table.checkout(versionAfterMod);
    await table.restore();
    const versionsAfterRollback = await table.listVersions();
    const versionCountAfterRollback = versionsAfterRollback.length;
    console.log(
      `Total number of versions after rollback: ${versionCountAfterRollback}`,
    );
    // --8<-- [end:versioning_rollback]
    expect(versionCountAfterRollback).toBe(versionCountAfterMod + 1);
    expect(await table.countRows()).toBe(5);

    // --8<-- [start:versioning_checkout_latest]
    await table.checkoutLatest();
    // --8<-- [end:versioning_checkout_latest]
    const latestVersion = await table.version();
    const versionsAfterCheckout = await table.listVersions();
    expect(latestVersion).toBe(
      versionsAfterCheckout[versionsAfterCheckout.length - 1].version,
    );

    // --8<-- [start:versioning_delete_data]
    await table.delete("author = 'Morty'");
    const rowsAfterDeletion = await table.countRows();
    console.log(`Number of rows after deletion: ${rowsAfterDeletion}`);
    // --8<-- [end:versioning_delete_data]
    expect(rowsAfterDeletion).toBe(3);

    const tagsTable = await db.createTable(
      "quotes_tags_example",
      [{ id: 1, author: "Richard", quote: "Wubba Lubba Dub Dub!" }],
      { mode: "overwrite" },
    ); // v1
    await tagsTable.add([
      { id: 2, author: "Morty", quote: "Aww geez, Rick!" },
    ]); // v2
    await tagsTable.add([
      { id: 3, author: "Summer", quote: "Whatever, Grandpa" },
    ]); // v3

    // --8<-- [start:versioning_tags]
    const tags = await tagsTable.tags();

    // Create a tag pointing at a specific version
    await tags.create("baseline", 1);
    await tags.create("with-edits", await tagsTable.version());

    // List all tags on this table
    console.log(await tags.list());

    // Look up the version a tag points at
    console.log(await tags.getVersion("baseline"));

    // Move an existing tag to a different version
    await tags.update("baseline", 2);

    // Check out a version by tag name
    await tagsTable.checkout("baseline");
    console.log(await tagsTable.version());

    // Delete a tag (does not delete the underlying version)
    await tags.delete("with-edits");

    // Return to the latest version
    await tagsTable.checkoutLatest();
    // --8<-- [end:versioning_tags]
    expect(await tagsTable.version()).toBe(3);
    const remainingTags = await tags.list();
    expect(remainingTags).toHaveProperty("baseline");
    expect(remainingTags).not.toHaveProperty("with-edits");
  });
});

test("branch snippets (async)", async () => {
  await withTempDirectory(async (databaseDir) => {
    const db = await lancedb.connect(databaseDir);
    const table = await db.createTable(
      "quotes_branches_example",
      [
        { id: 1, author: "Lancelot", quote: "My lance never fails." },
        { id: 2, author: "Arthur", quote: "Long live Camelot!" },
        { id: 3, author: "Merlin", quote: "Magic always has a price." },
      ],
      { mode: "overwrite" },
    );

    const branches = await table.branches();

    // --8<-- [start:branch_create]
    // Fork an isolated, writable branch from main's latest version.
    // `create` returns a table handle scoped to the new branch.
    const branch = await branches.create("exp");
    // --8<-- [end:branch_create]

    // --8<-- [start:branch_write]
    // Writes land on the branch handle only; main is left untouched.
    await branch.add([{ id: 4, author: "Lancelot", quote: "For the realm!" }]);
    console.log(await branch.countRows()); // 4 rows on the branch
    console.log(await table.countRows()); // 3 rows; main is unaffected

    // List every branch, each mapped to its metadata (including its fork point).
    console.log(await branches.list());
    // --8<-- [end:branch_write]

    // --8<-- [start:branch_reopen]
    // Reopen an existing branch by name from the table handle...
    const checkedOut = await branches.checkout("exp");
    // ...or open it directly from the database connection.
    const branchHandle = await db.openTable(
      "quotes_branches_example",
      undefined,
      { branch: "exp" },
    );
    console.log(await checkedOut.countRows(), await branchHandle.countRows()); // both 4
    // --8<-- [end:branch_reopen]

    // --8<-- [start:branch_delete]
    // Delete the branch and its branch-local history. Data on main is safe.
    await branches.delete("exp");
    // --8<-- [end:branch_delete]

    expect(await table.countRows()).toBe(3);
    expect(await branches.list()).not.toHaveProperty("exp");

    // Setup: a branch with row results that we want to apply to main.
    const candidate = await branches.create("candidate");
    await candidate.update({
      where: "id = 1",
      values: { quote: "Revised on the branch" },
    });
    await candidate.add([
      { id: 4, author: "Galahad", quote: "The grail awaits." },
    ]);

    // --8<-- [start:branch_upsert_to_main]
    // This is a row-level upsert, not a merge of branch histories.
    // `mergeInsert` updates matching rows and inserts new rows using a stable
    // unique key. Filter the branch read if you only want to apply some results.
    const rowsToApply = await candidate.toArrow();
    await table
      .mergeInsert("id")
      .whenMatchedUpdateAll() // update rows that already exist on main
      .whenNotMatchedInsertAll() // insert rows that are new on the branch
      .execute(rowsToApply);
    // --8<-- [end:branch_upsert_to_main]

    expect(await table.countRows()).toBe(4);
    await branches.delete("candidate");

    // Setup: a larger table with a vector and a text column to index.
    const products = await db.createTable(
      "products_branch_index",
      Array.from({ length: 512 }, (_, i) => ({
        id: i,
        vector: Array.from({ length: 4 }, () => Math.random()),
        text: `product number ${i}`,
      })),
      { mode: "overwrite" },
    );
    const productBranches = await products.branches();

    // --8<-- [start:branch_index]
    // Build and validate indexes on a branch before using the configuration on
    // main.
    const dev = await productBranches.create("index-dev");

    // A vector (ANN) index and a full-text search index, both branch-scoped.
    await dev.createIndex("vector", {
      config: lancedb.Index.ivfPq({
        distanceType: "cosine",
        numPartitions: 1,
        numSubVectors: 2,
      }),
    });
    await dev.createIndex("text", { config: lancedb.Index.fts() });

    // Both indexes live only on the branch; main still has none.
    console.log((await dev.listIndices()).map((ix) => ix.name)); // branch: two indexes
    console.log((await products.listIndices()).map((ix) => ix.name)); // main: [] (untouched)
    // --8<-- [end:branch_index]

    expect(await dev.listIndices()).toHaveLength(2);
    expect(await products.listIndices()).toHaveLength(0);
    await productBranches.delete("index-dev");
  });
});

test("consistency snippets (async)", async () => {
  await withTempDirectory(async (databaseDir) => {
    // --8<-- [start:consistency_strong]
    const strongWriterDb = await lancedb.connect(databaseDir);
    const strongReaderDb = await lancedb.connect(databaseDir, {
      readConsistencyInterval: 0,
    });
    const strongWriterTable = await strongWriterDb.createTable(
      "consistency_strong_table",
      [{ id: 1 }],
      { mode: "overwrite" },
    );
    const strongReaderTable = await strongReaderDb.openTable(
      "consistency_strong_table",
    );
    await strongWriterTable.add([{ id: 2 }]);
    const strongRowsAfterWrite = await strongReaderTable.countRows();
    console.log(`Rows visible with strong consistency: ${strongRowsAfterWrite}`);
    // --8<-- [end:consistency_strong]
    expect(strongRowsAfterWrite).toBe(2);

    // --8<-- [start:consistency_eventual]
    const eventualWriterDb = await lancedb.connect(databaseDir);
    const eventualReaderDb = await lancedb.connect(databaseDir, {
      readConsistencyInterval: 3600,
    });
    const eventualWriterTable = await eventualWriterDb.createTable(
      "consistency_eventual_table",
      [{ id: 1 }],
      { mode: "overwrite" },
    );
    const eventualReaderTable = await eventualReaderDb.openTable(
      "consistency_eventual_table",
    );
    await eventualWriterTable.add([{ id: 2 }]);
    const eventualRowsAfterWrite = await eventualReaderTable.countRows();
    console.log(
      `Rows visible before eventual refresh interval: ${eventualRowsAfterWrite}`,
    );
    // --8<-- [end:consistency_eventual]
    expect(eventualRowsAfterWrite).toBe(1);

    // --8<-- [start:consistency_checkout_latest]
    const checkoutWriterDb = await lancedb.connect(databaseDir);
    const checkoutReaderDb = await lancedb.connect(databaseDir);
    const checkoutWriterTable = await checkoutWriterDb.createTable(
      "consistency_checkout_latest_table",
      [{ id: 1 }],
      { mode: "overwrite" },
    );
    const checkoutReaderTable = await checkoutReaderDb.openTable(
      "consistency_checkout_latest_table",
    );
    await checkoutWriterTable.add([{ id: 2 }]);
    const rowsBeforeRefresh = await checkoutReaderTable.countRows();
    console.log(`Rows before checkoutLatest: ${rowsBeforeRefresh}`);
    await checkoutReaderTable.checkoutLatest();
    const rowsAfterRefresh = await checkoutReaderTable.countRows();
    console.log(`Rows after checkoutLatest: ${rowsAfterRefresh}`);
    // --8<-- [end:consistency_checkout_latest]
    expect(rowsBeforeRefresh).toBe(1);
    expect(rowsAfterRefresh).toBe(2);
  });
});
