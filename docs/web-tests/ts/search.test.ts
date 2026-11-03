// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors
import { expect, test } from "@jest/globals";
// --8<-- [start:import]
import * as lancedb from "@lancedb/lancedb";
// --8<-- [end:import]
// --8<-- [start:import_bin_util]
import { Field, FixedSizeList, Int32, Schema, Uint8 } from "apache-arrow";
// --8<-- [end:import_bin_util]
import { Float32, Struct } from "apache-arrow";
import { withTempDirectory } from "./util.ts";

async function buildIndexedTable(db: lancedb.Connection, name: string) {
  const data = Array.from({ length: 512 }, (_, i) => ({
    vector: Array.from({ length: 128 }, () => Math.random()),
    id: i,
  }));
  const tbl = await db.createTable(name, data, { mode: "overwrite" });
  await tbl.createIndex("vector", {
    config: lancedb.Index.ivfPq({ numPartitions: 4, numSubVectors: 8 }),
  });
  return tbl;
}

test("vector search", async () => {
  await withTempDirectory(async (databaseDir) => {
    {
      const db = await lancedb.connect(databaseDir);

      const data = Array.from({ length: 10_000 }, (_, i) => ({
        vector: Array(128).fill(i),
        id: `${i}`,
      }));

      await db.createTable("my_vectors", data);
    }

    // --8<-- [start:search1]
    const db = await lancedb.connect(databaseDir);
    const tbl = await db.openTable("my_vectors");

    const results1 = await tbl.search(Array(128).fill(1.2)).limit(10).toArray();
    // --8<-- [end:search1]
    expect(results1.length).toBe(10);

    // --8<-- [start:search2]
    const results2 = await (
      tbl.search(Array(128).fill(1.2)) as lancedb.VectorQuery
    )
      .distanceType("cosine")
      .limit(10)
      .toArray();
    // --8<-- [end:search2]
    expect(results2.length).toBe(10);

    // --8<-- [start:distance_range]
    const results3 = await (
      tbl.search(Array(128).fill(1.2)) as lancedb.VectorQuery
    )
      .distanceType("cosine")
      .distanceRange(0.1, 0.2)
      .limit(10)
      .toArray();
    // --8<-- [end:distance_range]
    for (const r of results3) {
      expect(r.distance).toBeGreaterThanOrEqual(0.1);
      expect(r.distance).toBeLessThan(0.2);
    }

    {
      // --8<-- [start:ingest_binary_data]
      const schema = new Schema([
        new Field("id", new Int32(), true),
        new Field("vec", new FixedSizeList(32, new Field("item", new Uint8()))),
      ]);
      const data = lancedb.makeArrowTable(
        Array(1_000)
          .fill(0)
          .map((_, i) => ({
            // the 256 bits would be store in 32 bytes,
            // if your data is already in this format, you can skip the packBits step
            id: i,
            vec: lancedb.packBits(Array(256).fill(i % 2)),
          })),
        { schema: schema },
      );

      const tbl = await db.createTable("binary_table", data);
      await tbl.createIndex("vec", {
        config: lancedb.Index.ivfFlat({
          numPartitions: 10,
          distanceType: "hamming",
        }),
      });
      // --8<-- [end:ingest_binary_data]

      // --8<-- [start:search_binary_data]
      const query = Array(32)
        .fill(1)
        .map(() => Math.floor(Math.random() * 255));
      const results = await tbl.query().nearestTo(query).limit(10).toArrow();
      // --8<-- [end:search_binary_data]
      expect(results.numRows).toBe(10);
    }
  });
});

test("vector search docs snippets", async () => {
  await withTempDirectory(async (databaseDir) => {
    const db = await lancedb.connect(databaseDir);

    // ---- Nested vector column: inference and explicit selection ----
    {
      const nestedSchema = new Schema([
        new Field("id", new Int32()),
        new Field(
          "image",
          new Struct([
            new Field(
              "embedding",
              new FixedSizeList(2, new Field("item", new Float32(), true)),
            ),
          ]),
        ),
      ]);
      const arrowTbl = lancedb.makeArrowTable(
        [{ id: 0, image: { embedding: [0.0, 1.0] } }],
        { schema: nestedSchema },
      );
      await db.createTable("nested", arrowTbl, { mode: "overwrite" });

      // --8<-- [start:select_vector_column]
      const table = await db.openTable("nested");

      // Inferred: LanceDB finds the single nested vector leaf automatically.
      await table.query().nearestTo([0.0, 1.0]).limit(1).toArray();

      // Explicit: required when more than one vector column matches.
      await table
        .query()
        .nearestTo([0.0, 1.0])
        .column("image.embedding")
        .limit(1)
        .toArray();
      // --8<-- [end:select_vector_column]
    }

    // ---- Index a nested vector column ----
    {
      const dim = 16;
      const nestedSchema = new Schema([
        new Field("id", new Int32()),
        new Field(
          "image",
          new Struct([
            new Field(
              "embedding",
              new FixedSizeList(dim, new Field("item", new Float32(), true)),
            ),
          ]),
        ),
      ]);
      const rows = Array.from({ length: 512 }, (_, i) => ({
        id: i,
        image: { embedding: Array.from({ length: dim }, () => Math.random()) },
      }));
      const arrowTbl = lancedb.makeArrowTable(rows, { schema: nestedSchema });
      const table = await db.createTable("nested_index", arrowTbl, {
        mode: "overwrite",
      });

      // --8<-- [start:index_nested_column]
      await table.createIndex("image.embedding");
      // --8<-- [end:index_nested_column]
    }

    // ---- Indexed queries: refine, fast search, bypass index ----
    {
      const table = await buildIndexedTable(db, "ann_table");
      const embedding = Array.from({ length: 128 }, () => Math.random());

      // --8<-- [start:exact_vs_approximate]
      // Indexed ANN search without refinement (fast, approximate `_distance`)
      const fastResults = await (table.search(embedding) as lancedb.VectorQuery)
        .limit(10)
        .toArray();

      // Recompute distances on full vectors for reranked candidates
      const exactDistanceResults = await (
        table.search(embedding) as lancedb.VectorQuery
      )
        .limit(10)
        .refineFactor(1)
        .toArray();

      // Rerank a larger candidate set for better recall (higher latency)
      const higherRecallResults = await (
        table.search(embedding) as lancedb.VectorQuery
      )
        .limit(10)
        .refineFactor(20)
        .toArray();
      // --8<-- [end:exact_vs_approximate]
      expect(fastResults.length).toBe(10);
      expect(exactDistanceResults.length).toBe(10);
      expect(higherRecallResults.length).toBe(10);

      // --8<-- [start:fast_search]
      await table
        .query()
        .nearestTo(embedding)
        .fastSearch()
        .limit(5)
        .toArray();
      // --8<-- [end:fast_search]

      // --8<-- [start:bypass_vector_index]
      await table
        .query()
        .nearestTo(embedding)
        .bypassVectorIndex()
        .limit(5)
        .toArray();
      // --8<-- [end:bypass_vector_index]
    }

    // ---- Brute force search (no index) ----
    {
      const data = Array.from({ length: 64 }, (_, i) => ({
        vector: Array(128).fill(i),
        id: `${i}`,
      }));
      await db.createTable("my_vectors", data, { mode: "overwrite" });

      // --8<-- [start:brute_force_search]
      const tbl = await db.openTable("my_vectors");

      const results1 = await tbl.search(Array(128).fill(1.2)).limit(3).toArray();
      // --8<-- [end:brute_force_search]
      expect(results1.length).toBe(3);
    }

    // ---- Binary (hamming) vector search ----
    {
      const schema = new Schema([
        new Field("id", new Int32(), true),
        new Field("vector", new FixedSizeList(32, new Field("item", new Uint8()))),
      ]);
      const data = lancedb.makeArrowTable(
        Array(1000)
          .fill(0)
          .map((_, i) => ({
            // the 256 bits are stored in 32 bytes; if your data is already in
            // this format, you can skip the packBits step
            id: i,
            vector: lancedb.packBits(Array(256).fill(i % 2)),
          })),
        { schema },
      );

      // --8<-- [start:binary_search]
      const tbl = await db.createTable("binary_vectors", data, {
        mode: "overwrite",
      });
      await tbl.createIndex("vector", {
        config: lancedb.Index.ivfFlat({
          numPartitions: 10,
          distanceType: "hamming",
        }),
      });

      const query = Array(32)
        .fill(1)
        .map(() => Math.floor(Math.random() * 255));
      const results = await tbl.query().nearestTo(query).limit(10).toArray();
      // --8<-- [end:binary_search]
      expect(results.length).toBeLessThanOrEqual(10);
    }

    // ---- Enterprise-style prefilter / postfilter / batch search ----
    {
      const dimensions = 768;
      const rows = Array.from({ length: 50 }, (_, i) => ({
        vector: Array.from({ length: dimensions }, () => Math.random() * 2 - 1),
        text: `story ${i}`,
        keywords: `kw${i}`,
        label: i % 4,
      }));
      await db.createTable("lancedb-enterprise-quickstart", rows, {
        mode: "overwrite",
      });

      {
        // --8<-- [start:vector_search_prefilter]
        // Generate a sample 768-dimension embedding vector (typical for BERT-based models)
        // In real applications, you would get this from an embedding model
        const dimensions = 768;
        const queryEmbed = Array.from(
          { length: dimensions },
          () => Math.random() * 2 - 1,
        );

        // Open table and perform search
        const tableName = "lancedb-enterprise-quickstart";
        const table = await db.openTable(tableName);

        // Vector search with filters (pre-filtering is the default)
        const vectorResults = await table
          .search(queryEmbed)
          .where("label > 2")
          .select(["text", "keywords", "label"])
          .limit(5)
          .toArray();

        console.log("Search results (with pre-filtering):");
        console.log(vectorResults);
        // --8<-- [end:vector_search_prefilter]

        // --8<-- [start:vector_search_postfilter]
        const vectorResultsWithPostFilter = await (
          table.search(queryEmbed) as lancedb.VectorQuery
        )
          .where("label > 2")
          .postfilter()
          .select(["text", "keywords", "label"])
          .limit(5)
          .toArray();

        console.log("Vector search results with post-filter:");
        console.log(vectorResultsWithPostFilter);
        // --8<-- [end:vector_search_postfilter]

        // --8<-- [start:batch_search]
        // Batch query
        console.log("Performing batch vector search...");
        const batchSize = 5;
        const queryVectors = Array.from({ length: batchSize }, () =>
          Array.from({ length: dimensions }, () => Math.random() * 2 - 1),
        );
        let batchQuery = table.search(queryVectors[0]) as lancedb.VectorQuery;
        for (let i = 1; i < batchSize; i++) {
          batchQuery = batchQuery.addQueryVector(queryVectors[i]);
        }
        const batchResults = await batchQuery
          .select(["text", "keywords", "label"])
          .limit(5)
          .toArray();
        console.log("Batch vector search results:");
        console.log(batchResults);
        // --8<-- [end:batch_search]
        expect(batchResults.length).toBeGreaterThan(0);
      }
    }
  });
});
