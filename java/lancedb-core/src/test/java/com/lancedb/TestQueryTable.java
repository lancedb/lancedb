/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.lancedb;

import org.lance.namespace.model.QueryTableRequest;

import com.google.common.collect.Lists;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** Tests for query operations including vector search, filters, and various query options. */
public class TestQueryTable extends LanceDbRestNamespaceTestBase {
  private static final Logger log = LoggerFactory.getLogger(TestQueryTable.class);

  @Test
  public void testBasicVectorQuery() throws IOException {
    skipIfNotConfigured();

    log.info("=== Test: Basic Vector Query ===");
    String tableName = Utils.generateTableName("test_vector_query");

    try {
      // Create table with 10 rows
      log.info("--- Creating table {} with 10 rows ---", tableName);
      Utils.createTable(namespace, allocator, tableName, 10);

      // Create vector query
      QueryTableRequest QueryTableRequest = Utils.createVectorQuery(tableName, 5, 128);

      QueryTableRequest.setK(5);

      byte[] queryResult = namespace.queryTable(QueryTableRequest);
      assertNotNull(queryResult, "Query result should not be null");

      // Verify results
      try (BufferAllocator verifyAllocator = new RootAllocator()) {
        Utils.readArrowFile(
            queryResult,
            verifyAllocator,
            root -> {
              Schema resultSchema = root.getSchema();
              List<String> resultColumns = new ArrayList<>();
              for (Field field : resultSchema.getFields()) {
                resultColumns.add(field.getName());
              }
              log.debug("Result columns: {}", resultColumns);

              // Verify columns
              assertTrue(resultColumns.contains("id"), "Result should contain 'id' column");
              assertTrue(resultColumns.contains("name"), "Result should contain 'name' column");
            });

        int totalRows = Utils.countRows(queryResult, verifyAllocator);
        log.info("Query returned {} rows", totalRows);
        assertTrue(totalRows == 5, "Query should return exactly 5 rows");
      }

    } finally {
      Utils.dropTable(namespace, tableName);
    }
  }

  @Test
  public void testQueryWithFilter() throws IOException {
    skipIfNotConfigured();

    log.info("=== Test: Query with Filter ===");
    String tableName = Utils.generateTableName("test_query_filter");

    try {
      // Create table with 100 rows for better filter testing
      log.info("--- Creating table {} with 100 rows ---", tableName);
      Utils.createTable(namespace, allocator, tableName, 100);

      // Test 1: Filter-only query (no vector)
      log.info("--- Test 1: Filter-only query ---");
      QueryTableRequest filterQuery = new QueryTableRequest();
      filterQuery.setId(Lists.newArrayList(tableName));
      filterQuery.setK(10);
      filterQuery.setFilter("id > 50");
      filterQuery.setColumns(Arrays.asList("id", "name", "embedding"));

      byte[] filterResult = namespace.queryTable(filterQuery);
      assertNotNull(filterResult, "Filter query result should not be null");

      int rowCount = Utils.countRows(filterResult, allocator);
      assertEquals(10, rowCount, "Filter query should return exactly 10 rows");
      log.info("Filter-only query returned {} rows", rowCount);

      // Test 2: Vector query with filter
      log.info("--- Test 2: Vector query with filter ---");
      QueryTableRequest vectorFilterQuery = Utils.createVectorQuery(tableName, 5, 128);
      vectorFilterQuery.setFilter("id < 20");

      byte[] vectorFilterResult = namespace.queryTable(vectorFilterQuery);
      assertNotNull(vectorFilterResult, "Vector filter query result should not be null");

      List<Integer> ids = Utils.extractColumn(vectorFilterResult, allocator, "id", Integer.class);
      assertTrue(ids.stream().allMatch(id -> id < 20), "All IDs should be less than 20");
      log.info("Vector query with filter returned IDs: {}", ids);

    } finally {
      Utils.dropTable(namespace, tableName);
    }
  }

  @Test
  public void testQueryWithPrefilter() throws IOException {
    skipIfNotConfigured();

    log.info("=== Test: Query with Prefilter ===");
    String tableName = Utils.generateTableName("test_prefilter");

    try {
      // Create table
      log.info("--- Creating table {} with 50 rows ---", tableName);
      Utils.createTable(namespace, allocator, tableName, 50);

      // Test prefilter = true
      log.info("--- Testing prefilter = true ---");
      QueryTableRequest prefilterQuery = new QueryTableRequest();
      prefilterQuery.setId(Lists.newArrayList(tableName));
      prefilterQuery.setK(5);
      prefilterQuery.setPrefilter(true);
      prefilterQuery.setFilter("id < 20");
      prefilterQuery.setColumns(Arrays.asList("id", "name"));

      byte[] prefilterResult = namespace.queryTable(prefilterQuery);
      assertNotNull(prefilterResult, "Prefilter query result should not be null");

      List<Integer> ids = Utils.extractColumn(prefilterResult, allocator, "id", Integer.class);
      assertTrue(
          ids.stream().allMatch(id -> id < 20), "All IDs should be less than 20 with prefilter");
      log.info("Prefilter query returned {} rows", ids.size());

      // Test prefilter = false (postfilter)
      log.info("--- Testing prefilter = false (postfilter) ---");
      QueryTableRequest postfilterQuery = Utils.createVectorQuery(tableName, 10, 128);
      postfilterQuery.setPrefilter(false);
      postfilterQuery.setFilter("id % 2 = 0"); // Even IDs only

      byte[] postfilterResult = namespace.queryTable(postfilterQuery);
      assertNotNull(postfilterResult, "Postfilter query result should not be null");

      List<Integer> evenIds = Utils.extractColumn(postfilterResult, allocator, "id", Integer.class);
      assertTrue(
          evenIds.stream().allMatch(id -> id % 2 == 0), "All IDs should be even with postfilter");
      log.info("Postfilter query returned {} even IDs", evenIds.size());

    } finally {
      Utils.dropTable(namespace, tableName);
    }
  }

  @Test
  public void testQueryWithFastSearch() throws IOException {
    skipIfNotConfigured();

    log.info("=== Test: Query with Fast Search ===");
    String tableName = Utils.generateTableName("test_fast_search");

    try {
      // Create table
      log.info("--- Creating table {} with 100 rows ---", tableName);
      Utils.createTable(namespace, allocator, tableName, 100);

      // Test fast_search = true
      log.info("--- Testing fast_search = true ---");
      QueryTableRequest fastSearchQuery = Utils.createVectorQuery(tableName, 10, 128);
      fastSearchQuery.setFastSearch(true);

      byte[] fastSearchResult = namespace.queryTable(fastSearchQuery);
      assertNotNull(fastSearchResult, "Fast search query result should not be null");

      int fastSearchRows = Utils.countRows(fastSearchResult, allocator);
      assertEquals(10, fastSearchRows, "Fast search should return requested k rows");
      log.info("Fast search returned {} rows", fastSearchRows);

      // Test fast_search = false
      log.info("--- Testing fast_search = false ---");
      QueryTableRequest noFastSearchQuery = Utils.createVectorQuery(tableName, 10, 128);
      noFastSearchQuery.setFastSearch(false);

      byte[] noFastSearchResult = namespace.queryTable(noFastSearchQuery);
      assertNotNull(noFastSearchResult, "No fast search query result should not be null");

      int noFastSearchRows = Utils.countRows(noFastSearchResult, allocator);
      assertEquals(10, noFastSearchRows, "No fast search should also return requested k rows");
      log.info("No fast search returned {} rows", noFastSearchRows);

    } finally {
      Utils.dropTable(namespace, tableName);
    }
  }

  @Test
  public void testQueryWithColumnSelection() throws IOException {
    skipIfNotConfigured();

    log.info("=== Test: Query with Column Selection ===");
    String tableName = Utils.generateTableName("test_column_selection");

    try {
      // Create table
      log.info("--- Creating table {} with 20 rows ---", tableName);
      Utils.createTable(namespace, allocator, tableName, 20);

      // Query with specific columns
      QueryTableRequest columnQuery = new QueryTableRequest();
      columnQuery.setId(Lists.newArrayList(tableName));
      columnQuery.setK(5);
      columnQuery.setColumns(Arrays.asList("id", "name")); // Don't include embedding

      byte[] result = namespace.queryTable(columnQuery);
      assertNotNull(result, "Column selection query result should not be null");

      // Verify only requested columns are returned
      try (BufferAllocator verifyAllocator = new RootAllocator()) {
        Utils.readArrowFile(
            result,
            verifyAllocator,
            root -> {
              Schema schema = root.getSchema();
              List<String> fieldNames = new ArrayList<>();
              for (Field field : schema.getFields()) {
                fieldNames.add(field.getName());
              }

              assertTrue(fieldNames.contains("id"), "Result should contain 'id' column");
              assertTrue(fieldNames.contains("name"), "Result should contain 'name' column");
              assertFalse(
                  fieldNames.contains("embedding"), "Result should NOT contain 'embedding' column");

              log.info("Query returned only requested columns: {}", fieldNames);
            });
      }

    } finally {
      Utils.dropTable(namespace, tableName);
    }
  }
}
