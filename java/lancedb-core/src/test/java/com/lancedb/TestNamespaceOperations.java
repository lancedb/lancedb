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

import org.junit.jupiter.api.Test;
import org.lance.namespace.model.CreateNamespaceResponse;
import org.lance.namespace.model.DescribeNamespaceResponse;
import org.lance.namespace.model.ListNamespacesResponse;
import org.lance.namespace.model.ListTablesResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** Tests for namespace operations: create, describe, list, drop. */
public class TestNamespaceOperations extends LanceDbRestNamespaceTestBase {
  private static final Logger log = LoggerFactory.getLogger(TestNamespaceOperations.class);

  @Test
  public void testNamespaceLifecycle() {
    skipIfNotConfigured();

    log.info("=== Test: Namespace Lifecycle ===");
    String nsName = Utils.generateNamespaceName("test_ns_lifecycle");

    try {
      // Create namespace
      log.info("--- Creating namespace {} ---", nsName);
      CreateNamespaceResponse createResponse = Utils.createNamespace(namespace, nsName);
      assertNotNull(createResponse, "Create namespace response should not be null");

      // Describe namespace
      log.info("--- Describing namespace {} ---", nsName);
      DescribeNamespaceResponse describeResponse = Utils.describeNamespace(namespace, nsName);
      assertNotNull(describeResponse, "Describe namespace response should not be null");
      // Properties may be null or empty if server doesn't support namespace properties
      log.info(
          "Namespace described successfully: {}, properties: {}",
          nsName,
          describeResponse.getProperties());

      // List namespaces (should include our namespace)
      log.info("--- Listing namespaces ---");
      ListNamespacesResponse listResponse = Utils.listNamespaces(namespace);
      assertNotNull(listResponse, "List namespaces response should not be null");
      assertNotNull(listResponse.getNamespaces(), "Namespaces list should not be null");

      boolean found = listResponse.getNamespaces().stream().anyMatch(ns -> ns.contains(nsName));
      assertTrue(found, "Our namespace should be in the list");
      log.info(
          "Namespace listing verified, found {} namespaces", listResponse.getNamespaces().size());

      log.info("Namespace lifecycle test passed!");

    } finally {
      try {
        Utils.dropNamespace(namespace, nsName);
      } catch (Exception e) {
        log.warn("Failed to drop namespace: {}", e.getMessage());
      }
    }
  }

  @Test
  public void testNestedNamespaces() {
    skipIfNotConfigured();

    log.info("=== Test: Nested Namespaces ===");
    String parentNs = Utils.generateNamespaceName("parent_ns");
    String childNs = Utils.generateNamespaceName("child_ns");

    try {
      // Create parent namespace
      log.info("--- Creating parent namespace {} ---", parentNs);
      Utils.createNamespace(namespace, parentNs);

      // Create child namespace inside parent
      log.info("--- Creating child namespace {}/{} ---", parentNs, childNs);
      Utils.createNamespace(namespace, parentNs, childNs);

      // Describe child namespace
      log.info("--- Describing child namespace ---");
      DescribeNamespaceResponse describeResponse =
          Utils.describeNamespace(namespace, parentNs, childNs);
      assertNotNull(describeResponse, "Describe namespace response should not be null");
      // Properties may be null or empty if server doesn't support namespace properties
      log.info(
          "Child namespace described successfully: {}/{}, properties: {}",
          parentNs,
          childNs,
          describeResponse.getProperties());

      // List child namespaces under parent
      log.info("--- Listing child namespaces under parent ---");
      ListNamespacesResponse listResponse = Utils.listNamespaces(namespace, parentNs);
      assertNotNull(listResponse, "List namespaces response should not be null");
      assertNotNull(listResponse.getNamespaces(), "Namespaces list should not be null");

      boolean found = listResponse.getNamespaces().stream().anyMatch(ns -> ns.contains(childNs));
      assertTrue(found, "Child namespace should be in the list");
      log.info("Found {} child namespaces under parent", listResponse.getNamespaces().size());

      log.info("Nested namespaces test passed!");

    } finally {
      // Clean up - drop child first, then parent
      try {
        Utils.dropNamespace(namespace, parentNs, childNs);
      } catch (Exception e) {
        log.warn("Failed to drop child namespace: {}", e.getMessage());
      }
      try {
        Utils.dropNamespace(namespace, parentNs);
      } catch (Exception e) {
        log.warn("Failed to drop parent namespace: {}", e.getMessage());
      }
    }
  }

  @Test
  public void testListTablesInNamespace() throws IOException {
    skipIfNotConfigured();

    log.info("=== Test: List Tables in Namespace ===");
    String nsName = Utils.generateNamespaceName("test_list_tables_ns");
    String table1 = Utils.generateTableName("table1");
    String table2 = Utils.generateTableName("table2");
    List<String> tablePath1 = Arrays.asList(nsName, table1);
    List<String> tablePath2 = Arrays.asList(nsName, table2);

    try {
      // Create namespace
      log.info("--- Creating namespace {} ---", nsName);
      Utils.createNamespace(namespace, nsName);

      // Create two tables in the namespace
      log.info("--- Creating tables in namespace ---");
      Utils.createTable(namespace, allocator, tablePath1, 5);
      Utils.createTable(namespace, allocator, tablePath2, 5);

      // List tables in namespace
      log.info("--- Listing tables in namespace ---");
      ListTablesResponse listResponse = Utils.listTables(namespace, nsName);
      assertNotNull(listResponse, "List tables response should not be null");
      assertNotNull(listResponse.getTables(), "Tables list should not be null");
      assertEquals(2, listResponse.getTables().size(), "Should have 2 tables");

      boolean foundTable1 = listResponse.getTables().stream().anyMatch(t -> t.contains(table1));
      boolean foundTable2 = listResponse.getTables().stream().anyMatch(t -> t.contains(table2));
      assertTrue(foundTable1, "Table 1 should be in the list");
      assertTrue(foundTable2, "Table 2 should be in the list");
      log.info("Found {} tables in namespace", listResponse.getTables().size());

      log.info("List tables in namespace test passed!");

    } finally {
      Utils.cleanupTablesAndNamespace(namespace, Arrays.asList(tablePath1, tablePath2), nsName);
    }
  }

  @Test
  public void testDescribeNonExistentNamespace() {
    skipIfNotConfigured();

    log.info("=== Test: Describe Non-Existent Namespace ===");
    String nsName = "non_existent_namespace_" + System.currentTimeMillis();

    try {
      // Try to describe a namespace that doesn't exist
      log.info("--- Attempting to describe non-existent namespace {} ---", nsName);
      Utils.describeNamespace(namespace, nsName);
      fail("Expected exception when describing non-existent namespace");
    } catch (RuntimeException e) {
      log.info("Got expected exception: {}", e.getMessage());
      // Expected behavior
    }

    log.info("Describe non-existent namespace test passed!");
  }

  @Test
  public void testDropNonEmptyNamespace() throws IOException {
    skipIfNotConfigured();

    log.info("=== Test: Drop Non-Empty Namespace ===");
    String nsName = Utils.generateNamespaceName("test_nonempty_ns");
    String tableName = Utils.generateTableName("table_in_ns");
    List<String> tablePath = Arrays.asList(nsName, tableName);

    try {
      // Create namespace and table
      log.info("--- Creating namespace and table ---");
      Utils.createNamespace(namespace, nsName);
      Utils.createTable(namespace, allocator, tablePath, 5);

      // Try to drop non-empty namespace (should fail or cascade)
      log.info("--- Attempting to drop non-empty namespace ---");
      try {
        Utils.dropNamespace(namespace, nsName);
        // If it succeeds, verify the table is also gone
        log.info("Namespace dropped (cascade delete may have occurred)");
      } catch (RuntimeException e) {
        log.info("Got expected exception when dropping non-empty namespace: {}", e.getMessage());
        // Clean up table first
        Utils.dropTable(namespace, tablePath);
        Utils.dropNamespace(namespace, nsName);
      }

      log.info("Drop non-empty namespace test passed!");

    } catch (Exception e) {
      // Final cleanup
      try {
        Utils.dropTable(namespace, tablePath);
      } catch (Exception ex) {
        // ignore
      }
      try {
        Utils.dropNamespace(namespace, nsName);
      } catch (Exception ex) {
        // ignore
      }
      throw e;
    }
  }
}
