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

import org.lance.namespace.RestNamespace;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Base test class for Lance Namespace tests. Provides common setup and configuration for all test
 * classes.
 */
public abstract class LanceDbRestNamespaceTestBase {
  private static final Logger log = LoggerFactory.getLogger(LanceDbRestNamespaceTestBase.class);

  // Configuration from environment variables
  protected static String DATABASE;
  protected static String API_KEY;
  protected static String ENDPOINT;
  protected static String REGION;

  protected RestNamespace namespace;
  protected BufferAllocator allocator;

  @BeforeAll
  public static void setUpClass() {
    // Get configuration from environment variables
    DATABASE = System.getenv("LANCEDB_DB");
    API_KEY = System.getenv("LANCEDB_API_KEY");
    ENDPOINT = System.getenv("LANCEDB_ENDPOINT");
    REGION = System.getenv("LANCEDB_REGION");

    // Default values if not set
    if (isNullOrEmpty(REGION)) {
      REGION = "us-east-1";
    }

    if (DATABASE != null && API_KEY != null) {
      log.info("Using configuration:");
      log.info("  Database: {}", DATABASE);
      log.info("  Region: {}", REGION);
      log.info("  Endpoint: {}", isNullOrEmpty(ENDPOINT) ? "none" : ENDPOINT);
    }
  }

  @BeforeEach
  public void setUp() {
    // Only initialize if required environment variables are set
    if (!isNullOrEmpty(DATABASE) && !isNullOrEmpty(API_KEY)) {
      namespace = initializeClient();
      allocator = new RootAllocator();
    }
  }

  @AfterEach
  public void tearDown() {
    if (allocator != null) {
      allocator.close();
    }
  }

  /**
   * Skip test if environment variables are not set. Call this at the beginning of each test method.
   */
  protected void skipIfNotConfigured() {
    assumeTrue(
        !isNullOrEmpty(DATABASE) && !isNullOrEmpty(API_KEY),
        "Skipping test: LANCEDB_DB and LANCEDB_API_KEY environment variables must be set");
  }

  /**
   * Initialize the REST client using the simplified builder API.
   *
   * @return Configured Lance RestNamespace instance
   */
  private RestNamespace initializeClient() {
    LanceDbRestNamespaceBuilder builder =
        LanceDbRestNamespaceBuilder.newBuilder().apiKey(API_KEY).database(DATABASE);

    if (!isNullOrEmpty(ENDPOINT)) {
      builder.endpoint(ENDPOINT);
    }

    if (!isNullOrEmpty(REGION)) {
      builder.region(REGION);
    }

    RestNamespace namespace = builder.build();

    // Log the configuration for debugging
    String baseUrl =
        !isNullOrEmpty(ENDPOINT)
            ? ENDPOINT
            : String.format("https://%s.%s.api.lancedb.com", DATABASE, REGION);
    log.info("Initialized client with base URL: {}", baseUrl);

    return namespace;
  }

  private static boolean isNullOrEmpty(String s) {
    return s == null || s.isEmpty();
  }
}
