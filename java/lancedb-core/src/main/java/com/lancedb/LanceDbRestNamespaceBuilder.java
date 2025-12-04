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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Util class to help construct a {@link RestNamespace} for LanceDB.
 *
 * <p>For LanceDB Cloud, use the simplified builder API:
 *
 * <pre>{@code
 * import org.lance.namespace.RestNamespace;
 *
 * // If your DB url is db://example-db, then your database here is example-db
 * RestNamespace namespace = LanceDbRestNamespaceBuilder.newBuilder()
 *     .apiKey("your_lancedb_cloud_api_key")
 *     .database("your_database_name")
 *     .build();
 * }</pre>
 *
 * <p>For LanceDB Enterprise deployments, use your VPC endpoint:
 *
 * <pre>{@code
 * RestNamespace namespace = LanceDbRestNamespaceBuilder.newBuilder()
 *     .apiKey("your_lancedb_enterprise_api_key")
 *     .database("your_database_name")
 *     .endpoint("http://<vpc_endpoint_dns_name>:80")
 *     .build();
 * }</pre>
 */
public class LanceDbRestNamespaceBuilder {
  private static final String DEFAULT_REGION = "us-east-1";
  private static final String CLOUD_URL_PATTERN = "https://%s.%s.api.lancedb.com";

  private String apiKey;
  private String database;
  private Optional<String> endpoint = Optional.empty();
  private Optional<String> region = Optional.empty();
  private Map<String, String> additionalConfig = new HashMap<>();

  private LanceDbRestNamespaceBuilder() {}

  /**
   * Create a new builder instance.
   *
   * @return A new RestNamespaceBuilder
   */
  public static LanceDbRestNamespaceBuilder newBuilder() {
    return new LanceDbRestNamespaceBuilder();
  }

  /**
   * Set the API key (required).
   *
   * @param apiKey The LanceDB API key
   * @return This builder
   */
  public LanceDbRestNamespaceBuilder apiKey(String apiKey) {
    if (apiKey == null || apiKey.trim().isEmpty()) {
      throw new IllegalArgumentException("API key cannot be null or empty");
    }
    this.apiKey = apiKey;
    return this;
  }

  /**
   * Set the database name (required).
   *
   * @param database The database name
   * @return This builder
   */
  public LanceDbRestNamespaceBuilder database(String database) {
    if (database == null || database.trim().isEmpty()) {
      throw new IllegalArgumentException("Database cannot be null or empty");
    }
    this.database = database;
    return this;
  }

  /**
   * Set a custom endpoint URL (optional). When set, this overrides the default LanceDB Cloud URL
   * construction. Use this for LanceDB Enterprise deployments.
   *
   * @param endpoint The complete base URL (e.g., "http://your-vpc-endpoint:80")
   * @return This builder
   */
  public LanceDbRestNamespaceBuilder endpoint(String endpoint) {
    this.endpoint = Optional.ofNullable(endpoint);
    return this;
  }

  /**
   * Set the region for LanceDB Cloud (optional). Defaults to "us-east-1" if not specified. This is
   * ignored when endpoint is set.
   *
   * @param region The AWS region (e.g., "us-east-1", "eu-west-1")
   * @return This builder
   */
  public LanceDbRestNamespaceBuilder region(String region) {
    this.region = Optional.ofNullable(region);
    return this;
  }

  /**
   * Add additional configuration parameters.
   *
   * @param key The configuration key
   * @param value The configuration value
   * @return This builder
   */
  public LanceDbRestNamespaceBuilder config(String key, String value) {
    this.additionalConfig.put(key, value);
    return this;
  }

  /**
   * Build the Lance RestNamespace instance.
   *
   * @return A configured Lance RestNamespace
   * @throws IllegalStateException if required parameters are missing
   */
  public RestNamespace build() {
    // Validate required fields
    if (apiKey == null) {
      throw new IllegalStateException("API key is required");
    }
    if (database == null) {
      throw new IllegalStateException("Database is required");
    }

    // Build configuration map
    Map<String, String> config = new HashMap<>(additionalConfig);
    config.put("header.x-lancedb-database", database);
    config.put("header.x-api-key", apiKey);

    // Determine base URL
    String uri;
    if (endpoint.isPresent()) {
      uri = endpoint.get();
    } else {
      String effectiveRegion = region.orElse(DEFAULT_REGION);
      uri = String.format(CLOUD_URL_PATTERN, database, effectiveRegion);
    }
    config.put("uri", uri);
    RestNamespace ns = new RestNamespace();
    ns.initialize(config, null);
    return ns;
  }
}
