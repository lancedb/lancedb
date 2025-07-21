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
package com.lancedb.lancedb;

import com.lancedb.lance.namespace.LanceRestNamespace;
import com.lancedb.lance.namespace.client.apache.ApiClient;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** Util class to help construct a {@link LanceRestNamespace} for LanceDB. */
public class LanceDbRestNamespaces {
  private static final String DEFAULT_REGION = "us-east-1";
  private static final String CLOUD_URL_PATTERN = "https://%s.%s.api.lancedb.com";

  private String apiKey;
  private String database;
  private Optional<String> hostOverride = Optional.empty();
  private Optional<String> region = Optional.empty();
  private Map<String, String> additionalConfig = new HashMap<>();

  private LanceDbRestNamespaces() {}

  /**
   * Create a new builder instance.
   *
   * @return A new LanceRestNamespaceBuilder
   */
  public static LanceDbRestNamespaces builder() {
    return new LanceDbRestNamespaces();
  }

  /**
   * Set the API key (required).
   *
   * @param apiKey The LanceDB API key
   * @return This builder
   */
  public LanceDbRestNamespaces apiKey(String apiKey) {
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
  public LanceDbRestNamespaces database(String database) {
    if (database == null || database.trim().isEmpty()) {
      throw new IllegalArgumentException("Database cannot be null or empty");
    }
    this.database = database;
    return this;
  }

  /**
   * Set a custom host override (optional). When set, this overrides the default LanceDB Cloud URL
   * construction. Use this for LanceDB Enterprise deployments.
   *
   * @param hostOverride The complete base URL (e.g., "http://your-vpc-endpoint:80")
   * @return This builder
   */
  public LanceDbRestNamespaces hostOverride(String hostOverride) {
    this.hostOverride = Optional.ofNullable(hostOverride);
    return this;
  }

  /**
   * Set the region for LanceDB Cloud (optional). Defaults to "us-east-1" if not specified. This is
   * ignored when hostOverride is set.
   *
   * @param region The AWS region (e.g., "us-east-1", "eu-west-1")
   * @return This builder
   */
  public LanceDbRestNamespaces region(String region) {
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
  public LanceDbRestNamespaces config(String key, String value) {
    this.additionalConfig.put(key, value);
    return this;
  }

  /**
   * Build the LanceRestNamespace instance.
   *
   * @return A configured LanceRestNamespace
   * @throws IllegalStateException if required parameters are missing
   */
  public LanceRestNamespace build() {
    // Validate required fields
    if (apiKey == null) {
      throw new IllegalStateException("API key is required");
    }
    if (database == null) {
      throw new IllegalStateException("Database is required");
    }

    // Build configuration map
    Map<String, String> config = new HashMap<>(additionalConfig);
    config.put("headers.x-lancedb-database", database);
    config.put("headers.x-api-key", apiKey);

    // Determine base URL
    String baseUrl;
    if (hostOverride.isPresent()) {
      baseUrl = hostOverride.get();
      config.put("host_override", hostOverride.get());
    } else {
      String effectiveRegion = region.orElse(DEFAULT_REGION);
      baseUrl = String.format(CLOUD_URL_PATTERN, database, effectiveRegion);
      config.put("region", effectiveRegion);
    }

    // Create and configure ApiClient
    ApiClient apiClient = new ApiClient();
    apiClient.setBasePath(baseUrl);

    return new LanceRestNamespace(apiClient, config);
  }
}
