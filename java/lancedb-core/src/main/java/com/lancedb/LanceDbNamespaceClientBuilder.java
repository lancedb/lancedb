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

import org.lance.namespace.LanceNamespace;
import org.lance.namespace.RestNamespace;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Util class to help construct a {@link LanceNamespace} for LanceDB.
 *
 * <p>For LanceDB Cloud (legacy hosted service), use the simplified builder API:
 *
 * <pre>{@code
 * import org.lance.namespace.LanceNamespace;
 *
 * // If your DB url is db://example-db, then your database here is example-db
 * LanceNamespace namespaceClient = LanceDbNamespaceClientBuilder.newBuilder()
 *     .apiKey("your_lancedb_cloud_api_key")
 *     .database("your_database_name")
 *     .build();
 * }</pre>
 *
 * <p>For LanceDB Enterprise deployments, use your custom endpoint:
 *
 * <pre>{@code
 * LanceNamespace namespaceClient = LanceDbNamespaceClientBuilder.newBuilder()
 *     .apiKey("your_lancedb_enterprise_api_key")
 *     .database("your_database_name")
 *     .endpoint("<your_enterprise_endpoint>")
 *     .build();
 * }</pre>
 */
public class LanceDbNamespaceClientBuilder {
  private static final String DEFAULT_REGION = "us-east-1";
  private static final String CLOUD_URL_PATTERN = "https://%s.%s.api.lancedb.com";

  private String apiKey;
  private String database;
  private Optional<String> endpoint = Optional.empty();
  private Optional<String> region = Optional.empty();
  private Map<String, String> additionalConfig = new HashMap<>();

  private LanceDbNamespaceClientBuilder() {}

  /**
   * Create a new builder instance.
   *
   * @return A new LanceDbNamespaceClientBuilder
   */
  public static LanceDbNamespaceClientBuilder newBuilder() {
    return new LanceDbNamespaceClientBuilder();
  }

  /**
   * Set the API key (required).
   *
   * @param apiKey The LanceDB API key
   * @return This builder
   */
  public LanceDbNamespaceClientBuilder apiKey(String apiKey) {
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
  public LanceDbNamespaceClientBuilder database(String database) {
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
   * @param endpoint The complete base URL for your LanceDB Enterprise deployment
   * @return This builder
   */
  public LanceDbNamespaceClientBuilder endpoint(String endpoint) {
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
  public LanceDbNamespaceClientBuilder region(String region) {
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
  public LanceDbNamespaceClientBuilder config(String key, String value) {
    this.additionalConfig.put(key, value);
    return this;
  }

  /**
   * Build the LanceNamespace instance.
   *
   * <p>Retries apply to calls made on the returned namespace client. They do not apply to
   * credential refresh or namespace commits that {@code Dataset.open} makes through the native
   * handle. Namespace error code 21 (throttling, what Lance maps from HTTP 429 with a namespace
   * error body) is retried on every such call, as is a pre-execution HTTP 429 whose body is not a
   * namespace error. Error code 17 (service unavailable, including HTTP 503, connect failures, and
   * timeouts) is retried on reads only. A 503 without a namespace error body, and any other
   * internal error, surfaces as {@link org.lance.namespace.errors.InternalException} and is not
   * retried. Other 4xx errors are not retried. {@code Retry-After} is not available through JNI.
   * The budget is four attempts in total, with base waits of 250 ms, 500 ms, and 1000 ms, plus up
   * to 250 ms of jitter. The RTE client's own 429/503 loop is unchanged and is not stacked on top
   * of this one.
   *
   * @return A configured LanceNamespace
   * @throws IllegalStateException if required parameters are missing
   */
  public LanceNamespace build() {
    validate();

    // Build configuration map
    Map<String, String> config = new HashMap<>(additionalConfig);
    config.put("header.x-lancedb-database", database);
    config.put("header.x-api-key", apiKey);
    config.put("uri", resolveUri());

    LanceNamespace namespace = LanceNamespace.connect("rest", config, null);
    if (namespace instanceof RestNamespace) {
      return new RetryingRestNamespace((RestNamespace) namespace);
    }
    return namespace;
  }

  /**
   * Build a {@link LanceDbRestClient} for the same endpoint.
   *
   * <p>Needed only for LanceDB routes that the Lance Namespace specification does not cover — the
   * RTE write path, reached through {@link LanceDbTableRTE}. Every other table operation belongs on
   * the {@link LanceNamespace} from {@link #build()}.
   *
   * <p>The returned client owns an HTTP connection pool; close it when you are done with it.
   *
   * @return A configured LanceDbRestClient
   * @throws IllegalStateException if required parameters are missing
   */
  public LanceDbRestClient buildRestClient() {
    validate();
    return new LanceDbRestClient(resolveUri(), apiKey, database);
  }

  private void validate() {
    if (apiKey == null) {
      throw new IllegalStateException("API key is required");
    }
    if (database == null) {
      throw new IllegalStateException("Database is required");
    }
  }

  /** The custom endpoint when set, else the LanceDB Cloud URL for this database and region. */
  private String resolveUri() {
    if (endpoint.isPresent()) {
      return endpoint.get();
    }
    return String.format(CLOUD_URL_PATTERN, database, region.orElse(DEFAULT_REGION));
  }
}
