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
import org.lance.namespace.client.apache.ApiClient;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Util class to help construct a {@link LanceNamespace} for LanceDB.
 *
 * <p>For LanceDB Cloud, use the simplified builder API:
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
   * @return A configured LanceNamespace
   * @throws IllegalStateException if required parameters are missing
   */
  public LanceNamespace build() {
    validateRequiredFields();

    // Build configuration map
    Map<String, String> config = new HashMap<>(additionalConfig);
    config.put("header.x-lancedb-database", database);
    config.put("header.x-api-key", apiKey);
    config.put("uri", resolveUri());

    return LanceNamespace.connect("rest", config, null);
  }

  /**
   * Build a client for creating FTS indexes with custom stop words.
   *
   * <p>The generated Lance Namespace request model currently has no {@code custom_stop_words}
   * field. Passing a subclass through the JNI-backed {@link LanceNamespace} client would silently
   * discard that field. This client uses the official HTTP transport directly so the resolved
   * client-side snapshot reaches LanceDB Cloud or Enterprise.
   *
   * <p>The helper supports the same endpoint, API key, database, identifier delimiter, and {@code
   * header.*}/{@code headers.*} configuration as {@link #build()}. TLS and other transport
   * configuration cannot be copied safely to the Apache HTTP client and is rejected with an
   * actionable error instead of being ignored. Callers needing a custom HTTP stack can construct
   * {@link LanceDbFtsIndexClient} with a preconfigured {@link ApiClient}.
   *
   * @return A configured custom-stop-word FTS index client
   * @throws IllegalStateException if required parameters are missing or an unsupported
   *     configuration key is present
   */
  public LanceDbFtsIndexClient buildFtsIndexClient() {
    validateRequiredFields();

    String delimiter = additionalConfig.getOrDefault("delimiter", "$");
    if (delimiter == null || delimiter.isEmpty()) {
      throw new IllegalStateException("FTS index client delimiter cannot be null or empty");
    }
    String uri = resolveUri();
    if (uri.trim().isEmpty()) {
      throw new IllegalStateException("FTS index client endpoint cannot be empty");
    }

    Map<String, String> headers = new HashMap<>();
    for (Map.Entry<String, String> entry : additionalConfig.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      if (key == null) {
        throw new IllegalStateException("FTS index client configuration key cannot be null");
      }
      if ("delimiter".equals(key)) {
        continue;
      }
      if ("uri".equals(key)) {
        throw new IllegalStateException(
            "FTS index client configuration key 'uri' is not accepted; use endpoint() instead");
      }

      String headerName = headerName(key);
      if (headerName != null) {
        if (headerName.isEmpty()) {
          throw new IllegalStateException(
              "FTS index client header configuration requires a non-empty header name");
        }
        if (value == null) {
          throw new IllegalStateException(
              "FTS index client header '" + headerName + "' cannot have a null value");
        }
        if (isReservedHeader(headerName)) {
          String builderMethod =
              "x-api-key".equalsIgnoreCase(headerName) ? "apiKey()" : "database()";
          throw new IllegalStateException(
              "FTS index client header '"
                  + headerName
                  + "' is reserved; configure it with "
                  + builderMethod
                  + " instead");
        }
        headers.put(headerName, value);
        continue;
      }

      throw new IllegalStateException(
          "FTS index client cannot safely apply configuration key '"
              + key
              + "'. Configure an org.lance.namespace.client.apache.ApiClient explicitly and "
              + "pass it to LanceDbFtsIndexClient instead.");
    }

    ApiClient apiClient = new ApiClient().setBasePath(uri).setApiKey(apiKey);
    for (Map.Entry<String, String> header : headers.entrySet()) {
      apiClient.addDefaultHeader(header.getKey(), header.getValue());
    }
    apiClient.addDefaultHeader("x-lancedb-database", database);
    return new LanceDbFtsIndexClient(apiClient, delimiter);
  }

  private void validateRequiredFields() {
    if (apiKey == null) {
      throw new IllegalStateException("API key is required");
    }
    if (database == null) {
      throw new IllegalStateException("Database is required");
    }
  }

  private String resolveUri() {
    if (endpoint.isPresent()) {
      return endpoint.get();
    }
    String effectiveRegion = region.orElse(DEFAULT_REGION);
    return String.format(CLOUD_URL_PATTERN, database, effectiveRegion);
  }

  private static String headerName(String key) {
    if (key.startsWith("header.")) {
      return key.substring("header.".length());
    }
    if (key.startsWith("headers.")) {
      return key.substring("headers.".length());
    }
    return null;
  }

  private static boolean isReservedHeader(String headerName) {
    return "x-api-key".equalsIgnoreCase(headerName)
        || "x-lancedb-database".equalsIgnoreCase(headerName);
  }
}
