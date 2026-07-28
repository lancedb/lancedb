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

import org.lance.namespace.client.apache.ApiClient;
import org.lance.namespace.client.apache.ApiException;
import org.lance.namespace.client.apache.api.IndexApi;
import org.lance.namespace.model.CreateTableIndexResponse;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * HTTP client for creating LanceDB FTS indexes with custom stop words.
 *
 * <p>This deliberately bypasses the current JNI-backed namespace index method. That method
 * deserializes requests into an upstream generated Rust model which does not yet have {@code
 * custom_stop_words}, silently dropping the resolved snapshot before the HTTP request.
 *
 * <p>Prefer {@link LanceDbNamespaceClientBuilder#buildFtsIndexClient()}. The public constructor is
 * available for callers that need to supply a custom Apache HTTP stack, such as mTLS. This client
 * owns the supplied {@link ApiClient}'s HTTP client and closes it from {@link #close()}.
 */
public final class LanceDbFtsIndexClient implements Closeable {
  private final ApiClient apiClient;
  private final IndexApi indexApi;
  private final String delimiter;

  /**
   * Create a client using a fully configured official Lance Namespace Apache client.
   *
   * @param apiClient configured HTTP client; ownership transfers to this instance
   * @param delimiter namespace identifier delimiter, normally {@code $}
   */
  public LanceDbFtsIndexClient(ApiClient apiClient, String delimiter) {
    this(apiClient, new IndexApi(Objects.requireNonNull(apiClient, "apiClient")), delimiter);
  }

  LanceDbFtsIndexClient(ApiClient apiClient, IndexApi indexApi, String delimiter) {
    this.apiClient = Objects.requireNonNull(apiClient, "apiClient");
    this.indexApi = Objects.requireNonNull(indexApi, "indexApi");
    if (delimiter == null || delimiter.isEmpty()) {
      throw new IllegalArgumentException("delimiter cannot be null or empty");
    }
    this.delimiter = delimiter;
  }

  /**
   * Resolve any client-local stop-word file and create the FTS index.
   *
   * <p>The outgoing JSON contains only the concrete {@code custom_stop_words} list (including a
   * meaningful distinction between {@code null} and an empty list), never a client-local path.
   *
   * @param request FTS index request
   * @return asynchronous index-creation transaction response
   * @throws IOException if a local stop-word file cannot be read as strict UTF-8
   * @throws ApiException if the LanceDB HTTP request fails
   */
  public CreateTableIndexResponse createTableIndex(LanceDbFtsIndexRequest request)
      throws IOException, ApiException {
    validateRequest(request);
    request.resolveCustomStopWordsSnapshot();
    String id = joinIdentifier(request.getId());
    return indexApi.createTableIndex(id, request, delimiter);
  }

  private void validateRequest(LanceDbFtsIndexRequest request) {
    if (request == null) {
      throw new IllegalArgumentException("FTS index request cannot be null");
    }
    if (!"FTS".equals(request.getIndexType())) {
      throw new IllegalArgumentException("FTS index request index type must be FTS");
    }
    if (request.getColumn() == null || request.getColumn().trim().isEmpty()) {
      throw new IllegalArgumentException("FTS index request requires a non-empty column");
    }
    List<String> id = request.getId();
    if (id == null || id.isEmpty()) {
      throw new IllegalArgumentException("FTS index request requires a non-empty table id");
    }
    for (int index = 0; index < id.size(); index++) {
      String part = id.get(index);
      if (part == null || part.isEmpty()) {
        throw new IllegalArgumentException(
            "FTS index request table id part at index " + index + " cannot be null or empty");
      }
    }
  }

  private String joinIdentifier(List<String> id) {
    StringBuilder result = new StringBuilder();
    for (String part : id) {
      if (result.length() > 0) {
        result.append(delimiter);
      }
      result.append(part);
    }
    return result.toString();
  }

  @Override
  public void close() throws IOException {
    apiClient.getHttpClient().close();
  }
}
