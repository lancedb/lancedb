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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Minimal HTTP client for LanceDB Cloud and Enterprise routes that the Lance Namespace
 * specification does not cover.
 *
 * <p>Most table operations reach LanceDB through {@link org.lance.namespace.LanceNamespace}, which
 * is generated from the namespace spec. A handful of routes — the MemWAL LSM write path in
 * particular — are served by the same endpoint but are not part of that spec, so they are issued
 * directly here. See {@link LanceDbTableLsm}.
 *
 * <p>Obtain one from {@link LanceDbNamespaceClientBuilder#buildRestClient()}.
 */
public class LanceDbRestClient implements Closeable {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final String baseUri;
  private final String apiKey;
  private final String database;
  private final CloseableHttpClient http;

  LanceDbRestClient(String baseUri, String apiKey, String database) {
    this.baseUri = baseUri.endsWith("/") ? baseUri.substring(0, baseUri.length() - 1) : baseUri;
    this.apiKey = apiKey;
    this.database = database;
    // Automatic retries off, deliberately. The default strategy retries 429 and 503 —
    // exactly the two statuses LanceDbTableLsm.checkpointLsm() acts on — which would
    // silently double its explicit retry budget and would also retry compact_lsm in
    // place, where the loop is designed to fall through to a fresh stats poll instead.
    // The checkpoint loop owns the 421/429/503 transitions; the transport must not.
    this.http = HttpClients.custom().disableAutomaticRetries().build();
  }

  /**
   * POST {@code path}, sending {@code body} as JSON when it is non-null.
   *
   * @param path Absolute request path, beginning with {@code /}.
   * @param body Object to serialize as the request body, or null to send no body.
   * @return The parsed response body, or null when the response carried no content.
   * @throws HttpException if the server returned a non-2xx status.
   */
  public JsonNode post(String path, Object body) {
    HttpPost request = new HttpPost(baseUri + path);
    request.setHeader("x-api-key", apiKey);
    request.setHeader("x-lancedb-database", database);
    try {
      if (body != null) {
        request.setEntity(
            new StringEntity(MAPPER.writeValueAsString(body), ContentType.APPLICATION_JSON));
      }
      return http.execute(
          request,
          response -> {
            String text =
                response.getEntity() == null ? "" : EntityUtils.toString(response.getEntity());
            int status = response.getCode();
            if (status < 200 || status >= 300) {
              throw new HttpException(status, "LanceDB request to " + path + " failed: " + text);
            }
            return text.isEmpty() ? null : MAPPER.readTree(text);
          });
    } catch (IOException e) {
      throw new UncheckedIOException("LanceDB request to " + path + " failed", e);
    }
  }

  @Override
  public void close() throws IOException {
    http.close();
  }

  /**
   * A non-2xx response.
   *
   * <p>The status is exposed because callers act on it: {@link LanceDbTableLsm#checkpointLsm()}
   * treats 429 and 503 as retryable and 421 as a lost node claim.
   */
  public static class HttpException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    private final int statusCode;

    public HttpException(int statusCode, String message) {
      super(message);
      this.statusCode = statusCode;
    }

    /** The HTTP status the failed response carried. */
    public int statusCode() {
      return statusCode;
    }
  }
}
