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
import org.junit.jupiter.api.io.TempDir;
import org.lance.namespace.client.apache.ApiClient;
import org.lance.namespace.client.apache.ApiException;
import org.lance.namespace.client.apache.api.IndexApi;
import org.lance.namespace.model.CreateTableIndexRequest;
import org.lance.namespace.model.CreateTableIndexResponse;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for {@link LanceDbFtsIndexClient}. */
public class LanceDbFtsIndexClientTest {

  @TempDir Path tempDir;

  @Test
  public void testCreateIndexResolvesFileAndReusesSnapshotOnRetry() throws Exception {
    ApiClient apiClient = new ApiClient();
    CreateTableIndexResponse response = new CreateTableIndexResponse();
    CapturingIndexApi indexApi = new CapturingIndexApi(apiClient, response);
    LanceDbFtsIndexRequest request = request();
    Path path = tempDir.resolve("stop-words.txt");
    Files.write(path, "cat\ncat\n dog \n".getBytes(StandardCharsets.UTF_8));
    request.setCustomStopWordsFile(path);

    try (LanceDbFtsIndexClient client = new LanceDbFtsIndexClient(apiClient, indexApi, "$")) {
      CreateTableIndexResponse actual = client.createTableIndex(request);
      Files.write(path, "changed\n".getBytes(StandardCharsets.UTF_8));
      CreateTableIndexResponse retry = client.createTableIndex(request);

      assertSame(response, actual);
      assertSame(response, retry);
      assertEquals(Arrays.asList("cat", " dog "), request.getCustomStopWords());
      assertEquals("namespace$table", indexApi.id);
      assertSame(request, indexApi.request);
      assertEquals("$", indexApi.delimiter);
      assertEquals(2, indexApi.calls);
    }
  }

  @Test
  public void testCreateIndexValidatesRequiredFieldsBeforeNetworkCall() throws Exception {
    ApiClient apiClient = new ApiClient();
    CapturingIndexApi indexApi =
        new CapturingIndexApi(apiClient, new CreateTableIndexResponse());
    try (LanceDbFtsIndexClient client = new LanceDbFtsIndexClient(apiClient, indexApi, "$")) {
      LanceDbFtsIndexRequest noColumn = new LanceDbFtsIndexRequest();
      noColumn.setId(Collections.singletonList("table"));
      IllegalArgumentException columnError =
          assertThrows(IllegalArgumentException.class, () -> client.createTableIndex(noColumn));
      assertTrue(columnError.getMessage().contains("non-empty column"));

      LanceDbFtsIndexRequest noId = new LanceDbFtsIndexRequest();
      noId.setColumn("text");
      IllegalArgumentException idError =
          assertThrows(IllegalArgumentException.class, () -> client.createTableIndex(noId));
      assertTrue(idError.getMessage().contains("non-empty table id"));
      assertEquals(0, indexApi.calls);
    }
  }

  @Test
  public void testDelimiterMustBeNonEmpty() throws Exception {
    ApiClient apiClient = new ApiClient();
    try {
      CapturingIndexApi indexApi =
          new CapturingIndexApi(apiClient, new CreateTableIndexResponse());
      IllegalArgumentException error =
          assertThrows(
              IllegalArgumentException.class,
              () -> new LanceDbFtsIndexClient(apiClient, indexApi, ""));

      assertTrue(error.getMessage().contains("delimiter"));
    } finally {
      apiClient.getHttpClient().close();
    }
  }

  private static LanceDbFtsIndexRequest request() {
    LanceDbFtsIndexRequest request = new LanceDbFtsIndexRequest();
    request.setId(Arrays.asList("namespace", "table"));
    request.setColumn("text");
    request.setRemoveStopWords(true);
    return request;
  }

  private static final class CapturingIndexApi extends IndexApi {
    private final CreateTableIndexResponse response;
    private String id;
    private CreateTableIndexRequest request;
    private String delimiter;
    private int calls;

    private CapturingIndexApi(ApiClient apiClient, CreateTableIndexResponse response) {
      super(apiClient);
      this.response = response;
    }

    @Override
    public CreateTableIndexResponse createTableIndex(
        String id, CreateTableIndexRequest request, String delimiter) throws ApiException {
      this.id = id;
      this.request = request;
      this.delimiter = delimiter;
      calls++;
      return response;
    }
  }
}
