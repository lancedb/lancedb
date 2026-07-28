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
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.lance.namespace.client.apache.ApiClient;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for {@link LanceDbFtsIndexRequest}. */
public class LanceDbFtsIndexRequestTest {

  @TempDir Path tempDir;

  @Test
  public void testInlineSnapshotPreservesContentAndCanonicalizesExactValues() {
    List<String> input =
        new ArrayList<>(Arrays.asList("cat", "", " dog ", "cat", "CAT", " dog "));
    LanceDbFtsIndexRequest request = new LanceDbFtsIndexRequest();

    request.setCustomStopWords(input);
    input.set(0, "mutated");

    assertEquals(Arrays.asList("cat", " dog ", "CAT"), request.getCustomStopWords());
    assertThrows(
        UnsupportedOperationException.class,
        () -> request.getCustomStopWords().add("later mutation"));
  }

  @Test
  public void testNullAndEmptySnapshotsRemainDistinctInApacheClientJson() throws Exception {
    ApiClient apiClient = new ApiClient();
    try {
      LanceDbFtsIndexRequest request = new LanceDbFtsIndexRequest();
      request.setColumn("text");

      JsonNode nullJson = serialize(apiClient, request);
      assertTrue(nullJson.has("custom_stop_words"));
      assertTrue(nullJson.get("custom_stop_words").isNull());

      request.setCustomStopWords(Collections.emptyList());
      JsonNode emptyJson = serialize(apiClient, request);
      assertTrue(emptyJson.get("custom_stop_words").isArray());
      assertEquals(0, emptyJson.get("custom_stop_words").size());
    } finally {
      apiClient.getHttpClient().close();
    }
  }

  @Test
  public void testInlineNullAndNonStringValuesAreRejected() {
    LanceDbFtsIndexRequest request = new LanceDbFtsIndexRequest();

    IllegalArgumentException nullError =
        assertThrows(
            IllegalArgumentException.class,
            () -> request.setCustomStopWords(Arrays.asList("cat", null)));
    assertTrue(nullError.getMessage().contains("index 1"));
    assertTrue(nullError.getMessage().contains("cannot be null"));

    @SuppressWarnings({"rawtypes", "unchecked"})
    List<String> invalid = (List) Arrays.asList("cat", 42);
    IllegalArgumentException typeError =
        assertThrows(IllegalArgumentException.class, () -> request.setCustomStopWords(invalid));
    assertTrue(typeError.getMessage().contains("index 1"));
    assertTrue(typeError.getMessage().contains("must be a string"));
  }

  @Test
  public void testInlineAndFileSourcesAreMutuallyExclusive() {
    Path path = tempDir.resolve("stop-words.txt");
    LanceDbFtsIndexRequest inline = new LanceDbFtsIndexRequest();
    inline.setCustomStopWords(Collections.emptyList());

    IllegalStateException inlineError =
        assertThrows(IllegalStateException.class, () -> inline.setCustomStopWordsFile(path));
    assertTrue(inlineError.getMessage().contains("mutually exclusive"));

    LanceDbFtsIndexRequest file = new LanceDbFtsIndexRequest();
    file.setCustomStopWordsFile(path);
    IllegalStateException fileError =
        assertThrows(
            IllegalStateException.class,
            () -> file.setCustomStopWords(Collections.singletonList("cat")));
    assertTrue(fileError.getMessage().contains("mutually exclusive"));

    inline.setCustomStopWords(null);
    inline.setCustomStopWordsFile(path);
  }

  @Test
  public void testFileSnapshotUsesStrictUtf8AndIsStable() throws Exception {
    Path path = tempDir.resolve("stop-words.txt");
    Files.write(path, "cat\n\n dog \r\ncat\nCAT\n".getBytes(StandardCharsets.UTF_8));
    LanceDbFtsIndexRequest request = new LanceDbFtsIndexRequest();
    request.setCustomStopWordsFile(path);

    assertNull(request.getCustomStopWords());
    request.resolveCustomStopWordsSnapshot();
    assertEquals(Arrays.asList("cat", " dog ", "CAT"), request.getCustomStopWords());

    Files.write(path, "changed\n".getBytes(StandardCharsets.UTF_8));
    request.resolveCustomStopWordsSnapshot();
    assertEquals(Arrays.asList("cat", " dog ", "CAT"), request.getCustomStopWords());
  }

  @Test
  public void testFilePathIsNeverSerialized() throws Exception {
    Path path = tempDir.resolve("private-client-path.txt");
    Files.write(path, "cat\n".getBytes(StandardCharsets.UTF_8));
    LanceDbFtsIndexRequest request = new LanceDbFtsIndexRequest();
    request.setColumn("text");
    request.setCustomStopWordsFile(path);
    request.resolveCustomStopWordsSnapshot();

    ApiClient apiClient = new ApiClient();
    try {
      JsonNode json = serialize(apiClient, request);
      assertEquals(Collections.singletonList("cat"), request.getCustomStopWords());
      assertEquals("cat", json.get("custom_stop_words").get(0).asText());
      assertFalse(json.has("customStopWordsFile"));
      assertFalse(json.has("custom_stop_words_file"));
      assertFalse(json.toString().contains(path.toString()));
    } finally {
      apiClient.getHttpClient().close();
    }
  }

  @Test
  public void testEmptyFileProducesExplicitEmptySnapshot() throws Exception {
    Path path = tempDir.resolve("empty.txt");
    Files.write(path, new byte[0]);
    LanceDbFtsIndexRequest request = new LanceDbFtsIndexRequest();
    request.setCustomStopWordsFile(path);

    request.resolveCustomStopWordsSnapshot();

    assertEquals(Collections.emptyList(), request.getCustomStopWords());
  }

  @Test
  public void testLoneCarriageReturnIsPreservedAsContent() throws Exception {
    Path path = tempDir.resolve("lone-cr.txt");
    Files.write(path, "cat\rdog\ncat\r\nbird\r".getBytes(StandardCharsets.UTF_8));
    LanceDbFtsIndexRequest request = new LanceDbFtsIndexRequest();
    request.setCustomStopWordsFile(path);

    request.resolveCustomStopWordsSnapshot();

    assertEquals(Arrays.asList("cat\rdog", "cat", "bird\r"), request.getCustomStopWords());
  }

  @Test
  public void testMissingFileIsRejectedWithPath() {
    Path path = tempDir.resolve("missing.txt");
    LanceDbFtsIndexRequest request = new LanceDbFtsIndexRequest();
    request.setCustomStopWordsFile(path);

    IOException error =
        assertThrows(IOException.class, request::resolveCustomStopWordsSnapshot);

    assertTrue(error.getMessage().contains("failed to read custom stop words file"));
    assertTrue(error.getMessage().contains(path.toString()));
  }

  @Test
  public void testEmptyFilePathIsRejectedDescriptively() {
    LanceDbFtsIndexRequest request = new LanceDbFtsIndexRequest();

    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () -> request.setCustomStopWordsFile(Paths.get("")));

    assertTrue(error.getMessage().contains("file source requires a non-empty path"));
  }

  @Test
  public void testMalformedUtf8IsRejected() throws Exception {
    Path path = tempDir.resolve("invalid.txt");
    Files.write(path, new byte[] {(byte) 0xc3, (byte) 0x28});
    LanceDbFtsIndexRequest request = new LanceDbFtsIndexRequest();
    request.setCustomStopWordsFile(path);

    IOException error =
        assertThrows(IOException.class, request::resolveCustomStopWordsSnapshot);

    assertTrue(error.getMessage().contains("not valid UTF-8"));
  }

  @Test
  public void testRequestCannotBeChangedToAnotherIndexType() {
    LanceDbFtsIndexRequest request = new LanceDbFtsIndexRequest();

    assertEquals("FTS", request.getIndexType());
    IllegalArgumentException error =
        assertThrows(IllegalArgumentException.class, () -> request.setIndexType("BTREE"));
    assertTrue(error.getMessage().contains("must be FTS"));
  }

  private static JsonNode serialize(ApiClient apiClient, LanceDbFtsIndexRequest request)
      throws Exception {
    HttpEntity entity =
        apiClient.serialize(request, Collections.emptyMap(), ContentType.APPLICATION_JSON);
    String json = EntityUtils.toString(entity);
    return new ObjectMapper().readTree(json);
  }
}
