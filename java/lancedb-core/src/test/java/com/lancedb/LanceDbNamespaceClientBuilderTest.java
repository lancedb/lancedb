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

import static org.junit.jupiter.api.Assertions.*;

/** Unit tests for LanceDbNamespaceClientBuilder. */
public class LanceDbNamespaceClientBuilderTest {

  @Test
  public void testBuilderRequiresApiKey() {
    LanceDbNamespaceClientBuilder builder =
        LanceDbNamespaceClientBuilder.newBuilder().database("test-db");

    IllegalStateException exception = assertThrows(IllegalStateException.class, builder::build);
    assertEquals("API key is required", exception.getMessage());
  }

  @Test
  public void testBuilderRequiresDatabase() {
    LanceDbNamespaceClientBuilder builder =
        LanceDbNamespaceClientBuilder.newBuilder().apiKey("test-api-key");

    IllegalStateException exception = assertThrows(IllegalStateException.class, builder::build);
    assertEquals("Database is required", exception.getMessage());
  }

  @Test
  public void testApiKeyCannotBeNull() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> LanceDbNamespaceClientBuilder.newBuilder().apiKey(null));
    assertEquals("API key cannot be null or empty", exception.getMessage());
  }

  @Test
  public void testApiKeyCannotBeEmpty() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> LanceDbNamespaceClientBuilder.newBuilder().apiKey("  "));
    assertEquals("API key cannot be null or empty", exception.getMessage());
  }

  @Test
  public void testDatabaseCannotBeNull() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> LanceDbNamespaceClientBuilder.newBuilder().database(null));
    assertEquals("Database cannot be null or empty", exception.getMessage());
  }

  @Test
  public void testDatabaseCannotBeEmpty() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> LanceDbNamespaceClientBuilder.newBuilder().database("  "));
    assertEquals("Database cannot be null or empty", exception.getMessage());
  }

  @Test
  public void testBuilderFluentApi() {
    // Verify the builder returns itself for chaining
    LanceDbNamespaceClientBuilder builder = LanceDbNamespaceClientBuilder.newBuilder();

    assertSame(builder, builder.apiKey("test-key"));
    assertSame(builder, builder.database("test-db"));
    assertSame(builder, builder.endpoint("http://localhost:8080"));
    assertSame(builder, builder.region("eu-west-1"));
    assertSame(builder, builder.config("custom-key", "custom-value"));
  }

  @Test
  public void testNewBuilderCreatesNewInstance() {
    LanceDbNamespaceClientBuilder builder1 = LanceDbNamespaceClientBuilder.newBuilder();
    LanceDbNamespaceClientBuilder builder2 = LanceDbNamespaceClientBuilder.newBuilder();

    assertNotSame(builder1, builder2);
  }

  @Test
  public void testFtsIndexClientRequiresApiKey() {
    LanceDbNamespaceClientBuilder builder =
        LanceDbNamespaceClientBuilder.newBuilder().database("test-db");

    IllegalStateException exception =
        assertThrows(IllegalStateException.class, builder::buildFtsIndexClient);
    assertEquals("API key is required", exception.getMessage());
  }

  @Test
  public void testFtsIndexClientRequiresDatabase() {
    LanceDbNamespaceClientBuilder builder =
        LanceDbNamespaceClientBuilder.newBuilder().apiKey("test-api-key");

    IllegalStateException exception =
        assertThrows(IllegalStateException.class, builder::buildFtsIndexClient);
    assertEquals("Database is required", exception.getMessage());
  }

  @Test
  public void testFtsIndexClientRejectsUnsupportedTransportConfiguration() {
    LanceDbNamespaceClientBuilder builder =
        LanceDbNamespaceClientBuilder.newBuilder()
            .apiKey("test-api-key")
            .database("test-db")
            .config("tls.cert_file", "/tmp/client.pem");

    IllegalStateException exception =
        assertThrows(IllegalStateException.class, builder::buildFtsIndexClient);

    assertTrue(exception.getMessage().contains("tls.cert_file"));
    assertTrue(exception.getMessage().contains("ApiClient"));
  }

  @Test
  public void testFtsIndexClientRejectsEmptyDelimiter() {
    LanceDbNamespaceClientBuilder builder =
        LanceDbNamespaceClientBuilder.newBuilder()
            .apiKey("test-api-key")
            .database("test-db")
            .config("delimiter", "");

    IllegalStateException exception =
        assertThrows(IllegalStateException.class, builder::buildFtsIndexClient);

    assertTrue(exception.getMessage().contains("delimiter"));
  }

  @Test
  public void testFtsIndexClientRejectsUriConfigurationAlias() {
    LanceDbNamespaceClientBuilder builder =
        LanceDbNamespaceClientBuilder.newBuilder()
            .apiKey("test-api-key")
            .database("test-db")
            .config("uri", "https://example.invalid");

    IllegalStateException exception =
        assertThrows(IllegalStateException.class, builder::buildFtsIndexClient);

    assertTrue(exception.getMessage().contains("uri"));
    assertTrue(exception.getMessage().contains("endpoint()"));
  }

  @Test
  public void testFtsIndexClientRejectsReservedHeaders() {
    LanceDbNamespaceClientBuilder apiKeyBuilder =
        LanceDbNamespaceClientBuilder.newBuilder()
            .apiKey("test-api-key")
            .database("test-db")
            .config("header.x-api-key", "other-key");
    IllegalStateException apiKeyError =
        assertThrows(IllegalStateException.class, apiKeyBuilder::buildFtsIndexClient);
    assertTrue(apiKeyError.getMessage().contains("reserved"));
    assertTrue(apiKeyError.getMessage().contains("apiKey()"));

    LanceDbNamespaceClientBuilder databaseBuilder =
        LanceDbNamespaceClientBuilder.newBuilder()
            .apiKey("test-api-key")
            .database("test-db")
            .config("headers.X-LanceDB-Database", "other-db");
    IllegalStateException databaseError =
        assertThrows(IllegalStateException.class, databaseBuilder::buildFtsIndexClient);
    assertTrue(databaseError.getMessage().contains("reserved"));
    assertTrue(databaseError.getMessage().contains("database()"));
  }
}
