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

/** Unit tests for LanceDbRestNamespaceBuilder. */
public class LanceDbRestNamespaceBuilderTest {

  @Test
  public void testBuilderRequiresApiKey() {
    LanceDbRestNamespaceBuilder builder =
        LanceDbRestNamespaceBuilder.newBuilder().database("test-db");

    IllegalStateException exception = assertThrows(IllegalStateException.class, builder::build);
    assertEquals("API key is required", exception.getMessage());
  }

  @Test
  public void testBuilderRequiresDatabase() {
    LanceDbRestNamespaceBuilder builder =
        LanceDbRestNamespaceBuilder.newBuilder().apiKey("test-api-key");

    IllegalStateException exception = assertThrows(IllegalStateException.class, builder::build);
    assertEquals("Database is required", exception.getMessage());
  }

  @Test
  public void testApiKeyCannotBeNull() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> LanceDbRestNamespaceBuilder.newBuilder().apiKey(null));
    assertEquals("API key cannot be null or empty", exception.getMessage());
  }

  @Test
  public void testApiKeyCannotBeEmpty() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> LanceDbRestNamespaceBuilder.newBuilder().apiKey("  "));
    assertEquals("API key cannot be null or empty", exception.getMessage());
  }

  @Test
  public void testDatabaseCannotBeNull() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> LanceDbRestNamespaceBuilder.newBuilder().database(null));
    assertEquals("Database cannot be null or empty", exception.getMessage());
  }

  @Test
  public void testDatabaseCannotBeEmpty() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> LanceDbRestNamespaceBuilder.newBuilder().database("  "));
    assertEquals("Database cannot be null or empty", exception.getMessage());
  }

  @Test
  public void testBuilderFluentApi() {
    // Verify the builder returns itself for chaining
    LanceDbRestNamespaceBuilder builder = LanceDbRestNamespaceBuilder.newBuilder();

    assertSame(builder, builder.apiKey("test-key"));
    assertSame(builder, builder.database("test-db"));
    assertSame(builder, builder.endpoint("http://localhost:8080"));
    assertSame(builder, builder.region("eu-west-1"));
    assertSame(builder, builder.config("custom-key", "custom-value"));
  }

  @Test
  public void testNewBuilderCreatesNewInstance() {
    LanceDbRestNamespaceBuilder builder1 = LanceDbRestNamespaceBuilder.newBuilder();
    LanceDbRestNamespaceBuilder builder2 = LanceDbRestNamespaceBuilder.newBuilder();

    assertNotSame(builder1, builder2);
  }
}
