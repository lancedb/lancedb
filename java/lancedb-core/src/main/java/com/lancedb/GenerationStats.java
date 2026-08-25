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

import java.util.OptionalLong;

/** One flushed L0 generation. */
public class GenerationStats {
  private static final String CONTEXT = "generation stats";

  private final long generation;
  private final long bytes;
  private final Long rows;

  GenerationStats(long generation, long bytes, Long rows) {
    this.generation = generation;
    this.bytes = bytes;
    this.rows = rows;
  }

  /** The generation number. Increases as memtables are sealed into L0. */
  public long generation() {
    return generation;
  }

  /** On-disk size of the generation. */
  public long bytes() {
    return bytes;
  }

  /**
   * Rows in this generation, present only when {@code includeGenerationRows} was requested. Off by
   * default because each count opens an uncached Lance dataset.
   */
  public OptionalLong rows() {
    return rows == null ? OptionalLong.empty() : OptionalLong.of(rows);
  }

  static GenerationStats fromJson(JsonNode node) {
    JsonFields.requiredObject(node, CONTEXT);
    return new GenerationStats(
        JsonFields.requiredLong(node, "generation", CONTEXT),
        JsonFields.requiredLong(node, "bytes", CONTEXT),
        JsonFields.optionalLong(node, "rows", CONTEXT));
  }

  @Override
  public String toString() {
    return "GenerationStats{generation=" + generation + ", bytes=" + bytes + ", rows=" + rows + "}";
  }
}
