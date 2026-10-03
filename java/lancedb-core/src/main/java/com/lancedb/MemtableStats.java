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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** One in-memory memtable. */
public class MemtableStats {
  private static final String CONTEXT = "memtable stats";

  private final long generation;
  private final long rows;
  private final long bytes;
  private final long batches;
  private final List<String> indexes;

  MemtableStats(long generation, long rows, long bytes, long batches, List<String> indexes) {
    this.generation = generation;
    this.rows = rows;
    this.bytes = bytes;
    this.batches = batches;
    this.indexes = Collections.unmodifiableList(indexes);
  }

  /** The generation this memtable will become once sealed. */
  public long generation() {
    return generation;
  }

  /** Rows currently buffered. */
  public long rows() {
    return rows;
  }

  /** Estimated in-memory size. */
  public long bytes() {
    return bytes;
  }

  /** Record batches currently buffered. */
  public long batches() {
    return batches;
  }

  /**
   * Names of the indexes this memtable carries. An absent name is the whole answer to "why is my
   * fresh-tier search on that column brute-force".
   */
  public List<String> indexes() {
    return indexes;
  }

  static MemtableStats fromJson(JsonNode node) {
    JsonFields.requiredObject(node, CONTEXT);
    List<String> indexes = new ArrayList<String>();
    for (JsonNode index : JsonFields.requiredArray(node, "indexes", CONTEXT)) {
      if (!index.isTextual()) {
        throw new IllegalStateException(CONTEXT + " has a non-string index name: " + index);
      }
      indexes.add(index.asText());
    }
    return new MemtableStats(
        JsonFields.requiredLong(node, "generation", CONTEXT),
        JsonFields.requiredLong(node, "rows", CONTEXT),
        JsonFields.requiredLong(node, "bytes", CONTEXT),
        JsonFields.requiredLong(node, "batches", CONTEXT),
        indexes);
  }

  @Override
  public String toString() {
    return "MemtableStats{generation="
        + generation
        + ", rows="
        + rows
        + ", bytes="
        + bytes
        + ", batches="
        + batches
        + ", indexes="
        + indexes
        + "}";
  }
}
