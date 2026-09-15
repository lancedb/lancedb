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

/**
 * Strict readers for decoding LanceDB JSON responses.
 *
 * <p>Every reader fails closed: a missing, null, or wrong-typed field throws rather than
 * defaulting. That mirrors the serde decoding the Rust client applies to the same payloads in
 * {@code rust/lancedb/src/table/lsm_stats.rs}, where a required field has no default and a
 * malformed response is an error rather than a zero.
 *
 * <p>The alternative — Jackson's {@code path()}, which yields a missing node that reads as an empty
 * array or a zero — is unsafe here because {@link LanceDbTableLsm#checkpointLsm()} decides
 * convergence from these numbers. A defaulted {@code generations} array is indistinguishable from a
 * drained one, so a malformed response would report a checkpoint that never happened.
 */
final class JsonFields {
  private JsonFields() {}

  /** The node itself, once confirmed to be a JSON object. */
  static JsonNode requiredObject(JsonNode node, String context) {
    if (node == null || !node.isObject()) {
      throw new IllegalStateException(context + " is not a JSON object: " + node);
    }
    return node;
  }

  static String requiredText(JsonNode owner, String field, String context) {
    JsonNode value = required(owner, field, context);
    if (!value.isTextual()) {
      throw new IllegalStateException(fieldIs(context, field, "a string", value));
    }
    return value.asText();
  }

  static long requiredLong(JsonNode owner, String field, String context) {
    JsonNode value = required(owner, field, context);
    if (!value.isIntegralNumber()) {
      throw new IllegalStateException(fieldIs(context, field, "an integer", value));
    }
    return value.asLong();
  }

  static boolean requiredBoolean(JsonNode owner, String field, String context) {
    JsonNode value = required(owner, field, context);
    if (!value.isBoolean()) {
      throw new IllegalStateException(fieldIs(context, field, "a boolean", value));
    }
    return value.asBoolean();
  }

  static JsonNode requiredArray(JsonNode owner, String field, String context) {
    JsonNode value = required(owner, field, context);
    if (!value.isArray()) {
      throw new IllegalStateException(fieldIs(context, field, "an array", value));
    }
    return value;
  }

  /** Null when the field is absent or JSON null, mirroring a serde {@code Option}. */
  static Long optionalLong(JsonNode owner, String field, String context) {
    JsonNode value = owner.get(field);
    if (value == null || value.isNull()) {
      return null;
    }
    if (!value.isIntegralNumber()) {
      throw new IllegalStateException(fieldIs(context, field, "an integer", value));
    }
    return value.asLong();
  }

  /** Null when the field is absent or JSON null, mirroring a serde {@code Option}. */
  static JsonNode optionalArray(JsonNode owner, String field, String context) {
    JsonNode value = owner.get(field);
    if (value == null || value.isNull()) {
      return null;
    }
    if (!value.isArray()) {
      throw new IllegalStateException(fieldIs(context, field, "an array", value));
    }
    return value;
  }

  private static JsonNode required(JsonNode owner, String field, String context) {
    JsonNode value = owner.get(field);
    if (value == null || value.isNull()) {
      throw new IllegalStateException(context + " is missing required field '" + field + "'");
    }
    return value;
  }

  private static String fieldIs(String context, String field, String expected, JsonNode value) {
    return context + " field '" + field + "' is not " + expected + ": " + value;
  }
}
