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
package com.lancedb.catalog.adapter.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;

import javax.annotation.Generated;
import javax.validation.constraints.*;

import java.util.*;
import java.util.Objects;

/** TableDefinition */
@Generated(
    value = "org.openapitools.codegen.languages.SpringCodegen",
    comments = "Generator version: 7.12.0")
public class TableDefinition {

  private Object columnDefinitions;

  private Object schema;

  public TableDefinition columnDefinitions(Object columnDefinitions) {
    this.columnDefinitions = columnDefinitions;
    return this;
  }

  /**
   * Get columnDefinitions
   *
   * @return columnDefinitions
   */
  @Schema(name = "column_definitions", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("column_definitions")
  public Object getColumnDefinitions() {
    return columnDefinitions;
  }

  public void setColumnDefinitions(Object columnDefinitions) {
    this.columnDefinitions = columnDefinitions;
  }

  public TableDefinition schema(Object schema) {
    this.schema = schema;
    return this;
  }

  /**
   * Get schema
   *
   * @return schema
   */
  @Schema(name = "schema", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("schema")
  public Object getSchema() {
    return schema;
  }

  public void setSchema(Object schema) {
    this.schema = schema;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TableDefinition tableDefinition = (TableDefinition) o;
    return Objects.equals(this.columnDefinitions, tableDefinition.columnDefinitions)
        && Objects.equals(this.schema, tableDefinition.schema);
  }

  @Override
  public int hashCode() {
    return Objects.hash(columnDefinitions, schema);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class TableDefinition {\n");
    sb.append("    columnDefinitions: ").append(toIndentedString(columnDefinitions)).append("\n");
    sb.append("    schema: ").append(toIndentedString(schema)).append("\n");
    sb.append("}");
    return sb.toString();
  }

  /**
   * Convert the given object to string with each line indented by 4 spaces (except the first line).
   */
  private String toIndentedString(Object o) {
    if (o == null) {
      return "null";
    }
    return o.toString().replace("\n", "\n    ");
  }
}
