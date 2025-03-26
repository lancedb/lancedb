package com.lancedb.catalog.adapter.model;

import java.net.URI;
import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import org.springframework.lang.Nullable;
import java.time.OffsetDateTime;
import javax.validation.Valid;
import javax.validation.constraints.*;
import io.swagger.v3.oas.annotations.media.Schema;


import java.util.*;
import javax.annotation.Generated;

/**
 * TableDefinition
 */

@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", date = "2025-03-26T15:36:49.834244+08:00[Asia/Shanghai]", comments = "Generator version: 7.12.0")
public class TableDefinition {

  private Object columnDefinitions;

  private Object schema;

  public TableDefinition columnDefinitions(Object columnDefinitions) {
    this.columnDefinitions = columnDefinitions;
    return this;
  }

  /**
   * Get columnDefinitions
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
    return Objects.equals(this.columnDefinitions, tableDefinition.columnDefinitions) &&
        Objects.equals(this.schema, tableDefinition.schema);
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
   * Convert the given object to string with each line indented by 4 spaces
   * (except the first line).
   */
  private String toIndentedString(Object o) {
    if (o == null) {
      return "null";
    }
    return o.toString().replace("\n", "\n    ");
  }
}

