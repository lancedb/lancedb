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
 * DatabaseMetadata
 */

@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", comments = "Generator version: 7.12.0")
public class DatabaseMetadata {

  private String name;

  private Integer tableCount;

  public DatabaseMetadata name(String name) {
    this.name = name;
    return this;
  }

  /**
   * Get name
   * @return name
   */
  
  @Schema(name = "name", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("name")
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public DatabaseMetadata tableCount(Integer tableCount) {
    this.tableCount = tableCount;
    return this;
  }

  /**
   * Get tableCount
   * @return tableCount
   */
  
  @Schema(name = "tableCount", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("tableCount")
  public Integer getTableCount() {
    return tableCount;
  }

  public void setTableCount(Integer tableCount) {
    this.tableCount = tableCount;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DatabaseMetadata databaseMetadata = (DatabaseMetadata) o;
    return Objects.equals(this.name, databaseMetadata.name) &&
        Objects.equals(this.tableCount, databaseMetadata.tableCount);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, tableCount);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class DatabaseMetadata {\n");
    sb.append("    name: ").append(toIndentedString(name)).append("\n");
    sb.append("    tableCount: ").append(toIndentedString(tableCount)).append("\n");
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

