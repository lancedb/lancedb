package com.lancedb.catalog.adapter.model;

import java.net.URI;
import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.springframework.lang.Nullable;
import java.time.OffsetDateTime;
import javax.validation.Valid;
import javax.validation.constraints.*;
import io.swagger.v3.oas.annotations.media.Schema;


import java.util.*;
import javax.annotation.Generated;

/**
 * V1DatabasesDbNameTablesTableNameNamePutRequest
 */

@JsonTypeName("_v1_databases__dbName__tables__tableName__name_put_request")
@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", comments = "Generator version: 7.12.0")
public class V1DatabasesDbNameTablesTableNameNamePutRequest {

  private String newName;

  public V1DatabasesDbNameTablesTableNameNamePutRequest() {
    super();
  }

  /**
   * Constructor with only required parameters
   */
  public V1DatabasesDbNameTablesTableNameNamePutRequest(String newName) {
    this.newName = newName;
  }

  public V1DatabasesDbNameTablesTableNameNamePutRequest newName(String newName) {
    this.newName = newName;
    return this;
  }

  /**
   * Get newName
   * @return newName
   */
  @NotNull 
  @Schema(name = "newName", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("newName")
  public String getNewName() {
    return newName;
  }

  public void setNewName(String newName) {
    this.newName = newName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    V1DatabasesDbNameTablesTableNameNamePutRequest v1DatabasesDbNameTablesTableNameNamePutRequest = (V1DatabasesDbNameTablesTableNameNamePutRequest) o;
    return Objects.equals(this.newName, v1DatabasesDbNameTablesTableNameNamePutRequest.newName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(newName);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class V1DatabasesDbNameTablesTableNameNamePutRequest {\n");
    sb.append("    newName: ").append(toIndentedString(newName)).append("\n");
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

