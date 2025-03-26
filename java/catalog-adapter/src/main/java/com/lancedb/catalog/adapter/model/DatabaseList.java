package com.lancedb.catalog.adapter.model;

import java.net.URI;
import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.springframework.lang.Nullable;
import java.time.OffsetDateTime;
import javax.validation.Valid;
import javax.validation.constraints.*;
import io.swagger.v3.oas.annotations.media.Schema;


import java.util.*;
import javax.annotation.Generated;

/**
 * DatabaseList
 */

@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", comments = "Generator version: 7.12.0")
public class DatabaseList {

  @Valid
  private List<String> databases = new ArrayList<>();

  private String nextToken;

  public DatabaseList databases(List<String> databases) {
    this.databases = databases;
    return this;
  }

  public DatabaseList addDatabasesItem(String databasesItem) {
    if (this.databases == null) {
      this.databases = new ArrayList<>();
    }
    this.databases.add(databasesItem);
    return this;
  }

  /**
   * Get databases
   * @return databases
   */
  
  @Schema(name = "databases", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("databases")
  public List<String> getDatabases() {
    return databases;
  }

  public void setDatabases(List<String> databases) {
    this.databases = databases;
  }

  public DatabaseList nextToken(String nextToken) {
    this.nextToken = nextToken;
    return this;
  }

  /**
   * Get nextToken
   * @return nextToken
   */
  
  @Schema(name = "nextToken", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("nextToken")
  public String getNextToken() {
    return nextToken;
  }

  public void setNextToken(String nextToken) {
    this.nextToken = nextToken;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DatabaseList databaseList = (DatabaseList) o;
    return Objects.equals(this.databases, databaseList.databases) &&
        Objects.equals(this.nextToken, databaseList.nextToken);
  }

  @Override
  public int hashCode() {
    return Objects.hash(databases, nextToken);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class DatabaseList {\n");
    sb.append("    databases: ").append(toIndentedString(databases)).append("\n");
    sb.append("    nextToken: ").append(toIndentedString(nextToken)).append("\n");
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

