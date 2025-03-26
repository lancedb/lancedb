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
 * TableList
 */

@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", comments = "Generator version: 7.12.0")
public class TableList {

  @Valid
  private List<String> tables = new ArrayList<>();

  private String nextToken;

  public TableList tables(List<String> tables) {
    this.tables = tables;
    return this;
  }

  public TableList addTablesItem(String tablesItem) {
    if (this.tables == null) {
      this.tables = new ArrayList<>();
    }
    this.tables.add(tablesItem);
    return this;
  }

  /**
   * Get tables
   * @return tables
   */
  
  @Schema(name = "tables", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("tables")
  public List<String> getTables() {
    return tables;
  }

  public void setTables(List<String> tables) {
    this.tables = tables;
  }

  public TableList nextToken(String nextToken) {
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
    TableList tableList = (TableList) o;
    return Objects.equals(this.tables, tableList.tables) &&
        Objects.equals(this.nextToken, tableList.nextToken);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tables, nextToken);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class TableList {\n");
    sb.append("    tables: ").append(toIndentedString(tables)).append("\n");
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

