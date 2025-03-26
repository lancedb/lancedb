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
 * TableUpdate
 */

@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", date = "2025-03-26T15:36:49.834244+08:00[Asia/Shanghai]", comments = "Generator version: 7.12.0")
public class TableUpdate {

  @Valid
  private List<Object> data = new ArrayList<>();

  private Object options;

  public TableUpdate data(List<Object> data) {
    this.data = data;
    return this;
  }

  public TableUpdate addDataItem(Object dataItem) {
    if (this.data == null) {
      this.data = new ArrayList<>();
    }
    this.data.add(dataItem);
    return this;
  }

  /**
   * Get data
   * @return data
   */
  
  @Schema(name = "data", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("data")
  public List<Object> getData() {
    return data;
  }

  public void setData(List<Object> data) {
    this.data = data;
  }

  public TableUpdate options(Object options) {
    this.options = options;
    return this;
  }

  /**
   * Get options
   * @return options
   */
  
  @Schema(name = "options", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("options")
  public Object getOptions() {
    return options;
  }

  public void setOptions(Object options) {
    this.options = options;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TableUpdate tableUpdate = (TableUpdate) o;
    return Objects.equals(this.data, tableUpdate.data) &&
        Objects.equals(this.options, tableUpdate.options);
  }

  @Override
  public int hashCode() {
    return Objects.hash(data, options);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class TableUpdate {\n");
    sb.append("    data: ").append(toIndentedString(data)).append("\n");
    sb.append("    options: ").append(toIndentedString(options)).append("\n");
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

