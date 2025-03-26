package com.lancedb.catalog.adapter.model;

import java.net.URI;
import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.lancedb.catalog.adapter.model.TableDefinition;
import org.springframework.lang.Nullable;
import java.time.OffsetDateTime;
import javax.validation.Valid;
import javax.validation.constraints.*;
import io.swagger.v3.oas.annotations.media.Schema;


import java.util.*;
import javax.annotation.Generated;

/**
 * CreateTableRequest
 */

@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", date = "2025-03-26T15:36:49.834244+08:00[Asia/Shanghai]", comments = "Generator version: 7.12.0")
public class CreateTableRequest {

  private String name;

  private TableDefinition definition;

  /**
   * Gets or Sets mode
   */
  public enum ModeEnum {
    CREATE("Create"),
    
    EXIST_OK("ExistOk"),
    
    OVERWRITE("Overwrite");

    private String value;

    ModeEnum(String value) {
      this.value = value;
    }

    @JsonValue
    public String getValue() {
      return value;
    }

    @Override
    public String toString() {
      return String.valueOf(value);
    }

    @JsonCreator
    public static ModeEnum fromValue(String value) {
      for (ModeEnum b : ModeEnum.values()) {
        if (b.value.equals(value)) {
          return b;
        }
      }
      throw new IllegalArgumentException("Unexpected value '" + value + "'");
    }
  }

  private ModeEnum mode;

  public CreateTableRequest() {
    super();
  }

  /**
   * Constructor with only required parameters
   */
  public CreateTableRequest(String name, TableDefinition definition) {
    this.name = name;
    this.definition = definition;
  }

  public CreateTableRequest name(String name) {
    this.name = name;
    return this;
  }

  /**
   * Get name
   * @return name
   */
  @NotNull 
  @Schema(name = "name", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("name")
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public CreateTableRequest definition(TableDefinition definition) {
    this.definition = definition;
    return this;
  }

  /**
   * Get definition
   * @return definition
   */
  @NotNull @Valid 
  @Schema(name = "definition", requiredMode = Schema.RequiredMode.REQUIRED)
  @JsonProperty("definition")
  public TableDefinition getDefinition() {
    return definition;
  }

  public void setDefinition(TableDefinition definition) {
    this.definition = definition;
  }

  public CreateTableRequest mode(ModeEnum mode) {
    this.mode = mode;
    return this;
  }

  /**
   * Get mode
   * @return mode
   */
  
  @Schema(name = "mode", requiredMode = Schema.RequiredMode.NOT_REQUIRED)
  @JsonProperty("mode")
  public ModeEnum getMode() {
    return mode;
  }

  public void setMode(ModeEnum mode) {
    this.mode = mode;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CreateTableRequest createTableRequest = (CreateTableRequest) o;
    return Objects.equals(this.name, createTableRequest.name) &&
        Objects.equals(this.definition, createTableRequest.definition) &&
        Objects.equals(this.mode, createTableRequest.mode);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, definition, mode);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class CreateTableRequest {\n");
    sb.append("    name: ").append(toIndentedString(name)).append("\n");
    sb.append("    definition: ").append(toIndentedString(definition)).append("\n");
    sb.append("    mode: ").append(toIndentedString(mode)).append("\n");
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

