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

/** RenameRequest */
@Generated(
    value = "org.openapitools.codegen.languages.SpringCodegen",
    comments = "Generator version: 7.12.0")
public class RenameRequest {

  private String newName;

  public RenameRequest() {
    super();
  }

  /** Constructor with only required parameters */
  public RenameRequest(String newName) {
    this.newName = newName;
  }

  public RenameRequest newName(String newName) {
    this.newName = newName;
    return this;
  }

  /**
   * Get newName
   *
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
    RenameRequest renameRequest = (RenameRequest) o;
    return Objects.equals(this.newName, renameRequest.newName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(newName);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class RenameRequest {\n");
    sb.append("    newName: ").append(toIndentedString(newName)).append("\n");
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
