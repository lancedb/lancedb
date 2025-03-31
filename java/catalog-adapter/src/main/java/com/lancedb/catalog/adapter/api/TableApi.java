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
package com.lancedb.catalog.adapter.api;

import com.lancedb.catalog.adapter.model.CreateTableRequest;
import com.lancedb.catalog.adapter.model.TableMetadata;
import com.lancedb.catalog.adapter.model.TableUpdate;
import com.lancedb.catalog.adapter.model.V1DatabasesDbNameTablesTableNameNamePutRequest;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.context.request.NativeWebRequest;

import javax.annotation.Generated;
import javax.validation.Valid;
import javax.validation.constraints.*;

import java.util.List;
import java.util.Optional;

@Generated(
    value = "org.openapitools.codegen.languages.SpringCodegen",
    comments = "Generator version: 7.12.0")
@Validated
@Tag(name = "Table", description = "the Table API")
public interface TableApi {

  default Optional<NativeWebRequest> getRequest() {
    return Optional.empty();
  }

  /**
   * DELETE /v1/databases/{dbName}/tables : Delete all tables
   *
   * @param dbName (required)
   * @return All tables deleted (status code 204)
   */
  @Operation(
      operationId = "v1DatabasesDbNameTablesDelete",
      summary = "Delete all tables",
      tags = {"Table"},
      responses = {@ApiResponse(responseCode = "204", description = "All tables deleted")})
  @RequestMapping(method = RequestMethod.DELETE, value = "/v1/databases/{dbName}/tables")
  default ResponseEntity<Void> v1DatabasesDbNameTablesDelete(
      @Parameter(name = "dbName", description = "", required = true, in = ParameterIn.PATH)
          @PathVariable("dbName")
          String dbName) {
    return new ResponseEntity<>(HttpStatus.NOT_IMPLEMENTED);
  }

  /**
   * GET /v1/databases/{dbName}/tables : List tables in database
   *
   * @param dbName (required)
   * @param startAfter (optional)
   * @param limit (optional)
   * @return Table list response (status code 200)
   */
  @Operation(
      operationId = "v1DatabasesDbNameTablesGet",
      summary = "List tables in database",
      tags = {"Table"},
      responses = {
        @ApiResponse(
            responseCode = "200",
            description = "Table list response",
            content = {
              @Content(
                  mediaType = "application/json",
                  array = @ArraySchema(schema = @Schema(implementation = String.class)))
            })
      })
  @RequestMapping(
      method = RequestMethod.GET,
      value = "/v1/databases/{dbName}/tables",
      produces = {"application/json"})
  default ResponseEntity<List<String>> v1DatabasesDbNameTablesGet(
      @Parameter(name = "dbName", description = "", required = true, in = ParameterIn.PATH)
          @PathVariable("dbName")
          String dbName,
      @Parameter(name = "startAfter", description = "", in = ParameterIn.QUERY)
          @Valid
          @RequestParam(value = "startAfter", required = false)
          Optional<String> startAfter,
      @Parameter(name = "limit", description = "", in = ParameterIn.QUERY)
          @Valid
          @RequestParam(value = "limit", required = false)
          Optional<Integer> limit) {
    getRequest()
        .ifPresent(
            request -> {
              for (MediaType mediaType : MediaType.parseMediaTypes(request.getHeader("Accept"))) {
                if (mediaType.isCompatibleWith(MediaType.valueOf("application/json"))) {
                  String exampleString = "[ \"\", \"\" ]";
                  ApiUtil.setExampleResponse(request, "application/json", exampleString);
                  break;
                }
              }
            });
    return new ResponseEntity<>(HttpStatus.NOT_IMPLEMENTED);
  }

  /**
   * POST /v1/databases/{dbName}/tables : Create new table
   *
   * @param dbName (required)
   * @param createTableRequest (required)
   * @return Table created (status code 201)
   */
  @Operation(
      operationId = "v1DatabasesDbNameTablesPost",
      summary = "Create new table",
      tags = {"Table"},
      responses = {@ApiResponse(responseCode = "201", description = "Table created")})
  @RequestMapping(
      method = RequestMethod.POST,
      value = "/v1/databases/{dbName}/tables",
      consumes = {"application/json"})
  default ResponseEntity<Void> v1DatabasesDbNameTablesPost(
      @Parameter(name = "dbName", description = "", required = true, in = ParameterIn.PATH)
          @PathVariable("dbName")
          String dbName,
      @Parameter(name = "CreateTableRequest", description = "", required = true) @Valid @RequestBody
          CreateTableRequest createTableRequest) {
    return new ResponseEntity<>(HttpStatus.NOT_IMPLEMENTED);
  }

  /**
   * DELETE /v1/databases/{dbName}/tables/{tableName} : Delete a specific table
   *
   * @param dbName (required)
   * @param tableName (required)
   * @return Table deleted (status code 204)
   */
  @Operation(
      operationId = "v1DatabasesDbNameTablesTableNameDelete",
      summary = "Delete a specific table",
      tags = {"Table"},
      responses = {@ApiResponse(responseCode = "204", description = "Table deleted")})
  @RequestMapping(
      method = RequestMethod.DELETE,
      value = "/v1/databases/{dbName}/tables/{tableName}")
  default ResponseEntity<Void> v1DatabasesDbNameTablesTableNameDelete(
      @Parameter(name = "dbName", description = "", required = true, in = ParameterIn.PATH)
          @PathVariable("dbName")
          String dbName,
      @Parameter(name = "tableName", description = "", required = true, in = ParameterIn.PATH)
          @PathVariable("tableName")
          String tableName) {
    return new ResponseEntity<>(HttpStatus.NOT_IMPLEMENTED);
  }

  /**
   * GET /v1/databases/{dbName}/tables/{tableName} : Get table metadata
   *
   * @param dbName (required)
   * @param tableName (required)
   * @return Table metadata (status code 200)
   */
  @Operation(
      operationId = "v1DatabasesDbNameTablesTableNameGet",
      summary = "Get table metadata",
      tags = {"Table"},
      responses = {
        @ApiResponse(
            responseCode = "200",
            description = "Table metadata",
            content = {
              @Content(
                  mediaType = "application/json",
                  schema = @Schema(implementation = TableMetadata.class))
            })
      })
  @RequestMapping(
      method = RequestMethod.GET,
      value = "/v1/databases/{dbName}/tables/{tableName}",
      produces = {"application/json"})
  default ResponseEntity<TableMetadata> v1DatabasesDbNameTablesTableNameGet(
      @Parameter(name = "dbName", description = "", required = true, in = ParameterIn.PATH)
          @PathVariable("dbName")
          String dbName,
      @Parameter(name = "tableName", description = "", required = true, in = ParameterIn.PATH)
          @PathVariable("tableName")
          String tableName) {
    getRequest()
        .ifPresent(
            request -> {
              for (MediaType mediaType : MediaType.parseMediaTypes(request.getHeader("Accept"))) {
                if (mediaType.isCompatibleWith(MediaType.valueOf("application/json"))) {
                  String exampleString =
                      "{ \"schema\" : \"{}\", \"location\" : \"location\", \"version\" : 0 }";
                  ApiUtil.setExampleResponse(request, "application/json", exampleString);
                  break;
                }
              }
            });
    return new ResponseEntity<>(HttpStatus.NOT_IMPLEMENTED);
  }

  /**
   * PUT /v1/databases/{dbName}/tables/{tableName}/name : Rename table
   *
   * @param dbName (required)
   * @param tableName (required)
   * @param v1DatabasesDbNameTablesTableNameNamePutRequest (optional)
   * @return Table renamed (status code 204)
   */
  @Operation(
      operationId = "v1DatabasesDbNameTablesTableNameNamePut",
      summary = "Rename table",
      tags = {"Table"},
      responses = {@ApiResponse(responseCode = "204", description = "Table renamed")})
  @RequestMapping(
      method = RequestMethod.PUT,
      value = "/v1/databases/{dbName}/tables/{tableName}/name",
      consumes = {"application/json"})
  default ResponseEntity<Void> v1DatabasesDbNameTablesTableNameNamePut(
      @Parameter(name = "dbName", description = "", required = true, in = ParameterIn.PATH)
          @PathVariable("dbName")
          String dbName,
      @Parameter(name = "tableName", description = "", required = true, in = ParameterIn.PATH)
          @PathVariable("tableName")
          String tableName,
      @Parameter(name = "V1DatabasesDbNameTablesTableNameNamePutRequest", description = "")
          @Valid
          @RequestBody(required = false)
          Optional<V1DatabasesDbNameTablesTableNameNamePutRequest>
              v1DatabasesDbNameTablesTableNameNamePutRequest) {
    return new ResponseEntity<>(HttpStatus.NOT_IMPLEMENTED);
  }

  /**
   * PUT /v1/databases/{dbName}/tables/{tableName} : Update table data
   *
   * @param dbName (required)
   * @param tableName (required)
   * @param tableUpdate (optional)
   * @return Update successful (status code 200)
   */
  @Operation(
      operationId = "v1DatabasesDbNameTablesTableNamePut",
      summary = "Update table data",
      tags = {"Table"},
      responses = {@ApiResponse(responseCode = "200", description = "Update successful")})
  @RequestMapping(
      method = RequestMethod.PUT,
      value = "/v1/databases/{dbName}/tables/{tableName}",
      consumes = {"application/json"})
  default ResponseEntity<Void> v1DatabasesDbNameTablesTableNamePut(
      @Parameter(name = "dbName", description = "", required = true, in = ParameterIn.PATH)
          @PathVariable("dbName")
          String dbName,
      @Parameter(name = "tableName", description = "", required = true, in = ParameterIn.PATH)
          @PathVariable("tableName")
          String tableName,
      @Parameter(name = "TableUpdate", description = "") @Valid @RequestBody(required = false)
          Optional<TableUpdate> tableUpdate) {
    return new ResponseEntity<>(HttpStatus.NOT_IMPLEMENTED);
  }
}
