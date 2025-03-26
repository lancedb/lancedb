/**
 * NOTE: This class is auto generated by OpenAPI Generator (https://openapi-generator.tech) (7.12.0).
 * https://openapi-generator.tech
 * Do not edit the class manually.
 */
package com.lancedb.catalog.adapter.api;

import com.lancedb.catalog.adapter.model.CreateDatabaseRequest;
import com.lancedb.catalog.adapter.model.DatabaseList;
import com.lancedb.catalog.adapter.model.DatabaseMetadata;
import com.lancedb.catalog.adapter.model.RenameRequest;
import io.swagger.v3.oas.annotations.ExternalDocumentation;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.Parameters;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.context.request.NativeWebRequest;
import org.springframework.web.multipart.MultipartFile;

import javax.validation.Valid;
import javax.validation.constraints.*;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Generated;

@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", date = "2025-03-26T15:36:49.834244+08:00[Asia/Shanghai]", comments = "Generator version: 7.12.0")
@Validated
@Tag(name = "Database", description = "the Database API")
public interface DatabaseApi {

    default Optional<NativeWebRequest> getRequest() {
        return Optional.empty();
    }

    /**
     * DELETE /v1/databases/{dbName} : Delete database
     *
     * @param dbName  (required)
     * @return Database deleted (status code 204)
     */
    @Operation(
        operationId = "v1DatabasesDbNameDelete",
        summary = "Delete database",
        tags = { "Database" },
        responses = {
            @ApiResponse(responseCode = "204", description = "Database deleted")
        }
    )
    @RequestMapping(
        method = RequestMethod.DELETE,
        value = "/v1/databases/{dbName}"
    )
    
    default ResponseEntity<Void> v1DatabasesDbNameDelete(
        @Parameter(name = "dbName", description = "", required = true, in = ParameterIn.PATH) @PathVariable("dbName") String dbName
    ) {
        return new ResponseEntity<>(HttpStatus.NOT_IMPLEMENTED);

    }


    /**
     * GET /v1/databases/{dbName} : Get database metadata
     *
     * @param dbName  (required)
     * @return Database metadata (status code 200)
     */
    @Operation(
        operationId = "v1DatabasesDbNameGet",
        summary = "Get database metadata",
        tags = { "Database" },
        responses = {
            @ApiResponse(responseCode = "200", description = "Database metadata", content = {
                @Content(mediaType = "application/json", schema = @Schema(implementation = DatabaseMetadata.class))
            })
        }
    )
    @RequestMapping(
        method = RequestMethod.GET,
        value = "/v1/databases/{dbName}",
        produces = { "application/json" }
    )
    
    default ResponseEntity<DatabaseMetadata> v1DatabasesDbNameGet(
        @Parameter(name = "dbName", description = "", required = true, in = ParameterIn.PATH) @PathVariable("dbName") String dbName
    ) {
        getRequest().ifPresent(request -> {
            for (MediaType mediaType: MediaType.parseMediaTypes(request.getHeader("Accept"))) {
                if (mediaType.isCompatibleWith(MediaType.valueOf("application/json"))) {
                    String exampleString = "{ \"tableCount\" : 0, \"name\" : \"name\" }";
                    ApiUtil.setExampleResponse(request, "application/json", exampleString);
                    break;
                }
            }
        });
        return new ResponseEntity<>(HttpStatus.NOT_IMPLEMENTED);

    }


    /**
     * PUT /v1/databases/{dbName} : Rename database
     *
     * @param dbName  (required)
     * @param renameRequest  (required)
     * @return Database renamed (status code 204)
     */
    @Operation(
        operationId = "v1DatabasesDbNamePut",
        summary = "Rename database",
        tags = { "Database" },
        responses = {
            @ApiResponse(responseCode = "204", description = "Database renamed")
        }
    )
    @RequestMapping(
        method = RequestMethod.PUT,
        value = "/v1/databases/{dbName}",
        consumes = { "application/json" }
    )
    
    default ResponseEntity<Void> v1DatabasesDbNamePut(
        @Parameter(name = "dbName", description = "", required = true, in = ParameterIn.PATH) @PathVariable("dbName") String dbName,
        @Parameter(name = "RenameRequest", description = "", required = true) @Valid @RequestBody RenameRequest renameRequest
    ) {
        return new ResponseEntity<>(HttpStatus.NOT_IMPLEMENTED);

    }


    /**
     * DELETE /v1/databases : Delete all databases
     *
     * @return All databases deleted (status code 204)
     */
    @Operation(
        operationId = "v1DatabasesDelete",
        summary = "Delete all databases",
        tags = { "Database" },
        responses = {
            @ApiResponse(responseCode = "204", description = "All databases deleted")
        }
    )
    @RequestMapping(
        method = RequestMethod.DELETE,
        value = "/v1/databases"
    )
    
    default ResponseEntity<Void> v1DatabasesDelete(
        
    ) {
        return new ResponseEntity<>(HttpStatus.NOT_IMPLEMENTED);

    }


    /**
     * GET /v1/databases : List databases
     *
     * @param startAfter  (optional)
     * @param limit  (optional)
     * @return Database list response (status code 200)
     */
    @Operation(
        operationId = "v1DatabasesGet",
        summary = "List databases",
        tags = { "Database" },
        responses = {
            @ApiResponse(responseCode = "200", description = "Database list response", content = {
                @Content(mediaType = "application/json", schema = @Schema(implementation = DatabaseList.class))
            })
        }
    )
    @RequestMapping(
        method = RequestMethod.GET,
        value = "/v1/databases",
        produces = { "application/json" }
    )
    
    default ResponseEntity<DatabaseList> v1DatabasesGet(
        @Parameter(name = "startAfter", description = "", in = ParameterIn.QUERY) @Valid @RequestParam(value = "startAfter", required = false) Optional<String> startAfter,
        @Parameter(name = "limit", description = "", in = ParameterIn.QUERY) @Valid @RequestParam(value = "limit", required = false) Optional<Integer> limit
    ) {
        getRequest().ifPresent(request -> {
            for (MediaType mediaType: MediaType.parseMediaTypes(request.getHeader("Accept"))) {
                if (mediaType.isCompatibleWith(MediaType.valueOf("application/json"))) {
                    String exampleString = "{ \"databases\" : [ \"databases\", \"databases\" ], \"nextToken\" : \"nextToken\" }";
                    ApiUtil.setExampleResponse(request, "application/json", exampleString);
                    break;
                }
            }
        });
        return new ResponseEntity<>(HttpStatus.NOT_IMPLEMENTED);

    }


    /**
     * POST /v1/databases : Create new database
     *
     * @param createDatabaseRequest  (required)
     * @return Database created (status code 201)
     */
    @Operation(
        operationId = "v1DatabasesPost",
        summary = "Create new database",
        tags = { "Database" },
        responses = {
            @ApiResponse(responseCode = "201", description = "Database created")
        }
    )
    @RequestMapping(
        method = RequestMethod.POST,
        value = "/v1/databases",
        consumes = { "application/json" }
    )
    
    default ResponseEntity<Void> v1DatabasesPost(
        @Parameter(name = "CreateDatabaseRequest", description = "", required = true) @Valid @RequestBody CreateDatabaseRequest createDatabaseRequest
    ) {
        return new ResponseEntity<>(HttpStatus.NOT_IMPLEMENTED);

    }

}
