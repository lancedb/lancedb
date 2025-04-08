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
package com.lancedb.catalog.adapter.hms.controller;

import com.lancedb.catalog.adapter.api.DatabaseApi;
import com.lancedb.catalog.adapter.hms.service.HmsCatalogService;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.context.request.NativeWebRequest;

import javax.annotation.Generated;

import java.util.List;
import java.util.Optional;

@Generated(
    value = "org.openapitools.codegen.languages.SpringCodegen",
    comments = "Generator version: 7.12.0")
@Controller
@RequestMapping("${openapi.lanceDBRESTCatalog.base-path:}")
public class DatabaseApiController implements DatabaseApi {

  private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseApiController.class);

  private final NativeWebRequest request;

  private final HmsCatalogService hmsCatalogService = new HmsCatalogService();

  @Autowired
  public DatabaseApiController(NativeWebRequest request) {
    this.request = request;
  }

  @Override
  public Optional<NativeWebRequest> getRequest() {
    return Optional.ofNullable(request);
  }

  @Override
  public ResponseEntity<List<String>> v1DatabasesGet(
      Optional<String> startAfter, Optional<Integer> limit) {
    try {
      List<String> databases = hmsCatalogService.getDatabases(startAfter, limit);
      return ResponseEntity.ok(databases);
    } catch (TException e) {
      LOGGER.error(e.getMessage(), e);
      return ResponseEntity.status(500)
          .body(List.of("Get database list failed: " + e.getMessage()));
    } catch (InterruptedException e) {
      LOGGER.error(e.getMessage(), e);
      Thread.currentThread().interrupt();
      return ResponseEntity.status(503)
          .body(List.of("Operation was interrupted: " + e.getMessage()));
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
      return ResponseEntity.status(500).body(List.of("Server inner error: " + e.getMessage()));
    }
  }
}
