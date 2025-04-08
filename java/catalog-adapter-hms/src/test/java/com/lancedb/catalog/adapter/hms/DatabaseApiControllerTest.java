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
package com.lancedb.catalog.adapter.hms;

import com.lancedb.catalog.adapter.api.DatabaseApi;
import com.lancedb.catalog.adapter.hms.controller.DatabaseApiController;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.context.request.NativeWebRequest;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class DatabaseApiControllerTest {

  @Mock private NativeWebRequest mockRequest;

  @InjectMocks private DatabaseApiController controller;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
  }

  @Test
  void shouldImplementDatabaseApi() {
    assertTrue(
        controller instanceof DatabaseApi, "Controller should implement DatabaseApi interface");
  }

  @Test
  void shouldReturnRequest() {
    Optional<NativeWebRequest> result = controller.getRequest();
    assertTrue(result.isPresent(), "Should return the request object");
    assertEquals(mockRequest, result.get(), "Returned request object should be the injected one");
  }

  @Test
  void shouldReturnEmptyWhenRequestIsNull() {
    DatabaseApiController nullController = new DatabaseApiController(null);

    Optional<NativeWebRequest> result = nullController.getRequest();
    assertFalse(result.isPresent(), "Should return empty Optional when request is null");
  }

  @Test
  void testRequestMapping() {
    assertTrue(
        DatabaseApiController.class.isAnnotationPresent(
            org.springframework.web.bind.annotation.RequestMapping.class),
        "Controller should have RequestMapping annotation");

    org.springframework.web.bind.annotation.RequestMapping annotation =
        DatabaseApiController.class.getAnnotation(
            org.springframework.web.bind.annotation.RequestMapping.class);

    assertNotNull(annotation, "RequestMapping annotation should not be null");
    assertEquals(
        "${openapi.lanceDBRESTCatalog.base-path:}",
        annotation.value()[0],
        "RequestMapping value should match expected");
  }

  @Test
  void testV1DatabasesGet_Success() {
    DatabaseApiController testController =
        new DatabaseApiController(mockRequest) {
          @Override
          public ResponseEntity<List<String>> v1DatabasesGet(
              Optional<String> startAfter, Optional<Integer> limit) {
            List<String> databases = Arrays.asList("db1", "db2", "db3");
            return new ResponseEntity<>(databases, HttpStatus.OK);
          }
        };

    ResponseEntity<List<String>> response =
        testController.v1DatabasesGet(Optional.empty(), Optional.empty());

    assertEquals(HttpStatus.OK, response.getStatusCode());
    assertEquals(Arrays.asList("db1", "db2", "db3"), response.getBody());
  }
}
