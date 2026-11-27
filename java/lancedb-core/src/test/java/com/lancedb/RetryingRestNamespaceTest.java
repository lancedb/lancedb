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
package com.lancedb;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.RestNamespace;
import org.lance.namespace.errors.InternalException;
import org.lance.namespace.errors.InvalidInputException;
import org.lance.namespace.errors.ServiceUnavailableException;
import org.lance.namespace.errors.ThrottlingException;
import org.lance.namespace.model.CreateNamespaceRequest;
import org.lance.namespace.model.InsertIntoTableRequest;
import org.lance.namespace.model.ListTablesRequest;
import org.lance.namespace.model.ListTablesResponse;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Mock HTTP coverage for namespace throttle retries.
 *
 * <p>The server speaks the namespace error body Lance's REST client classifies: code 21 is
 * throttling (HTTP 429) and code 17 is service unavailable (HTTP 503). A 429 with a plain-text body
 * is not a namespace error. The transport reports it as an internal error that still names status
 * 429, and that path is retried. A bare 503 is not.
 */
public class RetryingRestNamespaceTest {
  private HttpServer server;
  private final AtomicInteger calls = new AtomicInteger();
  private final Deque<Reply> replies = new ArrayDeque<Reply>();
  private ThrottleRetry.Sleeper originalSleeper;

  @BeforeEach
  public void setUp() throws IOException {
    originalSleeper = ThrottleRetry.sleeper;
    ThrottleRetry.sleeper = millis -> {};
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/",
        exchange -> {
          calls.incrementAndGet();
          drain(exchange.getRequestBody());
          Reply reply = replies.removeFirst();
          byte[] body = reply.body.getBytes(StandardCharsets.UTF_8);
          if (reply.retryAfter != null) {
            exchange.getResponseHeaders().set("Retry-After", reply.retryAfter);
          }
          exchange.sendResponseHeaders(reply.status, body.length);
          try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
          }
        });
    server.start();
  }

  @AfterEach
  public void tearDown() {
    ThrottleRetry.sleeper = originalSleeper;
    if (server != null) {
      server.stop(0);
    }
  }

  @Test
  public void retries429Then200() throws Exception {
    enqueue(429, 21, "Too many requests");
    enqueue(200, "{\"tables\":[\"events\"]}");

    ListTablesResponse response = namespace().listTables(listTables());

    assertTrue(response.getTables().contains("events"));
    assertEquals(2, calls.get());
  }

  @Test
  public void doesNotRetry400() {
    enqueue(400, 13, "bad request");

    LanceNamespace namespace = namespace();
    assertThrows(InvalidInputException.class, () -> namespace.listTables(listTables()));
    assertEquals(1, calls.get());
  }

  @Test
  public void retriesAreBounded() {
    for (int i = 0; i < 8; i++) {
      enqueue(429, 21, "Too many requests");
    }

    LanceNamespace namespace = namespace();
    assertThrows(ThrottlingException.class, () -> namespace.listTables(listTables()));
    assertEquals(ThrottleRetry.MAX_RETRIES + 1, calls.get());
  }

  @Test
  public void retries503OnARead() throws Exception {
    enqueue(503, 17, "unavailable");
    enqueue(200, "{\"tables\":[\"events\"]}");

    namespace().listTables(listTables());

    assertEquals(2, calls.get());
  }

  @Test
  public void doesNotRetry503OnAWrite() {
    enqueue(503, 17, "unavailable");
    enqueue(200, "{}");

    LanceNamespace namespace = namespace();
    assertThrows(
        ServiceUnavailableException.class,
        () ->
            namespace.createNamespace(
                new CreateNamespaceRequest().id(Collections.singletonList("child"))));
    assertEquals(1, calls.get());
  }

  @Test
  public void retriesPlainText429Then200() throws Exception {
    enqueue(429, "too many requests");
    enqueue(200, "{\"tables\":[\"events\"]}");

    ListTablesResponse response = namespace().listTables(listTables());

    assertTrue(response.getTables().contains("events"));
    assertEquals(2, calls.get());
  }

  @Test
  public void retriesPlainText429OnAppend() throws Exception {
    enqueue(429, "too many requests");
    enqueue(200, "{}");

    namespace()
        .insertIntoTable(
            new InsertIntoTableRequest().id(Collections.singletonList("events")), new byte[] {1});

    assertEquals(2, calls.get());
  }

  @Test
  public void plainText429ExhaustsAndSurfacesTheError() {
    for (int i = 0; i < 8; i++) {
      enqueue(429, "too many requests");
    }

    LanceNamespace namespace = namespace();
    InternalException error =
        assertThrows(InternalException.class, () -> namespace.listTables(listTables()));
    assertTrue(error.getMessage().contains("too many requests"));
    assertEquals(ThrottleRetry.MAX_RETRIES + 1, calls.get());
  }

  @Test
  public void doesNotRetryPlainText503OnAppend() {
    enqueue(503, "unavailable");
    enqueue(200, "{}");

    LanceNamespace namespace = namespace();
    assertThrows(
        InternalException.class,
        () ->
            namespace.insertIntoTable(
                new InsertIntoTableRequest().id(Collections.singletonList("events")),
                new byte[] {1}));
    assertEquals(1, calls.get());
  }

  @Test
  public void retriesThrottledWrite() throws Exception {
    enqueue(429, 21, "Too many requests");
    enqueue(200, "{}");

    namespace()
        .createNamespace(new CreateNamespaceRequest().id(Collections.singletonList("child")));

    assertEquals(2, calls.get());
  }

  @Test
  public void returnedClientStaysARestNamespace() {
    enqueue(200, "{\"tables\":[]}");
    LanceNamespace namespace = namespace();
    assertTrue(namespace instanceof RestNamespace);
    namespace.listTables(listTables());
    assertEquals(1, calls.get());
  }

  @Test
  public void getNativeHandleReturnsTheDelegateHandle() throws Exception {
    Map<String, String> config = new HashMap<String, String>();
    config.put("uri", "http://127.0.0.1:" + server.getAddress().getPort());
    config.put("header.x-api-key", "test-key");
    config.put("header.x-lancedb-database", "test-db");
    RestNamespace delegate = (RestNamespace) LanceNamespace.connect("rest", config, null);
    RetryingRestNamespace wrapped = new RetryingRestNamespace(delegate);
    try {
      assertNotEquals(0L, delegate.getNativeHandle());
      assertEquals(delegate.getNativeHandle(), wrapped.getNativeHandle());
    } finally {
      wrapped.close();
    }
  }

  /**
   * The wrapper is not initialized, so any {@link RestNamespace} method it does not override throws
   * "not initialized" instead of calling the delegate. Interface defaults that {@code
   * RestNamespace} itself leaves unimplemented do not touch the native handle.
   */
  @Test
  public void overridesEveryImplementedRestNamespaceMethod() {
    // Object methods never read the native handle. Do not add a RestNamespace method here.
    List<String> allowlist = Collections.emptyList();
    List<String> missing = new ArrayList<String>();
    for (Method method : nativeClientMethods()) {
      String signature = signature(method);
      if (allowlist.contains(signature)) {
        continue;
      }
      if (!declares(RetryingRestNamespace.class, method)) {
        missing.add(signature);
      }
    }
    assertTrue(
        missing.isEmpty(),
        "RetryingRestNamespace must override these or they throw 'not initialized': " + missing);
    assertFalse(nativeClientMethods().isEmpty());
  }

  /**
   * Public instance methods whose body lives on {@link RestNamespace}. That is every method it
   * declares, including the {@link LanceNamespace} operations it implements. Inherited interface
   * defaults stay on {@link LanceNamespace} and are excluded.
   */
  private static List<Method> nativeClientMethods() {
    Map<String, Method> methods = new LinkedHashMap<String, Method>();
    collectImplemented(methods, RestNamespace.class.getDeclaredMethods());
    for (Method iface : LanceNamespace.class.getMethods()) {
      if (iface.getDeclaringClass() == Object.class || iface.isSynthetic()) {
        continue;
      }
      if (!Modifier.isPublic(iface.getModifiers()) || Modifier.isStatic(iface.getModifiers())) {
        continue;
      }
      try {
        Method implemented =
            RestNamespace.class.getMethod(iface.getName(), iface.getParameterTypes());
        if (implemented.getDeclaringClass() == RestNamespace.class) {
          methods.put(signature(implemented), implemented);
        }
      } catch (NoSuchMethodException e) {
        // Not part of this RestNamespace build.
      }
    }
    return new ArrayList<Method>(methods.values());
  }

  private static void collectImplemented(Map<String, Method> methods, Method[] declared) {
    for (Method method : declared) {
      if (method.isSynthetic() || method.getDeclaringClass() == Object.class) {
        continue;
      }
      int modifiers = method.getModifiers();
      if (!Modifier.isPublic(modifiers) || Modifier.isStatic(modifiers)) {
        continue;
      }
      methods.put(signature(method), method);
    }
  }

  private static boolean declares(Class<?> type, Method method) {
    try {
      Method found = type.getDeclaredMethod(method.getName(), method.getParameterTypes());
      return found.getDeclaringClass() == type;
    } catch (NoSuchMethodException e) {
      return false;
    }
  }

  private static String signature(Method method) {
    StringBuilder signature = new StringBuilder(method.getName()).append('(');
    Class<?>[] parameters = method.getParameterTypes();
    for (int i = 0; i < parameters.length; i++) {
      if (i > 0) {
        signature.append(',');
      }
      signature.append(parameters[i].getSimpleName());
    }
    return signature.append(')').toString();
  }

  private LanceNamespace namespace() {
    return LanceDbNamespaceClientBuilder.newBuilder()
        .apiKey("test-key")
        .database("test-db")
        .endpoint("http://127.0.0.1:" + server.getAddress().getPort())
        .build();
  }

  private static ListTablesRequest listTables() {
    return new ListTablesRequest().id(Collections.singletonList("test"));
  }

  private void enqueue(int status, int code, String message) {
    replies.add(new Reply(status, "{\"code\":" + code + ",\"error\":\"" + message + "\"}", null));
  }

  private void enqueue(int status, String body) {
    replies.add(new Reply(status, body, null));
  }

  private static void drain(InputStream in) throws IOException {
    byte[] buf = new byte[4096];
    while (in.read(buf) != -1) {
      // Consume the request body so the client can finish the exchange.
    }
  }

  private static final class Reply {
    private final int status;
    private final String body;
    private final String retryAfter;

    private Reply(int status, String body, String retryAfter) {
      this.status = status;
      this.body = body;
      this.retryAfter = retryAfter;
    }
  }
}
