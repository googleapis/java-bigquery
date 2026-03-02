/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.bigquery.spi.v2;

import static org.junit.jupiter.api.Assertions.assertEquals;


import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.cloud.bigquery.RetryContext;
import com.sun.net.httpserver.HttpServer;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration test for HTTP tracing with real HTTP transport and server.
 * This test verifies that OpenTelemetry tracing works correctly with actual network calls.
 */
public class HttpTracingIntegrationTest {

  private InMemorySpanExporter spanExporter;
    private HttpTracingRequestInitializer initializer;
  private HttpServer testServer;
  private int serverPort;

  @BeforeEach
  public void setUp() throws IOException {
    // Set up OpenTelemetry with in-memory exporter
    spanExporter = InMemorySpanExporter.create();
    SdkTracerProvider tracerProvider =
        SdkTracerProvider.builder()
            .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
            .build();
    OpenTelemetrySdk openTelemetry =
        OpenTelemetrySdk.builder().setTracerProvider(tracerProvider).build();
      Tracer tracer = openTelemetry.getTracer("test-tracer");
    initializer = new HttpTracingRequestInitializer(null, tracer);

    // Start a test HTTP server
    testServer = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    serverPort = testServer.getAddress().getPort();
    testServer.start();
  }

  @AfterEach
  public void tearDown() {
    if (testServer != null) {
      testServer.stop(0);
    }
    RetryContext.clearRetryAttempt();
  }

  @Test
  public void testHttpTracingWithRealServer() throws IOException {
    testServer.createContext("/api/test", exchange -> {
      String response = "{\"status\": \"ok\"}";
      exchange.getResponseHeaders().add("Content-Type", "application/json");
      exchange.sendResponseHeaders(200, response.getBytes().length);
      try (OutputStream os = exchange.getResponseBody()) {
        os.write(response.getBytes());
      }
    });

    NetHttpTransport transport = new NetHttpTransport();
    HttpRequestFactory requestFactory = transport.createRequestFactory(initializer);
    HttpRequest request = requestFactory.buildGetRequest(
        new GenericUrl("http://localhost:" + serverPort + "/api/test"));

    HttpResponse response = request.execute();
    assertEquals(200, response.getStatusCode());
    response.disconnect();

    List<SpanData> spans = spanExporter.getFinishedSpanItems();
    assertEquals(1, spans.size());
    SpanData span = spans.get(0);
    assertEquals("GET", span.getAttributes().get(AttributeKey.stringKey("http.request.method")));
    assertEquals(200L, span.getAttributes().get(AttributeKey.longKey("http.response.status_code")));
    assertEquals("localhost", span.getAttributes().get(AttributeKey.stringKey("server.address")));
  }
}
