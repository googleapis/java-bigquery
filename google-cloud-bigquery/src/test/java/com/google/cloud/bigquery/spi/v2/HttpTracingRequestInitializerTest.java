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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.LowLevelHttpRequest;
import com.google.api.client.http.LowLevelHttpResponse;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.cloud.bigquery.RetryContext;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for TracingHttpRequestInitializer */
public class HttpTracingRequestInitializerTest {

  private InMemorySpanExporter spanExporter;
  private Tracer tracer;
  private HttpTracingRequestInitializer initializer;

  @BeforeEach
  public void setUp() {
    spanExporter = InMemorySpanExporter.create();
    SdkTracerProvider tracerProvider =
        SdkTracerProvider.builder()
            .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
            .build();
    OpenTelemetrySdk openTelemetry =
        OpenTelemetrySdk.builder().setTracerProvider(tracerProvider).build();
    tracer = openTelemetry.getTracer("test-tracer");
    initializer = new HttpTracingRequestInitializer(null, tracer);
  }

  @AfterEach
  public void tearDown() {
    // Clean up thread-local retry context after each test
    RetryContext.clearRetryAttempt();
  }

  @Test
  public void testSuccessResponseAttributesAreSet() throws IOException {
    HttpTransport transport =
        new MockHttpTransport() {
          @Override
          public LowLevelHttpRequest buildRequest(String method, String url) {
            return new MockLowLevelHttpRequest() {
              @Override
              public LowLevelHttpResponse execute() {
                MockLowLevelHttpResponse response = new MockLowLevelHttpResponse();
                response.setStatusCode(200);
                return response;
              }
            };
          }
        };

    HttpRequestFactory requestFactory = transport.createRequestFactory(initializer);
    String urlString = "https://bigquery.googleapis.com:443/bigquery/v2/projects/test/datasets";
    HttpRequest request = requestFactory.buildGetRequest(new GenericUrl(urlString));

    HttpResponse response = request.execute();
    response.disconnect();

    List<SpanData> spans = spanExporter.getFinishedSpanItems();
    assertEquals(1, spans.size());

    SpanData span = spans.get(0);
    assertEquals("GET", span.getAttributes().get(AttributeKey.stringKey("http.request.method")));
    assertEquals(
        "bigquery.googleapis.com",
        span.getAttributes().get(AttributeKey.stringKey("server.address")));
    assertEquals(Long.valueOf(443L), span.getAttributes().get(AttributeKey.longKey("server.port")));
    assertEquals(
        "bigquery.googleapis.com", span.getAttributes().get(AttributeKey.stringKey("url.domain")));
    assertEquals(urlString, span.getAttributes().get(AttributeKey.stringKey("url.full")));
    assertEquals(
        Long.valueOf(200L),
        span.getAttributes().get(AttributeKey.longKey("http.response.status_code")));
    assertEquals(
        "bigquery", span.getAttributes().get(AttributeKey.stringKey("gcp.client.service")));
    assertEquals(
        "googleapis/java-bigquery",
        span.getAttributes().get(AttributeKey.stringKey("gcp.client.repo")));
    assertEquals(
        "google-cloud-bigquery",
        span.getAttributes().get(AttributeKey.stringKey("gcp.client.artifact")));
    assertEquals("java", span.getAttributes().get(AttributeKey.stringKey("gcp.client.language")));
  }

  @Test
  public void testErrorAttributesAreSetOn404() throws IOException {
    HttpTransport transport =
        new MockHttpTransport() {
          @Override
          public LowLevelHttpRequest buildRequest(String method, String url) {
            return new MockLowLevelHttpRequest() {
              @Override
              public LowLevelHttpResponse execute() {
                MockLowLevelHttpResponse response = new MockLowLevelHttpResponse();
                response.setStatusCode(404);
                response.setReasonPhrase("Not Found");
                return response;
              }
            };
          }
        };

    HttpRequestFactory requestFactory = transport.createRequestFactory(initializer);
    HttpRequest request =
        requestFactory.buildGetRequest(
            new GenericUrl(
                "https://bigquery.googleapis.com/bigquery/v2/projects/test/datasets/notfound"));

    try {
      HttpResponse response = request.execute();
      response.disconnect();
    } catch (Exception e) {
      // Expected - 404 might throw
    }

    List<SpanData> spans = spanExporter.getFinishedSpanItems();
    assertEquals(1, spans.size());

    SpanData span = spans.get(0);
    assertEquals(
        Long.valueOf(404L),
        span.getAttributes().get(AttributeKey.longKey("http.response.status_code")));
    assertEquals("404", span.getAttributes().get(AttributeKey.stringKey("error.type")));
    assertNotNull(span.getAttributes().get(AttributeKey.stringKey("status.message")));
  }

  @Test
  public void testExceptionAttributesAreSetWhenOriginalUnsuccessfulHandlerThrowsIOException()
      throws IOException {
    HttpRequestInitializer delegateInitializer =
        request ->
            request.setUnsuccessfulResponseHandler(
                (request1, response, supportsRetry) -> {
                  throw new IOException("handler failure");
                });
    HttpTracingRequestInitializer tracingInitializer =
        new HttpTracingRequestInitializer(delegateInitializer, tracer);

    HttpTransport transport =
        new MockHttpTransport() {
          @Override
          public LowLevelHttpRequest buildRequest(String method, String url) {
            return new MockLowLevelHttpRequest() {
              @Override
              public LowLevelHttpResponse execute() {
                MockLowLevelHttpResponse response = new MockLowLevelHttpResponse();
                response.setStatusCode(500);
                response.setReasonPhrase("Internal Server Error");
                return response;
              }
            };
          }
        };

    HttpRequestFactory requestFactory = transport.createRequestFactory(tracingInitializer);
    HttpRequest request =
        requestFactory.buildGetRequest(
            new GenericUrl("https://bigquery.googleapis.com/bigquery/v2/projects/test/datasets"));
    request.setThrowExceptionOnExecuteError(false);

    IOException thrown = assertThrows(IOException.class, request::execute);
    assertEquals("handler failure", thrown.getMessage());

    List<SpanData> spans = spanExporter.getFinishedSpanItems();
    assertEquals(1, spans.size());

    SpanData span = spans.get(0);
    assertEquals(
        Long.valueOf(500L),
        span.getAttributes().get(AttributeKey.longKey("http.response.status_code")));
    assertEquals(
        IOException.class.getName(),
        span.getAttributes().get(AttributeKey.stringKey("exception.type")));
    assertEquals(
        IOException.class.getSimpleName(),
        span.getAttributes().get(AttributeKey.stringKey("error.type")));
    assertEquals(
        "handler failure", span.getAttributes().get(AttributeKey.stringKey("status.message")));
  }

  @Test
  public void testExceptionIsRecordedWhenOriginalUnsuccessfulHandlerThrowsIOException()
      throws IOException {
    HttpRequestInitializer delegateInitializer =
        request ->
            request.setUnsuccessfulResponseHandler(
                (request1, response, supportsRetry) -> {
                  throw new IOException("handler failure");
                });
    HttpTracingRequestInitializer tracingInitializer =
        new HttpTracingRequestInitializer(delegateInitializer, tracer);

    HttpTransport transport =
        new MockHttpTransport() {
          @Override
          public LowLevelHttpRequest buildRequest(String method, String url) {
            return new MockLowLevelHttpRequest() {
              @Override
              public LowLevelHttpResponse execute() {
                MockLowLevelHttpResponse response = new MockLowLevelHttpResponse();
                response.setStatusCode(500);
                response.setReasonPhrase("Internal Server Error");
                return response;
              }
            };
          }
        };

    HttpRequestFactory requestFactory = transport.createRequestFactory(tracingInitializer);
    HttpRequest request =
        requestFactory.buildGetRequest(
            new GenericUrl("https://bigquery.googleapis.com/bigquery/v2/projects/test/datasets"));
    request.setThrowExceptionOnExecuteError(false);

    assertThrows(IOException.class, request::execute);

    List<SpanData> spans = spanExporter.getFinishedSpanItems();
    assertEquals(1, spans.size());
    SpanData span = spans.get(0);

    assertTrue(span.getEvents().stream().anyMatch(event -> "exception".equals(event.getName())));
    assertTrue(
        span.getEvents().stream()
            .filter(event -> "exception".equals(event.getName()))
            .anyMatch(
                event ->
                    "handler failure"
                        .equals(
                            event
                                .getAttributes()
                                .get(AttributeKey.stringKey("exception.message")))));
  }

  @Test
  public void testDelegateInitializerIsCalledOnSuccessResponse() throws IOException {
    HttpRequestInitializer delegateInitializer = mock(HttpRequestInitializer.class);
    HttpTracingRequestInitializer tracingInitializer =
        new HttpTracingRequestInitializer(delegateInitializer, tracer);

    HttpTransport transport =
        new MockHttpTransport() {
          @Override
          public LowLevelHttpRequest buildRequest(String method, String url) {
            return new MockLowLevelHttpRequest() {
              @Override
              public LowLevelHttpResponse execute() {
                MockLowLevelHttpResponse response = new MockLowLevelHttpResponse();
                response.setStatusCode(200);
                return response;
              }
            };
          }
        };

    HttpRequestFactory requestFactory = transport.createRequestFactory(tracingInitializer);
    HttpRequest request =
        requestFactory.buildGetRequest(
            new GenericUrl("https://bigquery.googleapis.com/bigquery/v2/projects/test/datasets"));

    HttpResponse response = request.execute();
    response.disconnect();

    verify(delegateInitializer, times(1)).initialize(any(HttpRequest.class));
  }

  @Test
  public void testDelegateInitializerIsCalledOnErrorResponse() throws IOException {
    HttpRequestInitializer delegateInitializer = mock(HttpRequestInitializer.class);
    HttpTracingRequestInitializer tracingInitializer =
        new HttpTracingRequestInitializer(delegateInitializer, tracer);

    HttpTransport transport =
        new MockHttpTransport() {
          @Override
          public LowLevelHttpRequest buildRequest(String method, String url) {
            return new MockLowLevelHttpRequest() {
              @Override
              public LowLevelHttpResponse execute() {
                MockLowLevelHttpResponse response = new MockLowLevelHttpResponse();
                response.setStatusCode(404);
                response.setReasonPhrase("Not Found");
                return response;
              }
            };
          }
        };

    HttpRequestFactory requestFactory = transport.createRequestFactory(tracingInitializer);
    HttpRequest request =
        requestFactory.buildGetRequest(
            new GenericUrl(
                "https://bigquery.googleapis.com/bigquery/v2/projects/test/datasets/notfound"));

    try {
      HttpResponse response = request.execute();
      response.disconnect();
    } catch (Exception e) {
      // Expected - 404 might throw
    }

    verify(delegateInitializer, times(1)).initialize(any(HttpRequest.class));
  }
}
