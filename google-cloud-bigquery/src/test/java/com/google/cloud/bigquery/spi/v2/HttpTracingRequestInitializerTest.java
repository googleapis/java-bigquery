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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.api.client.http.ByteArrayContent;
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
import com.google.cloud.bigquery.telemetry.BigQueryTelemetryTracer;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for TracingHttpRequestInitializer */
public class HttpTracingRequestInitializerTest {

  private static final String BASE_URL =
      "https://bigquery.googleapis.com:443/bigquery/v2/projects/test/datasets";
  private static final String REQUEST_METHOD_GET = "GET";
  private static final String REQUEST_METHOD_POST = "POST";
  private static final String BIGQUERY_DOMAIN = "bigquery.googleapis.com";
  private static final String CLIENT_ROOT_URL = "https://bigquery.googleapis.com:443";

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
    initializer = new HttpTracingRequestInitializer(null, tracer, CLIENT_ROOT_URL);
  }

  @Test
  public void testSuccessResponseAttributesAreSet() throws IOException {
    HttpTransport transport = createTransport(200, null, 123L);
    HttpRequest request =
        buildPostRequest(
            transport,
            initializer,
            BASE_URL,
            new ByteArrayContent("application/json", new byte[] {1}));

    HttpResponse response = request.execute();
    response.disconnect();

    List<SpanData> spans = spanExporter.getFinishedSpanItems();
    assertEquals(1, spans.size());

    SpanData span = spans.get(0);
    assertEquals(
        200, span.getAttributes().get(HttpTracingRequestInitializer.HTTP_RESPONSE_STATUS_CODE));
    assertEquals(1, span.getAttributes().get(HttpTracingRequestInitializer.HTTP_REQUEST_BODY_SIZE));
    assertEquals(
        123, span.getAttributes().get(HttpTracingRequestInitializer.HTTP_RESPONSE_BODY_SIZE));
    verifyGeneralSpanData(span, REQUEST_METHOD_POST);
  }

  @Test
  public void testErrorAttributesAreSetOn404() throws IOException {
    HttpTransport transport = createTransport(404, "Not Found", null);
    HttpRequest request = buildGetRequest(transport, initializer, BASE_URL);

    try {
      HttpResponse response = request.execute();
      response.disconnect();
    } catch (Exception e) {
      // Expected
    }

    List<SpanData> spans = spanExporter.getFinishedSpanItems();
    assertEquals(1, spans.size());

    SpanData span = spans.get(0);
    assertEquals(
        404, span.getAttributes().get(HttpTracingRequestInitializer.HTTP_RESPONSE_STATUS_CODE));
    assertEquals("404", span.getAttributes().get(BigQueryTelemetryTracer.ERROR_TYPE));
    assertNotNull(span.getAttributes().get(BigQueryTelemetryTracer.STATUS_MESSAGE));
    verifyGeneralSpanData(span, REQUEST_METHOD_GET);
  }

  @Test
  public void testExceptionAttributesAreSetWhenOriginalUnsuccessfulHandlerThrowsIOException()
      throws IOException {
    String handlerFailureMessage = "handler failure";
    String serverErrorMessage = "Internal Server Error";

    HttpRequestInitializer delegateInitializer =
        request ->
            request.setUnsuccessfulResponseHandler(
                (request1, response, supportsRetry) -> {
                  throw new IOException(handlerFailureMessage);
                });
    HttpTracingRequestInitializer tracingInitializer =
        new HttpTracingRequestInitializer(delegateInitializer, tracer, CLIENT_ROOT_URL);

    HttpTransport transport = createTransport(500, serverErrorMessage, null);
    HttpRequest request = buildGetRequest(transport, tracingInitializer, BASE_URL);
    request.setThrowExceptionOnExecuteError(false);

    IOException thrown = assertThrows(IOException.class, request::execute);
    assertEquals(handlerFailureMessage, thrown.getMessage());

    List<SpanData> spans = spanExporter.getFinishedSpanItems();
    assertEquals(1, spans.size());

    SpanData span = spans.get(0);
    assertEquals(
        500, span.getAttributes().get(HttpTracingRequestInitializer.HTTP_RESPONSE_STATUS_CODE));
    assertEquals(
        IOException.class.getName(),
        span.getAttributes().get(BigQueryTelemetryTracer.EXCEPTION_TYPE));
    assertEquals(
        IOException.class.getSimpleName(),
        span.getAttributes().get(BigQueryTelemetryTracer.ERROR_TYPE));
    assertEquals(
        handlerFailureMessage, span.getAttributes().get(BigQueryTelemetryTracer.STATUS_MESSAGE));
    assertTrue(span.getEvents().stream().anyMatch(event -> "exception".equals(event.getName())));
    verifyGeneralSpanData(span, REQUEST_METHOD_GET);
  }

  @Test
  public void testDelegateInitializerIsCalledOnSuccessResponse() throws IOException {
    HttpRequestInitializer delegateInitializer = mock(HttpRequestInitializer.class);
    HttpTracingRequestInitializer tracingInitializer =
        new HttpTracingRequestInitializer(delegateInitializer, tracer, CLIENT_ROOT_URL);

    HttpTransport transport = createTransport(200, null, null);
    HttpRequest request = buildGetRequest(transport, tracingInitializer, BASE_URL);

    HttpResponse response = request.execute();
    response.disconnect();

    verify(delegateInitializer, times(1)).initialize(any(HttpRequest.class));
  }

  @Test
  public void testDelegateInitializerIsCalledOnErrorResponse() throws IOException {
    HttpRequestInitializer delegateInitializer = mock(HttpRequestInitializer.class);
    HttpTracingRequestInitializer tracingInitializer =
        new HttpTracingRequestInitializer(delegateInitializer, tracer, CLIENT_ROOT_URL);

    HttpTransport transport = createTransport(404, "Not Found", null);
    HttpRequest request = buildGetRequest(transport, tracingInitializer, BASE_URL);

    try {
      HttpResponse response = request.execute();
      response.disconnect();
    } catch (Exception e) {
      // Expected - 404 might throw
    }

    verify(delegateInitializer, times(1)).initialize(any(HttpRequest.class));
  }

  @Test
  public void testUrlDomainUsesClientRootUrlHost() throws IOException {
    HttpTracingRequestInitializer tracingInitializer =
        new HttpTracingRequestInitializer(null, tracer, "https://example-client-endpoint.test/");

    HttpTransport transport = createTransport(200, null, null);
    HttpRequest request = buildGetRequest(transport, tracingInitializer, BASE_URL);

    HttpResponse response = request.execute();
    response.disconnect();

    List<SpanData> spans = spanExporter.getFinishedSpanItems();
    assertEquals(1, spans.size());
    assertEquals(
        "example-client-endpoint.test",
        spans.get(0).getAttributes().get(HttpTracingRequestInitializer.URL_DOMAIN));
  }

  private static HttpTransport createTransport(
      int statusCode, String reasonPhrase, Long contentLength) {
    return new MockHttpTransport() {
      @Override
      public LowLevelHttpRequest buildRequest(String method, String url) {
        return new MockLowLevelHttpRequest() {
          @Override
          public LowLevelHttpResponse execute() {
            MockLowLevelHttpResponse response = new MockLowLevelHttpResponse();
            response.setStatusCode(statusCode);
            if (reasonPhrase != null) {
              response.setReasonPhrase(reasonPhrase);
            }
            if (contentLength != null) {
              response.addHeader("Content-Length", String.valueOf(contentLength));
            }
            return response;
          }
        };
      }
    };
  }

  private static HttpRequest buildGetRequest(
      HttpTransport transport, HttpRequestInitializer requestInitializer, String url)
      throws IOException {
    HttpRequestFactory requestFactory = transport.createRequestFactory(requestInitializer);
    return requestFactory.buildGetRequest(new GenericUrl(url));
  }

  private static HttpRequest buildPostRequest(
      HttpTransport transport,
      HttpRequestInitializer requestInitializer,
      String url,
      ByteArrayContent content)
      throws IOException {
    HttpRequestFactory requestFactory = transport.createRequestFactory(requestInitializer);
    return requestFactory.buildPostRequest(new GenericUrl(url), content);
  }

  private void verifyGeneralSpanData(SpanData span, String requestMethod) {
    assertEquals(
        requestMethod, span.getAttributes().get(HttpTracingRequestInitializer.HTTP_REQUEST_METHOD));
    assertEquals(BIGQUERY_DOMAIN, span.getAttributes().get(BigQueryTelemetryTracer.SERVER_ADDRESS));
    assertEquals(443, span.getAttributes().get(BigQueryTelemetryTracer.SERVER_PORT));
    assertEquals(
        BIGQUERY_DOMAIN, span.getAttributes().get(HttpTracingRequestInitializer.URL_DOMAIN));
    assertEquals(BASE_URL, span.getAttributes().get(HttpTracingRequestInitializer.URL_FULL));
    assertEquals(
        BigQueryTelemetryTracer.BQ_GCP_CLIENT_SERVICE,
        span.getAttributes().get(BigQueryTelemetryTracer.GCP_CLIENT_SERVICE));
    assertEquals(
        BigQueryTelemetryTracer.BQ_GCP_CLIENT_REPO,
        span.getAttributes().get(BigQueryTelemetryTracer.GCP_CLIENT_REPO));
    assertEquals(
        BigQueryTelemetryTracer.BQ_GCP_CLIENT_ARTIFACT,
        span.getAttributes().get(BigQueryTelemetryTracer.GCP_CLIENT_ARTIFACT));
    assertEquals(
        BigQueryTelemetryTracer.BQ_GCP_CLIENT_LANGUAGE,
        span.getAttributes().get(BigQueryTelemetryTracer.GCP_CLIENT_LANGUAGE));
    assertEquals(
        HttpTracingRequestInitializer.HTTP_RPC_SYSTEM_NAME,
        span.getAttributes().get(BigQueryTelemetryTracer.RPC_SYSTEM_NAME));
  }
}
