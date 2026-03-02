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

import com.google.api.client.http.*;
import com.google.api.core.InternalApi;
import com.google.cloud.bigquery.telemetry.BigQueryTelemetryTracer;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;

/**
 * HttpRequestInitializer that wraps a delegate initializer, intercepts all HTTP requests,
 * adds OpenTelemetry tracing and then invokes delegate interceptor.
 */
@InternalApi
public class HttpTracingRequestInitializer implements HttpRequestInitializer {

  // HTTP Specific Telemetry Attributes
  public static final AttributeKey<String> HTTP_REQUEST_METHOD =
          AttributeKey.stringKey("http.request.method");
  public static final AttributeKey<String> URL_FULL = AttributeKey.stringKey("url.full");
  public static final AttributeKey<String> URL_TEMPLATE = AttributeKey.stringKey("url.template");
  public static final AttributeKey<String> URL_DOMAIN = AttributeKey.stringKey("url.domain");
  public static final AttributeKey<Long> HTTP_RESPONSE_STATUS_CODE =
          AttributeKey.longKey("http.response.status_code");
  public static final AttributeKey<Long> HTTP_REQUEST_RESEND_COUNT =
          AttributeKey.longKey("http.request.resend_count");
  public static final AttributeKey<Long> HTTP_REQUEST_BODY_SIZE =
          AttributeKey.longKey("http.request.body.size");
  public static final AttributeKey<Long> HTTP_RESPONSE_BODY_SIZE =
          AttributeKey.longKey("http.response.body.size");

  private final HttpRequestInitializer delegate;
  private final Tracer tracer;
  private static final String BIGQUERY_DOMAIN = "bigquery.googleapis.com";

  public HttpTracingRequestInitializer(HttpRequestInitializer delegate, Tracer tracer) {
    this.delegate = delegate;
    this.tracer = tracer;
  }

  @Override
  public void initialize(HttpRequest request) throws IOException {
    if (delegate != null) {
      delegate.initialize(request);
    }

    if (tracer == null) {
      return;
    }

    String httpMethod = request.getRequestMethod();
    String url = request.getUrl().build();
    String host = request.getUrl().getHost();
    Integer port = request.getUrl().getPort();

    Long requestBodySize = getRequestBodySize(request);

    Span span = getSpan(httpMethod, url, host, port, requestBodySize);

    // Wrap the existing response interceptor
    HttpResponseInterceptor originalInterceptor = request.getResponseInterceptor();
    request.setResponseInterceptor(
            response -> {
              try {
                addSuccessResponseToSpan(response, httpMethod, span);
                if (originalInterceptor != null) {
                    originalInterceptor.interceptResponse(response);
                }
              } finally {
                span.end();
              }
            });

// Wrap the existing unsuccessful response handler
HttpUnsuccessfulResponseHandler originalHandler = request.getUnsuccessfulResponseHandler();
request.setUnsuccessfulResponseHandler(
        (request1, response, supportsRetry) -> {
          addErrorResponseToSpan(response, span);
          try {
            if (originalHandler != null) {
              return originalHandler.handleResponse(request1, response, supportsRetry);
            }
            return false;
          } catch (IOException e) {
            addExceptionToSpan(e, span);
            throw e;
          } finally {
            span.end();
          }
        });
  }

  private static void addExceptionToSpan(IOException e, Span span) {
    span.recordException(e);
    span.setAttribute(BigQueryTelemetryTracer.EXCEPTION_TYPE, e.getClass().getName());
    span.setAttribute(BigQueryTelemetryTracer.ERROR_TYPE, e.getClass().getSimpleName());
    span.setAttribute(BigQueryTelemetryTracer.STATUS_MESSAGE, e.getMessage() != null ? e.getMessage() : e.getClass().getName());
    span.setStatus(StatusCode.ERROR, e.getMessage());
  }

  private static void addErrorResponseToSpan(HttpResponse response, Span span) {
    int statusCode = response.getStatusCode();
    span.setAttribute(HTTP_RESPONSE_STATUS_CODE, statusCode);
    String errorMessage = "HTTP " + statusCode;
    try {
      String statusMessage = response.getStatusMessage();
      if (statusMessage != null && !statusMessage.isEmpty()) {
        errorMessage = statusMessage;
      }
    } catch (Exception ex) {
      // Ignore
    }
    span.setAttribute(BigQueryTelemetryTracer.STATUS_MESSAGE, errorMessage);
    span.setAttribute(BigQueryTelemetryTracer.ERROR_TYPE, String.valueOf(statusCode));
    span.setStatus(StatusCode.ERROR, errorMessage);
  }

  private static void addSuccessResponseToSpan(HttpResponse response, String httpMethod, Span span) {
    String actualMethod = response.getRequest().getRequestMethod();
    if (actualMethod != null && httpMethod == null) {
      span.updateName(actualMethod);
      span.setAttribute(HTTP_REQUEST_METHOD, actualMethod);
    }
    int statusCode = response.getStatusCode();
    span.setAttribute(HTTP_RESPONSE_STATUS_CODE, statusCode);
    try {
      long contentLength = response.getHeaders().getContentLength();
      if (contentLength > 0) {
        span.setAttribute(HTTP_RESPONSE_BODY_SIZE, contentLength);
      }
    } catch (Exception e) {
      // Ignore - body size not available
    }
    if (statusCode >= 400) {
      addErrorResponseToSpan(response, span);
    } else {
      span.setStatus(StatusCode.OK);
    }
  }

  private Span getSpan(String httpMethod, String url, String host, Integer port, Long requestBodySize) {
    //TODO: Determine span name: {method} {url.template} or {method}
    Span span =
        BigQueryTelemetryTracer.newSpanBuilder(tracer, httpMethod)
            // OpenTelemetry semantic convention attributes
            .setAttribute(HTTP_REQUEST_METHOD, httpMethod)
            .setAttribute(URL_FULL, url)
            .setAttribute(BigQueryTelemetryTracer.SERVER_ADDRESS, host)
            .setAttribute(URL_DOMAIN, BIGQUERY_DOMAIN)
            .setAttribute(BigQueryTelemetryTracer.RPC_SYSTEM_NAME , "http")
            .startSpan();

    // TODO: add url template && resource name
    if (port != null && port > 0) {
      span.setAttribute(BigQueryTelemetryTracer.SERVER_PORT, port.longValue());
    }
    if (requestBodySize != null && requestBodySize > 0) {
      span.setAttribute(HTTP_REQUEST_BODY_SIZE, requestBodySize);
    }
    return span;
  }

  private static @Nullable Long getRequestBodySize(HttpRequest request) {
    Long requestBodySize = null;
    try {
      HttpContent content = request.getContent();
      if (content != null) {
        requestBodySize = content.getLength();
      }
    } catch (Exception e) {
      // Ignore - body size not available
    }
    return requestBodySize;
  }
}
