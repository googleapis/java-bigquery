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
import com.google.common.annotations.VisibleForTesting;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * HttpRequestInitializer that wraps a delegate initializer, intercepts all HTTP requests, adds
 * OpenTelemetry tracing and then invokes delegate interceptor.
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

  @VisibleForTesting static final String HTTP_RPC_SYSTEM_NAME = "http";
  private static final String REDACTED_VALUE = "REDACTED";
  private static final Set<String> SENSITIVE_QUERY_KEYS =
      Collections.unmodifiableSet(
          new HashSet<>(Arrays.asList("AWSAccessKeyId", "Signature", "sig", "X-Goog-Signature")));

  private final HttpRequestInitializer delegate;
  private final Tracer tracer;
  private final String clientRootUrl;

  public HttpTracingRequestInitializer(
      HttpRequestInitializer delegate, Tracer tracer, String clientRootUrl) {
    this.delegate = delegate;
    this.tracer = tracer;
    this.clientRootUrl = clientRootUrl;
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

    Span span = createHttpTraceSpan(httpMethod, url, host, port);

    // Wrap the existing response interceptor
    HttpResponseInterceptor originalInterceptor = request.getResponseInterceptor();
    request.setResponseInterceptor(
        response -> {
          if (span.isRecording()) {
            try {
              int statusCode = response.getStatusCode();
              addCommonResponseAttributesToSpan(request, response, span, httpMethod, statusCode);
              addSuccessResponseToSpan(response, span, statusCode);
              if (originalInterceptor != null) {
                originalInterceptor.interceptResponse(response);
              }
            } finally {
              span.end();
            }
          } else if (originalInterceptor != null) {
            originalInterceptor.interceptResponse(response);
          }
        });

    // Wrap the existing unsuccessful response handler
    HttpUnsuccessfulResponseHandler originalHandler = request.getUnsuccessfulResponseHandler();
    request.setUnsuccessfulResponseHandler(
        (request1, response, supportsRetry) -> {
          int statusCode = response.getStatusCode();
          addCommonResponseAttributesToSpan(request, response, span, httpMethod, statusCode);
          addErrorResponseToSpan(response, span, statusCode);
          try {
            if (originalHandler != null) {
              return originalHandler.handleResponse(request1, response, supportsRetry);
            }
            return false;
          } catch (IOException e) {
            addExceptionToSpan(e, span);
            throw e;
          } finally {
            if (span.isRecording()) {
              span.end();
            }
          }
        });
  }

  /** Initial HTTP trace span creation with basic attributes from request */
  private Span createHttpTraceSpan(String httpMethod, String url, String host, Integer port) {
    // TODO: Determine span name: {method} {url.template} or {method}
    Span span =
        BigQueryTelemetryTracer.newSpanBuilder(tracer, httpMethod)
            .setAttribute(HTTP_REQUEST_METHOD, httpMethod)
            .setAttribute(URL_FULL, sanitizeUrlFull(url))
            .setAttribute(BigQueryTelemetryTracer.SERVER_ADDRESS, host)
            .setAttribute(URL_DOMAIN, resolveUrlDomain(host))
            .setAttribute(BigQueryTelemetryTracer.RPC_SYSTEM_NAME, HTTP_RPC_SYSTEM_NAME)
            .startSpan();

    // TODO: add url template && resource name
    if (port != null && port > 0) {
      span.setAttribute(BigQueryTelemetryTracer.SERVER_PORT, port.longValue());
    }
    return span;
  }

  private String resolveUrlDomain(String requestHost) {
    if (clientRootUrl != null) {
      try {
        String configuredHost = new GenericUrl(clientRootUrl).getHost();
        if (configuredHost != null && !configuredHost.isEmpty()) {
          return configuredHost;
        }
      } catch (IllegalArgumentException ex) {
        // Ignore malformed configured root URL and fall back to request host.
      }
    }
    return requestHost;
  }

  private static void addCommonResponseAttributesToSpan(
      HttpRequest request, HttpResponse response, Span span, String httpMethod, int statusCode) {
    // This is called after we get a response as sometimes the request body size isn't available
    // before the response is received.
    addRequestBodySizeToSpan(request, span);
    checkForUpdatedRequestMethod(response, httpMethod, span);
    setResponseBodySize(response, span);
    span.setAttribute(HTTP_RESPONSE_STATUS_CODE, statusCode);
  }

  private static void addSuccessResponseToSpan(HttpResponse response, Span span, int statusCode) {
    if (statusCode >= 400) {
      addErrorResponseToSpan(response, span, statusCode);
    } else {
      span.setStatus(StatusCode.OK);
    }
  }

  private static void addExceptionToSpan(IOException e, Span span) {
    span.recordException(e);
    String message = e.getMessage();
    String statusMessage = message != null ? message : e.getClass().getName();
    span.setAttribute(BigQueryTelemetryTracer.EXCEPTION_TYPE, e.getClass().getName());
    span.setAttribute(BigQueryTelemetryTracer.ERROR_TYPE, e.getClass().getSimpleName());
    span.setAttribute(BigQueryTelemetryTracer.STATUS_MESSAGE, statusMessage);
    span.setStatus(StatusCode.ERROR, statusMessage);
  }

  private static void addErrorResponseToSpan(HttpResponse response, Span span, int statusCode) {
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

  private static void addRequestBodySizeToSpan(HttpRequest request, Span span) {
    Long requestBodySize = null;
    try {
      HttpContent content = request.getContent();

      if (content != null) {
        requestBodySize = content.getLength();
      }
    } catch (Exception e) {
      // Ignore - body size not available
    }
    if (requestBodySize != null) {
      span.setAttribute(HTTP_REQUEST_BODY_SIZE, requestBodySize);
    }
  }

  private static void setResponseBodySize(HttpResponse response, Span span) {
    try {
      long contentLength = response.getHeaders().getContentLength();
      if (contentLength > 0) {
        span.setAttribute(HTTP_RESPONSE_BODY_SIZE, contentLength);
      }
    } catch (Exception e) {
      // Ignore - body size not available
    }
  }

  private static void checkForUpdatedRequestMethod(
      HttpResponse response, String httpMethod, Span span) {
    String actualMethod = response.getRequest().getRequestMethod();
    if (actualMethod != null && httpMethod == null) {
      span.updateName(actualMethod);
      span.setAttribute(HTTP_REQUEST_METHOD, actualMethod);
    }
  }

  @VisibleForTesting
  static String sanitizeUrlFull(String url) {
    try {
      URI uri = new URI(url);
      String sanitizedUserInfo =
          uri.getRawUserInfo() != null ? REDACTED_VALUE + ":" + REDACTED_VALUE : null;
      String sanitizedQuery = redactSensitiveQueryValues(uri.getRawQuery());
      URI sanitizedUri =
          new URI(
              uri.getScheme(),
              sanitizedUserInfo,
              uri.getHost(),
              uri.getPort(),
              uri.getRawPath(),
              sanitizedQuery,
              uri.getRawFragment());
      return sanitizedUri.toString();
    } catch (URISyntaxException | IllegalArgumentException ex) {
      return url;
    }
  }

  private static String redactSensitiveQueryValues(String rawQuery) {
    if (rawQuery == null || rawQuery.isEmpty()) {
      return rawQuery;
    }

    String[] params = rawQuery.split("&", -1);
    for (int i = 0; i < params.length; i++) {
      String param = params[i];
      int equalsIndex = param.indexOf('=');
      String key = equalsIndex >= 0 ? param.substring(0, equalsIndex) : param;
      if (SENSITIVE_QUERY_KEYS.contains(key)) {
        params[i] = key + "=" + REDACTED_VALUE;
      }
    }

    StringBuilder redactedQuery = new StringBuilder();
    for (int i = 0; i < params.length; i++) {
      if (i > 0) {
        redactedQuery.append('&');
      }
      redactedQuery.append(params[i]);
    }
    return redactedQuery.toString();
  }
}
