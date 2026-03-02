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

package com.google.cloud.bigquery.telemetry;

import com.google.api.core.InternalApi;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;

/**
 * General BigQuery Telemetry class that stores generic telemetry attributes and any associated
 * logic to calculate.
 */
@InternalApi
public final class BigQueryTelemetryTracer {

  private BigQueryTelemetryTracer() {}

  // Common GCP Attributes
  public static final AttributeKey<String> GCP_CLIENT_SERVICE =
      AttributeKey.stringKey("gcp.client.service");
  public static final AttributeKey<String> GCP_CLIENT_VERSION =
      AttributeKey.stringKey("gcp.client.version");
  public static final AttributeKey<String> GCP_CLIENT_REPO =
      AttributeKey.stringKey("gcp.client.repo");
  public static final AttributeKey<String> GCP_CLIENT_ARTIFACT =
      AttributeKey.stringKey("gcp.client.artifact");
  public static final AttributeKey<String> GCP_CLIENT_LANGUAGE =
      AttributeKey.stringKey("gcp.client.language");
  public static final AttributeKey<String> GCP_RESOURCE_NAME =
      AttributeKey.stringKey("gcp.resource.name");
  public static final AttributeKey<String> RPC_SYSTEM_NAME =
      AttributeKey.stringKey("rpc.system.name");

  // Common Error Attributes
  public static final AttributeKey<String> ERROR_TYPE = AttributeKey.stringKey("error.type");
  public static final AttributeKey<String> EXCEPTION_TYPE =
      AttributeKey.stringKey("exception.type");
  public static final AttributeKey<String> STATUS_MESSAGE =
      AttributeKey.stringKey("status.message");

  // Common Server Attributes
  public static final AttributeKey<String> SERVER_ADDRESS =
      AttributeKey.stringKey("server.address");
  public static final AttributeKey<Long> SERVER_PORT = AttributeKey.longKey("server.port");

  public static SpanBuilder newSpanBuilder(Tracer tracer, String spanName) {
    return tracer
        .spanBuilder(spanName)
        .setSpanKind(SpanKind.CLIENT)
        .setAttribute(GCP_CLIENT_SERVICE, "bigquery")
        .setAttribute(GCP_CLIENT_REPO, "googleapis/java-bigquery")
        .setAttribute(GCP_CLIENT_ARTIFACT, "google-cloud-bigquery")
        .setAttribute(GCP_CLIENT_LANGUAGE, "java");
    // TODO: add version
  }
}
