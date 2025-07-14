/*
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.example.bigquery;

// [START bigquery_enable_otel_tracing]
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetInfo;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class EnableOpenTelemetryTracing {
  // Maps Span names to their Span IDs.
  private static final Map<String, String> OTEL_SPAN_IDS = new HashMap<>();

  // Create a SpanExporter to determine how to handle captured Span data.
  // See more at https://opentelemetry.io/docs/languages/java/sdk/#spanexporter
  private static class SampleSpanExporter
      implements io.opentelemetry.sdk.trace.export.SpanExporter {

    // TODO(developer): Replace export output before running the sample.
    @Override
    public CompletableResultCode export(Collection<SpanData> collection) {
      // Export data. This data can be sent out of process via netowork calls, though
      // for this example local data structures are used.
      if (collection.isEmpty()) {
        // No span data was collected.
        return CompletableResultCode.ofFailure();
      }

      for (SpanData data : collection) {
        OTEL_SPAN_IDS.put(data.getName(), data.getSpanId());
      }
      return CompletableResultCode.ofSuccess();
    }

    // TODO(developer): Replace these functions to suit your needs.
    @Override
    public CompletableResultCode flush() {
      // Export any data that has been queued up but not yet exported.
      return CompletableResultCode.ofSuccess();
    }

    @Override
    public CompletableResultCode shutdown() {
      // Shut down the exporter and clean up any resources.
      return CompletableResultCode.ofSuccess();
    }
  }

  public static void main(String[] args) {
    // TODO(developer): Replace Tracer name
    final String tracerName = "Sample Tracer";
    enableOpenTelemetry(tracerName);
  }

  public static void enableOpenTelemetry(String tracerName) {
    // Create TracerProvider using the custom SpanExporter.
    SdkTracerProvider tracerProvider =
        SdkTracerProvider.builder()
            .addSpanProcessor(SimpleSpanProcessor.create(new SampleSpanExporter()))
            .setSampler(Sampler.alwaysOn())
            .build();

    // Create global OpenTelemetry instance using the TracerProvider.
    OpenTelemetry otel =
        OpenTelemetrySdk.builder().setTracerProvider(tracerProvider).build();

    // Create Tracer instance from the global OpenTelemetry object. Tracers are used to create
    // Spans. There can be multiple Tracers in a global OpenTelemetry instance.
    Tracer tracer = otel.getTracer(tracerName);

    // Create BigQuery client to trace. EnableOpenTelemetryTracing and OpenTelemetryTracer must
    // be set to enable tracing.
    BigQueryOptions otelOptions =
        BigQueryOptions.newBuilder()
            .setEnableOpenTelemetryTracing(true)
            .setOpenTelemetryTracer(tracer)
            .build();
    BigQuery bigquery = otelOptions.getService();

    // Create dataset.
    final String datasetId = "sampleDatasetId";
    DatasetInfo info = DatasetInfo.newBuilder(datasetId).build();
    Dataset dataset = bigquery.create(info);

    if (OTEL_SPAN_IDS.containsKey("com.google.cloud.bigquery.BigQuery.createDataset")) {
      System.out.println("createDataset Span was captured!");
    }

    bigquery.delete(datasetId);
  }
}
// [END bigquery_enable_otel_tracing]
