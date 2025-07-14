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

// [START bigquery_enable_otel_tracing_with_parent_span]
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

public class EnableOpenTelemetryTracingWithParentSpan {

  // Data structures for captured Span data
  private static final Map<String, Map<AttributeKey<?>, Object>> OTEL_ATTRIBUTES =
      new HashMap<String, Map<AttributeKey<?>, Object>>();
  private static final Map<String, String> OTEL_PARENT_SPAN_IDS = new HashMap<>();
  private static final Map<String, String> OTEL_SPAN_IDS_TO_NAMES = new HashMap<>();

  // Create a SpanExporter to determine how to handle captured Span data.
  // See more at https://opentelemetry.io/docs/languages/java/sdk/#spanexporter
  private static class SampleSpanExporter
      implements io.opentelemetry.sdk.trace.export.SpanExporter {
    @Override
    public CompletableResultCode export(Collection<SpanData> collection) {
      // Export data. This data can be sent out of process via netowork calls, though
      // for this example a local map is used.
      if (collection.isEmpty()) {
        // No span data was collected.
        return CompletableResultCode.ofFailure();
      }
      // TODO(developer): Replace export output before running the sample.
      for (SpanData data : collection) {
        OTEL_ATTRIBUTES.put(data.getName(), data.getAttributes().asMap());
        OTEL_PARENT_SPAN_IDS.put(data.getName(), data.getParentSpanId());
        OTEL_SPAN_IDS_TO_NAMES.put(data.getSpanId(), data.getName());
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
    // TODO(developer): Replace these variables before running the sample.
    String datasetName = "MY_DATASET_NAME";
    enableOpenTelemetryWithParentSpan("Sample Tracer", datasetName);
  }

  public static void enableOpenTelemetryWithParentSpan(String tracerName, String datasetName) {
    // Create TracerProvider using the custom SpanExporter.
    SdkTracerProvider tracerProvider =
        SdkTracerProvider.builder()
            .addSpanProcessor(SimpleSpanProcessor.create(new SampleSpanExporter()))
            .setSampler(Sampler.alwaysOn())
            .build();

    // Create global OpenTelemetry instance using the TracerProvider.
    OpenTelemetry otel =
        OpenTelemetrySdk.builder().setTracerProvider(tracerProvider).buildAndRegisterGlobal();

    // Create Tracer instance from the global OpenTelemetry object. Tracers are used to create
    // Spans. There can be multiple Tracers in a global OpenTelemetry instance.
    // TODO(developer): Replace Tracer name
    Tracer tracer = otel.getTracer(tracerName);

    // Create BigQuery client to trace. EnableOpenTelemetryTracing and OpenTelemetryTracer must
    // be set to enable tracing.
    BigQueryOptions otelOptions =
        BigQueryOptions.newBuilder()
            .setEnableOpenTelemetryTracing(true)
            .setOpenTelemetryTracer(tracer)
            .build();
    BigQuery bigquery = otelOptions.getService();

    // Create the root parent Span. setNoParent() ensures that it is a parent Span.
    // TODO(developer): Replace Span and attribute names.
    Span parentSpan =
        tracer
            .spanBuilder("Sample Parent Span")
            .setNoParent()
            .setAttribute("sample-parent-attribute", "sample-parent-value")
            .startSpan();

    // Wrap nested functions in try-catch-finally block to pass on the Span Context.
    try (Scope parentScope = parentSpan.makeCurrent()) {
      createDataset(bigquery, tracer, datasetName);
    } finally {
      // finally block ensures that Spans are cleaned up properly.
      parentSpan.end();

      if (OTEL_ATTRIBUTES.containsKey("Sample Parent Span")
          && OTEL_ATTRIBUTES
                  .get("Sample Parent Span")
                  .get(AttributeKey.stringKey("sample-parent-attribute"))
              == "sample-parent-value") {
        System.out.println("Parent Span was captured!");
      } else {
        System.out.println("Parent Span was not captured!");
      }
      if (OTEL_ATTRIBUTES.containsKey("Sample Child Span")
          && OTEL_ATTRIBUTES
                  .get("Sample Child Span")
                  .get(AttributeKey.stringKey("sample-child-attribute"))
              == "sample-child-value") {
        System.out.println("Child Span was captured!");
      } else {
        System.out.println("Child Span was not captured!");
      }
      if (OTEL_ATTRIBUTES.containsKey("Sample Child Span")
          && OTEL_ATTRIBUTES
                  .get("Sample Child Span")
                  .get(AttributeKey.stringKey("sample-child-attribute"))
              == "sample-child-value") {
        System.out.println("Child Span was captured!");
      } else {
        System.out.println("Child Span was not captured!");
      }
      String childSpanParentId = OTEL_PARENT_SPAN_IDS.get("Sample Child Span");
      String parentSpanId = OTEL_SPAN_IDS_TO_NAMES.get(childSpanParentId);
      if (parentSpanId == "Sample Parent Span") {
        System.out.println("Sample Child Span is the child of Sample Parent Span!");
      }
    }
  }

  public static void createDataset(BigQuery bigquery, Tracer tracer, String datasetId) {
    // Parent Span Context is passed on here.
    Span childSpan =
        tracer
            .spanBuilder("Sample Child Span")
            .setNoParent()
            .setAttribute("sample-child-attribute", "sample-child-value")
            .startSpan();

    DatasetInfo info = DatasetInfo.newBuilder(datasetId).build();

    Dataset dataset = bigquery.create(info);
  }
}
// [END bigquery_enable_otel_tracing_with_parent_span]
