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
  // Maps Span names to their attribute maps.
  private static final Map<String, Map<AttributeKey<?>, Object>> OTEL_ATTRIBUTES =
      new HashMap<String, Map<AttributeKey<?>, Object>>();
  // Maps Span names to their parent Span IDs.
  private static final Map<String, String> OTEL_PARENT_SPAN_IDS = new HashMap<>();
  // Maps Span IDs to their Span names.
  private static final Map<String, String> OTEL_SPAN_IDS_TO_NAMES = new HashMap<>();

  // Create a SpanExporter to determine how to handle captured Span data.
  // See more at https://opentelemetry.io/docs/languages/java/sdk/#spanexporter
  private static class SampleSpanExporter
      implements io.opentelemetry.sdk.trace.export.SpanExporter {
    @Override
    public CompletableResultCode export(Collection<SpanData> collection) {
      // Export data. This data can be sent out of process via netowork calls, though
      // for this example local data structures are used.
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
    final String tracerName = "Sample Tracer";
    enableOpenTelemetryWithParentSpan(tracerName);
  }

  public static void enableOpenTelemetryWithParentSpan(String tracerName) {
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

    // TODO(developer): Replace Span and attribute names.
    final String parentSpanName = "Sample Parent Span";
    final String attributeKey = "sample-parent-attribute";
    final String attributeValue = "sample-parent-value";
    final String datasetId = "sampleDatasetId";

    // Create the root parent Span. setNoParent() ensures that it is a parent Span with a Span ID
    // of 0.
    Span parentSpan =
        tracer
            .spanBuilder(parentSpanName)
            .setNoParent()
            .setAttribute(attributeKey, attributeValue)
            .startSpan();

    // The Span Context is automatically passed on to any functions called within the scope of the
    // try block. parentSpan.makeCurrent() sets parentSpan to be the parent of any Spans created in
    // this scope, or the scope of any functions called within this scope.
    try (Scope parentScope = parentSpan.makeCurrent()) {
      DatasetInfo info = DatasetInfo.newBuilder(datasetId).build();
      Dataset dataset = bigquery.create(info);
    } finally {
      // finally block ensures that Spans are cleaned up properly.
      parentSpan.end();

      // Unpack attribute maps to get attribute keys and values.
      Map<AttributeKey<?>, Object> parentSpanAttributes = OTEL_ATTRIBUTES.get(parentSpanName);
      Object parentSpanAttributeValue =
          parentSpanAttributes.get(AttributeKey.stringKey(attributeKey));
      if (parentSpanAttributeValue == attributeValue) {
        System.out.println("Parent Span was captured!");
      } else {
        System.out.println("Parent Span was not captured!");
      }

      String childSpanParentId =
          OTEL_PARENT_SPAN_IDS.get("com.google.cloud.bigquery.BigQuery.createDataset");
      String parentSpanId = OTEL_SPAN_IDS_TO_NAMES.get(childSpanParentId);
      if (OTEL_SPAN_IDS_TO_NAMES.get(childSpanParentId) == parentSpanName) {
        System.out.println("createDataset is the child of Sample Parent Span!");
      } else {
        System.out.println("createDataset is not the child of Parent!");
      }

      bigquery.delete(datasetId);
    }
  }

  public static void createDataset(BigQuery bigquery, String datasetId, Tracer tracer) {
    // Parent Span Context is automatically passed on here. childSpan's parent Span is set to be
    // parentSpan.
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
