package com.google.cloud.bigquery;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.context.Scope;
import java.util.logging.Logger;
import java.time.Instant;
import org.junit.Test;

public class OpenTelemetryTest {
  private static final Logger log = Logger.getLogger(OpenTelemetryTest.class.getName());
  private static OpenTelemetrySdk sdk = null;

  private void fillInSpanData(Tracer tracer) {
    Span childSpan = tracer.spanBuilder("childSpan").setSpanKind(SpanKind.INTERNAL).setAttribute(AttributeKey.stringKey("fillInSpanData-key"), "running fillInSpanData()").startSpan();
    try {
      int result = 0;
      for (int i = 0; i < 5000; ++i) {
        if (i % 2 == 0) {
          result += i;
        }
      }
      childSpan.addEvent("finished loop at: ", Instant.now());
    } finally {
      childSpan.end();
      log.info(childSpan.toString());
    }
  }

  @Test
  public void testOtel() {
    Sampler alwaysOn = Sampler.alwaysOn();
    sdk =
        OpenTelemetrySdk.builder()
            .setTracerProvider(
                SdkTracerProvider.builder()
                    .setSampler(alwaysOn)
                    .build())
            .build();

    Tracer tracer = sdk.getTracer("TracerExample");
    Span parentSpan = tracer.spanBuilder("parentSpan").setNoParent().setSpanKind(SpanKind.INTERNAL).setAttribute(
        AttributeKey.longKey("long-key"), 25L).setAttribute(AttributeKey.booleanKey("bool-key"), true).setAttribute(AttributeKey.stringKey("string-key"), "qwerty").startSpan();

    try (Scope parentScope = parentSpan.makeCurrent()) {
      fillInSpanData(tracer);
    }
    finally {
      parentSpan.end();
      log.info(parentSpan.toString());
    }
  }
}
