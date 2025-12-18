/*
 * Copyright 2025 Google LLC
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

package com.google.cloud.bigquery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.api.core.ApiClock;
import com.google.api.core.NanoClock;
import com.google.api.gax.retrying.BasicResultRetryAlgorithm;
import com.google.api.gax.retrying.ResultRetryAlgorithm;
import com.google.api.gax.retrying.RetrySettings;
import io.opentelemetry.api.trace.Tracer;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.threeten.bp.Duration;

public class BigQueryRetryHelperTest {

  private static final ApiClock CLOCK = NanoClock.getDefaultClock();

  @Test
  public void runWithRetries_happyPath_noRetries_success() {
    AtomicInteger calls = new AtomicInteger(0);

    Callable<String> ok =
        () -> {
          calls.incrementAndGet();
          return "OK";
        };

    String result =
        BigQueryRetryHelper.runWithRetries(
            ok,
            retrySettingsMaxAttempts(3),
            retryAlgorithm(),
            CLOCK,
            defaultRetryConfig(),
            /* isOpenTelemetryEnabled= */ false,
            /* openTelemetryTracer= */ (Tracer) null);

    assertEquals("OK", result);
    assertEquals(1, calls.get(), "Callable should be invoked exactly once");
  }

  @Test
  public void runWithRetries_oneFail_thenSuccess_succeeds() {
    AtomicInteger calls = new AtomicInteger(0);

    RuntimeException first = new RuntimeException("A");

    Callable<String> flaky =
        () -> {
          int n = calls.incrementAndGet();
          if (n == 1) {
            throw first;
          }
          return "OK";
        };

    String result =
        BigQueryRetryHelper.runWithRetries(
            flaky,
            retrySettingsMaxAttempts(3),
            retryAlgorithm(),
            CLOCK,
            defaultRetryConfig(),
            /* isOpenTelemetryEnabled= */ false,
            /* openTelemetryTracer= */ null);

    assertEquals("OK", result);
    assertEquals(2, calls.get(), "Expected exactly 2 calls (1 fail + 1 success)");
  }

  @Test
  public void runWithRetries_twoFails_thenSuccess_succeedsWithinThreshold() {
    AtomicInteger calls = new AtomicInteger(0);

    RuntimeException exA = new RuntimeException("A");
    RuntimeException exB = new RuntimeException("B");

    Callable<String> flaky =
        () -> {
          int n = calls.incrementAndGet();
          if (n == 1) {
            throw exA;
          }
          if (n == 2) {
            throw exB;
          }
          return "OK";
        };

    String result =
        BigQueryRetryHelper.runWithRetries(
            flaky,
            retrySettingsMaxAttempts(3),
            retryAlgorithm(),
            CLOCK,
            defaultRetryConfig(),
            /* isOpenTelemetryEnabled= */ false,
            /* openTelemetryTracer= */ null);

    assertEquals("OK", result);
    assertEquals(3, calls.get(), "Expected 3 calls (A fail, B fail, then success)");
  }

  @Test
  public void runWithRetries_threeFails_threshold3_throws_withSuppressedHistory() {
    AtomicInteger calls = new AtomicInteger(0);

    RuntimeException exA = new RuntimeException("A");
    RuntimeException exB = new RuntimeException("B");
    RuntimeException exC = new RuntimeException("C");

    Callable<String> alwaysFail3Times =
        () -> {
          int n = calls.incrementAndGet();
          if (n == 1) {
            throw exA;
          }
          if (n == 2) {
            throw exB;
          }
          throw exC; // 3rd attempt fails and should be terminal at maxAttempts=3
        };

    try {
      BigQueryRetryHelper.runWithRetries(
          alwaysFail3Times,
          retrySettingsMaxAttempts(3),
          retryAlgorithm(),
          CLOCK,
          defaultRetryConfig(),
          /* isOpenTelemetryEnabled= */ false,
          /* openTelemetryTracer= */ null);
      fail("Expected BigQueryRetryHelperException");
    } catch (BigQueryRetryHelper.BigQueryRetryHelperException e) {
      assertEquals(3, calls.get(), "Expected exactly 3 attempts");

      Throwable terminal = e.getCause();
      assertNotNull(terminal);

      // Terminal cause should be exactly Exception C (identity check).
      assertSame(exC, terminal);

      // Suppressed should contain exactly A and B (identity + order).
      Throwable[] suppressed = terminal.getSuppressed();
      assertEquals(2, suppressed.length, "Expected 2 suppressed exceptions (A,B)");
      assertSame(exA, suppressed[0]);
      assertSame(exB, suppressed[1]);
    }
  }

  private RetrySettings retrySettingsMaxAttempts(int maxAttempts) {
    // Keep delays tiny so tests run fast.
    return RetrySettings.newBuilder()
        .setMaxAttempts(maxAttempts)
        .setInitialRetryDelay(Duration.ofMillis(1))
        .setRetryDelayMultiplier(1.0)
        .setMaxRetryDelay(Duration.ofMillis(5))
        .setInitialRpcTimeout(Duration.ofMillis(50))
        .setRpcTimeoutMultiplier(1.0)
        .setMaxRpcTimeout(Duration.ofMillis(50))
        .setTotalTimeout(Duration.ofSeconds(2))
        .build();
  }

  private BigQueryRetryConfig defaultRetryConfig() {
    return BigQueryRetryConfig.newBuilder().build();
  }

  @SuppressWarnings("unchecked")
  private <V> ResultRetryAlgorithm<V> retryAlgorithm() {
    return new BasicResultRetryAlgorithm<>();
  }
}
