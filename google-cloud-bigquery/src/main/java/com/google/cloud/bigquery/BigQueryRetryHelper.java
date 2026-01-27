/*
 * Copyright 2021 Google LLC
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

import com.google.api.core.ApiClock;
import com.google.api.gax.retrying.DirectRetryingExecutor;
import com.google.api.gax.retrying.ExponentialRetryAlgorithm;
import com.google.api.gax.retrying.ResultRetryAlgorithm;
import com.google.api.gax.retrying.RetryAlgorithm;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.retrying.RetryingExecutor;
import com.google.api.gax.retrying.RetryingFuture;
import com.google.api.gax.retrying.TimedRetryAlgorithm;
import com.google.cloud.RetryHelper;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BigQueryRetryHelper extends RetryHelper {

  private static final Logger LOG = Logger.getLogger(BigQueryRetryHelper.class.getName());

  public static <V> V runWithRetries(
      Callable<V> callable,
      RetrySettings retrySettings,
      ResultRetryAlgorithm<?> resultRetryAlgorithm,
      ApiClock clock,
      BigQueryRetryConfig bigQueryRetryConfig,
      boolean isOpenTelemetryEnabled,
      Tracer openTelemetryTracer)
      throws RetryHelperException {
    Span runWithRetries = null;
    if (isOpenTelemetryEnabled && openTelemetryTracer != null) {
      runWithRetries =
          openTelemetryTracer
              .spanBuilder("com.google.cloud.bigquery.BigQueryRetryHelper.runWithRetries")
              .startSpan();
    }
    final List<Throwable> attemptFailures = new ArrayList<>();

    try (Scope runWithRetriesScope = runWithRetries != null ? runWithRetries.makeCurrent() : null) {
      // Suppressing should be ok as a workaraund. Current and only ResultRetryAlgorithm
      // implementation does not use response at all, so ignoring its type is ok.
      @SuppressWarnings("unchecked")
      ResultRetryAlgorithm<V> algorithm = (ResultRetryAlgorithm<V>) resultRetryAlgorithm;
      return run(
          callable,
          new ExponentialRetryAlgorithm(retrySettings, clock),
          algorithm,
          bigQueryRetryConfig,
          attemptFailures);

    } catch (Exception e) {
      Throwable cause = e.getCause() != null ? e.getCause() : e;

      // Attach previous retry failures (the terminal cause is not added to its own suppressed list).
      for (Throwable prev : attemptFailures) {
        if (prev != cause) {
          cause.addSuppressed(prev);
        }
      }

      if (cause instanceof IOException) {
        BigQueryException bq = new BigQueryException((IOException) cause);
        // Preserve suppressed info after wrapping.
        for (Throwable s : cause.getSuppressed()) {
          bq.addSuppressed(s);
        }
        throw new BigQueryRetryHelperException(bq);
      }

      throw new BigQueryRetryHelperException(cause);
    } finally {
      if (runWithRetries != null) {
        runWithRetries.end();
      }
    }
  }

  private static <V> V run(
      Callable<V> callable,
      TimedRetryAlgorithm timedAlgorithm,
      ResultRetryAlgorithm<V> resultAlgorithm,
      BigQueryRetryConfig bigQueryRetryConfig,
      List<Throwable> attemptFailures)
      throws ExecutionException, InterruptedException {
    RetryAlgorithm<V> retryAlgorithm =
        new BigQueryRetryAlgorithm<>(
            resultAlgorithm,
            timedAlgorithm,
            bigQueryRetryConfig); // using BigQueryRetryAlgorithm in place of
    // com.google.api.gax.retrying.RetryAlgorithm, as
    // BigQueryRetryAlgorithm retries considering bigQueryRetryConfig
    RetryingExecutor<V> executor = new DirectRetryingExecutor<>(retryAlgorithm);

    Callable<V> recordingCallable =
        () -> {
          try {
            return callable.call();
          } catch (Throwable t) {
            attemptFailures.add(t);
            throw t;
          }
        };

    if (LOG.isLoggable(Level.FINEST)) {
      LOG.log(
          Level.FINEST,
          "Retrying with:\n{0}\n{1}",
          new Object[] {
            "BigQuery retried method: " + callable.getClass().getEnclosingMethod().getName(),
            "BigQuery retry settings: " + timedAlgorithm.createFirstAttempt().getGlobalSettings()
          });
    }

    RetryingFuture<V> retryingFuture = executor.createFuture(recordingCallable);
    executor.submit(retryingFuture);
    return retryingFuture.get();
  }

  public static class BigQueryRetryHelperException extends RuntimeException {

    private static final long serialVersionUID = -8519852520090965314L;

    BigQueryRetryHelperException(Throwable cause) {
      super(cause);
    }
  }
}
