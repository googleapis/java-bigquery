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

import static com.google.common.truth.Truth.assertThat;

import com.google.api.core.NanoClock;
import com.google.api.gax.retrying.BasicResultRetryAlgorithm;
import com.google.api.gax.retrying.RetrySettings;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Test;
import org.threeten.bp.Duration;

public class BigQueryRetryHelperTest {

  @Test
  public void testRetriesMultithread() throws ExecutionException, InterruptedException {
    BigQueryRetryConfig config =
        BigQueryRetryConfig.newBuilder().retryOnMessage("asdf").retryOnRegEx(".*dfdf.*").build();

    ThreadFactory threadFactory =
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("testing-%02d").build();

    ListeningExecutorService exec =
        MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(4, threadFactory));

    int jobCount = 16;
    int maxAttempts = 6;
    RetrySettings retrySettings =
        RetrySettings.newBuilder()
            .setMaxAttempts(maxAttempts)
            .setInitialRetryDelay(Duration.ofMillis(20))
            .setRetryDelayMultiplier(1.25)
            .setMaxRetryDelay(Duration.ofMinutes(1))
            .build();
    ListenableFuture<List<Result>> f =
        Futures.allAsList(
            IntStream.rangeClosed(1, jobCount)
                .mapToObj(i -> exec.submit(new ResultCallable(i, config, retrySettings)))
                .collect(Collectors.toList()));

    List<Result> allJobResults = f.get();

    List<String> allJobResultValues =
        allJobResults.stream().map(r -> r.val).collect(Collectors.toList());
    int allJobActualAttemptSum = allJobResults.stream().mapToInt(r -> r.numAttempts).sum();

    // ensure the number of jobs that produced values matches the number of jobs we ran
    assertThat(allJobResultValues).hasSize(jobCount);
    // ensure the total number of attempts across all jobs matches our max attempt count per job
    //   times the number of jobs we ran
    assertThat(allJobActualAttemptSum).isEqualTo(maxAttempts * jobCount);
  }

  private static class Result {
    private final String val;
    private final int numAttempts;

    public Result(String val, int numAttempts) {
      this.val = val;
      this.numAttempts = numAttempts;
    }
  }

  private static class ResultCallable implements Callable<Result> {
    private static final Logger LOGGER = Logger.getLogger(ResultCallable.class.getName());
    private static final BasicResultRetryAlgorithm<String> RESULT_RETRY_ALGORITHM =
        new BasicResultRetryAlgorithm<String>() {
          @Override
          public boolean shouldRetry(Throwable previousThrowable, String previousResponse) {
            return previousThrowable instanceof ConnectException
                || previousThrowable instanceof UnknownHostException;
          }
        };

    private final BigQueryRetryConfig config;
    private final int i;
    private final RetrySettings settings;
    private final int maxAttempts;

    public ResultCallable(int i, BigQueryRetryConfig config, RetrySettings settings) {
      LOGGER.info(String.format("i = 0x%08x", i));
      this.i = i;
      this.config = config;
      this.settings = settings;
      this.maxAttempts = settings.getMaxAttempts();
    }

    @Override
    public Result call() {
      StringCallable stringCallable = new StringCallable(i, maxAttempts);
      String s =
          BigQueryRetryHelper.runWithRetries(
              stringCallable,
              settings,
              RESULT_RETRY_ALGORITHM,
              NanoClock.getDefaultClock(),
              config);
      return new Result(s, stringCallable.counter.get() - 1);
    }
  }

  private static class StringCallable implements Callable<String> {
    private final AtomicInteger counter = new AtomicInteger(1);
    private final int i;
    private final int maxAttempts;

    public StringCallable(int i, int maxAttempts) {
      this.i = i;
      this.maxAttempts = maxAttempts;
    }

    @Override
    public String call() throws Exception {
      int count = counter.getAndIncrement();
      if (count == 1) {
        throw new RuntimeException("asasdfdfasas");
      } else if (count == 2) {
        throw new IOException("asdf");
      } else if (count == 3) {
        throw new ConnectException("connect error");
      } else if (count == 4) {
        throw new UnknownHostException("unknown host");
      } else if (count >= maxAttempts) {
        return "Hello World";
      } else {
        throw new RuntimeException("asdf");
      }
    }

    @Override
    public String toString() {
      return String.format("StringCallable{i=0x%08x,counter=%03d}", i, counter.get());
    }
  }
}
