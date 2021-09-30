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

import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpResponseException;
import com.google.api.core.NanoClock;
import com.google.api.gax.retrying.BasicResultRetryAlgorithm;
import com.google.api.gax.retrying.RetrySettings;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.net.SocketException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Test;
import org.threeten.bp.Duration;

public class BigQueryRetryHelperTest {

  @Test
  public void parallelRetriesAllBehaveAsExpected() throws ExecutionException, InterruptedException {
    BigQueryRetryConfig config =
        BigQueryRetryConfig.newBuilder().retryOnMessage("asdf").retryOnRegEx(".*dfdf.*").build();

    ThreadFactory threadFactory =
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("testing-%02d").build();

    ListeningExecutorService exec =
        MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(4, threadFactory));

    int endInclusive = 16;
    RetrySettings retrySettings =
        RetrySettings.newBuilder()
            .setMaxAttempts(6)
            .setInitialRetryDelay(Duration.ofMillis(20))
            .setRetryDelayMultiplier(1.25)
            .setMaxRetryDelay(Duration.ofMinutes(1))
            .build();
    ListenableFuture<List<Result>> f =
        Futures.allAsList(
            IntStream.rangeClosed(1, endInclusive)
                .mapToObj(i -> exec.submit(new ResultCallable(i, config, retrySettings)))
                .collect(Collectors.toList()));

    List<Result> results = f.get();

    List<String> values = results.stream().map(r -> r.val).collect(Collectors.toList());
    Map<String, List<String>> grouped =
        values.stream().collect(Collectors.groupingBy(Function.identity()));
    int sum = results.stream().mapToInt(r -> r.count).sum();

    assertThat(values).hasSize(endInclusive);
    assertThat(grouped.values()).hasSize(1);
    //noinspection OptionalGetWithoutIsPresent
    assertThat(grouped.values().stream().findFirst().get()).hasSize(endInclusive);
    assertThat(sum).isEqualTo(6 * endInclusive);
  }

  private static class Result {
    private final String val;
    private final int count;

    public Result(String val, int count) {
      this.val = val;
      this.count = count;
    }
  }

  private static class ResultCallable implements Callable<Result> {
    private static final Logger LOGGER = Logger.getLogger(ResultCallable.class.getName());
    private static final BasicResultRetryAlgorithm<String> RESULT_RETRY_ALGORITHM =
        new BasicResultRetryAlgorithm<String>() {
          @Override
          public boolean shouldRetry(Throwable previousThrowable, String previousResponse) {
            return previousThrowable instanceof GoogleJsonResponseException;
          }
        };

    private final BigQueryRetryConfig config;
    private final int i;
    private final RetrySettings settings;

    public ResultCallable(int i, BigQueryRetryConfig config, RetrySettings settings) {
      LOGGER.info(String.format("i = 0x%08x", i));
      this.i = i;
      this.config = config;
      this.settings = settings;
    }

    @Override
    public Result call() {
      StringCallable stringCallable = new StringCallable(i);
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

    public StringCallable(int i) {
      this.i = i;
    }

    @Override
    public String call() throws Exception {
      int count = counter.getAndIncrement();
      if (count == 1) {
        throw new RuntimeException("asasdfdfasas");
      } else if (count == 2) {
        throw new IOException("asdf");
      } else if (count == 3) {
        throw new SocketException("err connect dfdf");
      } else if (count == 4) {
        GoogleJsonError googleJsonError = new GoogleJsonError();
        googleJsonError.setCode(503);
        throw new GoogleJsonResponseException(
            new HttpResponseException.Builder(503, "service unavailable", new HttpHeaders()),
            googleJsonError);
      } else if (count >= 6) {
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
