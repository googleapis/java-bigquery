/*
 * Copyright 2015 Google LLC
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

package com.google.cloud.bigquery.testing;

import com.google.api.gax.retrying.RetrySettings;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.http.HttpTransportOptions;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Utility to create a remote BigQuery configuration for testing. BigQuery options can be obtained
 * via the {@link #getOptions()} method. Returned options have custom {@link
 * BigQueryOptions#getRetrySettings()}: {@link RetrySettings#getMaxAttempts()} is {@code 10}, {@link
 * RetrySettings#getMaxRetryDelay()} is {@code 30000}, {@link RetrySettings#getTotalTimeout()} is
 * {@code 120000} and {@link RetrySettings#getInitialRetryDelay()} is {@code 250}. {@link
 * HttpTransportOptions#getConnectTimeout()} and {@link HttpTransportOptions#getReadTimeout()} are
 * both set to {@code 60000}.
 */
public class RemoteBigQueryHelper {

  private static final Logger log = Logger.getLogger(RemoteBigQueryHelper.class.getName());
  private static final String DATASET_NAME_PREFIX = "gcloud_test_dataset_temp_";
  private static final String MODEL_NAME_PREFIX = "model_";
  private static final String ROUTINE_NAME_PREFIX = "routine_";
  private final BigQueryOptions options;
  private static final int connectTimeout = 60000;

  private RemoteBigQueryHelper(BigQueryOptions options) {
    this.options = options;
  }

  /** Returns a {@link BigQueryOptions} object to be used for testing. */
  public BigQueryOptions getOptions() {
    return options;
  }

  /**
   * Deletes a dataset, even if non-empty.
   *
   * @param bigquery the BigQuery service to be used to issue the delete request
   * @param dataset the dataset to be deleted
   * @return {@code true} if deletion succeeded, {@code false} if the dataset was not found
   * @throws BigQueryException upon failure
   */
  public static boolean forceDelete(BigQuery bigquery, String dataset) {
    return bigquery.delete(dataset, BigQuery.DatasetDeleteOption.deleteContents());
  }

  /** Returns a dataset name generated using a random UUID. */
  public static String generateDatasetName() {
    return DATASET_NAME_PREFIX + UUID.randomUUID().toString().replace('-', '_');
  }

  public static String generateModelName() {
    return MODEL_NAME_PREFIX + UUID.randomUUID().toString().replace('-', '_');
  }

  public static String generateRoutineName() {
    return ROUTINE_NAME_PREFIX + UUID.randomUUID().toString().replace('-', '_');
  }

  /**
   * Creates a {@code RemoteBigQueryHelper} object for the given project id and JSON key input
   * stream.
   *
   * @param projectId id of the project to be used for running the tests
   * @param keyStream input stream for a JSON key
   * @return A {@code RemoteBigQueryHelper} object for the provided options
   * @throws BigQueryHelperException if {@code keyStream} is not a valid JSON key stream
   */
  public static RemoteBigQueryHelper create(String projectId, InputStream keyStream)
      throws BigQueryHelperException {
    try {
      HttpTransportOptions transportOptions = BigQueryOptions.getDefaultHttpTransportOptions();
      transportOptions =
          transportOptions.toBuilder()
              .setConnectTimeout(connectTimeout)
              .setReadTimeout(connectTimeout)
              .build();
      BigQueryOptions bigqueryOptions =
          BigQueryOptions.newBuilder()
              .setCredentials(ServiceAccountCredentials.fromStream(keyStream))
              .setProjectId(projectId)
              .setRetrySettings(retrySettings())
              .setTransportOptions(transportOptions)
              .build();
      return new RemoteBigQueryHelper(bigqueryOptions);
    } catch (IOException ex) {
      if (log.isLoggable(Level.WARNING)) {
        log.log(Level.WARNING, ex.getMessage());
      }
      throw BigQueryHelperException.translate(ex);
    }
  }

  /**
   * Creates a {@code RemoteBigQueryHelper} object using default project id and authentication
   * credentials.
   */
  public static RemoteBigQueryHelper create() {
    HttpTransportOptions transportOptions = BigQueryOptions.getDefaultHttpTransportOptions();
    transportOptions =
        transportOptions.toBuilder()
            .setConnectTimeout(connectTimeout)
            .setReadTimeout(connectTimeout)
            .build();
    BigQueryOptions bigqueryOptions =
        BigQueryOptions.newBuilder()
            .setRetrySettings(retrySettings())
            .setTransportOptions(transportOptions)
            .build();
    return new RemoteBigQueryHelper(bigqueryOptions);
  }

  private static RetrySettings retrySettings() {
    double retryDelayMultiplier = 1.0;
    int maxAttempts = 10;
    long initialRetryDelay = 250L;
    long maxRetryDelay = 30000L;
    long totalTimeOut = 120000L;
    return RetrySettings.newBuilder()
        .setMaxAttempts(maxAttempts)
        .setMaxRetryDelayDuration(Duration.ofMillis(maxRetryDelay))
        .setTotalTimeoutDuration(Duration.ofMillis(totalTimeOut))
        .setInitialRetryDelayDuration(Duration.ofMillis(initialRetryDelay))
        .setRetryDelayMultiplier(retryDelayMultiplier)
        .setInitialRpcTimeoutDuration(Duration.ofMillis(totalTimeOut))
        .setRpcTimeoutMultiplier(retryDelayMultiplier)
        .setMaxRpcTimeoutDuration(Duration.ofMillis(totalTimeOut))
        .build();
  }

  public static class BigQueryHelperException extends RuntimeException {

    private static final long serialVersionUID = 3984993496060055562L;

    public BigQueryHelperException(String message, Throwable cause) {
      super(message, cause);
    }

    public static BigQueryHelperException translate(Exception ex) {
      return new BigQueryHelperException(ex.getMessage(), ex);
    }
  }
}
