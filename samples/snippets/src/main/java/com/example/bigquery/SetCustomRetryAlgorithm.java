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

// [START bigquery_set_custom_retry_algorithm]
import com.google.api.gax.retrying.ResultRetryAlgorithm;
import com.google.cloud.BaseService;
import com.google.cloud.ExceptionHandler;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;

public class SetCustomRetryAlgorithm {
  public static void main(String... args) {
    // TODO(developer): Replace projectId and retryAlgorithm classes before
    // running the sample. The ResultRetryAlgorithm abortOn and retryOn methods
    // can be used to specify retry behavior when the client encounters
    // exceptions during its execution. Alternatively, you can create your own
    // custom class that implements the ResultRetryAlgorithm interface.
    String projectId = "project-id";
    ResultRetryAlgorithm<?> retryAlgorithm =
        ExceptionHandler.newBuilder()
            .abortOn(RuntimeException.class)
            .retryOn(java.net.ConnectException.class)
            .retryOn(java.net.UnknownHostException.class)
            .retryOn(java.net.SocketException.class)
            .addInterceptors(BaseService.EXCEPTION_HANDLER_INTERCEPTOR)
            .build();
    setCustomRetryAlgorithm(projectId, retryAlgorithm);
  }

  public static void setCustomRetryAlgorithm(
      String projectId, ResultRetryAlgorithm<?> retryAlgorithm) {
    BigQueryOptions options =
        BigQueryOptions.newBuilder()
            .setProjectId(projectId)
            .setResultRetryAlgorithm(retryAlgorithm)
            .build();

    BigQuery bigquery = options.getService();

    System.out.println(bigquery.getOptions().getResultRetryAlgorithm());
  }
}
// [END bigquery_set_custom_retry_algorithm]
