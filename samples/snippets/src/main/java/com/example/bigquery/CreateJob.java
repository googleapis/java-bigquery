/*
 * Copyright 2020 Google LLC
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

// [START bigquery_create_job]
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.common.collect.ImmutableMap;
import java.util.UUID;

public class CreateJob {

  public static void runCreateJob() {
    // TODO(developer): Replace these variables before running the sample.
    String projectId = "MY_PROJECT_ID";
    String datasetName = "MY_DATASET_NAME";
    String tableName = "MY_TABLE_NAME";
    // i.e. SELECT country_name from `bigquery-public-data.utility_us.country_code_iso`
    String query =
        "SELECT country_name from `" + projectId + "." + datasetName + "." + tableName + "`";
    createJob(query);
  }

  public static void createJob(String query) {
    try {
      // Initialize client that will be used to send requests. This client only needs to be created
      // once, and can be reused for multiple requests.
      BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

      // Specify a job configuration to set optional job resource properties.
      QueryJobConfiguration queryConfig =
          QueryJobConfiguration.newBuilder(query)
              .setLabels(ImmutableMap.of("example-label", "example-value"))
              .build();

      // The client libraries automatically generate a job ID.
      // Override the generated ID with either the job_id_prefix or job_id parameters.
      String jobId = "code_sample_" + UUID.randomUUID().toString().substring(0, 8);
      Job job = bigquery.create(JobInfo.of(JobId.of(jobId), queryConfig));
      job = job.waitFor();
      if (job.isDone()) {
        System.out.println("Job created successfully");
      } else {
        System.out.println("Job was not created");
      }
    } catch (BigQueryException | InterruptedException e) {
      System.out.println("Job was not created. \n" + e.toString());
    }
  }
}
// [END bigquery_create_job]
