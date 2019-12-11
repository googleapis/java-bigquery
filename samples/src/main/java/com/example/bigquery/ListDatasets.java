/*
 * Copyright 2019 Google LLC
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

// [START bigquery_list_datasets]
import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQuery.DatasetListOption;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;

public class ListDatasets {

  public static void runListDatasets() {
    // TODO(developer): Replace these variables before running the sample.
    String projectId = "my-project-id";
    listDatasets(projectId);
  }

  public static void listDatasets(String projectId) {
    // Initialize client that will be used to send requests. This client only needs to be created
    // once, and can be reused for multiple requests.
    BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

    // List datasets in a specified project
    try {
      Page<Dataset> datasets = bigquery.listDatasets(projectId, DatasetListOption.pageSize(100));
      for (Dataset dataset : datasets.iterateAll()) {
        System.out.println(dataset.getDatasetId() + " dataset in project listed successfully");
      }
    } catch (BigQueryException e) {
      System.out.println("Project does not contain any datasets \n" + e.toString());
    }
  }
}
// [END bigquery_list_datasets]
