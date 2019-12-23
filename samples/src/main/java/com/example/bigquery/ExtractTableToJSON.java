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

// [START bigquery_extract_table]
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;

public class ExtractTableToJSON {
  // Initialize client that will be used to send requests. This client only needs to be created
  // once, and can be reused for multiple requests.
  private static BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

  private static String projectId = "bigquery-public-data";
  private static String datasetName = "samples";
  private static String tableName = "shakespeare";
  private static TableId tableId = TableId.of(projectId, datasetName, tableName);
  private static Table table = bigquery.getTable(tableId);

  public static void runExtractTableToJSON() {
    // TODO(developer): Replace these variables before running the sample.
    // For more information on export format available see:
    // https://cloud.google.com/bigquery/docs/exporting-data#export_formats_and_compression_types
    String format = "CSV";
    String bucketName = "my-bucket";
    String destinationUri = "gs://" + bucketName + "/path/to/file";

    //Extract table
    extractTableToJSON(format, destinationUri);
  }

  // Exports my-dataset-name:my_table to gcs://my-bucket/my-file as raw CSV
  public static void extractTableToJSON(String format, String destinationUri) {
    Job job = table.extract(format, destinationUri);
    try {
      if (job != null && job.getStatus().getError() == null)
        System.out.println("Table extraction job completed successfully. Check in GCS bucket for the CSV file.");
      else
        System.out.println("Table extraction job failed");
    } catch (BigQueryException e) {
      System.out.println("Table extraction job was interrupted. \n" + e.toString());
    }
  }
}
// [END bigquery_extract_table]
