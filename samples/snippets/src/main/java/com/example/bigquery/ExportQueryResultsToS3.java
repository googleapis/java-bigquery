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

// [START bigquery_omni_export_query_result_to_s3]
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;

// Sample to queries an external data source aws s3 using a permanent table
public class ExportQueryResultsToS3 {

  public static void main(String[] args) throws InterruptedException {
    // TODO(developer): Replace these variables before running the sample.
    String projectId = "MY_PROJECT_ID";
    String datasetName = "MY_DATASET_NAME";
    String externalTableName = "MY_EXTERNAL_TABLE_NAME";
    // connectionName should be in the format of connection_region.connection_name. e.g.
    // aws-us-east-1.s3-write-conn
    String connectionName = "MY_CONNECTION_REGION.MY_CONNECTION_NAME";
    String destinationUri = "s3://your-bucket-name/*";
    String format = "EXPORT_FORMAT";
    // Export result of query to find states starting with 'W'
    String query =
        String.format(
            "EXPORT DATA WITH CONNECTION %s AS SELECT * FROM %s.%s.%s WHERE name LIKE 'W%%'",
            connectionName, destinationUri, format, projectId, datasetName, externalTableName);
    exportQueryResultsToS3(query);
  }

  public static void exportQueryResultsToS3(String query) throws InterruptedException {
    try {
      // Initialize client that will be used to send requests. This client only needs to be created
      // once, and can be reused for multiple requests.
      BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

      TableResult results = bigquery.query(QueryJobConfiguration.of(query));

      results
          .iterateAll()
          .forEach(row -> row.forEach(val -> System.out.printf("%s,", val.toString())));

      System.out.println("Query results exported to Amazon S3 successfully.");
    } catch (BigQueryException e) {
      System.out.println("Query not performed \n" + e.toString());
    }
  }
}
// [END bigquery_omni_export_query_result_to_s3]
