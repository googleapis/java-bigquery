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
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;

public class ExtractTableToJSON {

  public static void runExtractTableToJSON() {
    // TODO(developer): Replace these variables before running the sample.
    String datasetName = "MY_DATASET_NAME";
    String format = "CSV";
    String bucketName = "my-bucket";
    String gcsFileName = "gs://" + bucketName + "/path/to/file";
    // Create a new table to extract to GCS as CSV
    String tableName = "MY_TABLE_NAME";
    Schema schema =
        Schema.of(
            Field.of("stringField", LegacySQLTypeName.STRING),
            Field.of("booleanField", LegacySQLTypeName.BOOLEAN));
    Table table = createTableHelper(datasetName, tableName, schema);

    //Extract table
    extractTableToJSON(table, format, gcsFileName);
  }

  // Exports my-dataset-name:my_table to gcs://my-bucket/my-file as raw CSV
  public static void extractTableToJSON(Table table, String format, String gcsFileName) {
    try {
      Job job = table.extract(format, gcsFileName);
      if(job != null && job.getStatus().getError() == null)
        System.out.println("Table extraction job completed successfully. Check in GCS bucket for the CSV file.");
      else
        System.out.println("Table extraction job failed");
    } catch (BigQueryException e) {
      System.out.println("Table extraction job was interrupted. \n" + e.toString());
    }
  }

  private static Table createTableHelper(String datasetName, String tableName, Schema schema) {
    // Initialize client that will be used to send requests. This client only needs to be created
    // once, and can be reused for multiple requests.
    BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

    TableId tableId = TableId.of(datasetName, tableName);
    TableDefinition tableDefinition = StandardTableDefinition.of(schema);
    TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();

    try {
      Table table = bigquery.create(tableInfo);
      return table;
    } catch (BigQueryException e) {
      System.out.println("Table was not created. \n" + e.toString());
      return null;
    }
  }
}
// [END bigquery_extract_table]
