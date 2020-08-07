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

// [START bigquery_relax_column_load_append]
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDataWriteChannel;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.WriteChannelConfiguration;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

// Sample to append relax column in a table.
public class RelaxColumnLoadAppend {

  public static void runRelaxColumnLoadAppend() {
    // TODO(developer): Replace these variables before running the sample.
    String datasetName = "MY_DATASET_NAME";
    String tableName = "MY_TABLE_NAME";
    Path csvPath = FileSystems.getDefault().getPath("/path/to/file.csv");
    relaxColumnLoadAppend(datasetName, tableName, csvPath);
  }

  public static void relaxColumnLoadAppend(String datasetName, String tableName, Path csvPath) {
    try {
      // Initialize client that will be used to send requests. This client only needs to be created
      // once, and can be reused for multiple requests.
      BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

      // Retrieve destination table reference
      Table table = bigquery.getTable(TableId.of(datasetName, tableName));

      // column as a 'REQUIRED' field.
      Field age =
          Field.newBuilder("Age", StandardSQLTypeName.INT64).setMode(Field.Mode.REQUIRED).build();
      Field weight =
          Field.newBuilder("Weight", StandardSQLTypeName.FLOAT64)
              .setMode(Field.Mode.REQUIRED)
              .build();
      Field isMagic =
          Field.newBuilder("IsMagic", StandardSQLTypeName.BOOL)
              .setMode(Field.Mode.REQUIRED)
              .build();
      Schema schema = Schema.of(age, weight, isMagic);

      // Set job options
      WriteChannelConfiguration writeChannelConfiguration =
          WriteChannelConfiguration.newBuilder(table.getTableId())
              .setSchema(schema)
              .setFormatOptions(FormatOptions.csv())
              .setSchemaUpdateOptions(
                  ImmutableList.of(JobInfo.SchemaUpdateOption.ALLOW_FIELD_RELAXATION))
              .setWriteDisposition(JobInfo.WriteDisposition.WRITE_APPEND)
              .build();

      // The location and JobName must be specified; other fields can be auto-detected.
      String jobName = "jobId_" + UUID.randomUUID().toString();
      JobId jobId = JobId.newBuilder().setLocation("us").setJob(jobName).build();

      // Imports a local file into a table.
      try (TableDataWriteChannel writer = bigquery.writer(jobId, writeChannelConfiguration);
          OutputStream stream = Channels.newOutputStream(writer)) {
        Files.copy(csvPath, stream);
      }

      // Get the Job created by the TableDataWriteChannel and wait for it to complete.
      Job job = bigquery.getJob(jobId);
      job = job.waitFor();
      // Check the job's status for errors
      if (job.isDone() && job.getStatus().getError() == null) {
        System.out.println("Relax column append successfully loaded in a table");
      } else {
        System.out.println(
            "BigQuery was unable to load into the table due to an error:"
                + job.getStatus().getError());
      }
    } catch (BigQueryException | InterruptedException | IOException e) {
      System.out.println("Column not added during load append \n" + e.toString());
    }
  }
}
// [END bigquery_relax_column_load_append]
