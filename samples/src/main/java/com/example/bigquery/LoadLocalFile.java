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

// [START bigquery_load_from_file]
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobStatistics.LoadStatistics;
import com.google.cloud.bigquery.TableDataWriteChannel;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.WriteChannelConfiguration;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;

public class LoadLocalFile {

  public static void runLoadLocalFile() {
    String datasetName = "MY_DATASET_NAME";
    String tableName = "MY_TABLE_NAME";
    Path csvPath = FileSystems.getDefault().getPath(".", "my-data.csv");
    loadLocalFile(datasetName, tableName, csvPath);
  }

  public static void loadLocalFile(String datasetName, String tableName, Path csvPath) {
    try {
      // Initialize client that will be used to send requests. This client only needs to be created
      // once, and can be reused for multiple requests.
      BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
      TableId tableId = TableId.of(datasetName, tableName);

      WriteChannelConfiguration writeChannelConfiguration =
          WriteChannelConfiguration.newBuilder(tableId)
              .setFormatOptions(FormatOptions.csv())
              .build();

      // The location must be specified; other fields can be auto-detected.
      JobId jobId = JobId.newBuilder().setLocation("us").build();

      TableDataWriteChannel writer = bigquery.writer(jobId, writeChannelConfiguration);

      // Imports a local file into a table.
      try (OutputStream stream = Channels.newOutputStream(writer)) {
        Files.copy(csvPath, stream);
      } finally {
        writer.close();
      }

      Job job = writer.getJob();
      Job completedJob = job.waitFor();
      if (completedJob == null) {
        System.out.println("Job not executed since it no longer exists.");
        return;
      } else if (completedJob.getStatus().getError() != null) {
        System.out.println(
            "BigQuery was unable to load local file to the table due to an error: \n"
                + job.getStatus().getError());
        return;
      }

      // Get output status
      LoadStatistics stats = job.getStatistics();
      System.out.printf("Successfully loaded %d rows. \n", stats.getOutputRows());
    } catch (BigQueryException | IOException | InterruptedException e) {
      System.out.println("Local file not loaded. \n" + e.toString());
    }
  }
}
// [END bigquery_load_from_file]
