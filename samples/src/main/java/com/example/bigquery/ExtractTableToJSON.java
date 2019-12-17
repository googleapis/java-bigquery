package com.example.bigquery;

// [START bigquery_extract_table]
import com.google.cloud.RetryOption;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQuery.TableOption;
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
import org.threeten.bp.Duration;

public class ExtractTableToJSON {

  public static void runExtractTableToJSON() {
    // TODO(developer): Replace these variables before running the sample.
    Table table = null;
    String format = "CSV";
    String bucketName = "my-bucket";
    String gcsFileName = "gs://" + bucketName + "/extractTest.csv";
    extractTableToJSON(table, format, gcsFileName);
  }

  // Exports my-dataset-name:my_table to gcs://my-bucket/my-file as raw CSV
  public static void extractTableToJSON(Table table, String format, String gcsFileName) {
    Job job = table.extract(format, gcsFileName);
    try {
      Job completedJob =
          job.waitFor(
              RetryOption.initialRetryDelay(Duration.ofSeconds(1)),
              RetryOption.totalTimeout(Duration.ofMinutes(3)));
      if (completedJob != null && completedJob.getStatus().getError() == null) {
        System.out.println("table extraction job completed successfully");
      } else {
        System.out.println("table extraction job failed" + table.toString() + completedJob.toString());
      }
    } catch (InterruptedException e) {
      System.out.println("table extraction job was interrupted. \n" + e.toString());
    }
  }
}
// [END bigquery_extract_table]