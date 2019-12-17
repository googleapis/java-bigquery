package com.example.bigquery;

// [START bigquery_extract_table]
import com.google.cloud.RetryOption;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQuery.TableOption;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import org.threeten.bp.Duration;

public class ExtractTableToJSON {

  public static void runExtractTableToJSON() {
    // TODO(developer): Replace these variables before running the sample.
    String datasetName = "my-dataset-name";
    String format = "CSV";
    String tableName = "my_table";
    String bucketName = "my-bucket";
    String gcsFileName = "gs://" + bucketName + "/extractTest.csv";
    extractTableToJSON(datasetName, tableName, format, gcsFileName);
  }


  // Exports my-dataset-name:my_table to gcs://my-bucket/my-file as raw CSV
  public static void extractTableToJSON(String datasetName, String tableName, String format, String gcsFileName) {
    // Initialize client that will be used to send requests. This client only needs to be created
    // once, and can be reused for multiple requests.
    BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

    //Create a table to extract to GCS
    StandardTableDefinition.Builder builder = StandardTableDefinition.newBuilder();
    Table table = bigquery.create(TableInfo.of(TableId.of(datasetName, tableName), builder.build()));

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