package com.google.cloud;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.QueryJobConfiguration;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

// Launcher for single query execution
public class App {

    private static final String project = "bigquery-smallqueries-ram";
    private static final String dataset = "turbosql";

    private static final String query = "select\n"
        + "sum(NASDelay) as C1,\n"
        + "count(NASDelay) as C2\n"
        + "from\n"
        + "(\n"
        + "  select\n"
        + "  NASDelay\n"
        + "  from\n"
        + "  flight_data\n"
        + "  where\n"
        + "  (\n"
        + "    cast(FlightDate as TIMESTAMP) < TIMESTAMP('2018-12-31 12:00:01')\n"
        + "    and\n"
        + "    cast(FlightDate as TIMESTAMP) >= TIMESTAMP('2018-01-01 00:00:00')\n"
        + "  )\n"
        + "  and\n"
        + "  Origin = 'LAS'\n"
        + ")\n"
        + "as ITBL";

    private static final String credentials = "{\n"
        + "  \"type\": \"service_account\",\n"
        + "  \"project_id\": \"bigquery-smallqueries-ram\",\n"
        + "  \"private_key_id\": \"1e08ae70aedd777112ee5b0c4246e14a2c1778d2\",\n"
        + "  \"private_key\": \"-----BEGIN PRIVATE KEY-----\\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCre4kgo2QFnblN\\n3qan0j+tISBK+Y/lgOCuMcSP2JVET+vu4GzYGjXOk+ciEETDVm2RO6RT91yxkGou\\n41cEyafsVavYwSOaE13sTJ+rBq62WI3tJjIXY02ujq2rIacDkSvNFSArxFh4g5y/\\npOlP1PDBEfJ+BW/n7il6STyfaHY9DeiDqRuD/Ru/FptjpsHtxZF0f3hVBN50tqNE\\nBsDOERN3YJUd/DXE9LYtsOjdnehZO6u7aGB7y9oZOZPrQHV+cOidd8vRMja4qtnq\\nHphB0DBWAWhVsbpz/3YXRIazbpN172qDR4IeGWcAk+dB1wW+fozXeBMGdzCXmQ3s\\nW9kgjHLRAgMBAAECggEAEGfrQKIhKaBjYB3TF+83hFrM+h9SMwTXehLs2U5BSZ3T\\n1rDF+Q7Y4wCndkzUJ1QXe3OXTyk1Rw+n+QBrDNw2Ipg3rq8bttvvenICPJyRDoT+\\nXxfuZuprPi4MU3kOv3qeFCrXPxiz1iPRVUxzvd2DgxUye/gNMsVIVpi3IhEebszD\\ntIk2DNVR8blP3iHvG627oA6n1xlyRDgtuYeTcOoLE8QGL6BuWrG7CCbXuVbB3ARc\\nzUoWWkNOgbZL74LRUdPHraPtRv1zhOlqMotCz+K0mu6EzfIWAzZ8K3HiWH2Qc40j\\ncfLdXOwN/xJRlAVAWGmWW6dbi/qrj9kh+zCAEv0vOQKBgQDb+kYyybjE2YglbT42\\nipsLz0IfijhD4xSD54SIDST48QHHJtMg0/nH3tQmFn81ZqF2SUeTp7aVlwyQl/wS\\nkEQUa3IiQ5LPanuH7FtbhnWC5xzcGDcSnnu7RMtMF+Us66PPD0cSIjtZQ7MbCj2D\\nigcQqnGbcfnNrTN6F4+Eqi1p9wKBgQDHkEo5Z5KJZpuK+LPySwrd7VUyA6muBG1f\\njpgRDCanOwk6bCb1nr8WFKUTH1+RmWwJxcq/GHIAHlsqHvmNnI2dWCw/2XQvb6B0\\nNTsjc9dEwvSQTdstibcJYXSqI1a70/fdzBlKL0EmWk2fSoK1/YNgMqvbzWWkd2Sa\\nwlLaE60XdwKBgFSqVyplCYB6WTROf4tufY5mDwUkpdM7K0I5cYELzhcia5TDFK+l\\n5pVO5khikEN1ZN+qBKqH+nZI1MUyOgrLC+jwEdWuPGsoiLMf/WmUrtXbLfhoOYop\\nBWZma/i1mbdYWovvTWNlWYJZ1C2sG1DtZxq6/07c51CKQS3Us6BT/3axAoGAbo6G\\nQEUbzlj05MmhTyK5s3bvEtUqpIg5W43wuskDhPPUyfPupXY7oGzxgqWH2W6ohsV3\\n6+QMC/rFQJGGaSiI39lgMkMy9bCesKJoz9w2LxbeTC+FtDWuHFlMO5F2VHo6wDp4\\n7Ds/mZK/m/a4cUAwDxQjV5Lzs2idaIstQTlTVEMCgYBfqABqnx1Nm9FAgheD1rpL\\nZQlYiOyKPdOfyE1KF2uzKVE4FAmGmKtCvfFVc979CNQEPlmve5GiS8GRIG0FKlU+\\nHYO5KN5XezuH+osoMgbCnzXrbAgwHcZ/pYLqPPi85xKYTY+c+wUmJBl5q3Qpp8VR\\nAxWLJJGjiztrcpiTbD9DIg==\\n-----END PRIVATE KEY-----\\n\",\n"
        + "  \"client_email\": \"temp-testing@bigquery-smallqueries-ram.iam.gserviceaccount.com\",\n"
        + "  \"client_id\": \"107395172672076342797\",\n"
        + "  \"auth_uri\": \"https://accounts.google.com/o/oauth2/auth\",\n"
        + "  \"token_uri\": \"https://oauth2.googleapis.com/token\",\n"
        + "  \"auth_provider_x509_cert_url\": \"https://www.googleapis.com/oauth2/v1/certs\",\n"
        + "  \"client_x509_cert_url\": \"https://www.googleapis.com/robot/v1/metadata/x509/temp-testing%40bigquery-smallqueries-ram.iam.gserviceaccount.com\"\n"
        + "}\n";

    private BigQuery bigQuery;

    public static void main( String[] args ) {
        App app = new App();
        app.benchmarkQuery();
    }

    /**
     * Method to create connection and benchmark a single query on BigQuery.
     */
    private void benchmarkQuery() {
        try {
            ServiceAccountCredentials serviceAccountCredentials =
                ServiceAccountCredentials.fromStream(
                    new ByteArrayInputStream(credentials.getBytes())
                );
            GoogleCredentials googleCredentials = serviceAccountCredentials.createScoped("https://www.googleapis.com/auth/cloud-platform");
            googleCredentials.refreshIfExpired();
            this.bigQuery = BigQueryOptions.newBuilder()
                .setCredentials(googleCredentials)
                .setProjectId(project)
                .build().getService();
        } catch (IOException e) {
            System.out.println("Failed to prepare execution." + e.getMessage());
        }

        try {
            this.executeQuery();
        } catch (InterruptedException e) {
            System.out.println("Execution failure.");
        }
    }

    /**
     * Benchmark a query on BigQuery and return performance details.
     *
     * @return A serialized json string containing the query performance details.
     * @throws InterruptedException
     */
    public void executeQuery() throws InterruptedException {
        String queryId = UUID.randomUUID().toString();
        List<Double> results = new ArrayList<>();
        for(int i=0; i<=2; i++) {
            // Use standard SQL syntax for queries.
            // See: https://cloud.google.com/bigquery/sql-reference/
            QueryJobConfiguration queryConfig =
                QueryJobConfiguration.newBuilder(this.query)
                    .setUseLegacySql(false)
                    .setUseQueryCache(false)
                    .setDefaultDataset(this.dataset)
                    .setLabels(Collections.singletonMap("query_id", queryId))
                    .build();
            long start = System.currentTimeMillis();
            this.bigQuery.query(queryConfig);
            long end = System.currentTimeMillis();
            double executionTimeInSecs = (end - start) / 1000.0;
            results.add(executionTimeInSecs);
        }
        System.out.println("query_wall_time_in_secs: " + results);
    }
}
