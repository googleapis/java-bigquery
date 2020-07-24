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

// [START bigquery_query_pagination]
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;

// Sample to run query with pagination.
public class QueryPagination {

  public static void runQueryPagination() {
    String query =
        "SELECT name, SUM(number) as total_people"
            + " FROM `bigquery-public-data.usa_names.usa_1910_2013`"
            + " GROUP BY name"
            + " ORDER BY total_people DESC"
            + " LIMIT 100";
    queryPagination(query);
  }

  public static void queryPagination(String query) {
    try {
      // Initialize client that will be used to send requests. This client only needs to be created
      // once, and can be reused for multiple requests.
      BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

      QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();

      Job queryJob = bigquery.create(JobInfo.of(queryConfig));

      // Get the destination table for the query job.
      // All queries write to a destination table. If a destination table is not
      // specified, the BigQuery populates it with a reference to a temporary
      // anonymous table after the query completes.
      TableId tableId = ((QueryJobConfiguration) queryJob.getConfiguration()).getDestinationTable();

      // Download rows.
      // The client library automatically handles pagination.
      TableResult results =
          bigquery.listTableData(tableId, BigQuery.TableDataListOption.pageSize(20));

      results
          .getNextPage()
          .iterateAll()
          .forEach(row -> row.forEach(val -> System.out.printf("%s,", val.toString())));

      System.out.println("Query pagination performed successfully.");
    } catch (BigQueryException e) {
      System.out.println("Query not performed \n" + e.toString());
    }
  }
}
// [END bigquery_query_pagination]
