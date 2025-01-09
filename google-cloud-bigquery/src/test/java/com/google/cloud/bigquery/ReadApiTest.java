/*
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.bigquery;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.ResultSet;
import org.junit.Test;

public class ReadApiTest {
  public int rowLimit = 5000;
  private final String QUERY =
      "SELECT * FROM bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2017 LIMIT %s";

  @Test
  public void testWithReadApi() throws BigQuerySQLException {
    // Job timeout is somewhat arbitrary - just ensures that fast query is not used.
    // min result size and page row count ratio ensure that the ReadAPI is used.
    ConnectionSettings connectionSettingsReadAPIEnabledFastQueryDisabled =
        ConnectionSettings.newBuilder()
            .setUseReadAPI(true)
            .setJobTimeoutMs(Long.MAX_VALUE)
            .setMinResultSize(500)
            .setTotalToPageRowCountRatio(1)
            .build();

    Connection connectionReadAPIEnabled =
        BigQueryOptions.getDefaultInstance()
            .getService()
            .createConnection(connectionSettingsReadAPIEnabledFastQueryDisabled);

    String selectQuery = String.format(QUERY, rowLimit);
    try {
      BigQueryResult bigQueryResultSet = connectionReadAPIEnabled.executeSelect(selectQuery);
      ResultSet rs = bigQueryResultSet.getResultSet();
      // Paginate results to avoid an InterruptedException
      while (rs.next()) {}

      assertTrue(bigQueryResultSet.getBigQueryResultStats().getQueryStatistics().getUseReadApi());
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      connectionReadAPIEnabled.close();
    }
  }

  @Test
  public void testWithFastQueryAndReadApi() throws BigQuerySQLException {
    ConnectionSettings connectionSettingsReadAPIEnabled =
        ConnectionSettings.newBuilder()
            .setUseReadAPI(true)
            .setMinResultSize(500)
            .setTotalToPageRowCountRatio(1)
            .build();

    Connection connectionReadAPIEnabled =
        BigQueryOptions.getDefaultInstance()
            .getService()
            .createConnection(connectionSettingsReadAPIEnabled);

    String selectQuery = String.format(QUERY, rowLimit);
    try {
      BigQueryResult bigQueryResultSet = connectionReadAPIEnabled.executeSelect(selectQuery);
      ResultSet rs = bigQueryResultSet.getResultSet();
      while (rs.next()) {}

      assertTrue(bigQueryResultSet.getBigQueryResultStats().getQueryStatistics().getUseReadApi());
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      connectionReadAPIEnabled.close();
    }
  }

  @Test
  public void testWithFastQueryAndWithoutReadApi() throws BigQuerySQLException {
    ConnectionSettings connectionSettingsReadAPIDisabled =
        ConnectionSettings.newBuilder().setUseReadAPI(false).build();

    Connection connectionReadAPIDisabled =
        BigQueryOptions.getDefaultInstance()
            .getService()
            .createConnection(connectionSettingsReadAPIDisabled);

    String selectQuery = String.format(QUERY, rowLimit);
    try {
      BigQueryResult bigQueryResultSet = connectionReadAPIDisabled.executeSelect(selectQuery);
      ResultSet rs = bigQueryResultSet.getResultSet();
      while (rs.next()) {}

      assertFalse(bigQueryResultSet.getBigQueryResultStats().getQueryStatistics().getUseReadApi());
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      connectionReadAPIDisabled.close();
    }
  }

  @Test
  public void testWithoutReadApi() throws BigQuerySQLException {
    ConnectionSettings connectionSettingsReadAPIDisabledFastQueryDisabled =
        ConnectionSettings.newBuilder()
            .setUseReadAPI(false)
            .setJobTimeoutMs(Long.MAX_VALUE)
            .build();

    Connection connectionReadAPIDisabled =
        BigQueryOptions.getDefaultInstance()
            .getService()
            .createConnection(connectionSettingsReadAPIDisabledFastQueryDisabled);

    String selectQuery = String.format(QUERY, rowLimit);
    try {
      BigQueryResult bigQueryResultSet = connectionReadAPIDisabled.executeSelect(selectQuery);
      ResultSet rs = bigQueryResultSet.getResultSet();
      while (rs.next()) {}

      assertFalse(bigQueryResultSet.getBigQueryResultStats().getQueryStatistics().getUseReadApi());
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      connectionReadAPIDisabled.close();
    }
  }
}
