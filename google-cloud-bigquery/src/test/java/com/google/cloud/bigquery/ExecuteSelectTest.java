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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import org.junit.Before;
import org.junit.Test;

public class ExecuteSelectTest {

  public int rowLimit = 500000;

  private ConnectionSettings connectionSettingsReadAPIEnabled,
      connectionSettingsReadAPIDisabled,
      connectionSettingsReadAPIEnabledFastQueryDisabled,
      connectionSettingsReadAPIDisabledFastQueryDisabled;
  private final String QUERY =
      "SELECT * FROM bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2017 LIMIT %s";

  private void getResultHash(BigQueryResult bigQueryResultSet) throws SQLException {
    ResultSet rs = bigQueryResultSet.getResultSet();
    int cnt = 0;
    long lastTime = System.currentTimeMillis();
    System.out.println("\n Running");
    while (rs.next()) {
      if (++cnt % 100_000 == 0) {
        long now = System.currentTimeMillis();
        long duration = now - lastTime;
        System.out.println("ROW " + cnt + " Time: " + duration + " ms");
        lastTime = now;
      }
    }
  }

  @Before
  public void setup() {
    java.util.logging.Logger.getGlobal().setLevel(Level.ALL);

    connectionSettingsReadAPIEnabled = ConnectionSettings.newBuilder().setUseReadAPI(true).build();
    // Max billing tier is somewhat arbitrary - just ensures that fast query is not used.
    connectionSettingsReadAPIEnabledFastQueryDisabled =
        ConnectionSettings.newBuilder().setUseReadAPI(true).setMaximumBillingTier(100).build();
    connectionSettingsReadAPIDisabled =
        ConnectionSettings.newBuilder().setUseReadAPI(false).build();
    connectionSettingsReadAPIDisabledFastQueryDisabled =
        ConnectionSettings.newBuilder().setUseReadAPI(false).setMaximumBillingTier(100).build();
  }

  @Test
  public void testWithReadApi() throws BigQuerySQLException {
    System.out.println("\nTesting ReadAPI without fast query");
    Connection connectionReadAPIEnabled =
        BigQueryOptions.getDefaultInstance()
            .getService()
            .createConnection(connectionSettingsReadAPIEnabledFastQueryDisabled);
    String selectQuery = String.format(QUERY, rowLimit);
    try {
      BigQueryResult bigQueryResultSet = connectionReadAPIEnabled.executeSelect(selectQuery);
      getResultHash(bigQueryResultSet);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      connectionReadAPIEnabled.close(); // IMP to kill the bg workers
    }
  }

  @Test
  public void testWithFastQueryAndReadApi() throws BigQuerySQLException {
    System.out.println("\nTesting ReadAPI with fast query");
    Connection connectionReadAPIEnabled =
        BigQueryOptions.getDefaultInstance()
            .getService()
            .createConnection(connectionSettingsReadAPIEnabled);
    String selectQuery = String.format(QUERY, rowLimit);
    try {
      BigQueryResult bigQueryResultSet = connectionReadAPIEnabled.executeSelect(selectQuery);
      getResultHash(bigQueryResultSet);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      connectionReadAPIEnabled.close(); // IMP to kill the bg workers
    }
  }

  @Test
  public void testWithFastQueryAndWithoutReadApi() throws BigQuerySQLException {
    System.out.println("\nTesting query api with fast query");
    Connection connectionReadAPIDisabled =
        BigQueryOptions.getDefaultInstance()
            .getService()
            .createConnection(connectionSettingsReadAPIDisabled);
    String selectQuery = String.format(QUERY, rowLimit);
    try {
      BigQueryResult bigQueryResultSet = connectionReadAPIDisabled.executeSelect(selectQuery);
      getResultHash(bigQueryResultSet);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      connectionReadAPIDisabled.close(); // IMP to kill the bg workers
    }
  }

  @Test
  public void testWithoutReadApi() throws BigQuerySQLException {
    System.out.println("\nTesting query api without fast query");
    connectionSettingsReadAPIEnabled.toBuilder().setMaximumBillingTier(100).build();
    Connection connectionReadAPIDisabled =
        BigQueryOptions.getDefaultInstance()
            .getService()
            .createConnection(connectionSettingsReadAPIDisabledFastQueryDisabled);
    String selectQuery = String.format(QUERY, rowLimit);
    try {
      BigQueryResult bigQueryResultSet = connectionReadAPIDisabled.executeSelect(selectQuery);
      getResultHash(bigQueryResultSet);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      connectionReadAPIDisabled.close(); // IMP to kill the bg workers
    }
  }
}
