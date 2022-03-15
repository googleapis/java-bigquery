/*
 * Copyright 2022 Google LLC
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

package com.google.cloud.bigquery.it;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.cloud.ServiceOptions;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.BigQueryResultSet;
import com.google.cloud.bigquery.Connection;
import com.google.cloud.bigquery.ConnectionSettings;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.ReadClientConnectionConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.testing.RemoteBigQueryHelper;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

public class ITNightlyBigQueryTest {
  private static final Logger logger = Logger.getLogger(ITNightlyBigQueryTest.class.getName());
  private static final String DATASET = RemoteBigQueryHelper.generateDatasetName();
  private static final String TABLE = "TEMP_RS_TEST_TABLE";
  // Script will populate NUM_BATCHES*REC_PER_BATCHES number of records (eg: 100*10000 = 1M)
  private static final int NUM_BATCHES = 100;
  private static final int REC_PER_BATCHES = 10000;
  private static final int LIMIT_RECS = 900000;
  private static int rowCnt = 0;
  private static final String PROJECT_ID = ServiceOptions.getDefaultProjectId();
  private static BigQuery bigquery;
  private static final String QUERY =
      "select StringField, GeographyField, BooleanField, BigNumericField, IntegerField, NumericField, BytesField, TimestampField, "
          + " TimeField, DateField, IntegerArrayField,   RecordField.BooleanField, RecordField.StringField , JSONField"
          + " from BQ_RS_ARROW_TESTING.Table1 order by IntegerField asc LIMIT "
          + LIMIT_RECS;

  private static final Schema BQ_SCHEMA =
      Schema.of(
          Field.newBuilder("TimestampField", StandardSQLTypeName.TIMESTAMP)
              .setMode(Field.Mode.NULLABLE)
              .setDescription("TimestampDescription")
              .build(),
          Field.newBuilder("StringField", StandardSQLTypeName.STRING)
              .setMode(Field.Mode.NULLABLE)
              .setDescription("StringDescription")
              .build(),
          Field.newBuilder("IntegerArrayField", StandardSQLTypeName.NUMERIC)
              .setMode(Field.Mode.REPEATED)
              .setDescription("IntegerArrayDescription")
              .build(),
          Field.newBuilder("BooleanField", StandardSQLTypeName.BOOL)
              .setMode(Field.Mode.NULLABLE)
              .setDescription("BooleanDescription")
              .build(),
          Field.newBuilder("BytesField", StandardSQLTypeName.BYTES)
              .setMode(Field.Mode.NULLABLE)
              .setDescription("BytesDescription")
              .build(),
          Field.newBuilder(
                  "RecordField",
                  StandardSQLTypeName.STRUCT,
                  Field.newBuilder("StringField", StandardSQLTypeName.STRING)
                      .setMode(Field.Mode.NULLABLE)
                      .setDescription("StringDescription")
                      .build(),
                  Field.newBuilder("BooleanField", StandardSQLTypeName.BOOL)
                      .setMode(Field.Mode.NULLABLE)
                      .setDescription("BooleanDescription")
                      .build())
              .setMode(Field.Mode.NULLABLE)
              .setDescription("RecordDescription")
              .build(),
          Field.newBuilder("IntegerField", StandardSQLTypeName.NUMERIC)
              .setMode(Field.Mode.NULLABLE)
              .setDescription("IntegerDescription")
              .build(),
          Field.newBuilder("GeographyField", StandardSQLTypeName.GEOGRAPHY)
              .setMode(Field.Mode.NULLABLE)
              .setDescription("GeographyDescription")
              .build(),
          Field.newBuilder("NumericField", StandardSQLTypeName.NUMERIC)
              .setMode(Field.Mode.NULLABLE)
              .setDescription("NumericDescription")
              .build(),
          Field.newBuilder("BigNumericField", StandardSQLTypeName.BIGNUMERIC)
              .setMode(Field.Mode.NULLABLE)
              .setDescription("BigNumericDescription")
              .build(),
          Field.newBuilder("TimeField", StandardSQLTypeName.TIME)
              .setMode(Field.Mode.NULLABLE)
              .setDescription("TimeDescription")
              .build(),
          Field.newBuilder("DateField", StandardSQLTypeName.DATE)
              .setMode(Field.Mode.NULLABLE)
              .setDescription("DateDescription")
              .build(),
          Field.newBuilder("DateTimeField", StandardSQLTypeName.DATETIME)
              .setMode(Field.Mode.NULLABLE)
              .setDescription("DateTimeDescription")
              .build(),
          Field.newBuilder("JSONField", StandardSQLTypeName.JSON)
              .setMode(Field.Mode.NULLABLE)
              .setDescription("JSONFieldDescription")
              .build(),
          Field.newBuilder("IntervalField", StandardSQLTypeName.INTERVAL)
              .setMode(Field.Mode.NULLABLE)
              .setDescription("IntervalFieldDescription")
              .build());

  @Rule public Timeout globalTimeout = Timeout.seconds(1800); // setting 30 mins as the timeout

  @BeforeClass
  public static void beforeClass() throws InterruptedException, IOException {
    RemoteBigQueryHelper bigqueryHelper = RemoteBigQueryHelper.create();
    bigquery = bigqueryHelper.getOptions().getService();
    createDataset(DATASET);
    createTable(DATASET, TABLE, BQ_SCHEMA);
    populateTestRecords(DATASET, TABLE);
  }

  @AfterClass
  public static void afterClass() throws ExecutionException, InterruptedException {
    try {
      if (bigquery != null) {
        deleteTable(DATASET, TABLE);
        RemoteBigQueryHelper.forceDelete(bigquery, DATASET);
      } else {
        fail("Error clearing the test dataset");
      }
    } catch (BigQueryException e) {
      fail("Error clearing the test dataset " + e);
    }
  }

  @Test
  public void testIterateAndOrder() throws SQLException {
    ReadClientConnectionConfiguration clientConnectionConfiguration =
        ReadClientConnectionConfiguration.newBuilder()
            .setTotalToPageRowCountRatio(10L)
            .setMinResultSize(200000L)
            .setBufferSize(10000L)
            .build();
    ConnectionSettings connectionSettings =
        ConnectionSettings.newBuilder()
            .setDefaultDataset(DatasetId.of("BQ_RS_ARROW_TESTING"))
            .setNumBufferedRows(10000L) // page size
            .setPriority(
                QueryJobConfiguration.Priority
                    .INTERACTIVE) // so that isFastQuerySupported returns false
            .setReadClientConnectionConfiguration(clientConnectionConfiguration)
            .setUseReadAPI(true)
            .build();
    Connection connection =
        BigQueryOptions.getDefaultInstance().getService().createConnection(connectionSettings);

    BigQueryResultSet bigQueryResultSet = connection.executeSelect(QUERY);
    ResultSet rs = bigQueryResultSet.getResultSet();
    int cnt = 0;

    assertTrue(rs.next()); // read first record, which is null
    ++cnt;
    assertNull(rs.getString("StringField"));
    assertNull(rs.getString("GeographyField"));
    assertNull(rs.getObject("IntegerArrayField"));
    assertFalse(rs.getBoolean("BooleanField"));
    assertTrue(0.0d == rs.getDouble("BigNumericField"));
    assertTrue(0 == rs.getInt("IntegerField"));
    assertTrue(0L == rs.getLong("NumericField"));
    assertNull(rs.getBytes("BytesField"));
    assertNull(rs.getTimestamp("TimestampField"));
    assertNull(rs.getTime("TimeField"));
    assertNull(rs.getDate("DateField"));
    assertNull(rs.getString("JSONField"));
    assertFalse(rs.getBoolean("BooleanField_1"));
    assertNull(rs.getString("StringField_1"));
    int prevIntegerFieldVal = 0;
    while (rs.next()) {
      ++cnt;

      assertNotNull(rs.getString("StringField"));
      assertNotNull(rs.getString("GeographyField"));
      assertNotNull(rs.getObject("IntegerArrayField"));
      assertTrue(rs.getBoolean("BooleanField"));
      assertTrue(0.0d < rs.getDouble("BigNumericField"));
      assertTrue(0 < rs.getInt("IntegerField"));
      assertTrue(0L < rs.getLong("NumericField"));
      assertNotNull(rs.getBytes("BytesField"));
      assertNotNull(rs.getTimestamp("TimestampField"));
      assertNotNull(rs.getTime("TimeField"));
      assertNotNull(rs.getDate("DateField"));
      assertNotNull(rs.getString("JSONField"));
      assertFalse(rs.getBoolean("BooleanField_1"));
      assertNotNull(rs.getString("StringField_1"));

      // check the order of the records
      assertTrue(prevIntegerFieldVal < rs.getInt("IntegerField"));
      prevIntegerFieldVal = rs.getInt("IntegerField");
    }

    assertEquals(LIMIT_RECS, cnt);
  }

  private static void populateTestRecords(String datasetName, String tableName) {
    TableId tableId = TableId.of(datasetName, tableName);
    for (int batchCnt = 1; batchCnt <= NUM_BATCHES; batchCnt++) {
      addBatchRecords(tableId);
    }
  }

  private static void addBatchRecords(TableId tableId) {
    Map<String, Object> nullRow = new HashMap<>();
    try {
      InsertAllRequest.Builder reqBuilder = InsertAllRequest.newBuilder(tableId);
      if (rowCnt == 0) {
        reqBuilder.addRow(nullRow);
      }
      for (int i = 0; i < REC_PER_BATCHES; i++) {
        reqBuilder.addRow(getNextRow());
      }
      InsertAllResponse response = bigquery.insertAll(reqBuilder.build());

      if (response.hasErrors()) {
        // If any of the insertions failed, this lets you inspect the errors
        for (Map.Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
          logger.log(
              Level.WARNING,
              "Exception while adding records {0}",
              entry.getValue()); // gracefully log it, it's okay if a few batches fail during IT
        }
      }
    } catch (BigQueryException e) {
      logger.log(
          Level.WARNING,
          "Exception while adding records {0}",
          e); // gracefully log it, it's okay if a few batches fail during IT
    }
  }

  private static void createTable(String datasetName, String tableName, Schema schema) {
    try {
      TableId tableId = TableId.of(datasetName, tableName);
      TableDefinition tableDefinition = StandardTableDefinition.of(schema);
      TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
      Table table = bigquery.create(tableInfo);
      assertTrue(table.exists());
    } catch (BigQueryException e) {
      fail("Table was not created. \n" + e);
    }
  }

  public static void deleteTable(String datasetName, String tableName) {
    try {
      assertTrue(bigquery.delete(TableId.of(datasetName, tableName)));
    } catch (BigQueryException e) {
      fail("Table was not deleted. \n" + e);
    }
  }

  public static void createDataset(String datasetName) {
    try {
      DatasetInfo datasetInfo = DatasetInfo.newBuilder(datasetName).build();
      Dataset newDataset = bigquery.create(datasetInfo);
      assertNotNull(newDataset.getDatasetId().getDataset());
    } catch (BigQueryException e) {
      fail("Dataset was not created. \n" + e);
    }
  }

  public static void deleteDataset(String datasetName) {
    try {
      DatasetInfo datasetInfo = DatasetInfo.newBuilder(datasetName).build();
      assertTrue(bigquery.delete(datasetInfo.getDatasetId()));
    } catch (BigQueryException e) {
      fail("Dataset was not deleted. \n" + e);
    }
  }

  private static Map<String, Object> getNextRow() {
    rowCnt++;
    Map<String, Object> row = new HashMap<>();
    Map<String, Object> structVal = new HashMap<>();
    structVal.put("StringField", "Str Val " + rowCnt);
    structVal.put("BooleanField", false);
    row.put("RecordField", structVal); // struct
    row.put("TimestampField", "2022-01-01 01:01:01");
    row.put("StringField", "String Val " + rowCnt);
    row.put("IntegerArrayField", new int[] {1, 2, 3});
    row.put("BooleanField", true);
    row.put("BytesField", "DQ4KDQ==");
    row.put("IntegerField", 1 + rowCnt);
    row.put("GeographyField", "POINT(1 2)");
    row.put("NumericField", 100 + rowCnt);
    row.put("BigNumericField", 10000000L + rowCnt);
    row.put("TimeField", "12:11:35");
    row.put("DateField", "2022-01-01");
    row.put("DateTimeField", "0001-01-01 00:00:00");
    row.put("JSONField", "{\"hello\":\"world\"}");
    row.put("IntervalField", "10000-0 3660000 87840000:0:0");
    return row;
  }
}
