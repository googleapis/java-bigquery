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
package com.google.cloud.bigquery.it;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.DataFormatOptions;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryParameterValue;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigquery.testing.RemoteBigQueryHelper;
import com.google.protobuf.Timestamp;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class ITHighPrecisionTimestamp {

  public static final String TEST_HIGH_PRECISION_TIMESTAMP_TABLE_NAME =
      "test_high_precision_timestamp";
  private static BigQuery bigquery;
  private static final String DATASET = RemoteBigQueryHelper.generateDatasetName();
  private static TableId default_table_id;
  public static final long TIMESTAMP_PICOSECOND_PRECISION = 12L;
  private static final Field TIMESTAMP_HIGH_PRECISION_FIELD_SCHEMA =
      Field.newBuilder("timestampHighPrecisionField", StandardSQLTypeName.TIMESTAMP)
          .setTimestampPrecision(TIMESTAMP_PICOSECOND_PRECISION)
          .build();
  private static final Schema TABLE_SCHEMA = Schema.of(TIMESTAMP_HIGH_PRECISION_FIELD_SCHEMA);

  private static final String TIMESTAMP1 = "2025-01-01T12:34:56.123456789123Z";
  private static final String TIMESTAMP2 = "1970-01-01T12:12:12.123456789123Z";

  @BeforeClass
  public static void beforeClass() {
    BigQueryOptions.Builder builder =
        BigQueryOptions.newBuilder()
            .setDataFormatOptions(
                DataFormatOptions.newBuilder()
                    .timestampFormatOptions(DataFormatOptions.TimestampFormatOptions.ISO8601_STRING)
                    .build());
    RemoteBigQueryHelper bigqueryHelper = RemoteBigQueryHelper.create(builder);
    bigquery = bigqueryHelper.getOptions().getService();
    DatasetInfo info = DatasetInfo.newBuilder(DATASET).build();
    bigquery.create(info);

    StandardTableDefinition tableDefinition =
        StandardTableDefinition.newBuilder().setSchema(TABLE_SCHEMA).build();
    default_table_id = TableId.of(DATASET, TEST_HIGH_PRECISION_TIMESTAMP_TABLE_NAME);
    Table createdTable = bigquery.create(TableInfo.of(default_table_id, tableDefinition));
    assertNotNull(createdTable);

    // Populate with some starter data
    Map<String, Object> timestamp1 =
        Collections.singletonMap("timestampHighPrecisionField", TIMESTAMP1);
    Map<String, Object> timestamp2 =
        Collections.singletonMap("timestampHighPrecisionField", TIMESTAMP2);
    InsertAllRequest request =
        InsertAllRequest.newBuilder(default_table_id).addRow(timestamp1).addRow(timestamp2).build();
    InsertAllResponse response = bigquery.insertAll(request);
    assertFalse(response.hasErrors());
    assertEquals(0, response.getInsertErrors().size());
  }

  @AfterClass
  public static void afterClass() {
    if (bigquery != null) {
      bigquery.delete(default_table_id);
      RemoteBigQueryHelper.forceDelete(bigquery, DATASET);
    }
  }

  @Test
  public void query_highPrecisionTimestamp() throws InterruptedException {
    String sql = "SELECT timestampHighPrecisionField FROM " + default_table_id.getTable() + ";";
    QueryJobConfiguration queryJobConfiguration =
        QueryJobConfiguration.newBuilder(sql)
            .setDefaultDataset(DatasetId.of(DATASET))
            .setUseLegacySql(false)
            .build();
    TableResult result = bigquery.query(queryJobConfiguration);
    assertNotNull(result.getJobId());
    String[] expected = new String[] {TIMESTAMP1, TIMESTAMP2};
    List<FieldValueList> list =
        StreamSupport.stream(result.getValues().spliterator(), false).collect(Collectors.toList());
    assertEquals(expected.length, list.size());
    for (int i = 0; i < list.size(); i++) {
      assertEquals(expected[i], list.get(i).get(0).getValue());
    }
  }

  @Test
  public void insert_highPrecisionTimestamp_ISOValidFormat() {
    StandardTableDefinition tableDefinition =
        StandardTableDefinition.newBuilder().setSchema(TABLE_SCHEMA).build();
    String tempTable = "insert_temp_" + TEST_HIGH_PRECISION_TIMESTAMP_TABLE_NAME;
    TableId tableId = TableId.of(DATASET, tempTable);
    Table createdTable = bigquery.create(TableInfo.of(tableId, tableDefinition));
    assertNotNull(createdTable);

    Map<String, Object> timestampISO =
        Collections.singletonMap("timestampHighPrecisionField", "2025-01-01 12:34:56.123456Z");
    InsertAllRequest request = InsertAllRequest.newBuilder(tableId).addRow(timestampISO).build();
    InsertAllResponse response = bigquery.insertAll(request);
    assertFalse(response.hasErrors());
    assertEquals(0, response.getInsertErrors().size());

    bigquery.delete(tableId);
  }

  @Test
  public void insert_highPrecisionTimestamp_invalidFormats() {
    StandardTableDefinition tableDefinition =
        StandardTableDefinition.newBuilder().setSchema(TABLE_SCHEMA).build();
    String tempTable = "insert_temp_" + TEST_HIGH_PRECISION_TIMESTAMP_TABLE_NAME;
    TableId tableId = TableId.of(DATASET, tempTable);
    Table createdTable = bigquery.create(TableInfo.of(tableId, tableDefinition));
    assertNotNull(createdTable);

    Map<String, Object> timestampInMicros =
        Collections.singletonMap("timestampHighPrecisionField", 123456);
    Map<String, Object> timestampInMicrosString =
        Collections.singletonMap("timestampHighPrecisionField", "123456");
    Map<String, Object> timestampNegative =
        Collections.singletonMap("timestampHighPrecisionField", -123456);
    Map<String, Object> timestampFloat =
        Collections.singletonMap("timestampHighPrecisionField", 1000.0);
    Map<String, Object> timestampProtobuf =
        Collections.singletonMap(
            "timestampHighPrecisionField",
            Timestamp.newBuilder().setSeconds(123456789).setNanos(123456789).build());
    Map<String, Object> timestampProtobufNegative =
        Collections.singletonMap(
            "timestampHighPrecisionField",
            Timestamp.newBuilder().setSeconds(-123456789).setNanos(-123456789).build());
    InsertAllRequest request =
        InsertAllRequest.newBuilder(tableId)
            .addRow(timestampInMicros)
            .addRow(timestampInMicrosString)
            .addRow(timestampNegative)
            .addRow(timestampFloat)
            .addRow(timestampProtobuf)
            .addRow(timestampProtobufNegative)
            .build();
    InsertAllResponse response = bigquery.insertAll(request);
    assertTrue(response.hasErrors());
    assertEquals(request.getRows().size(), response.getInsertErrors().size());

    bigquery.delete(tableId);
  }

  @Test
  public void queryNamedParameter_highPrecisionTimestamp() throws InterruptedException {
    String query =
        String.format(
            "SELECT * FROM %s.%s WHERE timestampHighPrecisionField >= CAST(@timestampParam AS TIMESTAMP(12))",
            DATASET, default_table_id.getTable());

    QueryJobConfiguration queryConfig =
        QueryJobConfiguration.newBuilder(query)
            .setDefaultDataset(DATASET)
            .setUseLegacySql(false)
            .addNamedParameter(
                "timestampParam", QueryParameterValue.timestamp("2000-01-01 12:34:56.123456789123"))
            .build();

    TableResult result = bigquery.query(queryConfig);
    assertNotNull(result);
    List<String> timestamps =
        StreamSupport.stream(result.getValues().spliterator(), false)
            .map(x -> (String) x.get(0).getValue())
            .collect(Collectors.toList());
    assertEquals(1, timestamps.size());
    assertEquals(TIMESTAMP1, timestamps.get(0));
  }

  @Test
  public void queryNamedParameter_highPrecisionTimestamp_microsLong() throws InterruptedException {
    String query =
        String.format(
            "SELECT * FROM %s.%s WHERE timestampHighPrecisionField >= CAST(@timestampParam AS TIMESTAMP(12))",
            DATASET, default_table_id.getTable());

    QueryJobConfiguration queryConfig =
        QueryJobConfiguration.newBuilder(query)
            .setDefaultDataset(DATASET)
            .setUseLegacySql(false)
            .addNamedParameter(
                "timestampParam",
                QueryParameterValue.timestamp(946730096123456L)) // 2000-01-01 12:34:56.123456
            .build();

    TableResult result = bigquery.query(queryConfig);
    assertNotNull(result);
    List<String> timestamps =
        StreamSupport.stream(result.getValues().spliterator(), false)
            .map(x -> (String) x.get(0).getValue())
            .collect(Collectors.toList());
    assertEquals(1, timestamps.size());
    assertEquals(TIMESTAMP1, timestamps.get(0));
  }

  @Test
  public void queryNamedParameter_highPrecisionTimestamp_microsISOString()
      throws InterruptedException {
    String query =
        String.format(
            "SELECT * FROM %s.%s WHERE timestampHighPrecisionField >= CAST(@timestampParam AS TIMESTAMP(12))",
            DATASET, default_table_id.getTable());

    QueryJobConfiguration queryConfig =
        QueryJobConfiguration.newBuilder(query)
            .setDefaultDataset(DATASET)
            .setUseLegacySql(false)
            .addNamedParameter(
                "timestampParam", QueryParameterValue.timestamp("2000-01-01 12:34:56.123456"))
            .build();

    TableResult result = bigquery.query(queryConfig);
    assertNotNull(result);
    List<String> timestamps =
        StreamSupport.stream(result.getValues().spliterator(), false)
            .map(x -> (String) x.get(0).getValue())
            .collect(Collectors.toList());
    assertEquals(1, timestamps.size());
    assertEquals(TIMESTAMP1, timestamps.get(0));
  }

  @Test
  public void queryNamedParameter_highPrecisionTimestamp_noExplicitCastInQuery_fails() {
    String query =
        String.format(
            "SELECT * FROM %s.%s WHERE timestampHighPrecisionField >= @timestampParam",
            DATASET, default_table_id.getTable());

    QueryJobConfiguration queryConfig =
        QueryJobConfiguration.newBuilder(query)
            .setDefaultDataset(DATASET)
            .setUseLegacySql(false)
            .addNamedParameter(
                "timestampParam", QueryParameterValue.timestamp("2000-01-01 12:34:56.123456789123"))
            .build();

    BigQueryException exception =
        assertThrows(BigQueryException.class, () -> bigquery.query(queryConfig));
    assertEquals("Invalid argument type passed to a function", exception.getMessage());
  }
}
