/*
 * Copyright 2021 Google LLC
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

package com.google.cloud.bigquery;

import static org.junit.Assert.assertEquals;

import com.google.cloud.bigquery.JobInfo.CreateDisposition;
import com.google.cloud.bigquery.JobInfo.SchemaUpdateOption;
import com.google.cloud.bigquery.JobInfo.WriteDisposition;
import com.google.cloud.bigquery.QueryJobConfiguration.Priority;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class ConnectionSettingsTest {
  private static final String TEST_PROJECT_ID = "test-project-id";
  private static final DatasetId DATASET_ID = DatasetId.of("dataset");
  private static final TableId TABLE_ID = TableId.of("dataset", "table");
  private static final Long REQUEST_TIMEOUT = 10l;
  private static final Integer NUM_BUFFERED_ROWS = 100;
  private static final Long MAX_RESULTS = 1000l;
  private static final List<String> SOURCE_URIS = ImmutableList.of("uri1", "uri2");
  private static final String KEY = "time_zone";
  private static final String VALUE = "US/Eastern";
  private static final ConnectionProperty CONNECTION_PROPERTY =
      ConnectionProperty.newBuilder().setKey(KEY).setValue(VALUE).build();
  private static final List<ConnectionProperty> CONNECTION_PROPERTIES =
      ImmutableList.of(CONNECTION_PROPERTY);
  private static final Field FIELD_SCHEMA1 =
      Field.newBuilder("StringField", StandardSQLTypeName.STRING)
          .setMode(Field.Mode.NULLABLE)
          .setDescription("FieldDescription1")
          .build();
  private static final Field FIELD_SCHEMA2 =
      Field.newBuilder("IntegerField", StandardSQLTypeName.INT64)
          .setMode(Field.Mode.REPEATED)
          .setDescription("FieldDescription2")
          .build();
  private static final Schema TABLE_SCHEMA = Schema.of(FIELD_SCHEMA1, FIELD_SCHEMA2);
  private static final Integer MAX_BAD_RECORDS = 42;
  private static final Boolean IGNORE_UNKNOWN_VALUES = true;
  private static final String COMPRESSION = "GZIP";
  private static final CsvOptions CSV_OPTIONS = CsvOptions.newBuilder().build();
  private static final ExternalTableDefinition TABLE_CONFIGURATION =
      ExternalTableDefinition.newBuilder(SOURCE_URIS, TABLE_SCHEMA, CSV_OPTIONS)
          .setCompression(COMPRESSION)
          .setIgnoreUnknownValues(IGNORE_UNKNOWN_VALUES)
          .setMaxBadRecords(MAX_BAD_RECORDS)
          .build();
  private static final Map<String, ExternalTableDefinition> TABLE_DEFINITIONS =
      ImmutableMap.of("tableName", TABLE_CONFIGURATION);
  private static final CreateDisposition CREATE_DISPOSITION = CreateDisposition.CREATE_IF_NEEDED;
  private static final WriteDisposition WRITE_DISPOSITION = WriteDisposition.WRITE_APPEND;
  private static final Priority PRIORITY = Priority.BATCH;
  private static final boolean ALLOW_LARGE_RESULTS = true;
  private static final boolean USE_QUERY_CACHE = false;
  private static final boolean FLATTEN_RESULTS = true;
  private static final Integer MAX_BILLING_TIER = 123;
  private static final Long MAX_BYTES_BILL = 12345L;
  private static final List<SchemaUpdateOption> SCHEMA_UPDATE_OPTIONS =
      ImmutableList.of(SchemaUpdateOption.ALLOW_FIELD_RELAXATION);
  private static final List<UserDefinedFunction> USER_DEFINED_FUNCTIONS =
      ImmutableList.of(UserDefinedFunction.inline("Function"), UserDefinedFunction.fromUri("URI"));
  private static final EncryptionConfiguration JOB_ENCRYPTION_CONFIGURATION =
      EncryptionConfiguration.newBuilder().setKmsKeyName("KMS_KEY_1").build();
  private static final TimePartitioning TIME_PARTITIONING =
      TimePartitioning.of(TimePartitioning.Type.DAY);
  private static final Clustering CLUSTERING =
      Clustering.newBuilder().setFields(ImmutableList.of("Foo", "Bar")).build();
  private static final Long TIMEOUT = 10L;
  private static final RangePartitioning.Range RANGE =
      RangePartitioning.Range.newBuilder().setStart(1L).setInterval(2L).setEnd(10L).build();
  private static final RangePartitioning RANGE_PARTITIONING =
      RangePartitioning.newBuilder().setField("IntegerField").setRange(RANGE).build();

  private static final ConnectionSettings CONNECTION_SETTINGS =
      ConnectionSettings.newBuilder()
          .setRequestTimeout(REQUEST_TIMEOUT)
          .setNumBufferedRows(NUM_BUFFERED_ROWS)
          .setMaxResults(MAX_RESULTS)
          .setUseQueryCache(USE_QUERY_CACHE)
          .setTableDefinitions(TABLE_DEFINITIONS)
          .setAllowLargeResults(ALLOW_LARGE_RESULTS)
          .setCreateDisposition(CREATE_DISPOSITION)
          .setDefaultDataset(DATASET_ID)
          .setDestinationTable(TABLE_ID)
          .setWriteDisposition(WRITE_DISPOSITION)
          .setPriority(PRIORITY)
          .setFlattenResults(FLATTEN_RESULTS)
          .setUserDefinedFunctions(USER_DEFINED_FUNCTIONS)
          .setMaximumBillingTier(MAX_BILLING_TIER)
          .setMaximumBytesBilled(MAX_BYTES_BILL)
          .setSchemaUpdateOptions(SCHEMA_UPDATE_OPTIONS)
          .setDestinationEncryptionConfiguration(JOB_ENCRYPTION_CONFIGURATION)
          .setTimePartitioning(TIME_PARTITIONING)
          .setClustering(CLUSTERING)
          .setJobTimeoutMs(TIMEOUT)
          .setRangePartitioning(RANGE_PARTITIONING)
          .setConnectionProperties(CONNECTION_PROPERTIES)
          .build();

  @Test
  public void testToBuilder() {
    compareConnectionSettings(CONNECTION_SETTINGS, CONNECTION_SETTINGS.toBuilder().build());
  }

  @Test
  public void testToBuilderIncomplete() {
    ConnectionSettings connectionSettings =
        ConnectionSettings.newBuilder().setDefaultDataset(DATASET_ID).build();
    compareConnectionSettings(connectionSettings, connectionSettings.toBuilder().build());
  }

  @Test
  public void testBuilder() {
    assertEquals(REQUEST_TIMEOUT, CONNECTION_SETTINGS.getRequestTimeout());
    assertEquals(NUM_BUFFERED_ROWS, CONNECTION_SETTINGS.getNumBufferedRows());
    assertEquals(MAX_RESULTS, CONNECTION_SETTINGS.getMaxResults());
  }

  private void compareConnectionSettings(ConnectionSettings expected, ConnectionSettings value) {
    assertEquals(expected, value);
    assertEquals(expected.hashCode(), value.hashCode());
    assertEquals(expected.toString(), value.toString());
    assertEquals(expected.getRequestTimeout(), value.getRequestTimeout());
    assertEquals(expected.getNumBufferedRows(), value.getNumBufferedRows());
    assertEquals(expected.getMaxResults(), value.getMaxResults());
    assertEquals(expected.getAllowLargeResults(), value.getAllowLargeResults());
    assertEquals(expected.getCreateDisposition(), value.getCreateDisposition());
    assertEquals(expected.getDefaultDataset(), value.getDefaultDataset());
    assertEquals(expected.getDestinationTable(), value.getDestinationTable());
    assertEquals(expected.getFlattenResults(), value.getFlattenResults());
    assertEquals(expected.getPriority(), value.getPriority());
    assertEquals(expected.getTableDefinitions(), value.getTableDefinitions());
    assertEquals(expected.getUseQueryCache(), value.getUseQueryCache());
    assertEquals(expected.getUserDefinedFunctions(), value.getUserDefinedFunctions());
    assertEquals(expected.getWriteDisposition(), value.getWriteDisposition());
    assertEquals(expected.getMaximumBillingTier(), value.getMaximumBillingTier());
    assertEquals(expected.getMaximumBytesBilled(), value.getMaximumBytesBilled());
    assertEquals(expected.getSchemaUpdateOptions(), value.getSchemaUpdateOptions());
    assertEquals(
        expected.getDestinationEncryptionConfiguration(),
        value.getDestinationEncryptionConfiguration());
    assertEquals(expected.getTimePartitioning(), value.getTimePartitioning());
    assertEquals(expected.getClustering(), value.getClustering());
    assertEquals(expected.getJobTimeoutMs(), value.getJobTimeoutMs());
    assertEquals(expected.getRangePartitioning(), value.getRangePartitioning());
    assertEquals(expected.getConnectionProperties(), value.getConnectionProperties());
  }
}
