/*
 * Copyright 2016 Google LLC
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

import static org.junit.Assert.assertEquals;

import com.google.cloud.bigquery.JobInfo.CreateDisposition;
import com.google.cloud.bigquery.JobInfo.SchemaUpdateOption;
import com.google.cloud.bigquery.JobInfo.WriteDisposition;
import com.google.cloud.bigquery.TimePartitioning.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class LoadJobConfigurationTest {

  private static final String TEST_PROJECT_ID = "test-project-id";
  private static final CsvOptions CSV_OPTIONS =
      CsvOptions.newBuilder()
          .setAllowJaggedRows(true)
          .setAllowQuotedNewLines(false)
          .setEncoding(StandardCharsets.UTF_8)
          .build();
  private static final TableId TABLE_ID = TableId.of("dataset", "table");
  private static final CreateDisposition CREATE_DISPOSITION = CreateDisposition.CREATE_IF_NEEDED;
  private static final WriteDisposition WRITE_DISPOSITION = WriteDisposition.WRITE_APPEND;
  private static final Integer MAX_BAD_RECORDS = 42;
  private static final String FORMAT = "CSV";
  private static final Boolean IGNORE_UNKNOWN_VALUES = true;
  private static final Field FIELD_SCHEMA =
      Field.newBuilder("IntegerField", LegacySQLTypeName.INTEGER)
          .setMode(Field.Mode.REQUIRED)
          .setDescription("FieldDescription")
          .build();
  private static final List<String> SOURCE_URIS = ImmutableList.of("uri1", "uri2");
  private static final List<String> DECIMAL_TARGET_TYPES =
      ImmutableList.of("NUMERIC", "BIGNUMERIC", "STRING");
  private static final List<SchemaUpdateOption> SCHEMA_UPDATE_OPTIONS =
      ImmutableList.of(SchemaUpdateOption.ALLOW_FIELD_ADDITION);
  private static final Schema TABLE_SCHEMA = Schema.of(FIELD_SCHEMA);
  private static final Boolean AUTODETECT = true;
  private static final Boolean USE_AVRO_LOGICAL_TYPES = true;

  private static final boolean CREATE_SESSION = true;
  private static final EncryptionConfiguration JOB_ENCRYPTION_CONFIGURATION =
      EncryptionConfiguration.newBuilder().setKmsKeyName("KMS_KEY_1").build();
  private static final TimePartitioning TIME_PARTITIONING = TimePartitioning.of(Type.DAY);
  private static final Clustering CLUSTERING =
      Clustering.newBuilder().setFields(ImmutableList.of("Foo", "Bar")).build();
  private static final Map<String, String> LABELS =
      ImmutableMap.of("test-job-name", "test-load-job");
  private static final Long TIMEOUT = 10L;
  private static final RangePartitioning.Range RANGE =
      RangePartitioning.Range.newBuilder().setStart(1L).setInterval(2L).setEnd(10L).build();
  private static final RangePartitioning RANGE_PARTITIONING =
      RangePartitioning.newBuilder().setField("IntegerField").setRange(RANGE).build();
  private static final String MODE = "STRING";
  private static final String SOURCE_URI_PREFIX = "gs://bucket/path_to_table";

  private static final String KEY = "session_id";
  private static final String VALUE = "session_id_1234567890";
  private static final ConnectionProperty CONNECTION_PROPERTY =
      ConnectionProperty.newBuilder().setKey(KEY).setValue(VALUE).build();
  private static final List<ConnectionProperty> CONNECTION_PROPERTIES =
      ImmutableList.of(CONNECTION_PROPERTY);
  private static final HivePartitioningOptions HIVE_PARTITIONING_OPTIONS =
      HivePartitioningOptions.newBuilder()
          .setMode(MODE)
          .setSourceUriPrefix(SOURCE_URI_PREFIX)
          .build();
  private static final LoadJobConfiguration LOAD_CONFIGURATION_CSV =
      LoadJobConfiguration.newBuilder(TABLE_ID, SOURCE_URIS)
          .setDecimalTargetTypes(DECIMAL_TARGET_TYPES)
          .setCreateDisposition(CREATE_DISPOSITION)
          .setWriteDisposition(WRITE_DISPOSITION)
          .setFormatOptions(CSV_OPTIONS)
          .setIgnoreUnknownValues(IGNORE_UNKNOWN_VALUES)
          .setMaxBadRecords(MAX_BAD_RECORDS)
          .setSchema(TABLE_SCHEMA)
          .setSchemaUpdateOptions(SCHEMA_UPDATE_OPTIONS)
          .setAutodetect(AUTODETECT)
          .setDestinationEncryptionConfiguration(JOB_ENCRYPTION_CONFIGURATION)
          .setTimePartitioning(TIME_PARTITIONING)
          .setClustering(CLUSTERING)
          .setLabels(LABELS)
          .setJobTimeoutMs(TIMEOUT)
          .setRangePartitioning(RANGE_PARTITIONING)
          .setNullMarker("nullMarker")
          .setHivePartitioningOptions(HIVE_PARTITIONING_OPTIONS)
          .setConnectionProperties(CONNECTION_PROPERTIES)
          .setCreateSession(CREATE_SESSION)
          .build();

  private static final DatastoreBackupOptions BACKUP_OPTIONS =
      DatastoreBackupOptions.newBuilder()
          .setProjectionFields(ImmutableList.of("field_1", "field_2"))
          .build();
  private static final LoadJobConfiguration LOAD_CONFIGURATION_BACKUP =
      LoadJobConfiguration.newBuilder(TABLE_ID, SOURCE_URIS)
          .setCreateDisposition(CREATE_DISPOSITION)
          .setWriteDisposition(WRITE_DISPOSITION)
          .setFormatOptions(BACKUP_OPTIONS)
          .setIgnoreUnknownValues(IGNORE_UNKNOWN_VALUES)
          .setMaxBadRecords(MAX_BAD_RECORDS)
          .setSchema(TABLE_SCHEMA)
          .setSchemaUpdateOptions(SCHEMA_UPDATE_OPTIONS)
          .setAutodetect(AUTODETECT)
          .setLabels(LABELS)
          .setJobTimeoutMs(TIMEOUT)
          .setRangePartitioning(RANGE_PARTITIONING)
          .build();
  private static final LoadJobConfiguration LOAD_CONFIGURATION_AVRO =
      LoadJobConfiguration.newBuilder(TABLE_ID, SOURCE_URIS)
          .setCreateDisposition(CREATE_DISPOSITION)
          .setWriteDisposition(WRITE_DISPOSITION)
          .setFormatOptions(FormatOptions.avro())
          .setIgnoreUnknownValues(IGNORE_UNKNOWN_VALUES)
          .setMaxBadRecords(MAX_BAD_RECORDS)
          .setSchema(TABLE_SCHEMA)
          .setSchemaUpdateOptions(SCHEMA_UPDATE_OPTIONS)
          .setAutodetect(AUTODETECT)
          .setDestinationEncryptionConfiguration(JOB_ENCRYPTION_CONFIGURATION)
          .setTimePartitioning(TIME_PARTITIONING)
          .setClustering(CLUSTERING)
          .setUseAvroLogicalTypes(USE_AVRO_LOGICAL_TYPES)
          .setLabels(LABELS)
          .setJobTimeoutMs(TIMEOUT)
          .setRangePartitioning(RANGE_PARTITIONING)
          .build();

  @Test
  public void testToBuilder() {
    compareLoadJobConfiguration(LOAD_CONFIGURATION_CSV, LOAD_CONFIGURATION_CSV.toBuilder().build());
    LoadJobConfiguration configurationCSV =
        LOAD_CONFIGURATION_CSV
            .toBuilder()
            .setDestinationTable(TableId.of("dataset", "newTable"))
            .build();
    assertEquals("newTable", configurationCSV.getDestinationTable().getTable());
    configurationCSV = configurationCSV.toBuilder().setDestinationTable(TABLE_ID).build();
    compareLoadJobConfiguration(LOAD_CONFIGURATION_CSV, configurationCSV);

    compareLoadJobConfiguration(
        LOAD_CONFIGURATION_BACKUP, LOAD_CONFIGURATION_BACKUP.toBuilder().build());
    LoadJobConfiguration configurationBackup =
        LOAD_CONFIGURATION_BACKUP
            .toBuilder()
            .setDestinationTable(TableId.of("dataset", "newTable"))
            .build();
    assertEquals("newTable", configurationBackup.getDestinationTable().getTable());
    configurationBackup = configurationBackup.toBuilder().setDestinationTable(TABLE_ID).build();
    compareLoadJobConfiguration(LOAD_CONFIGURATION_BACKUP, configurationBackup);

    compareLoadJobConfiguration(
        LOAD_CONFIGURATION_AVRO, LOAD_CONFIGURATION_AVRO.toBuilder().build());
    LoadJobConfiguration configurationAvro =
        LOAD_CONFIGURATION_AVRO
            .toBuilder()
            .setDestinationTable(TableId.of("dataset", "newTable"))
            .build();
    assertEquals("newTable", configurationAvro.getDestinationTable().getTable());
    configurationAvro = configurationAvro.toBuilder().setDestinationTable(TABLE_ID).build();
    compareLoadJobConfiguration(LOAD_CONFIGURATION_AVRO, configurationAvro);
  }

  @Test
  public void testOf() {
    LoadJobConfiguration configuration = LoadJobConfiguration.of(TABLE_ID, SOURCE_URIS);
    assertEquals(TABLE_ID, configuration.getDestinationTable());
    assertEquals(SOURCE_URIS, configuration.getSourceUris());
    configuration = LoadJobConfiguration.of(TABLE_ID, SOURCE_URIS, CSV_OPTIONS);
    assertEquals(TABLE_ID, configuration.getDestinationTable());
    assertEquals(FORMAT, configuration.getFormat());
    assertEquals(CSV_OPTIONS, configuration.getCsvOptions());
    assertEquals(SOURCE_URIS, configuration.getSourceUris());
    configuration = LoadJobConfiguration.of(TABLE_ID, "uri1");
    assertEquals(TABLE_ID, configuration.getDestinationTable());
    assertEquals(ImmutableList.of("uri1"), configuration.getSourceUris());
    configuration = LoadJobConfiguration.of(TABLE_ID, "uri1", CSV_OPTIONS);
    assertEquals(TABLE_ID, configuration.getDestinationTable());
    assertEquals(FORMAT, configuration.getFormat());
    assertEquals(CSV_OPTIONS, configuration.getCsvOptions());
    assertEquals(ImmutableList.of("uri1"), configuration.getSourceUris());
  }

  @Test
  public void testToBuilderIncomplete() {
    LoadJobConfiguration configuration = LoadJobConfiguration.of(TABLE_ID, SOURCE_URIS);
    compareLoadJobConfiguration(configuration, configuration.toBuilder().build());
  }

  @Test
  public void testToPbAndFromPb() {
    compareLoadJobConfiguration(
        LOAD_CONFIGURATION_CSV, LoadJobConfiguration.fromPb(LOAD_CONFIGURATION_CSV.toPb()));
    LoadJobConfiguration configuration = LoadJobConfiguration.of(TABLE_ID, SOURCE_URIS);
    compareLoadJobConfiguration(configuration, LoadJobConfiguration.fromPb(configuration.toPb()));
  }

  @Test
  public void testSetProjectId() {
    LoadConfiguration configuration = LOAD_CONFIGURATION_CSV.setProjectId(TEST_PROJECT_ID);
    assertEquals(TEST_PROJECT_ID, configuration.getDestinationTable().getProject());
  }

  @Test
  public void testSetProjectIdDoNotOverride() {
    LoadConfiguration configuration =
        LOAD_CONFIGURATION_CSV
            .toBuilder()
            .setDestinationTable(TABLE_ID.setProjectId(TEST_PROJECT_ID))
            .build()
            .setProjectId("do-not-update");
    assertEquals(TEST_PROJECT_ID, configuration.getDestinationTable().getProject());
  }

  @Test
  public void testGetType() {
    assertEquals(JobConfiguration.Type.LOAD, LOAD_CONFIGURATION_CSV.getType());
  }

  private void compareLoadJobConfiguration(
      LoadJobConfiguration expected, LoadJobConfiguration value) {
    assertEquals(expected, value);
    assertEquals(expected.hashCode(), value.hashCode());
    assertEquals(expected.toString(), value.toString());
    assertEquals(expected.getDestinationTable(), value.getDestinationTable());
    assertEquals(expected.getDecimalTargetTypes(), value.getDecimalTargetTypes());
    assertEquals(expected.getCreateDisposition(), value.getCreateDisposition());
    assertEquals(expected.getWriteDisposition(), value.getWriteDisposition());
    assertEquals(expected.getCsvOptions(), value.getCsvOptions());
    assertEquals(expected.getFormat(), value.getFormat());
    assertEquals(expected.ignoreUnknownValues(), value.ignoreUnknownValues());
    assertEquals(expected.getMaxBadRecords(), value.getMaxBadRecords());
    assertEquals(expected.getSchema(), value.getSchema());
    assertEquals(expected.getDatastoreBackupOptions(), value.getDatastoreBackupOptions());
    assertEquals(expected.getAutodetect(), value.getAutodetect());
    assertEquals(expected.getSchemaUpdateOptions(), value.getSchemaUpdateOptions());
    assertEquals(
        expected.getDestinationEncryptionConfiguration(),
        value.getDestinationEncryptionConfiguration());
    assertEquals(expected.getTimePartitioning(), value.getTimePartitioning());
    assertEquals(expected.getClustering(), value.getClustering());
    assertEquals(expected.getUseAvroLogicalTypes(), value.getUseAvroLogicalTypes());
    assertEquals(expected.getLabels(), value.getLabels());
    assertEquals(expected.getJobTimeoutMs(), value.getJobTimeoutMs());
    assertEquals(expected.getRangePartitioning(), value.getRangePartitioning());
    assertEquals(expected.getNullMarker(), value.getNullMarker());
    assertEquals(expected.getHivePartitioningOptions(), value.getHivePartitioningOptions());
    assertEquals(expected.getConnectionProperties(), value.getConnectionProperties());
    assertEquals(expected.getCreateSession(), value.getCreateSession());
  }
}
