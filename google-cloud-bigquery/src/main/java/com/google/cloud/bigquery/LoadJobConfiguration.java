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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.services.bigquery.model.JobConfigurationLoad;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Google BigQuery load job configuration. A load job loads data from one of several formats into a
 * table. Data is provided as URIs that point to objects in Google Cloud Storage. Load job
 * configurations have {@link JobConfiguration.Type#LOAD} type.
 */
public final class LoadJobConfiguration extends JobConfiguration implements LoadConfiguration {

  private static final long serialVersionUID = -2673554846792429829L;

  private final List<String> sourceUris;
  private final TableId destinationTable;
  private final List<String> decimalTargetTypes;
  private final EncryptionConfiguration destinationEncryptionConfiguration;
  private final JobInfo.CreateDisposition createDisposition;
  private final JobInfo.WriteDisposition writeDisposition;
  private final FormatOptions formatOptions;
  private final String nullMarker;
  private final Integer maxBadRecords;
  private final Schema schema;
  private final Boolean ignoreUnknownValues;
  private final List<JobInfo.SchemaUpdateOption> schemaUpdateOptions;
  private final Boolean autodetect;
  private final TimePartitioning timePartitioning;
  private final Clustering clustering;
  private final Boolean useAvroLogicalTypes;
  private final Map<String, String> labels;
  private final Long jobTimeoutMs;
  private final RangePartitioning rangePartitioning;
  private final HivePartitioningOptions hivePartitioningOptions;
  private final String referenceFileSchemaUri;

  private final List<ConnectionProperty> connectionProperties;

  private final Boolean createSession;

  public static final class Builder extends JobConfiguration.Builder<LoadJobConfiguration, Builder>
      implements LoadConfiguration.Builder {

    private List<String> sourceUris;
    private TableId destinationTable;
    private List<String> decimalTargetTypes;
    private EncryptionConfiguration destinationEncryptionConfiguration;
    private JobInfo.CreateDisposition createDisposition;
    private JobInfo.WriteDisposition writeDisposition;
    private FormatOptions formatOptions;
    private String nullMarker;
    private Integer maxBadRecords;
    private Schema schema;
    private Boolean ignoreUnknownValues;
    private List<String> projectionFields;
    private List<JobInfo.SchemaUpdateOption> schemaUpdateOptions;
    private Boolean autodetect;
    private TimePartitioning timePartitioning;
    private Clustering clustering;
    private Boolean useAvroLogicalTypes;
    private Map<String, String> labels;
    private Long jobTimeoutMs;
    private RangePartitioning rangePartitioning;
    private HivePartitioningOptions hivePartitioningOptions;
    private String referenceFileSchemaUri;
    private List<ConnectionProperty> connectionProperties;
    private Boolean createSession;

    private Builder() {
      super(Type.LOAD);
    }

    private Builder(LoadJobConfiguration loadConfiguration) {
      this();
      this.destinationTable = loadConfiguration.destinationTable;
      this.decimalTargetTypes = loadConfiguration.decimalTargetTypes;
      this.createDisposition = loadConfiguration.createDisposition;
      this.writeDisposition = loadConfiguration.writeDisposition;
      this.formatOptions = loadConfiguration.formatOptions;
      this.nullMarker = loadConfiguration.nullMarker;
      this.maxBadRecords = loadConfiguration.maxBadRecords;
      this.schema = loadConfiguration.schema;
      this.ignoreUnknownValues = loadConfiguration.ignoreUnknownValues;
      this.sourceUris = loadConfiguration.sourceUris;
      this.schemaUpdateOptions = loadConfiguration.schemaUpdateOptions;
      this.autodetect = loadConfiguration.autodetect;
      this.destinationEncryptionConfiguration =
          loadConfiguration.destinationEncryptionConfiguration;
      this.timePartitioning = loadConfiguration.timePartitioning;
      this.clustering = loadConfiguration.clustering;
      this.useAvroLogicalTypes = loadConfiguration.useAvroLogicalTypes;
      this.labels = loadConfiguration.labels;
      this.jobTimeoutMs = loadConfiguration.jobTimeoutMs;
      this.rangePartitioning = loadConfiguration.rangePartitioning;
      this.hivePartitioningOptions = loadConfiguration.hivePartitioningOptions;
      this.referenceFileSchemaUri = loadConfiguration.referenceFileSchemaUri;
      this.connectionProperties = loadConfiguration.connectionProperties;
      this.createSession = loadConfiguration.createSession;
    }

    private Builder(com.google.api.services.bigquery.model.JobConfiguration configurationPb) {
      this();
      JobConfigurationLoad loadConfigurationPb = configurationPb.getLoad();
      this.destinationTable = TableId.fromPb(loadConfigurationPb.getDestinationTable());
      if (loadConfigurationPb.getDecimalTargetTypes() != null) {
        this.decimalTargetTypes = ImmutableList.copyOf(loadConfigurationPb.getDecimalTargetTypes());
      }
      if (loadConfigurationPb.getCreateDisposition() != null) {
        this.createDisposition =
            JobInfo.CreateDisposition.valueOf(loadConfigurationPb.getCreateDisposition());
      }
      if (loadConfigurationPb.getWriteDisposition() != null) {
        this.writeDisposition =
            JobInfo.WriteDisposition.valueOf(loadConfigurationPb.getWriteDisposition());
      }
      if (loadConfigurationPb.getSourceFormat() != null) {
        this.formatOptions = FormatOptions.of(loadConfigurationPb.getSourceFormat());
      }
      if (loadConfigurationPb.getNullMarker() != null) {
        this.nullMarker = loadConfigurationPb.getNullMarker();
      }
      if (loadConfigurationPb.getAllowJaggedRows() != null
          || loadConfigurationPb.getAllowQuotedNewlines() != null
          || loadConfigurationPb.getEncoding() != null
          || loadConfigurationPb.getFieldDelimiter() != null
          || loadConfigurationPb.getQuote() != null
          || loadConfigurationPb.getSkipLeadingRows() != null) {
        CsvOptions.Builder builder =
            CsvOptions.newBuilder()
                .setEncoding(loadConfigurationPb.getEncoding())
                .setFieldDelimiter(loadConfigurationPb.getFieldDelimiter())
                .setQuote(loadConfigurationPb.getQuote());
        if (loadConfigurationPb.getAllowJaggedRows() != null) {
          builder.setAllowJaggedRows(loadConfigurationPb.getAllowJaggedRows());
        }
        if (loadConfigurationPb.getAllowQuotedNewlines() != null) {
          builder.setAllowQuotedNewLines(loadConfigurationPb.getAllowQuotedNewlines());
        }
        if (loadConfigurationPb.getSkipLeadingRows() != null) {
          builder.setSkipLeadingRows(loadConfigurationPb.getSkipLeadingRows());
        }
        this.formatOptions = builder.build();
      }
      this.maxBadRecords = loadConfigurationPb.getMaxBadRecords();
      if (loadConfigurationPb.getSchema() != null) {
        this.schema = Schema.fromPb(loadConfigurationPb.getSchema());
      }
      this.ignoreUnknownValues = loadConfigurationPb.getIgnoreUnknownValues();
      this.projectionFields = loadConfigurationPb.getProjectionFields();
      if (loadConfigurationPb.getSourceUris() != null) {
        this.sourceUris = ImmutableList.copyOf(configurationPb.getLoad().getSourceUris());
      }
      if (loadConfigurationPb.getSchemaUpdateOptions() != null) {
        ImmutableList.Builder<JobInfo.SchemaUpdateOption> schemaUpdateOptionsBuilder =
            new ImmutableList.Builder<>();
        for (String rawSchemaUpdateOption : loadConfigurationPb.getSchemaUpdateOptions()) {
          schemaUpdateOptionsBuilder.add(JobInfo.SchemaUpdateOption.valueOf(rawSchemaUpdateOption));
        }
        this.schemaUpdateOptions = schemaUpdateOptionsBuilder.build();
      }
      if (loadConfigurationPb.getTimePartitioning() != null) {
        this.timePartitioning = TimePartitioning.fromPb(loadConfigurationPb.getTimePartitioning());
      }
      if (loadConfigurationPb.getClustering() != null) {
        this.clustering = Clustering.fromPb(loadConfigurationPb.getClustering());
      }
      this.autodetect = loadConfigurationPb.getAutodetect();
      this.useAvroLogicalTypes = loadConfigurationPb.getUseAvroLogicalTypes();
      if (loadConfigurationPb.getDestinationEncryptionConfiguration() != null) {
        this.destinationEncryptionConfiguration =
            new EncryptionConfiguration.Builder(
                    loadConfigurationPb.getDestinationEncryptionConfiguration())
                .build();
      }
      if (configurationPb.getLabels() != null) {
        this.labels = configurationPb.getLabels();
      }
      if (configurationPb.getJobTimeoutMs() != null) {
        this.jobTimeoutMs = configurationPb.getJobTimeoutMs();
      }
      if (loadConfigurationPb.getRangePartitioning() != null) {
        this.rangePartitioning =
            RangePartitioning.fromPb(loadConfigurationPb.getRangePartitioning());
      }
      if (loadConfigurationPb.getHivePartitioningOptions() != null) {
        this.hivePartitioningOptions =
            HivePartitioningOptions.fromPb(loadConfigurationPb.getHivePartitioningOptions());
      }
      if (loadConfigurationPb.getReferenceFileSchemaUri() != null) {
        this.referenceFileSchemaUri = loadConfigurationPb.getReferenceFileSchemaUri();
      }
      if (loadConfigurationPb.getConnectionProperties() != null) {

        this.connectionProperties =
            Lists.transform(
                loadConfigurationPb.getConnectionProperties(), ConnectionProperty.FROM_PB_FUNCTION);
      }
      createSession = loadConfigurationPb.getCreateSession();
    }

    @Override
    public Builder setDestinationTable(TableId destinationTable) {
      this.destinationTable = destinationTable;
      return this;
    }

    @Override
    public Builder setDestinationEncryptionConfiguration(
        EncryptionConfiguration encryptionConfiguration) {
      this.destinationEncryptionConfiguration = encryptionConfiguration;
      return this;
    }

    @Override
    public Builder setCreateDisposition(JobInfo.CreateDisposition createDisposition) {
      this.createDisposition = createDisposition;
      return this;
    }

    @Override
    public Builder setWriteDisposition(JobInfo.WriteDisposition writeDisposition) {
      this.writeDisposition = writeDisposition;
      return this;
    }

    @Override
    public Builder setFormatOptions(FormatOptions formatOptions) {
      this.formatOptions = formatOptions;
      return this;
    }

    @Override
    public Builder setNullMarker(String nullMarker) {
      this.nullMarker = nullMarker;
      return this;
    }

    @Override
    public Builder setMaxBadRecords(Integer maxBadRecords) {
      this.maxBadRecords = maxBadRecords;
      return this;
    }

    @Override
    public Builder setSchema(Schema schema) {
      this.schema = schema;
      return this;
    }

    @Override
    public Builder setIgnoreUnknownValues(Boolean ignoreUnknownValues) {
      this.ignoreUnknownValues = ignoreUnknownValues;
      return this;
    }

    @Override
    public Builder setTimePartitioning(TimePartitioning timePartitioning) {
      this.timePartitioning = timePartitioning;
      return this;
    }

    @Override
    public Builder setClustering(Clustering clustering) {
      this.clustering = clustering;
      return this;
    }

    @Override
    public Builder setUseAvroLogicalTypes(Boolean useAvroLogicalTypes) {
      this.useAvroLogicalTypes = useAvroLogicalTypes;
      return this;
    }

    /**
     * Sets the fully-qualified URIs that point to source data in Google Cloud Storage (e.g.
     * gs://bucket/path). Each URI can contain one '*' wildcard character and it must come after the
     * 'bucket' name.
     */
    public Builder setSourceUris(List<String> sourceUris) {
      this.sourceUris = ImmutableList.copyOf(checkNotNull(sourceUris));
      return this;
    }

    /**
     * Defines the list of possible SQL data types to which the source decimal values are converted.
     * This list and the precision and the scale parameters of the decimal field determine the
     * target type. In the order of NUMERIC, BIGNUMERIC, and STRING, a type is picked if it is in
     * the specified list and if it supports the precision and the scale. STRING supports all
     * precision and scale values.
     *
     * @param decimalTargetTypes decimalTargetType or {@code null} for none
     */
    public Builder setDecimalTargetTypes(List<String> decimalTargetTypes) {
      this.decimalTargetTypes = decimalTargetTypes;
      return this;
    }

    public Builder setAutodetect(Boolean autodetect) {
      this.autodetect = autodetect;
      return this;
    }

    @Override
    public Builder setSchemaUpdateOptions(List<JobInfo.SchemaUpdateOption> schemaUpdateOptions) {
      this.schemaUpdateOptions =
          schemaUpdateOptions != null ? ImmutableList.copyOf(schemaUpdateOptions) : null;
      return this;
    }

    /**
     * The labels associated with this job. You can use these to organize and group your jobs. Label
     * keys and values can be no longer than 63 characters, can only contain lowercase letters,
     * numeric characters, underscores and dashes. International characters are allowed. Label
     * values are optional. Label keys must start with a letter and each label in the list must have
     * a different key.
     *
     * @param labels labels or {@code null} for none
     */
    public Builder setLabels(Map<String, String> labels) {
      this.labels = labels;
      return this;
    }

    /**
     * [Optional] Job timeout in milliseconds. If this time limit is exceeded, BigQuery may attempt
     * to terminate the job.
     *
     * @param jobTimeoutMs jobTimeoutMs or {@code null} for none
     */
    public Builder setJobTimeoutMs(Long jobTimeoutMs) {
      this.jobTimeoutMs = jobTimeoutMs;
      return this;
    }

    /**
     * Range partitioning specification for this table. Only one of timePartitioning and
     * rangePartitioning should be specified.
     *
     * @param rangePartitioning rangePartitioning or {@code null} for none
     */
    public Builder setRangePartitioning(RangePartitioning rangePartitioning) {
      this.rangePartitioning = rangePartitioning;
      return this;
    }

    public Builder setHivePartitioningOptions(HivePartitioningOptions hivePartitioningOptions) {
      this.hivePartitioningOptions = hivePartitioningOptions;
      return this;
    }

    /**
     * When creating an external table, the user can provide a reference file with the table schema.
     * This is enabled for the following formats: AVRO, PARQUET, ORC.
     *
     * @param referenceFileSchemaUri or {@code null} for none
     */
    public Builder setReferenceFileSchemaUri(String referenceFileSchemaUri) {
      this.referenceFileSchemaUri = referenceFileSchemaUri;
      return this;
    }

    public Builder setConnectionProperties(List<ConnectionProperty> connectionProperties) {
      this.connectionProperties = ImmutableList.copyOf(connectionProperties);
      return this;
    }

    public Builder setCreateSession(Boolean createSession) {
      this.createSession = createSession;
      return this;
    }

    @Override
    public LoadJobConfiguration build() {
      return new LoadJobConfiguration(this);
    }
  }

  private LoadJobConfiguration(Builder builder) {
    super(builder);
    this.sourceUris = builder.sourceUris;
    this.destinationTable = builder.destinationTable;
    this.decimalTargetTypes = builder.decimalTargetTypes;
    this.createDisposition = builder.createDisposition;
    this.writeDisposition = builder.writeDisposition;
    this.formatOptions = builder.formatOptions;
    this.nullMarker = builder.nullMarker;
    this.maxBadRecords = builder.maxBadRecords;
    this.schema = builder.schema;
    this.ignoreUnknownValues = builder.ignoreUnknownValues;
    this.schemaUpdateOptions = builder.schemaUpdateOptions;
    this.autodetect = builder.autodetect;
    this.destinationEncryptionConfiguration = builder.destinationEncryptionConfiguration;
    this.timePartitioning = builder.timePartitioning;
    this.clustering = builder.clustering;
    this.useAvroLogicalTypes = builder.useAvroLogicalTypes;
    this.labels = builder.labels;
    this.jobTimeoutMs = builder.jobTimeoutMs;
    this.rangePartitioning = builder.rangePartitioning;
    this.hivePartitioningOptions = builder.hivePartitioningOptions;
    this.referenceFileSchemaUri = builder.referenceFileSchemaUri;
    this.connectionProperties = builder.connectionProperties;
    this.createSession = builder.createSession;
  }

  @Override
  public TableId getDestinationTable() {
    return destinationTable;
  }

  @Override
  public EncryptionConfiguration getDestinationEncryptionConfiguration() {
    return destinationEncryptionConfiguration;
  }

  @Override
  public JobInfo.CreateDisposition getCreateDisposition() {
    return this.createDisposition;
  }

  @Override
  public JobInfo.WriteDisposition getWriteDisposition() {
    return writeDisposition;
  }

  @Override
  public String getNullMarker() {
    return nullMarker;
  }

  @Override
  public CsvOptions getCsvOptions() {
    return formatOptions instanceof CsvOptions ? (CsvOptions) formatOptions : null;
  }

  public ParquetOptions getParquetOptions() {
    return formatOptions instanceof ParquetOptions ? (ParquetOptions) formatOptions : null;
  }

  @Override
  public DatastoreBackupOptions getDatastoreBackupOptions() {
    return formatOptions instanceof DatastoreBackupOptions
        ? (DatastoreBackupOptions) formatOptions
        : null;
  }

  @Override
  public String getFormat() {
    return formatOptions != null ? formatOptions.getType() : null;
  }

  @Override
  public Integer getMaxBadRecords() {
    return maxBadRecords;
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  public Boolean ignoreUnknownValues() {
    return ignoreUnknownValues;
  }

  /**
   * Returns the fully-qualified URIs that point to source data in Google Cloud Storage (e.g.
   * gs://bucket/path). Each URI can contain one '*' wildcard character and it must come after the
   * 'bucket' name.
   */
  public List<String> getSourceUris() {
    return sourceUris;
  }

  public List<String> getDecimalTargetTypes() {
    return decimalTargetTypes;
  }

  public Boolean getAutodetect() {
    return autodetect;
  }

  @Override
  public TimePartitioning getTimePartitioning() {
    return timePartitioning;
  }

  @Override
  public Clustering getClustering() {
    return clustering;
  }

  @Override
  public Boolean getUseAvroLogicalTypes() {
    return useAvroLogicalTypes;
  }

  @Override
  public List<JobInfo.SchemaUpdateOption> getSchemaUpdateOptions() {
    return schemaUpdateOptions;
  }

  /** Returns the labels associated with this job */
  public Map<String, String> getLabels() {
    return labels;
  }

  /** Returns the timeout associated with this job */
  public Long getJobTimeoutMs() {
    return jobTimeoutMs;
  }

  /** Returns the range partitioning specification for the table */
  public RangePartitioning getRangePartitioning() {
    return rangePartitioning;
  }

  public HivePartitioningOptions getHivePartitioningOptions() {
    return hivePartitioningOptions;
  }

  public String getReferenceFileSchemaUri() {
    return referenceFileSchemaUri;
  }

  public List<ConnectionProperty> getConnectionProperties() {
    return connectionProperties;
  }

  public Boolean getCreateSession() {
    return createSession;
  }

  @Override
  public Builder toBuilder() {
    return new Builder(this);
  }

  @Override
  ToStringHelper toStringHelper() {
    return super.toStringHelper()
        .add("destinationTable", destinationTable)
        .add("decimalTargetTypes", decimalTargetTypes)
        .add("destinationEncryptionConfiguration", destinationEncryptionConfiguration)
        .add("createDisposition", createDisposition)
        .add("writeDisposition", writeDisposition)
        .add("formatOptions", formatOptions)
        .add("nullMarker", nullMarker)
        .add("maxBadRecords", maxBadRecords)
        .add("schema", schema)
        .add("ignoreUnknownValue", ignoreUnknownValues)
        .add("sourceUris", sourceUris)
        .add("schemaUpdateOptions", schemaUpdateOptions)
        .add("autodetect", autodetect)
        .add("timePartitioning", timePartitioning)
        .add("clustering", clustering)
        .add("useAvroLogicalTypes", useAvroLogicalTypes)
        .add("labels", labels)
        .add("jobTimeoutMs", jobTimeoutMs)
        .add("rangePartitioning", rangePartitioning)
        .add("hivePartitioningOptions", hivePartitioningOptions)
        .add("referenceFileSchemaUri", referenceFileSchemaUri)
        .add("connectionProperties", connectionProperties)
        .add("createSession", createSession);
  }

  @Override
  public boolean equals(Object obj) {
    return obj == this
        || obj instanceof LoadJobConfiguration && baseEquals((LoadJobConfiguration) obj);
  }

  @Override
  public int hashCode() {
    return Objects.hash(baseHashCode(), sourceUris);
  }

  @Override
  LoadJobConfiguration setProjectId(String projectId) {
    if (Strings.isNullOrEmpty(getDestinationTable().getProject())) {
      return toBuilder().setDestinationTable(getDestinationTable().setProjectId(projectId)).build();
    }
    return this;
  }

  @Override
  com.google.api.services.bigquery.model.JobConfiguration toPb() {
    JobConfigurationLoad loadConfigurationPb = new JobConfigurationLoad();
    com.google.api.services.bigquery.model.JobConfiguration jobConfiguration =
        new com.google.api.services.bigquery.model.JobConfiguration();
    loadConfigurationPb.setDestinationTable(destinationTable.toPb());
    if (createDisposition != null) {
      loadConfigurationPb.setCreateDisposition(createDisposition.toString());
    }
    if (writeDisposition != null) {
      loadConfigurationPb.setWriteDisposition(writeDisposition.toString());
    }
    if (nullMarker != null) {
      loadConfigurationPb.setNullMarker(nullMarker);
    }
    if (getCsvOptions() != null) {
      CsvOptions csvOptions = getCsvOptions();
      loadConfigurationPb
          .setFieldDelimiter(csvOptions.getFieldDelimiter())
          .setAllowJaggedRows(csvOptions.allowJaggedRows())
          .setAllowQuotedNewlines(csvOptions.allowQuotedNewLines())
          .setEncoding(csvOptions.getEncoding())
          .setQuote(csvOptions.getQuote());
      if (csvOptions.getSkipLeadingRows() != null) {
        // todo(mziccard) remove checked cast or comment when #1044 is closed
        loadConfigurationPb.setSkipLeadingRows(Ints.checkedCast(csvOptions.getSkipLeadingRows()));
      }
    }
    if (getParquetOptions() != null) {
      ParquetOptions parquetOptions = getParquetOptions();
      loadConfigurationPb.setParquetOptions(parquetOptions.toPb());
    }
    if (schema != null) {
      loadConfigurationPb.setSchema(schema.toPb());
    }
    if (formatOptions != null) {
      loadConfigurationPb.setSourceFormat(formatOptions.getType());
    }
    loadConfigurationPb.setMaxBadRecords(maxBadRecords);
    loadConfigurationPb.setIgnoreUnknownValues(ignoreUnknownValues);
    if (getDatastoreBackupOptions() != null) {
      DatastoreBackupOptions backOptions = getDatastoreBackupOptions();
      loadConfigurationPb.setProjectionFields(backOptions.getProjectionFields());
    }
    if (sourceUris != null) {
      loadConfigurationPb.setSourceUris(ImmutableList.copyOf(sourceUris));
    }
    if (decimalTargetTypes != null) {
      loadConfigurationPb.setDecimalTargetTypes(ImmutableList.copyOf(decimalTargetTypes));
    }
    if (schemaUpdateOptions != null) {
      ImmutableList.Builder<String> schemaUpdateOptionsBuilder = new ImmutableList.Builder<>();
      for (JobInfo.SchemaUpdateOption schemaUpdateOption : schemaUpdateOptions) {
        schemaUpdateOptionsBuilder.add(schemaUpdateOption.name());
      }
      loadConfigurationPb.setSchemaUpdateOptions(schemaUpdateOptionsBuilder.build());
    }
    loadConfigurationPb.setAutodetect(autodetect);
    if (destinationEncryptionConfiguration != null) {
      loadConfigurationPb.setDestinationEncryptionConfiguration(
          destinationEncryptionConfiguration.toPb());
    }
    if (timePartitioning != null) {
      loadConfigurationPb.setTimePartitioning(timePartitioning.toPb());
    }
    if (clustering != null) {
      loadConfigurationPb.setClustering(clustering.toPb());
    }
    loadConfigurationPb.setUseAvroLogicalTypes(useAvroLogicalTypes);
    if (labels != null) {
      jobConfiguration.setLabels(labels);
    }
    if (jobTimeoutMs != null) {
      jobConfiguration.setJobTimeoutMs(jobTimeoutMs);
    }
    if (rangePartitioning != null) {
      loadConfigurationPb.setRangePartitioning(rangePartitioning.toPb());
    }
    if (hivePartitioningOptions != null) {
      loadConfigurationPb.setHivePartitioningOptions(hivePartitioningOptions.toPb());
    }
    if (referenceFileSchemaUri != null) {
      loadConfigurationPb.setReferenceFileSchemaUri(referenceFileSchemaUri);
    }
    if (connectionProperties != null) {
      loadConfigurationPb.setConnectionProperties(
          Lists.transform(connectionProperties, ConnectionProperty.TO_PB_FUNCTION));
    }
    if (createSession != null) {
      loadConfigurationPb.setCreateSession(createSession);
    }

    jobConfiguration.setLoad(loadConfigurationPb);
    return jobConfiguration;
  }

  /**
   * Creates a builder for a BigQuery Load Job configuration given the destination table and source
   * URIs.
   */
  public static Builder newBuilder(TableId destinationTable, List<String> sourceUris) {
    return new Builder().setDestinationTable(destinationTable).setSourceUris(sourceUris);
  }

  /**
   * Creates a builder for a BigQuery Load Job configuration given the destination table and source
   * URI.
   */
  public static Builder builder(TableId destinationTable, String sourceUri) {
    return newBuilder(destinationTable, ImmutableList.of(sourceUri));
  }

  /**
   * Creates a builder for a BigQuery Load Job configuration given the destination table and source
   * URI.
   */
  public static Builder newBuilder(TableId destinationTable, String sourceUri) {
    return newBuilder(destinationTable, ImmutableList.of(sourceUri));
  }

  /**
   * Creates a builder for a BigQuery Load Job configuration given the destination table, format and
   * source URIs.
   */
  public static Builder newBuilder(
      TableId destinationTable, List<String> sourceUris, FormatOptions format) {
    return newBuilder(destinationTable, sourceUris).setFormatOptions(format);
  }

  /**
   * Creates a builder for a BigQuery Load Job configuration given the destination table, format and
   * source URI.
   */
  public static Builder newBuilder(
      TableId destinationTable, String sourceUri, FormatOptions format) {
    return newBuilder(destinationTable, ImmutableList.of(sourceUri), format);
  }

  /** Returns a BigQuery Load Job Configuration for the given destination table and source URIs. */
  public static LoadJobConfiguration of(TableId destinationTable, List<String> sourceUris) {
    return newBuilder(destinationTable, sourceUris).build();
  }

  /** Returns a BigQuery Load Job Configuration for the given destination table and source URI. */
  public static LoadJobConfiguration of(TableId destinationTable, String sourceUri) {
    return of(destinationTable, ImmutableList.of(sourceUri));
  }

  /**
   * Returns a BigQuery Load Job Configuration for the given destination table, format and source
   * URI.
   */
  public static LoadJobConfiguration of(
      TableId destinationTable, List<String> sourceUris, FormatOptions format) {
    return newBuilder(destinationTable, sourceUris, format).build();
  }

  /**
   * Returns a BigQuery Load Job Configuration for the given destination table, format and source
   * URI.
   */
  public static LoadJobConfiguration of(
      TableId destinationTable, String sourceUri, FormatOptions format) {
    return of(destinationTable, ImmutableList.of(sourceUri), format);
  }

  @SuppressWarnings("unchecked")
  static LoadJobConfiguration fromPb(
      com.google.api.services.bigquery.model.JobConfiguration confPb) {
    return new Builder(confPb).build();
  }
}
