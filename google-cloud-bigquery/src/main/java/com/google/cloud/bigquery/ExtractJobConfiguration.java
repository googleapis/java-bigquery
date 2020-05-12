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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;

import com.google.api.services.bigquery.model.JobConfigurationExtract;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Google BigQuery extract job configuration. An extract job exports a BigQuery table to Google
 * Cloud Storage. The extract destination provided as URIs that point to objects in Google Cloud
 * Storage. Extract job configurations have {@link JobConfiguration.Type#EXTRACT} type.
 */
public final class ExtractJobConfiguration extends JobConfiguration {

  private static final long serialVersionUID = 4147749733166593761L;

  private final TableId sourceTable;
  private final ModelId sourceModel;
  private final List<String> destinationUris;
  private final Boolean printHeader;
  private final String fieldDelimiter;
  private final String format;
  private final String compression;
  private final Boolean useAvroLogicalTypes;
  private final Map<String, String> labels;
  private final Long jobTimeoutMs;

  public static final class Builder
      extends JobConfiguration.Builder<ExtractJobConfiguration, Builder> {

    private TableId sourceTable;
    private ModelId sourceModel;
    private List<String> destinationUris;
    private Boolean printHeader;
    private String fieldDelimiter;
    private String format;
    private String compression;
    private Boolean useAvroLogicalTypes;
    private Map<String, String> labels;
    private Long jobTimeoutMs;

    private Builder() {
      super(Type.EXTRACT);
    }

    private Builder(ExtractJobConfiguration jobInfo) {
      this();
      this.sourceTable = jobInfo.sourceTable;
      this.sourceModel = jobInfo.sourceModel;
      this.destinationUris = jobInfo.destinationUris;
      this.printHeader = jobInfo.printHeader;
      this.fieldDelimiter = jobInfo.fieldDelimiter;
      this.format = jobInfo.format;
      this.compression = jobInfo.compression;
      this.useAvroLogicalTypes = jobInfo.useAvroLogicalTypes;
      this.labels = jobInfo.labels;
      this.jobTimeoutMs = jobInfo.jobTimeoutMs;
    }

    private Builder(com.google.api.services.bigquery.model.JobConfiguration configurationPb) {
      this();
      JobConfigurationExtract extractConfigurationPb = configurationPb.getExtract();
      if (extractConfigurationPb.getSourceTable() != null) {
        this.sourceTable = TableId.fromPb(extractConfigurationPb.getSourceTable());
      }
      if (extractConfigurationPb.getSourceModel() != null) {
        this.sourceModel = ModelId.fromPb(extractConfigurationPb.getSourceModel());
      }
      this.destinationUris = extractConfigurationPb.getDestinationUris();
      this.printHeader = extractConfigurationPb.getPrintHeader();
      this.fieldDelimiter = extractConfigurationPb.getFieldDelimiter();
      this.format = extractConfigurationPb.getDestinationFormat();
      this.compression = extractConfigurationPb.getCompression();
      this.useAvroLogicalTypes = extractConfigurationPb.getUseAvroLogicalTypes();
      if (configurationPb.getLabels() != null) {
        this.labels = configurationPb.getLabels();
      }
      if (configurationPb.getJobTimeoutMs() != null) {
        this.jobTimeoutMs = configurationPb.getJobTimeoutMs();
      }
    }

    /** Sets the table to export. */
    public Builder setSourceTable(TableId sourceTable) {
      this.sourceTable = sourceTable;
      return this;
    }

    /** Sets the model to export. */
    public Builder setSourceModel(ModelId sourceModel) {
      this.sourceModel = sourceModel;
      return this;
    }

    /**
     * Sets the list of fully-qualified Google Cloud Storage URIs (e.g. gs://bucket/path) where the
     * extracted table should be written.
     */
    public Builder setDestinationUris(List<String> destinationUris) {
      this.destinationUris = destinationUris != null ? ImmutableList.copyOf(destinationUris) : null;
      return this;
    }

    /** Sets whether to print out a header row in the results. By default an header is printed. */
    public Builder setPrintHeader(Boolean printHeader) {
      this.printHeader = printHeader;
      return this;
    }

    /** Sets the delimiter to use between fields in the exported data. By default "," is used. */
    public Builder setFieldDelimiter(String fieldDelimiter) {
      this.fieldDelimiter = fieldDelimiter;
      return this;
    }

    /**
     * Sets the exported file format. If not set table is exported in CSV format.
     *
     * <p><a
     * href="https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.extract.destinationFormat">
     * Destination Format</a>
     */
    public Builder setFormat(String format) {
      this.format = format;
      return this;
    }

    /**
     * Sets the compression value to use for exported files. If not set exported files are not
     * compressed.
     *
     * <p><a
     * href="https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.extract.compression">
     * Compression</a>
     */
    public Builder setCompression(String compression) {
      this.compression = compression;
      return this;
    }

    /**
     * [Optional] If destinationFormat is set to "AVRO", this flag indicates whether to enable
     * extracting applicable column types (such as TIMESTAMP) to their corresponding AVRO logical
     * types (timestamp-micros), instead of only using their raw types (avro-long).
     *
     * @param useAvroLogicalTypes useAvroLogicalTypes or {@code null} for none
     */
    public Builder setUseAvroLogicalTypes(Boolean useAvroLogicalTypes) {
      this.useAvroLogicalTypes = useAvroLogicalTypes;
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

    public ExtractJobConfiguration build() {
      return new ExtractJobConfiguration(this);
    }
  }

  private ExtractJobConfiguration(Builder builder) {
    super(builder);
    this.sourceTable = builder.sourceTable;
    this.sourceModel = builder.sourceModel;
    this.destinationUris = checkNotNull(builder.destinationUris);
    this.printHeader = builder.printHeader;
    this.fieldDelimiter = builder.fieldDelimiter;
    this.format = builder.format;
    this.compression = builder.compression;
    this.useAvroLogicalTypes = builder.useAvroLogicalTypes;
    this.labels = builder.labels;
    this.jobTimeoutMs = builder.jobTimeoutMs;
  }

  /** Returns the table to export. */
  public TableId getSourceTable() {
    return sourceTable;
  }

  /** Returns the model to export. */
  public ModelId getSourceModel() {
    return sourceModel;
  }

  /**
   * Returns the list of fully-qualified Google Cloud Storage URIs where the extracted table should
   * be written.
   *
   * @see <a
   *     href="https://cloud.google.com/bigquery/exporting-data-from-bigquery#exportingmultiple">
   *     Exporting Data Into One or More Files</a>
   */
  public List<String> getDestinationUris() {
    return destinationUris;
  }

  /** Returns whether an header row is printed with the result. */
  public Boolean printHeader() {
    return printHeader;
  }

  /** Returns the delimiter used between fields in the exported data. */
  public String getFieldDelimiter() {
    return fieldDelimiter;
  }

  /** Returns the exported files format. */
  public String getFormat() {
    return format;
  }

  /** Returns the compression value of exported files. */
  public String getCompression() {
    return compression;
  }

  /** Returns True/False. Indicates whether exported avro files include logical type annotations. */
  public Boolean getUseAvroLogicalTypes() {
    return useAvroLogicalTypes;
  }

  /** Returns the labels associated with this job */
  public Map<String, String> getLabels() {
    return labels;
  }

  /** Returns the timeout associated with this job */
  public Long getJobTimeoutMs() {
    return jobTimeoutMs;
  }

  @Override
  public Builder toBuilder() {
    return new Builder(this);
  }

  @Override
  ToStringHelper toStringHelper() {
    return super.toStringHelper()
        .add("sourceTable", sourceTable)
        .add("sourceModel", sourceModel)
        .add("destinationUris", destinationUris)
        .add("format", format)
        .add("printHeader", printHeader)
        .add("fieldDelimiter", fieldDelimiter)
        .add("compression", compression)
        .add("useAvroLogicalTypes", useAvroLogicalTypes)
        .add("labels", labels)
        .add("jobTimeoutMs", jobTimeoutMs);
  }

  @Override
  public boolean equals(Object obj) {
    return obj == this
        || obj instanceof ExtractJobConfiguration && baseEquals((ExtractJobConfiguration) obj);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        baseHashCode(),
        sourceTable,
        sourceModel,
        destinationUris,
        printHeader,
        fieldDelimiter,
        format,
        compression,
        useAvroLogicalTypes,
        labels,
        jobTimeoutMs);
  }

  @Override
  ExtractJobConfiguration setProjectId(String projectId) {
    if (getSourceTable() != null && Strings.isNullOrEmpty(getSourceTable().getProject())) {
      return toBuilder().setSourceTable(getSourceTable().setProjectId(projectId)).build();
    }
    if (getSourceModel() != null && Strings.isNullOrEmpty(getSourceModel().getProject())) {
      return toBuilder().setSourceModel(getSourceModel().setProjectId(projectId)).build();
    }
    return this;
  }

  @Override
  com.google.api.services.bigquery.model.JobConfiguration toPb() {
    JobConfigurationExtract extractConfigurationPb = new JobConfigurationExtract();
    com.google.api.services.bigquery.model.JobConfiguration jobConfiguration =
        new com.google.api.services.bigquery.model.JobConfiguration();
    extractConfigurationPb.setDestinationUris(destinationUris);
    if (sourceTable != null) {
      extractConfigurationPb.setSourceTable(sourceTable.toPb());
    }
    if (sourceModel != null) {
      extractConfigurationPb.setSourceModel(sourceModel.toPb());
    }
    extractConfigurationPb.setPrintHeader(printHeader);
    extractConfigurationPb.setFieldDelimiter(fieldDelimiter);
    extractConfigurationPb.setDestinationFormat(format);
    extractConfigurationPb.setCompression(compression);
    extractConfigurationPb.setUseAvroLogicalTypes(useAvroLogicalTypes);
    if (labels != null) {
      jobConfiguration.setLabels(labels);
    }
    if (jobTimeoutMs != null) {
      jobConfiguration.setJobTimeoutMs(jobTimeoutMs);
    }
    jobConfiguration.setExtract(extractConfigurationPb);
    return jobConfiguration;
  }

  /**
   * Creates a builder for a BigQuery Extract Job configuration given source table and destination
   * URI.
   */
  public static Builder newBuilder(TableId sourceTable, String destinationUri) {
    checkArgument(!isNullOrEmpty(destinationUri), "Provided destinationUri is null or empty");
    return newBuilder(sourceTable, ImmutableList.of(destinationUri));
  }

  /**
   * Creates a builder for a BigQuery Extract Job configuration given source model and destination
   * URI.
   */
  public static Builder newBuilder(ModelId sourceModel, String destinationUri) {
    checkArgument(!isNullOrEmpty(destinationUri), "Provided destinationUri is null or empty");
    return newBuilder(sourceModel, ImmutableList.of(destinationUri));
  }

  /**
   * Creates a builder for a BigQuery Extract Job configuration given source table and destination
   * URIs.
   */
  public static Builder newBuilder(TableId sourceTable, List<String> destinationUris) {
    return new Builder().setSourceTable(sourceTable).setDestinationUris(destinationUris);
  }

  /**
   * Creates a builder for a BigQuery Extract Job configuration given source model and destination
   * URIs.
   */
  public static Builder newBuilder(ModelId sourceModel, List<String> destinationUris) {
    return new Builder().setSourceModel(sourceModel).setDestinationUris(destinationUris);
  }

  /**
   * Returns a BigQuery Extract Job configuration for the given source table and destination URI.
   */
  public static ExtractJobConfiguration of(TableId sourceTable, String destinationUri) {
    return newBuilder(sourceTable, destinationUri).build();
  }

  /**
   * Returns a BigQuery Extract Job configuration for the given source model and destination URI.
   */
  public static ExtractJobConfiguration of(ModelId sourceModel, String destinationUri) {
    return newBuilder(sourceModel, destinationUri).build();
  }

  /**
   * Returns a BigQuery Extract Job configuration for the given source table and destination URIs.
   */
  public static ExtractJobConfiguration of(TableId sourceTable, List<String> destinationUris) {
    return newBuilder(sourceTable, destinationUris).build();
  }

  /**
   * Returns a BigQuery Extract Job configuration for the given source model and destination URIs.
   */
  public static ExtractJobConfiguration of(ModelId sourceModel, List<String> destinationUris) {
    return newBuilder(sourceModel, destinationUris).build();
  }

  /**
   * Returns a BigQuery Extract Job configuration for the given source table, format and destination
   * URI.
   */
  public static ExtractJobConfiguration of(
      TableId sourceTable, String destinationUri, String format) {
    checkArgument(!isNullOrEmpty(format), "Provided format is null or empty");
    return newBuilder(sourceTable, destinationUri).setFormat(format).build();
  }

  /**
   * Returns a BigQuery Extract Job configuration for the given source model, format and destination
   * URI.
   */
  public static ExtractJobConfiguration of(
      ModelId sourceTable, String destinationUri, String format) {
    checkArgument(!isNullOrEmpty(format), "Provided format is null or empty");
    return newBuilder(sourceTable, destinationUri).setFormat(format).build();
  }

  /**
   * Returns a BigQuery Extract Job configuration for the given source table, format and destination
   * URIs.
   */
  public static ExtractJobConfiguration of(
      TableId sourceTable, List<String> destinationUris, String format) {
    checkArgument(!isNullOrEmpty(format), "Provided format is null or empty");
    return newBuilder(sourceTable, destinationUris).setFormat(format).build();
  }

  /**
   * Returns a BigQuery Extract Job configuration for the given source table, format and destination
   * URIs.
   */
  public static ExtractJobConfiguration of(
      ModelId sourceModel, List<String> destinationUris, String format) {
    checkArgument(!isNullOrEmpty(format), "Provided format is null or empty");
    return newBuilder(sourceModel, destinationUris).setFormat(format).build();
  }

  @SuppressWarnings("unchecked")
  static ExtractJobConfiguration fromPb(
      com.google.api.services.bigquery.model.JobConfiguration confPb) {
    return new Builder(confPb).build();
  }
}
