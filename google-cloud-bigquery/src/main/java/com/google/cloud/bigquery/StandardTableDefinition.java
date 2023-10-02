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

import com.google.api.services.bigquery.model.Streamingbuffer;
import com.google.api.services.bigquery.model.Table;
import com.google.auto.value.AutoValue;
import com.google.common.base.MoreObjects;
import java.io.Serializable;
import java.math.BigInteger;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * A Google BigQuery default table definition. This definition is used for standard, two-dimensional
 * tables with individual records organized in rows, and a data type assigned to each column (also
 * called a field). Individual fields within a record may contain nested and repeated children
 * fields. Every table is described by a schema that describes field names, types, and other
 * information.
 *
 * @see <a href="https://cloud.google.com/bigquery/docs/tables">Managing Tables</a>
 */
@AutoValue
public abstract class StandardTableDefinition extends TableDefinition {

  private static final long serialVersionUID = 2113445776046717900L;

  /**
   * Google BigQuery Table's Streaming Buffer information. This class contains information on a
   * table's streaming buffer as the estimated size in number of rows/bytes.
   */
  public static class StreamingBuffer implements Serializable {

    private static final long serialVersionUID = 822027055549277843L;
    private final Long estimatedRows;
    private final Long estimatedBytes;
    private final Long oldestEntryTime;

    StreamingBuffer(Long estimatedRows, Long estimatedBytes, Long oldestEntryTime) {
      this.estimatedRows = estimatedRows;
      this.estimatedBytes = estimatedBytes;
      this.oldestEntryTime = oldestEntryTime;
    }

    /** Returns a lower-bound estimate of the number of rows currently in the streaming buffer. */
    public Long getEstimatedRows() {
      return estimatedRows;
    }

    /** Returns a lower-bound estimate of the number of bytes currently in the streaming buffer. */
    public Long getEstimatedBytes() {
      return estimatedBytes;
    }

    /**
     * Returns the timestamp of the oldest entry in the streaming buffer, in milliseconds since
     * epoch. Returns {@code null} if the streaming buffer is empty.
     */
    public Long getOldestEntryTime() {
      return oldestEntryTime;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("estimatedRows", estimatedRows)
          .add("estimatedBytes", estimatedBytes)
          .add("oldestEntryTime", oldestEntryTime)
          .toString();
    }

    @Override
    public int hashCode() {
      return Objects.hash(estimatedRows, estimatedBytes, oldestEntryTime);
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof StreamingBuffer
          && Objects.equals(toPb(), ((StreamingBuffer) obj).toPb());
    }

    Streamingbuffer toPb() {
      Streamingbuffer buffer = new Streamingbuffer();
      if (estimatedBytes != null) {
        buffer.setEstimatedBytes(BigInteger.valueOf(estimatedBytes));
      }
      if (estimatedRows != null) {
        buffer.setEstimatedRows(BigInteger.valueOf(estimatedRows));
      }
      if (oldestEntryTime != null) {
        buffer.setOldestEntryTime(BigInteger.valueOf(oldestEntryTime));
      }
      return buffer;
    }

    static StreamingBuffer fromPb(Streamingbuffer streamingBufferPb) {
      Long oldestEntryTime = null;
      if (streamingBufferPb.getOldestEntryTime() != null) {
        oldestEntryTime = streamingBufferPb.getOldestEntryTime().longValue();
      }
      return new StreamingBuffer(
          streamingBufferPb.getEstimatedRows() != null
              ? streamingBufferPb.getEstimatedRows().longValue()
              : null,
          streamingBufferPb.getEstimatedBytes() != null
              ? streamingBufferPb.getEstimatedBytes().longValue()
              : null,
          oldestEntryTime);
    }
  }

  @AutoValue.Builder
  public abstract static class Builder
      extends TableDefinition.Builder<StandardTableDefinition, Builder> {

    public abstract Builder setNumBytes(Long numBytes);

    public abstract Builder setNumLongTermBytes(Long numLongTermBytes);

    public abstract Builder setNumTimeTravelPhysicalBytes(Long numTimeTravelPhysicalBytes);

    public abstract Builder setNumTotalLogicalBytes(Long numTotalLogicalBytes);

    public abstract Builder setNumActiveLogicalBytes(Long numActiveLogicalBytes);

    public abstract Builder setNumLongTermLogicalBytes(Long numLongTermLogicalBytes);

    public abstract Builder setNumTotalPhysicalBytes(Long numTotalPhysicalBytes);

    public abstract Builder setNumActivePhysicalBytes(Long numActivePhysicalBytes);

    public abstract Builder setNumLongTermPhysicalBytes(Long numLongTermPhysicalBytes);

    public abstract Builder setNumRows(Long numRows);

    public abstract Builder setLocation(String location);

    public abstract Builder setStreamingBuffer(StreamingBuffer streamingBuffer);

    public abstract Builder setType(Type type);

    /** Sets the table schema. */
    public abstract Builder setSchema(Schema schema);

    /**
     * Sets the time partitioning configuration for the table. If not set, the table is not
     * time-partitioned.
     */
    public abstract Builder setTimePartitioning(TimePartitioning timePartitioning);

    /**
     * Sets the range partitioning configuration for the table. Only one of timePartitioning and
     * rangePartitioning should be specified.
     */
    public abstract Builder setRangePartitioning(RangePartitioning rangePartitioning);

    /**
     * Set the clustering configuration for the table. If not set, the table is not clustered.
     * BigQuery supports clustering for both partitioned and non-partitioned tables.
     */
    public abstract Builder setClustering(Clustering clustering);

    public abstract Builder setTableConstraints(TableConstraints tableConstraints);

    /**
     * Set the configuration of a BigLake managed table. If not set, the table is not a BigLake
     * managed table.
     */
    public abstract Builder setBigLakeConfiguration(BigLakeConfiguration biglakeConfiguration);

    /** Creates a {@code StandardTableDefinition} object. */
    public abstract StandardTableDefinition build();
  }

  /** Returns the size of this table in bytes, excluding any data in the streaming buffer. */
  @Nullable
  public abstract Long getNumBytes();

  /**
   * Returns the number of bytes considered "long-term storage" for reduced billing purposes.
   *
   * @see <a href="https://cloud.google.com/bigquery/pricing#long-term-storage">Long Term Storage
   *     Pricing</a>
   */
  @Nullable
  public abstract Long getNumLongTermBytes();

  /**
   * Returns the number of time travel physical bytes.
   *
   * @see <a href="https://cloud.google.com/bigquery/pricing#storage">Storage Pricing</a>
   */
  @Nullable
  public abstract Long getNumTimeTravelPhysicalBytes();

  /**
   * Returns the number of total logical bytes.
   *
   * @see <a href="https://cloud.google.com/bigquery/pricing#storage">Storage Pricing</a>
   */
  @Nullable
  public abstract Long getNumTotalLogicalBytes();

  /**
   * Returns the number of active logical bytes.
   *
   * @see <a href="https://cloud.google.com/bigquery/pricing#storage">Storage Pricing</a>
   */
  @Nullable
  public abstract Long getNumActiveLogicalBytes();

  /**
   * Returns the number of long term logical bytes.
   *
   * @see <a href="https://cloud.google.com/bigquery/pricing#storage">Storage Pricing</a>
   */
  @Nullable
  public abstract Long getNumLongTermLogicalBytes();

  /**
   * Returns the number of total physical bytes.
   *
   * @see <a href="https://cloud.google.com/bigquery/pricing#storage">Storage Pricing</a>
   */
  @Nullable
  public abstract Long getNumTotalPhysicalBytes();

  /**
   * Returns the number of active physical bytes.
   *
   * @see <a href="https://cloud.google.com/bigquery/pricing#storage">Storage Pricing</a>
   */
  @Nullable
  public abstract Long getNumActivePhysicalBytes();

  /**
   * Returns the number of long term physical bytes.
   *
   * @see <a href="https://cloud.google.com/bigquery/pricing#storage">Storage Pricing</a>
   */
  @Nullable
  public abstract Long getNumLongTermPhysicalBytes();

  /** Returns the number of rows in this table, excluding any data in the streaming buffer. */
  @Nullable
  public abstract Long getNumRows();

  /**
   * Returns the geographic location where the table should reside. This value is inherited from the
   * dataset.
   *
   * @see <a
   *     href="https://cloud.google.com/bigquery/docs/managing_jobs_datasets_projects#dataset-location">
   *     Dataset Location</a>
   */
  @Nullable
  public abstract String getLocation();

  /**
   * Returns information on the table's streaming buffer if any exists. Returns {@code null} if no
   * streaming buffer exists.
   */
  @Nullable
  public abstract StreamingBuffer getStreamingBuffer();

  /**
   * Returns the time partitioning configuration for this table. If {@code null}, the table is not
   * time-partitioned.
   */
  @Nullable
  public abstract TimePartitioning getTimePartitioning();

  /**
   * Returns the range partitioning configuration for this table. If {@code null}, the table is not
   * range-partitioned.
   */
  @Nullable
  public abstract RangePartitioning getRangePartitioning();

  /**
   * Returns the clustering configuration for this table. If {@code null}, the table is not
   * clustered.
   */
  @Nullable
  public abstract Clustering getClustering();

  /**
   * Returns the table constraints for this table. Returns {@code null} if no table constraints are
   * set for this table.
   */
  @Nullable
  public abstract TableConstraints getTableConstraints();

  /**
   * [Optional] Specifies the configuration of a BigLake managed table. The value may be {@code
   * null}.
   */
  @Nullable
  public abstract BigLakeConfiguration getBigLakeConfiguration();

  /** Returns a builder for a BigQuery standard table definition. */
  public static Builder newBuilder() {
    return new AutoValue_StandardTableDefinition.Builder().setType(Type.TABLE);
  }

  /**
   * Creates a BigQuery standard table definition given its schema.
   *
   * @param schema the schema of the table
   */
  public static StandardTableDefinition of(Schema schema) {
    return newBuilder().setSchema(schema).build();
  }

  /** Returns a builder for the {@code StandardTableDefinition} object. */
  public abstract Builder toBuilder();

  @Override
  Table toPb() {
    Table tablePb = super.toPb();
    if (getNumRows() != null) {
      tablePb.setNumRows(BigInteger.valueOf(getNumRows()));
    }
    tablePb.setNumBytes(getNumBytes());
    tablePb.setNumLongTermBytes(getNumLongTermBytes());
    tablePb.setNumTimeTravelPhysicalBytes(getNumTimeTravelPhysicalBytes());
    tablePb.setNumTotalLogicalBytes(getNumTotalLogicalBytes());
    tablePb.setNumActiveLogicalBytes(getNumActiveLogicalBytes());
    tablePb.setNumLongTermLogicalBytes(getNumLongTermLogicalBytes());
    tablePb.setNumTotalPhysicalBytes(getNumTotalPhysicalBytes());
    tablePb.setNumActivePhysicalBytes(getNumActivePhysicalBytes());
    tablePb.setNumLongTermPhysicalBytes(getNumLongTermPhysicalBytes());
    tablePb.setLocation(getLocation());
    if (getStreamingBuffer() != null) {
      tablePb.setStreamingBuffer(getStreamingBuffer().toPb());
    }
    if (getTimePartitioning() != null) {
      tablePb.setTimePartitioning(getTimePartitioning().toPb());
    }
    if (getRangePartitioning() != null) {
      tablePb.setRangePartitioning(getRangePartitioning().toPb());
    }
    if (getClustering() != null) {
      tablePb.setClustering(getClustering().toPb());
    }
    if (getTableConstraints() != null) {
      tablePb.setTableConstraints(getTableConstraints().toPb());
    }
    if (getBigLakeConfiguration() != null) {
      tablePb.setBiglakeConfiguration(getBigLakeConfiguration().toPb());
    }
    return tablePb;
  }

  @SuppressWarnings("unchecked")
  static StandardTableDefinition fromPb(Table tablePb) {
    Builder builder = newBuilder().table(tablePb);
    if (tablePb.getNumRows() != null) {
      builder.setNumRows(tablePb.getNumRows().longValue());
    }
    if (tablePb.getStreamingBuffer() != null) {
      builder.setStreamingBuffer(StreamingBuffer.fromPb(tablePb.getStreamingBuffer()));
    }
    if (tablePb.getTimePartitioning() != null) {
      try {
        builder.setTimePartitioning(TimePartitioning.fromPb(tablePb.getTimePartitioning()));
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            "Illegal Argument - Got unexpected time partitioning "
                + tablePb.getTimePartitioning().getType()
                + " in project "
                + tablePb.getTableReference().getProjectId()
                + " in dataset "
                + tablePb.getTableReference().getDatasetId()
                + " in table "
                + tablePb.getTableReference().getTableId(),
            e);
      }
    }
    if (tablePb.getRangePartitioning() != null) {
      builder.setRangePartitioning(RangePartitioning.fromPb(tablePb.getRangePartitioning()));
    }
    if (tablePb.getClustering() != null) {
      builder.setClustering(Clustering.fromPb(tablePb.getClustering()));
    }
    if (tablePb.getNumLongTermBytes() != null) {
      builder.setNumLongTermBytes(tablePb.getNumLongTermBytes());
    }
    if (tablePb.getNumTimeTravelPhysicalBytes() != null) {
      builder.setNumTimeTravelPhysicalBytes(tablePb.getNumTimeTravelPhysicalBytes());
    }
    if (tablePb.getNumTotalLogicalBytes() != null) {
      builder.setNumTotalLogicalBytes(tablePb.getNumTotalLogicalBytes());
    }
    if (tablePb.getNumActiveLogicalBytes() != null) {
      builder.setNumActiveLogicalBytes(tablePb.getNumActiveLogicalBytes());
    }
    if (tablePb.getNumLongTermLogicalBytes() != null) {
      builder.setNumLongTermLogicalBytes(tablePb.getNumLongTermLogicalBytes());
    }
    if (tablePb.getNumTotalPhysicalBytes() != null) {
      builder.setNumTotalPhysicalBytes(tablePb.getNumTotalPhysicalBytes());
    }
    if (tablePb.getNumActivePhysicalBytes() != null) {
      builder.setNumActivePhysicalBytes(tablePb.getNumActivePhysicalBytes());
    }
    if (tablePb.getNumLongTermPhysicalBytes() != null) {
      builder.setNumLongTermPhysicalBytes(tablePb.getNumLongTermPhysicalBytes());
    }
    if (tablePb.getTableConstraints() != null) {
      builder.setTableConstraints(TableConstraints.fromPb(tablePb.getTableConstraints()));
    }
    if (tablePb.getBiglakeConfiguration() != null) {
      builder.setBigLakeConfiguration(
          BigLakeConfiguration.fromPb(tablePb.getBiglakeConfiguration()));
    }

    return builder.setNumBytes(tablePb.getNumBytes()).setLocation(tablePb.getLocation()).build();
  }
}
