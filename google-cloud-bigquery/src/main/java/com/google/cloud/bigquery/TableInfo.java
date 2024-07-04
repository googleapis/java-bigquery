/*
 * Copyright 2015 Google LLC
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

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.client.util.Data;
import com.google.api.client.util.Strings;
import com.google.api.core.BetaApi;
import com.google.api.services.bigquery.model.Table;
import com.google.common.base.Function;
import com.google.common.base.MoreObjects;
import java.io.Serializable;
import java.math.BigInteger;
import java.util.Map;
import java.util.Objects;

/**
 * Google BigQuery table information. Use {@link StandardTableDefinition} to create simple BigQuery
 * table. Use {@link ViewDefinition} to create a BigQuery view. Use {@link ExternalTableDefinition}
 * to create a BigQuery a table backed by external data.
 *
 * @see <a href="https://cloud.google.com/bigquery/docs/tables">Managing Tables</a>
 */
public class TableInfo implements Serializable {

  static final Function<Table, TableInfo> FROM_PB_FUNCTION =
      new Function<Table, TableInfo>() {
        @Override
        public TableInfo apply(Table pb) {
          return TableInfo.fromPb(pb);
        }
      };
  static final Function<TableInfo, Table> TO_PB_FUNCTION =
      new Function<TableInfo, Table>() {
        @Override
        public Table apply(TableInfo tableInfo) {
          return tableInfo.toPb();
        }
      };

  private static final long serialVersionUID = 609769795097719407L;

  private final String etag;
  private final String generatedId;
  private final String selfLink;
  private final TableId tableId;
  private final String friendlyName;
  private final String description;
  private final Long creationTime;
  private final Long expirationTime;
  private final Long lastModifiedTime;
  private final Long numBytes;
  private final Long numLongTermBytes;
  private final Long numTimeTravelPhysicalBytes;
  private final Long numTotalLogicalBytes;
  private final Long numActiveLogicalBytes;
  private final Long numLongTermLogicalBytes;
  private final Long numTotalPhysicalBytes;
  private final Long numActivePhysicalBytes;
  private final Long numLongTermPhysicalBytes;
  private final BigInteger numRows;
  private final TableDefinition definition;
  private final EncryptionConfiguration encryptionConfiguration;
  private final Annotations labels;

  private final Annotations resourceTags;
  private final Boolean requirePartitionFilter;
  private final String defaultCollation;

  private final CloneDefinition cloneDefinition;
  private final TableConstraints tableConstraints;

  /** A builder for {@code TableInfo} objects. */
  public abstract static class Builder {

    abstract Builder setCreationTime(Long creationTime);

    /** Sets a user-friendly description for the table. */
    public abstract Builder setDescription(String description);

    abstract Builder setEtag(String etag);

    /**
     * Sets the time when this table expires, in milliseconds since the epoch. If not present, the
     * table will persist indefinitely. Expired tables will be deleted and their storage reclaimed.
     */
    public abstract Builder setExpirationTime(Long expirationTime);

    /** Sets a user-friendly name for the table. */
    public abstract Builder setFriendlyName(String friendlyName);

    abstract Builder setGeneratedId(String generatedId);

    abstract Builder setLastModifiedTime(Long lastModifiedTime);

    abstract Builder setNumBytes(Long numBytes);

    abstract Builder setNumLongTermBytes(Long numLongTermBytes);

    abstract Builder setNumTimeTravelPhysicalBytes(Long numTimeTravelPhysicalBytes);

    abstract Builder setNumTotalLogicalBytes(Long numTotalLogicalBytes);

    abstract Builder setNumActiveLogicalBytes(Long numActiveLogicalBytes);

    abstract Builder setNumLongTermLogicalBytes(Long numLongTermLogicalBytes);

    abstract Builder setNumTotalPhysicalBytes(Long numTotalPhysicalBytes);

    abstract Builder setNumActivePhysicalBytes(Long numActivePhysicalBytes);

    abstract Builder setNumLongTermPhysicalBytes(Long numLongTermPhysicalBytes);

    abstract Builder setNumRows(BigInteger numRows);

    abstract Builder setSelfLink(String selfLink);

    /** Sets the table identity. */
    public abstract Builder setTableId(TableId tableId);

    /**
     * Sets the table definition. Use {@link StandardTableDefinition} to create simple BigQuery
     * table. Use {@link ViewDefinition} to create a BigQuery view. Use {@link
     * ExternalTableDefinition} to create a BigQuery a table backed by external data.
     */
    public abstract Builder setDefinition(TableDefinition definition);

    /**
     * Sets the labels applied to this table.
     *
     * <p>Unstable, because labels are <a
     * href="https://cloud.google.com/bigquery/docs/reference/rest/v2/tables">experimental</a>.
     *
     * <p>When used with {@link BigQuery#update(TableInfo, TableOption...)}, setting {@code labels}
     * to {@code null} removes all labels; otherwise all keys that are mapped to {@code null} values
     * are removed and other keys are updated to their respective values.
     */
    @BetaApi
    public abstract Builder setLabels(Map<String, String> labels);

    /** Sets the resource tags applied to this table. */
    public abstract Builder setResourceTags(Map<String, String> resourceTags);

    /** Creates a {@code TableInfo} object. */
    public abstract TableInfo build();

    public abstract Builder setEncryptionConfiguration(EncryptionConfiguration configuration);

    public Builder setRequirePartitionFilter(Boolean requirePartitionFilter) {
      return this;
    }

    public abstract Builder setDefaultCollation(String defaultCollation);

    public abstract Builder setCloneDefinition(CloneDefinition cloneDefinition);

    public abstract Builder setTableConstraints(TableConstraints tableConstraints);
  }

  static class BuilderImpl extends Builder {

    private String etag;
    private String generatedId;
    private String selfLink;
    private TableId tableId;
    private String friendlyName;
    private String description;
    private Long creationTime;
    private Long expirationTime;
    private Long lastModifiedTime;
    private Long numBytes;
    private Long numLongTermBytes;
    private Long numTimeTravelPhysicalBytes;
    private Long numTotalLogicalBytes;
    private Long numActiveLogicalBytes;
    private Long numLongTermLogicalBytes;
    private Long numTotalPhysicalBytes;
    private Long numActivePhysicalBytes;
    private Long numLongTermPhysicalBytes;
    private BigInteger numRows;
    private TableDefinition definition;
    private EncryptionConfiguration encryptionConfiguration;
    private Annotations labels = Annotations.ZERO;

    private Annotations resourceTags = Annotations.ZERO;
    private Boolean requirePartitionFilter;
    private String defaultCollation;
    private CloneDefinition cloneDefinition;
    private TableConstraints tableConstraints;

    BuilderImpl() {}

    BuilderImpl(TableInfo tableInfo) {
      this.etag = tableInfo.etag;
      this.generatedId = tableInfo.generatedId;
      this.selfLink = tableInfo.selfLink;
      this.tableId = tableInfo.tableId;
      this.friendlyName = tableInfo.friendlyName;
      this.description = tableInfo.description;
      this.creationTime = tableInfo.creationTime;
      this.expirationTime = tableInfo.expirationTime;
      this.lastModifiedTime = tableInfo.lastModifiedTime;
      this.numBytes = tableInfo.numBytes;
      this.numLongTermBytes = tableInfo.numLongTermBytes;
      this.numTimeTravelPhysicalBytes = tableInfo.numTimeTravelPhysicalBytes;
      this.numTotalLogicalBytes = tableInfo.numTotalLogicalBytes;
      this.numActiveLogicalBytes = tableInfo.numActiveLogicalBytes;
      this.numLongTermLogicalBytes = tableInfo.numLongTermLogicalBytes;
      this.numTotalPhysicalBytes = tableInfo.numTotalPhysicalBytes;
      this.numActivePhysicalBytes = tableInfo.numActivePhysicalBytes;
      this.numLongTermPhysicalBytes = tableInfo.numLongTermPhysicalBytes;
      this.numRows = tableInfo.numRows;
      this.definition = tableInfo.definition;
      this.encryptionConfiguration = tableInfo.encryptionConfiguration;
      this.labels = tableInfo.labels;
      this.resourceTags = tableInfo.resourceTags;
      this.requirePartitionFilter = tableInfo.requirePartitionFilter;
      this.defaultCollation = tableInfo.defaultCollation;
      this.cloneDefinition = tableInfo.cloneDefinition;
      this.tableConstraints = tableInfo.tableConstraints;
    }

    BuilderImpl(Table tablePb) {
      this.tableId = TableId.fromPb(tablePb.getTableReference());
      if (tablePb.getLastModifiedTime() != null) {
        this.setLastModifiedTime(tablePb.getLastModifiedTime().longValue());
      }
      this.description = tablePb.getDescription();
      this.expirationTime = tablePb.getExpirationTime();
      this.friendlyName = tablePb.getFriendlyName();
      this.creationTime = tablePb.getCreationTime();
      this.etag = tablePb.getEtag();
      this.generatedId = tablePb.getId();
      this.selfLink = tablePb.getSelfLink();
      this.numBytes = tablePb.getNumBytes();
      this.numLongTermBytes = tablePb.getNumLongTermBytes();
      this.numTimeTravelPhysicalBytes = tablePb.getNumTimeTravelPhysicalBytes();
      this.numTotalLogicalBytes = tablePb.getNumTotalLogicalBytes();
      this.numActiveLogicalBytes = tablePb.getNumActiveLogicalBytes();
      this.numLongTermLogicalBytes = tablePb.getNumLongTermLogicalBytes();
      this.numTotalPhysicalBytes = tablePb.getNumTotalPhysicalBytes();
      this.numActivePhysicalBytes = tablePb.getNumActivePhysicalBytes();
      this.numLongTermPhysicalBytes = tablePb.getNumLongTermPhysicalBytes();
      this.numRows = tablePb.getNumRows();
      this.definition = TableDefinition.fromPb(tablePb);
      if (tablePb.getEncryptionConfiguration() != null) {
        this.encryptionConfiguration =
            new EncryptionConfiguration.Builder(tablePb.getEncryptionConfiguration()).build();
      }
      this.labels = Annotations.fromPb(tablePb.getLabels());
      this.resourceTags = Annotations.fromPb(tablePb.getResourceTags());
      this.requirePartitionFilter = tablePb.getRequirePartitionFilter();
      this.defaultCollation = tablePb.getDefaultCollation();
      if (tablePb.getCloneDefinition() != null) {
        this.cloneDefinition = CloneDefinition.fromPb(tablePb.getCloneDefinition());
      }
      if (tablePb.getTableConstraints() != null) {
        this.tableConstraints = TableConstraints.fromPb(tablePb.getTableConstraints());
      }
    }

    @Override
    Builder setCreationTime(Long creationTime) {
      this.creationTime = creationTime;
      return this;
    }

    @Override
    public Builder setDescription(String description) {
      this.description = firstNonNull(description, Data.<String>nullOf(String.class));
      return this;
    }

    @Override
    Builder setEtag(String etag) {
      this.etag = etag;
      return this;
    }

    @Override
    public Builder setExpirationTime(Long expirationTime) {
      this.expirationTime = firstNonNull(expirationTime, Data.<Long>nullOf(Long.class));
      return this;
    }

    @Override
    public Builder setFriendlyName(String friendlyName) {
      this.friendlyName = firstNonNull(friendlyName, Data.<String>nullOf(String.class));
      return this;
    }

    @Override
    Builder setGeneratedId(String generatedId) {
      this.generatedId = generatedId;
      return this;
    }

    @Override
    Builder setLastModifiedTime(Long lastModifiedTime) {
      this.lastModifiedTime = lastModifiedTime;
      return this;
    }

    @Override
    Builder setNumBytes(Long numBytes) {
      this.numBytes = numBytes;
      return this;
    }

    @Override
    Builder setNumLongTermBytes(Long numLongTermBytes) {
      this.numLongTermBytes = numLongTermBytes;
      return this;
    }

    @Override
    Builder setNumTimeTravelPhysicalBytes(Long numTimeTravelPhysicalBytes) {
      this.numTimeTravelPhysicalBytes = numTimeTravelPhysicalBytes;
      return this;
    }

    @Override
    Builder setNumTotalLogicalBytes(Long numTotalLogicalBytes) {
      this.numTotalLogicalBytes = numTotalLogicalBytes;
      return this;
    }

    @Override
    Builder setNumActiveLogicalBytes(Long numActiveLogicalBytes) {
      this.numActiveLogicalBytes = numActiveLogicalBytes;
      return this;
    }

    @Override
    Builder setNumLongTermLogicalBytes(Long numLongTermLogicalBytes) {
      this.numLongTermLogicalBytes = numLongTermLogicalBytes;
      return this;
    }

    @Override
    Builder setNumTotalPhysicalBytes(Long numTotalPhysicalBytes) {
      this.numTotalPhysicalBytes = numTotalPhysicalBytes;
      return this;
    }

    @Override
    Builder setNumActivePhysicalBytes(Long numActivePhysicalBytes) {
      this.numActivePhysicalBytes = numActivePhysicalBytes;
      return this;
    }

    @Override
    Builder setNumLongTermPhysicalBytes(Long numLongTermPhysicalBytes) {
      this.numLongTermPhysicalBytes = numLongTermPhysicalBytes;
      return this;
    }

    @Override
    Builder setNumRows(BigInteger numRows) {
      this.numRows = numRows;
      return this;
    }

    @Override
    Builder setSelfLink(String selfLink) {
      this.selfLink = selfLink;
      return this;
    }

    @Override
    public Builder setTableId(TableId tableId) {
      this.tableId = checkNotNull(tableId);
      return this;
    }

    @Override
    public Builder setDefinition(TableDefinition definition) {
      this.definition = checkNotNull(definition);
      return this;
    }

    @Override
    public Builder setEncryptionConfiguration(EncryptionConfiguration configuration) {
      this.encryptionConfiguration = configuration;
      return this;
    }

    @Override
    public Builder setLabels(Map<String, String> labels) {
      this.labels = Annotations.fromUser(labels);
      return this;
    }

    @Override
    public Builder setResourceTags(Map<String, String> resourceTags) {
      this.resourceTags = Annotations.fromUser(resourceTags);
      return this;
    }

    @Override
    public Builder setRequirePartitionFilter(Boolean requirePartitionFilter) {
      this.requirePartitionFilter = requirePartitionFilter;
      return this;
    }

    @Override
    public Builder setDefaultCollation(String defaultCollation) {
      this.defaultCollation = defaultCollation;
      return this;
    }

    public Builder setCloneDefinition(CloneDefinition cloneDefinition) {
      this.cloneDefinition = cloneDefinition;
      return this;
    }

    public Builder setTableConstraints(TableConstraints tableConstraints) {
      this.tableConstraints = tableConstraints;
      return this;
    }

    @Override
    public TableInfo build() {
      return new TableInfo(this);
    }
  }

  TableInfo(BuilderImpl builder) {
    this.tableId = checkNotNull(builder.tableId);
    this.etag = builder.etag;
    this.generatedId = builder.generatedId;
    this.selfLink = builder.selfLink;
    this.friendlyName = builder.friendlyName;
    this.description = builder.description;
    this.creationTime = builder.creationTime;
    this.expirationTime = builder.expirationTime;
    this.lastModifiedTime = builder.lastModifiedTime;
    this.numBytes = builder.numBytes;
    this.numLongTermBytes = builder.numLongTermBytes;
    this.numTimeTravelPhysicalBytes = builder.numTimeTravelPhysicalBytes;
    this.numTotalLogicalBytes = builder.numTotalLogicalBytes;
    this.numActiveLogicalBytes = builder.numActiveLogicalBytes;
    this.numLongTermLogicalBytes = builder.numLongTermLogicalBytes;
    this.numTotalPhysicalBytes = builder.numTotalPhysicalBytes;
    this.numActivePhysicalBytes = builder.numActivePhysicalBytes;
    this.numLongTermPhysicalBytes = builder.numLongTermPhysicalBytes;
    this.numRows = builder.numRows;
    this.definition = builder.definition;
    this.encryptionConfiguration = builder.encryptionConfiguration;
    this.labels = builder.labels;
    this.resourceTags = builder.resourceTags;
    this.requirePartitionFilter = builder.requirePartitionFilter;
    this.defaultCollation = builder.defaultCollation;
    this.cloneDefinition = builder.cloneDefinition;
    this.tableConstraints = builder.tableConstraints;
  }

  /** Returns the hash of the table resource. */
  public String getEtag() {
    return etag;
  }

  /** Returns the service-generated id for the table. */
  public String getGeneratedId() {
    return generatedId;
  }

  /**
   * Returns an URL that can be used to access the resource again. The returned URL can be used for
   * get or update requests.
   */
  public String getSelfLink() {
    return selfLink;
  }

  /** Returns the table identity. */
  public TableId getTableId() {
    return tableId;
  }

  /** Returns a user-friendly name for the table. */
  public String getFriendlyName() {
    return Data.isNull(friendlyName) ? null : friendlyName;
  }

  /** Returns a user-friendly description for the table. */
  public String getDescription() {
    return Data.isNull(description) ? null : description;
  }

  /** Returns the time when this table was created, in milliseconds since the epoch. */
  public Long getCreationTime() {
    return creationTime;
  }

  /**
   * Returns the time when this table expires, in milliseconds since the epoch. If not present, the
   * table will persist indefinitely. Expired tables will be deleted and their storage reclaimed.
   */
  public Long getExpirationTime() {
    return Data.isNull(expirationTime) ? null : expirationTime;
  }

  /** Returns the time when this table was last modified, in milliseconds since the epoch. */
  public Long getLastModifiedTime() {
    return lastModifiedTime;
  }

  public EncryptionConfiguration getEncryptionConfiguration() {
    return encryptionConfiguration;
  }

  /** Returns the table definition. */
  @SuppressWarnings("unchecked")
  public <T extends TableDefinition> T getDefinition() {
    return (T) definition;
  }

  /** Returns the size of this table in bytes */
  public Long getNumBytes() {
    return numBytes;
  }

  /**
   * Returns the number of bytes considered "long-term storage" for reduced billing purposes.
   *
   * @see <a href="https://cloud.google.com/bigquery/pricing#long-term-storage">Long Term Storage
   *     Pricing</a>
   */
  public Long getNumLongTermBytes() {
    return numLongTermBytes;
  }

  /**
   * Returns the number of time travel physical bytes.
   *
   * @see <a href="https://cloud.google.com/bigquery/pricing#storage">Storage Pricing</a>
   */
  public Long getNumTimeTravelPhysicalBytes() {
    return this.numTimeTravelPhysicalBytes;
  }

  /**
   * Returns the number of total logical bytes.
   *
   * @see <a href="https://cloud.google.com/bigquery/pricing#storage">Storage Pricing</a>
   */
  public Long getNumTotalLogicalBytes() {
    return this.numTotalLogicalBytes;
  }

  /**
   * Returns the number of active logical bytes.
   *
   * @see <a href="https://cloud.google.com/bigquery/pricing#storage">Storage Pricing</a>
   */
  public Long getNumActiveLogicalBytes() {
    return this.numActiveLogicalBytes;
  }

  /**
   * Returns the number of long term logical bytes.
   *
   * @see <a href="https://cloud.google.com/bigquery/pricing#storage">Storage Pricing</a>
   */
  public Long getNumLongTermLogicalBytes() {
    return this.numLongTermLogicalBytes;
  }

  /**
   * Returns the number of total physical bytes.
   *
   * @see <a href="https://cloud.google.com/bigquery/pricing#storage">Storage Pricing</a>
   */
  public Long getNumTotalPhysicalBytes() {
    return this.numTotalPhysicalBytes;
  }

  /**
   * Returns the number of active physical bytes.
   *
   * @see <a href="https://cloud.google.com/bigquery/pricing#storage">Storage Pricing</a>
   */
  public Long getNumActivePhysicalBytes() {
    return this.numActivePhysicalBytes;
  }

  /**
   * Returns the number of long term physical bytes.
   *
   * @see <a href="https://cloud.google.com/bigquery/pricing#storage">Storage Pricing</a>
   */
  public Long getNumLongTermPhysicalBytes() {
    return this.numLongTermPhysicalBytes;
  }

  /** Returns the number of rows of data in this table */
  public BigInteger getNumRows() {
    return numRows;
  }

  /**
   * Return a map for labels applied to the table.
   *
   * <p>Unstable, because labels are <a
   * href="https://cloud.google.com/bigquery/docs/reference/rest/v2/tables">experimental</a>.
   */
  @BetaApi
  public Map<String, String> getLabels() {
    return labels.userMap();
  }

  /** Return a map for resource tags applied to the table. */
  public Map<String, String> getResourceTags() {
    return resourceTags.userMap();
  }

  /**
   * Returns true if a partition filter (that can be used for partition elimination) is required for
   * queries over this table.
   */
  public Boolean getRequirePartitionFilter() {
    return requirePartitionFilter;
  }

  public String getDefaultCollation() {
    return defaultCollation;
  }

  public CloneDefinition getCloneDefinition() {
    return cloneDefinition;
  }

  public TableConstraints getTableConstraints() {
    return tableConstraints;
  }

  /** Returns a builder for the table object. */
  public Builder toBuilder() {
    return new BuilderImpl(this);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("tableId", tableId)
        .add("etag", etag)
        .add("generatedId", generatedId)
        .add("selfLink", selfLink)
        .add("friendlyName", friendlyName)
        .add("description", description)
        .add("expirationTime", expirationTime)
        .add("creationTime", creationTime)
        .add("lastModifiedTime", lastModifiedTime)
        .add("numBytes", numBytes)
        .add("numLongTermBytes", numLongTermBytes)
        .add("numTimeTravelPhysicalBytes", numTimeTravelPhysicalBytes)
        .add("numTotalLogicalBytes", numTotalLogicalBytes)
        .add("numActiveLogicalBytes", numActiveLogicalBytes)
        .add("numLongTermLogicalBytes", numLongTermLogicalBytes)
        .add("numTotalPhysicalBytes", numTotalPhysicalBytes)
        .add("numActivePhysicalBytes", numActivePhysicalBytes)
        .add("numLongTermPhysicalBytes", numLongTermPhysicalBytes)
        .add("numRows", numRows)
        .add("definition", definition)
        .add("encryptionConfiguration", encryptionConfiguration)
        .add("labels", labels)
        .add("resourceTags", resourceTags)
        .add("requirePartitionFilter", requirePartitionFilter)
        .add("defaultCollation", defaultCollation)
        .add("cloneDefinition", cloneDefinition)
        .add("tableConstraints", tableConstraints)
        .toString();
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableId);
  }

  @Override
  public boolean equals(Object obj) {
    return obj == this
        || obj != null
            && obj.getClass().equals(TableInfo.class)
            && Objects.equals(toPb(), ((TableInfo) obj).toPb());
  }

  /**
   * Returns a builder for a {@code TableInfo} object given table identity and definition. Use
   * {@link StandardTableDefinition} to create simple BigQuery table. Use {@link ViewDefinition} to
   * create a BigQuery view. Use {@link ExternalTableDefinition} to create a BigQuery a table backed
   * by external data.
   */
  public static Builder newBuilder(TableId tableId, TableDefinition definition) {
    return new BuilderImpl().setTableId(tableId).setDefinition(definition);
  }

  /**
   * Returns a {@code TableInfo} object given table identity and definition. Use {@link
   * StandardTableDefinition} to create simple BigQuery table. Use {@link ViewDefinition} to create
   * a BigQuery view. Use {@link ExternalTableDefinition} to create a BigQuery a table backed by
   * external data.
   */
  public static TableInfo of(TableId tableId, TableDefinition definition) {
    return newBuilder(tableId, definition).build();
  }

  TableInfo setProjectId(String projectId) {
    if (Strings.isNullOrEmpty(getTableId().getProject())) {
      return toBuilder().setTableId(getTableId().setProjectId(projectId)).build();
    }
    return this;
  }

  Table toPb() {
    Table tablePb = definition.toPb();
    tablePb.setTableReference(tableId.toPb());
    if (lastModifiedTime != null) {
      tablePb.setLastModifiedTime(BigInteger.valueOf(lastModifiedTime));
    }
    tablePb.setCreationTime(creationTime);
    tablePb.setDescription(description);
    tablePb.setEtag(etag);
    tablePb.setExpirationTime(expirationTime);
    tablePb.setFriendlyName(friendlyName);
    tablePb.setId(generatedId);
    tablePb.setSelfLink(selfLink);
    if (encryptionConfiguration != null) {
      tablePb.setEncryptionConfiguration(encryptionConfiguration.toPb());
    }
    tablePb.setLabels(labels.toPb());
    tablePb.setResourceTags(resourceTags.toPb());
    tablePb.setRequirePartitionFilter(requirePartitionFilter);
    if (defaultCollation != null) {
      tablePb.setDefaultCollation(defaultCollation);
    }
    if (cloneDefinition != null) {
      tablePb.setCloneDefinition(cloneDefinition.toPb());
    }
    if (tableConstraints != null) {
      tablePb.setTableConstraints(tableConstraints.toPb());
    }
    return tablePb;
  }

  static TableInfo fromPb(Table tablePb) {
    return new BuilderImpl(tablePb).build();
  }
}
