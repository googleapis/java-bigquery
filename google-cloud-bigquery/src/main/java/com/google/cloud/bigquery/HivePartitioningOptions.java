/*
 * Copyright 2020 Google LLC
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

import com.google.common.base.MoreObjects;
import java.util.Objects;

/**
 * HivePartitioningOptions for currently supported types include: AVRO, CSV, JSON, ORC and Parquet.
 */
public final class HivePartitioningOptions {

  private final String mode;
  private final String sourceUriPrefix;

  public static final class Builder {

    private String mode;
    private String sourceUriPrefix;

    private Builder() {}

    private Builder(HivePartitioningOptions options) {
      this.mode = options.mode;
      this.sourceUriPrefix = options.sourceUriPrefix;
    }

    public Builder setMode(String mode) {
      this.mode = mode;
      return this;
    }

    public Builder setSourceUriPrefix(String sourceUriPrefix) {
      this.sourceUriPrefix = sourceUriPrefix;
      return this;
    }

    /** Creates a {@link HivePartitioningOptions} object. */
    public HivePartitioningOptions build() {
      return new HivePartitioningOptions(this);
    }
  }

  private HivePartitioningOptions(Builder builder) {
    this.mode = builder.mode;
    this.sourceUriPrefix = builder.sourceUriPrefix;
  }

  public String getMode() {
    return mode;
  }

  public String getSourceUriPrefix() {
    return sourceUriPrefix;
  }

  /** Returns a builder for the {@link HivePartitioningOptions} object. */
  public Builder toBuilder() {
    return new Builder(this);
  }

  /** Returns a builder for the {@link HivePartitioningOptions} object. */
  public static Builder newBuilder() {
    return new Builder();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("mode", mode)
        .add("sourceUriPrefix", sourceUriPrefix)
        .toString();
  }

  @Override
  public int hashCode() {
    return Objects.hash(mode, sourceUriPrefix);
  }

  com.google.api.services.bigquery.model.HivePartitioningOptions toPb() {
    com.google.api.services.bigquery.model.HivePartitioningOptions options =
        new com.google.api.services.bigquery.model.HivePartitioningOptions();
    options.setMode(mode);
    options.setSourceUriPrefix(sourceUriPrefix);
    return options;
  }

  static HivePartitioningOptions fromPb(
      com.google.api.services.bigquery.model.HivePartitioningOptions options) {
    Builder builder = newBuilder();
    if (options.getMode() != null) {
      builder.setMode(options.getMode());
    }
    if (options.getSourceUriPrefix() != null) {
      builder.setSourceUriPrefix(options.getSourceUriPrefix());
    }
    return builder.build();
  }
}
