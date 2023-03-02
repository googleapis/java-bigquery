/*
 * Copyright 2023 Google LLC
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

import com.google.api.client.util.DateTime;
import com.google.api.core.BetaApi;
import com.google.api.services.bigquery.model.Table;
import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;
import javax.annotation.Nullable;

@AutoValue
@BetaApi
public abstract class CloneTableDefinition extends TableDefinition {

  private static final long serialVersionUID = 1460853787400450649L;

  @AutoValue.Builder
  public abstract static class Builder
      extends TableDefinition.Builder<CloneTableDefinition, CloneTableDefinition.Builder> {

    /** Reference describing the ID of the table that was Cloned. * */
    public abstract CloneTableDefinition.Builder setBaseTableId(TableId baseTableId);

    /**
     * The time at which the base table was Cloned. This value is reported in the JSON response
     * using RFC3339 format. *
     */
    public abstract CloneTableDefinition.Builder setCloneTime(String dateTime);

    public abstract CloneTableDefinition.Builder setTimePartitioning(
        TimePartitioning timePartitioning);

    public abstract CloneTableDefinition.Builder setRangePartitioning(
        RangePartitioning rangePartitioning);

    public abstract CloneTableDefinition.Builder setClustering(Clustering clustering);

    /** Creates a {@code CloneTableDefinition} object. */
    public abstract CloneTableDefinition build();
  }

  @Nullable
  public abstract TableId getBaseTableId();

  @Nullable
  public abstract String getCloneTime();

  @Nullable
  public abstract TimePartitioning getTimePartitioning();

  @Nullable
  public abstract RangePartitioning getRangePartitioning();

  @Nullable
  public abstract Clustering getClustering();

  /** Returns a builder for a Clone table definition. */
  public static CloneTableDefinition.Builder newBuilder() {
    return new AutoValue_CloneTableDefinition.Builder().setType(Type.CLONE);
  }

  @VisibleForTesting
  public abstract CloneTableDefinition.Builder toBuilder();

  @Override
  com.google.api.services.bigquery.model.Table toPb() {
    com.google.api.services.bigquery.model.Table tablePb = super.toPb();
    com.google.api.services.bigquery.model.CloneDefinition cloneDefinition =
        new com.google.api.services.bigquery.model.CloneDefinition();
    cloneDefinition.setBaseTableReference(getBaseTableId().toPb());
    cloneDefinition.setCloneTime(DateTime.parseRfc3339(getCloneTime()));
    tablePb.setCloneDefinition(cloneDefinition);
    if (getTimePartitioning() != null) {
      tablePb.setTimePartitioning(getTimePartitioning().toPb());
    }
    if (getRangePartitioning() != null) {
      tablePb.setRangePartitioning(getRangePartitioning().toPb());
    }
    if (getClustering() != null) {
      tablePb.setClustering(getClustering().toPb());
    }
    return tablePb;
  }

  static CloneTableDefinition fromPb(Table tablePb) {
    CloneTableDefinition.Builder builder = newBuilder().table(tablePb);
    com.google.api.services.bigquery.model.CloneDefinition cloneDefinition =
        tablePb.getCloneDefinition();
    if (cloneDefinition != null) {
      if (cloneDefinition.getBaseTableReference() != null) {
        builder.setBaseTableId(TableId.fromPb(cloneDefinition.getBaseTableReference()));
      }
      if (cloneDefinition.getCloneTime() != null) {
        builder.setCloneTime(cloneDefinition.getCloneTime().toStringRfc3339());
      }
    }
    return builder.build();
  }
}
