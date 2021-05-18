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

import com.google.api.client.util.DateTime;
import com.google.api.core.BetaApi;
import com.google.api.services.bigquery.model.Table;
import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;
import javax.annotation.Nullable;

@AutoValue
@BetaApi
public abstract class SnapshotDefinition extends TableDefinition {

  private static final long serialVersionUID = 2113445776046717526L;

  @AutoValue.Builder
  public abstract static class Builder
      extends TableDefinition.Builder<SnapshotDefinition, Builder> {

    public abstract Builder setBaseTableId(TableId baseTableId);

    public abstract Builder setSnapshotTime(DateTime dateTime);

    /** Creates a {@code SnapshotDefinition} object. */
    public abstract SnapshotDefinition build();
  }

  @Nullable
  public abstract TableId getBaseTableId();

  @Nullable
  public abstract DateTime getSnapshotTime();

  /** Returns a builder for a snapshot table definition. */
  public static SnapshotDefinition.Builder newBuilder() {
    return new AutoValue_SnapshotDefinition.Builder().setType(Type.SNAPSHOT);
  }

  @VisibleForTesting
  public abstract SnapshotDefinition.Builder toBuilder();

  static SnapshotDefinition fromPb(Table tablePb) {
    Builder builder = newBuilder().table(tablePb);
    com.google.api.services.bigquery.model.SnapshotDefinition snapshotDefinition =
        tablePb.getSnapshotDefinition();
    if (snapshotDefinition != null) {
      if (snapshotDefinition.getBaseTableReference() != null) {
        builder.setBaseTableId(TableId.fromPb(snapshotDefinition.getBaseTableReference()));
      }
      if (snapshotDefinition.getSnapshotTime() != null) {
        builder.setSnapshotTime(snapshotDefinition.getSnapshotTime());
      }
    }
    return builder.build();
  }
}
