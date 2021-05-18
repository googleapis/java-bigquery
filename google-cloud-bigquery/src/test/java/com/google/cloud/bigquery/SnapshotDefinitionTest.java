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

import com.google.api.client.util.DateTime;
import org.junit.Test;

public class SnapshotDefinitionTest {

  private static final TableId BASE_TABLE_ID = TableId.of("DATASET_NAME", "BASE_TABLE_NAME");
  private static final DateTime SNAPSHOT_TIME = new DateTime("2021-05-18");
  private static final SnapshotDefinition SNAPSHOT_DEFINITION =
      SnapshotDefinition.newBuilder()
          .setBaseTableId(BASE_TABLE_ID)
          .setSnapshotTime(SNAPSHOT_TIME)
          .build();

  @Test
  public void testToBuilder() {
    compareSnapshotDefinition(SNAPSHOT_DEFINITION, SNAPSHOT_DEFINITION.toBuilder().build());
  }

  @Test
  public void testBuilder() {
    assertEquals(TableDefinition.Type.SNAPSHOT, SNAPSHOT_DEFINITION.getType());
    assertEquals(BASE_TABLE_ID, SNAPSHOT_DEFINITION.getBaseTableId());
    assertEquals(SNAPSHOT_TIME, SNAPSHOT_DEFINITION.getSnapshotTime());
    SnapshotDefinition snapshotDefinition =
        SnapshotDefinition.newBuilder()
            .setBaseTableId(BASE_TABLE_ID)
            .setSnapshotTime(SNAPSHOT_TIME)
            .build();
    assertEquals(SNAPSHOT_DEFINITION, snapshotDefinition);
  }

  private void compareSnapshotDefinition(SnapshotDefinition expected, SnapshotDefinition value) {
    assertEquals(expected, value);
    assertEquals(expected.getBaseTableId(), value.getBaseTableId());
    assertEquals(expected.getSnapshotTime(), value.getSnapshotTime());
  }
}
