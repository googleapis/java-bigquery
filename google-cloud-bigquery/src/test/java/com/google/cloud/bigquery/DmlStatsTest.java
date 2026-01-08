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

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class DmlStatsTest {

  private static final Long DELETED_ROW_COUNT = 10L;
  private static final Long INSERTED_ROW_COUNT = 20L;
  private static final Long UPDATED_ROW_COUNT = 30L;
  private static final DmlStats DML_STATS =
      DmlStats.newBuilder()
          .setDeletedRowCount(DELETED_ROW_COUNT)
          .setInsertedRowCount(INSERTED_ROW_COUNT)
          .setUpdatedRowCount(UPDATED_ROW_COUNT)
          .build();

  @Test
  void testBuilder() {
    assertEquals(DELETED_ROW_COUNT, DML_STATS.getDeletedRowCount());
    assertEquals(UPDATED_ROW_COUNT, DML_STATS.getUpdatedRowCount());
    assertEquals(INSERTED_ROW_COUNT, DML_STATS.getInsertedRowCount());
  }

  @Test
  void testToPbAndFromPb() {
    compareDmlStats(DmlStats.fromPb(DML_STATS.toPb()));
  }

  private void compareDmlStats(DmlStats actual) {
    assertEquals(DmlStatsTest.DML_STATS, actual);
    assertEquals(DmlStatsTest.DML_STATS.hashCode(), actual.hashCode());
    assertEquals(DmlStatsTest.DML_STATS.toString(), actual.toString());
    assertEquals(DmlStatsTest.DML_STATS.getDeletedRowCount(), actual.getDeletedRowCount());
    assertEquals(DmlStatsTest.DML_STATS.getInsertedRowCount(), actual.getInsertedRowCount());
    assertEquals(DmlStatsTest.DML_STATS.getUpdatedRowCount(), actual.getUpdatedRowCount());
  }
}
