/*
 * Copyright 2018 Google LLC
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

import static com.google.common.truth.Truth.assertThat;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class BigtableOptionsTest {

  private static final BigtableColumn COL1 =
      BigtableColumn.newBuilder()
          .setQualifierEncoded("aaa")
          .setFieldName("field1")
          .setOnlyReadLatest(true)
          .setEncoding("BINARY")
          .setType("BYTES")
          .build();
  private static final BigtableColumn COL2 =
      BigtableColumn.newBuilder()
          .setQualifierEncoded("bbb")
          .setFieldName("field2")
          .setOnlyReadLatest(true)
          .setEncoding("TEXT")
          .setType("STRING")
          .build();
  private static final BigtableColumnFamily TESTFAMILY =
      BigtableColumnFamily.newBuilder()
          .setFamilyID("fooFamily")
          .setEncoding("TEXT")
          .setOnlyReadLatest(true)
          .setType("INTEGER")
          .setColumns(ImmutableList.of(COL1, COL2))
          .build();
  private static final BigtableOptions OPTIONS =
      BigtableOptions.newBuilder()
          .setIgnoreUnspecifiedColumnFamilies(true)
          .setReadRowkeyAsString(true)
          .setColumnFamilies(ImmutableList.of(TESTFAMILY))
          .build();

  @Test
  void testConstructors() {
    // column
    assertThat(COL1.getQualifierEncoded()).isEqualTo("aaa");
    assertThat(COL1.getFieldName()).isEqualTo("field1");
    assertThat(COL1.getOnlyReadLatest()).isEqualTo(true);
    assertThat(COL1.getEncoding()).isEqualTo("BINARY");
    assertThat(COL1.getType()).isEqualTo("BYTES");
    assertThat(COL1).isNotEqualTo(TESTFAMILY);

    // family
    assertThat(TESTFAMILY.getFamilyID()).isEqualTo("fooFamily");
    assertThat(TESTFAMILY.getEncoding()).isEqualTo("TEXT");
    assertThat(TESTFAMILY.getOnlyReadLatest()).isEqualTo(true);
    assertThat(TESTFAMILY.getType()).isEqualTo("INTEGER");
    assertThat(TESTFAMILY.getColumns()).isEqualTo(ImmutableList.of(COL1, COL2));

    // options
    assertThat(OPTIONS.getIgnoreUnspecifiedColumnFamilies()).isEqualTo(true);
    assertThat(OPTIONS.getReadRowkeyAsString()).isEqualTo(true);
    assertThat(OPTIONS.getColumnFamilies()).isEqualTo(ImmutableList.of(TESTFAMILY));
    compareBigtableOptions(OPTIONS.toBuilder().build());
  }

  @Test
  void testNullPointerException() {
    BigtableColumnFamily.Builder builder = BigtableColumnFamily.newBuilder();
    NullPointerException ex =
        Assertions.assertThrows(NullPointerException.class, () -> builder.setFamilyID(null));
    assertThat(ex.getMessage()).isNotNull();
    BigtableColumnFamily.Builder builder1 = BigtableColumnFamily.newBuilder();
    ex = Assertions.assertThrows(NullPointerException.class, () -> builder1.setColumns(null));
    assertThat(ex.getMessage()).isNotNull();
    BigtableColumnFamily.Builder builder2 = BigtableColumnFamily.newBuilder();
    ex = Assertions.assertThrows(NullPointerException.class, () -> builder2.setEncoding(null));
    assertThat(ex.getMessage()).isNotNull();
    BigtableColumnFamily.Builder builder3 = BigtableColumnFamily.newBuilder();
    ex =
        Assertions.assertThrows(NullPointerException.class, () -> builder3.setOnlyReadLatest(null));
    assertThat(ex.getMessage()).isNotNull();
    BigtableColumnFamily.Builder builder4 = BigtableColumnFamily.newBuilder();
    ex = Assertions.assertThrows(NullPointerException.class, () -> builder4.setType(null));
    assertThat(ex.getMessage()).isNotNull();
  }

  @Test
  void testIllegalStateException() {
    try {
      BigtableColumnFamily.newBuilder().build();
    } catch (IllegalStateException ex) {
      assertThat(ex.getMessage()).isNotNull();
    }
  }

  @Test
  void testToAndFromPb() {
    compareBigtableColumn(BigtableColumn.fromPb(COL1.toPb()));
    compareBigtableColumnFamily(BigtableColumnFamily.fromPb(TESTFAMILY.toPb()));
    compareBigtableOptions(BigtableOptions.fromPb(OPTIONS.toPb()));
  }

  @Test
  void testEquals() {
    compareBigtableColumn(COL1);
    compareBigtableColumnFamily(TESTFAMILY);
    assertThat(TESTFAMILY).isNotEqualTo(COL1);
    assertThat(OPTIONS).isNotEqualTo(TESTFAMILY);
    compareBigtableOptions(OPTIONS);
  }

  private void compareBigtableColumn(BigtableColumn value) {
    assertThat(BigtableOptionsTest.COL1).isEqualTo(value);
    assertThat(BigtableOptionsTest.COL1.getEncoding()).isEqualTo(value.getEncoding());
    assertThat(BigtableOptionsTest.COL1.getFieldName()).isEqualTo(value.getFieldName());
    assertThat(BigtableOptionsTest.COL1.getQualifierEncoded())
        .isEqualTo(value.getQualifierEncoded());
    assertThat(BigtableOptionsTest.COL1.getOnlyReadLatest()).isEqualTo(value.getOnlyReadLatest());
    assertThat(BigtableOptionsTest.COL1.getType()).isEqualTo(value.getType());
    assertThat(BigtableOptionsTest.COL1.toString()).isEqualTo(value.toString());
    assertThat(BigtableOptionsTest.COL1.hashCode()).isEqualTo(value.hashCode());
  }

  private void compareBigtableColumnFamily(BigtableColumnFamily value) {
    assertThat(BigtableOptionsTest.TESTFAMILY).isEqualTo(value);
    assertThat(BigtableOptionsTest.TESTFAMILY.getFamilyID()).isEqualTo(value.getFamilyID());
    assertThat(BigtableOptionsTest.TESTFAMILY.getOnlyReadLatest())
        .isEqualTo(value.getOnlyReadLatest());
    assertThat(BigtableOptionsTest.TESTFAMILY.getColumns()).isEqualTo(value.getColumns());
    assertThat(BigtableOptionsTest.TESTFAMILY.getEncoding()).isEqualTo(value.getEncoding());
    assertThat(BigtableOptionsTest.TESTFAMILY.getType()).isEqualTo(value.getType());
    assertThat(BigtableOptionsTest.TESTFAMILY.toString()).isEqualTo(value.toString());
    assertThat(BigtableOptionsTest.TESTFAMILY.hashCode()).isEqualTo(value.hashCode());
  }

  private void compareBigtableOptions(BigtableOptions value) {
    assertThat(BigtableOptionsTest.OPTIONS).isEqualTo(value);
    assertThat(BigtableOptionsTest.OPTIONS.getIgnoreUnspecifiedColumnFamilies())
        .isEqualTo(value.getIgnoreUnspecifiedColumnFamilies());
    assertThat(BigtableOptionsTest.OPTIONS.getReadRowkeyAsString())
        .isEqualTo(value.getReadRowkeyAsString());
    assertThat(BigtableOptionsTest.OPTIONS.getColumnFamilies())
        .isEqualTo(value.getColumnFamilies());
    assertThat(BigtableOptionsTest.OPTIONS.hashCode()).isEqualTo(value.hashCode());
    assertThat(BigtableOptionsTest.OPTIONS.toString()).isEqualTo(value.toString());
  }
}
