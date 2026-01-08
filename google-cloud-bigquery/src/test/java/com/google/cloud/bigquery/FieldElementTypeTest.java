/*
 * Copyright 2024 Google LLC
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

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.api.services.bigquery.model.QueryParameterType;
import org.junit.jupiter.api.Test;

class FieldElementTypeTest {
  private static final FieldElementType FIELD_ELEMENT_TYPE =
      FieldElementType.newBuilder().setType("DATE").build();

  @Test
  void testToBuilder() {
    compareFieldElementType(FIELD_ELEMENT_TYPE.toBuilder().build());
  }

  @Test
  void testBuilder() {
    assertEquals("DATE", FIELD_ELEMENT_TYPE.getType());
  }

  @Test
  void testFromAndPb() {
    assertEquals(FIELD_ELEMENT_TYPE, FieldElementType.fromPb(FIELD_ELEMENT_TYPE.toPb()));
    assertEquals(
        FIELD_ELEMENT_TYPE,
        FieldElementType.fromPb(
            new QueryParameterType()
                .setRangeElementType(new QueryParameterType().setType("DATE"))));
  }

  private void compareFieldElementType(FieldElementType value) {
    assertEquals(FieldElementTypeTest.FIELD_ELEMENT_TYPE.getType(), value.getType());
    assertEquals(FieldElementTypeTest.FIELD_ELEMENT_TYPE.hashCode(), value.hashCode());
    assertEquals(FieldElementTypeTest.FIELD_ELEMENT_TYPE.toString(), value.toString());
  }
}
