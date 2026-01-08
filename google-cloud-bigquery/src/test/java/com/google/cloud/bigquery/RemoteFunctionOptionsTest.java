/*
 * Copyright 2022 Google LLC
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

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class RemoteFunctionOptionsTest {
  private static final String ENDPOINT = "https://aaabbbccc-uc.a.run.app";
  private static final String CONNECTION =
      "projects/{projectId}/locations/{locationId}/connections/{connectionId}";
  private static final Map<String, String> userDefinedContext =
      new HashMap<String, String>() {
        {
          put("key1", "value1");
          put("key2", "value2");
        }
      };
  private static final Long MAX_BATCHING_ROWS = 20L;

  private static final RemoteFunctionOptions REMOTE_FUNCTION_OPTIONS =
      RemoteFunctionOptions.newBuilder()
          .setEndpoint(ENDPOINT)
          .setConnection(CONNECTION)
          .setUserDefinedContext(userDefinedContext)
          .setMaxBatchingRows(MAX_BATCHING_ROWS)
          .build();

  @Test
  void testToBuilder() {
    compareRemoteFunctionOptions(
        REMOTE_FUNCTION_OPTIONS, REMOTE_FUNCTION_OPTIONS.toBuilder().build());
  }

  @Test
  void testBuilder() {
    assertEquals(ENDPOINT, REMOTE_FUNCTION_OPTIONS.getEndpoint());
    assertEquals(CONNECTION, REMOTE_FUNCTION_OPTIONS.getConnection());
    assertEquals(userDefinedContext, REMOTE_FUNCTION_OPTIONS.getUserDefinedContext());
    assertEquals(MAX_BATCHING_ROWS, REMOTE_FUNCTION_OPTIONS.getMaxBatchingRows());
  }

  @Test
  void testToAndFromPb() {
    compareRemoteFunctionOptions(
        REMOTE_FUNCTION_OPTIONS, RemoteFunctionOptions.fromPb(REMOTE_FUNCTION_OPTIONS.toPb()));
  }

  public void compareRemoteFunctionOptions(
      RemoteFunctionOptions expected, RemoteFunctionOptions actual) {
    assertEquals(expected, actual);
    assertEquals(expected.getEndpoint(), actual.getEndpoint());
    assertEquals(expected.getConnection(), actual.getConnection());
    assertEquals(expected.getMaxBatchingRows(), actual.getMaxBatchingRows());
    assertEquals(expected.getUserDefinedContext(), actual.getUserDefinedContext());
  }
}
