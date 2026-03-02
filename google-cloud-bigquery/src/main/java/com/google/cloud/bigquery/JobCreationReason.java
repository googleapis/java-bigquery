/*
 * Copyright 2026 Google LLC
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

/**
 * Enum that maps to <a
 * href="https://docs.cloud.google.com/bigquery/docs/reference/rest/v2/JobCreationReason">JobCreationReason</a>
 * when used with {@link
 * com.google.cloud.bigquery.QueryJobConfiguration.JobCreationMode#JOB_CREATION_OPTIONAL}.
 *
 * <p>Indicates the high level reason why a job was created.
 */
public enum JobCreationReason {
  REQUESTED("REQUESTED"),
  LONG_RUNNING("LONG_RUNNING"),
  LARGE_RESULTS("LARGE_RESULTS"),
  OTHER("OTHER");

  private final String reason;

  JobCreationReason(String reason) {
    this.reason = reason;
  }

  static JobCreationReason fromValue(String reason) {
    for (JobCreationReason authType : values()) {
      if (authType.reason.equals(reason)) {
        return authType;
      }
    }
    throw new IllegalStateException("Invalid JobCreationReason: " + reason);
  }
}
