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

public interface BigQueryResultSet {

  /** Returns the schema of the results. Null if the schema is not supplied. */
  Schema getSchema();

  /**
   * Returns the total number of rows in the complete result set, which can be more than the number
   * of rows in the first page of results.
   */
  long getTotalRows();

  /** Returns the next page of result set. Null if there is no more pages left. */
  BigQueryResultSet getNextPage();
}
