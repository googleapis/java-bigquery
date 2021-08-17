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

import java.sql.SQLException;

public interface BigQueryResultSet<T> {

  /** Returns the schema of the results. */
  Schema getSchema();

  /**
   * Returns the total number of rows in the complete result set, which can be more than the number
   * of rows in the first page of results.
   */
  long getTotalRows();

  /** Returns the next row. Null if there is no more rows left. */
  // ResultSet getNext();

  /*Advances the result set to the next row, returning false if no such row exists. Potentially blocking operation*/
  boolean next() throws SQLException;

  /*Returns the value of a String field if the field exists, otherwise returns null*/
  String getString(String fieldName) throws SQLException;
}
