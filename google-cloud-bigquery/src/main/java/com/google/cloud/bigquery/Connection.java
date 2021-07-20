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

import com.google.api.services.bigquery.model.QueryParameter;
import java.util.List;
import java.util.Map;

/**
 * A Connection is a session between a Java application and BigQuery. SQL statements are executed
 * and results are returned within the context of a connection.
 */
public interface Connection {

  /** Sends a query cancel request. This call will return immediately */
  Boolean cancel() throws BigQuerySQLException;

  /**
   * Execute a query dry run that does not return any BigQueryResultSet // TODO: explain more about
   * what this method does here
   *
   * @param sql typically a static SQL SELECT statement
   * @exception BigQuerySQLException if a database access error occurs
   */
  BigQueryDryRunResult dryRun(String sql) throws BigQuerySQLException;

  /**
   * Execute a SQL statement that returns a single BigQueryResultSet
   *
   * @param sql a static SQL SELECT statement
   * @return a BigQueryResultSet that contains the data produced by the query
   * @exception BigQuerySQLException if a database access error occurs
   */
  <T> BigQueryResultSet<T> executeSelect(String sql) throws BigQuerySQLException;

  /**
   * Execute a SQL statement with query parameters that returns a single BigQueryResultSet
   *
   * @param sql typically a static SQL SELECT statement
   * @param parameters named or positional parameters
   * @return a BigQueryResultSet that contains the data produced by the query
   * @exception BigQuerySQLException if a database access error occurs
   */
  <T> BigQueryResultSet<T> executeSelect(
      String sql, List<QueryParameter> parameters, Map<String, String> labels)
      throws BigQuerySQLException;
}
