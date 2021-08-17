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
import java.sql.ResultSet;
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
   * Execute a SQL statement that returns a single ResultSet
   *
   * <p>Example of running a query.
   *
   * <pre>
   * {
   *   &#64;code
   *   // ConnectionSettings connectionSettings =
   *   //     ConnectionSettings.newBuilder()
   *   //         .setRequestTimeout(10L)
   *   //         .setMaxResults(100L)
   *   //         .setUseQueryCache(true)
   *   //         .build();
   *   // Connection connection = bigquery.createConnection(connectionSettings);
   *   String selectQuery = "SELECT corpus FROM `bigquery-public-data.samples.shakespeare` GROUP BY corpus;";
   *   try (ResultSet rs = connection.executeSelect(selectQuery)) {
   *       while (rs.next()) {
   *           System.out.printf("%s,", rs.getString("corpus"));
   *       }
   *   } catch (BigQuerySQLException ex) {
   *       // handle exception
   *   }
   * }
   * </pre>
   *
   * @param sql a static SQL SELECT statement
   * @return a ResultSet that contains the data produced by the query
   * @exception BigQuerySQLException if a database access error occurs
   */
  ResultSet executeSelect(String sql) throws BigQuerySQLException;

  /**
   * Execute a SQL statement with query parameters that returns a single ResultSet
   *
   * @param sql typically a static SQL SELECT statement
   * @param parameters named or positional parameters. The set of query parameters must either be
   *     all positional or all named parameters. Named parameters are denoted using an @ prefix,
   *     e.g. @myParam for a parameter named "myParam".
   * @param labels the labels associated with this query. You can use these to organize and group
   *     your query jobs. Label keys and values can be no longer than 63 characters, can only
   *     contain lowercase letters, numeric characters, underscores and dashes. International
   *     characters are allowed. Label values are optional. Label keys must start with a letter and
   *     each label in the list must have a different key.
   * @return a ResultSet that contains the data produced by the query
   * @exception BigQuerySQLException if a database access error occurs
   */
  ResultSet executeSelect(String sql, List<QueryParameter> parameters, Map<String, String> labels)
      throws BigQuerySQLException;
}
